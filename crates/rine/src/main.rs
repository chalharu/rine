use std::{net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use rine_etcd::backend::BackendFactory;

// On Linux/musl the system allocator limited concurrent gRPC throughput.
#[cfg(all(target_os = "linux", target_env = "musl"))]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(
    name = "rine",
    version,
    about = "etcd-compatible KV store with pluggable backends"
)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:2379", env = "RINE_LISTEN")]
    listen: SocketAddr,
    /// Client listen URLs, comma-separated (etcd --listen-client-urls style).
    /// Each entry is http(s)://host:port; https:// entries serve TLS while
    /// http:// entries stay plaintext. Takes precedence over --listen.
    #[arg(long, env = "RINE_LISTEN_CLIENT_URLS")]
    listen_client_urls: Option<String>,
    #[arg(long, default_value = "memory", env = "RINE_BACKEND")]
    backend: String,
    /// SQLite options (used with `--backend sqlite`).
    #[cfg(feature = "sqlite")]
    #[command(flatten)]
    sqlite: rine_store_sqlite::SqliteConfig,
    /// Oracle options (used with `--backend oracle`).
    #[cfg(feature = "oracle")]
    #[command(flatten)]
    oracle: rine_store_oracle::OracleConfig,
    /// PEM-encoded server certificate chain (enables TLS).
    #[arg(long, env = "RINE_CERT_FILE")]
    cert_file: Option<PathBuf>,
    /// PEM-encoded server private key (enables TLS).
    #[arg(long, env = "RINE_KEY_FILE")]
    key_file: Option<PathBuf>,
    /// Require clients to present a certificate signed by --trusted-ca-file.
    #[arg(long, env = "RINE_CLIENT_CERT_AUTH", default_value_t = false)]
    client_cert_auth: bool,
    /// PEM-encoded CA certificate used to verify client certificates.
    #[arg(long, env = "RINE_TRUSTED_CA_FILE")]
    trusted_ca_file: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    // Backend implementations register themselves here; `--backend` selects
    // one by the name its factory reports.
    let factories: Vec<Box<dyn BackendFactory>> = {
        // Without backend features nothing is pushed; keep the binding
        // immutable-friendly across all feature combinations.
        #[allow(unused_mut)]
        let mut factories: Vec<Box<dyn BackendFactory>> = vec![Box::new(rine_etcd::MemoryFactory)];
        #[cfg(feature = "sqlite")]
        factories.push(Box::new(rine_store_sqlite::SqliteFactory::new(
            args.sqlite.clone(),
        )));
        #[cfg(feature = "oracle")]
        factories.push(Box::new(rine_store_oracle::OracleFactory::new(
            args.oracle.clone(),
        )));
        factories
    };
    let factory = factories
        .iter()
        .find(|f| f.name() == args.backend)
        .ok_or_else(|| {
            let names = factories
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
                .join("|");
            anyhow::anyhow!("unknown backend: {} (expected {})", args.backend, names)
        })?;
    let backend = factory.build()?;
    backend.initialize().await?;

    // Identity is loaded once and applied to every https:// listener.
    let identity: Option<(Vec<u8>, Vec<u8>)> = match (&args.cert_file, &args.key_file) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some((std::fs::read(cert)?, std::fs::read(key)?)),
        _ => anyhow::bail!("--cert-file and --key-file must be given together"),
    };
    if identity.is_none() && args.client_cert_auth {
        anyhow::bail!("--client-cert-auth needs TLS (--cert-file/--key-file)");
    }
    let client_ca: Option<Vec<u8>> = if args.client_cert_auth {
        let ca = args.trusted_ca_file.clone().ok_or_else(|| {
            anyhow::anyhow!("--client-cert-auth needs --trusted-ca-file (or RINE_TRUSTED_CA_FILE)")
        })?;
        Some(std::fs::read(ca)?)
    } else {
        None
    };

    let urls = match &args.listen_client_urls {
        Some(urls) => {
            let mut parsed = Vec::new();
            for s in urls.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                parsed.push(parse_listen_url(s).await?);
            }
            parsed
        }
        None => vec![(args.listen, false)],
    };
    if urls.is_empty() {
        anyhow::bail!("no listen URLs");
    }
    let mut listeners = Vec::with_capacity(urls.len());
    for (addr, https) in urls {
        let tls = if https {
            let (cert, key) = identity.clone().ok_or_else(|| {
                anyhow::anyhow!("https:// listener {addr} needs --cert-file/--key-file")
            })?;
            let mut config = tonic::transport::ServerTlsConfig::new()
                .identity(tonic::transport::Identity::from_pem(cert, key));
            if let Some(ca) = &client_ca {
                config = config.client_ca_root(tonic::transport::Certificate::from_pem(ca.clone()));
            }
            Some(config)
        } else {
            None
        };
        listeners.push(rine_etcd::server::ListenAddr { addr, tls });
    }
    if identity.is_some() && listeners.iter().all(|l| l.tls.is_none()) {
        tracing::warn!("--cert-file/--key-file given but no https:// listener; TLS unused");
    }
    rine_etcd::server::EtcdServer::new(backend)
        .serve_many(listeners)
        .await?;
    Ok(())
}

/// Split `http(s)://host:port` (bare `host:port` means http) into an address
/// plus whether it serves TLS.
async fn parse_listen_url(s: &str) -> Result<(SocketAddr, bool)> {
    let s = s.trim_end_matches('/');
    let (scheme, rest) = match s.split_once("://") {
        Some((scheme, rest)) => (scheme, rest),
        None => ("http", s),
    };
    let https = match scheme {
        "http" => false,
        "https" => true,
        other => anyhow::bail!(
            "unsupported scheme in listen URL: {other} (expected http:// or https://)"
        ),
    };
    if let Ok(addr) = rest.parse::<SocketAddr>() {
        return Ok((addr, https));
    }
    let mut addrs = tokio::net::lookup_host(rest).await?;
    addrs
        .next()
        .map(|addr| (addr, https))
        .ok_or_else(|| anyhow::anyhow!("could not resolve listen address: {rest}"))
}
