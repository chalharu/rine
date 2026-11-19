use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::Parser;
use rine_etcd::Backend;

// On Linux/musl the system allocator limited concurrent gRPC throughput.
#[cfg(all(target_os = "linux", target_env = "musl"))]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser, Debug)]
#[command(name = "rine", version)]
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
    /// Oracle connect string: Easy Connect (`host/service`), a full connect
    /// descriptor, or a TNS alias (resolved via TNS_ADMIN or --wallet-dir).
    #[cfg(feature = "oracle")]
    #[arg(long, env = "RINE_ORACLE_CONNECT")]
    oracle_connect: Option<String>,
    /// SQLite database file path (for `--backend sqlite`).
    #[cfg(feature = "sqlite")]
    #[arg(long, default_value = "rine.db", env = "RINE_SQLITE_PATH")]
    sqlite_path: PathBuf,
    /// Wallet directory (`tnsnames.ora` + `ewallet.pem`): used as the Oracle
    /// Net configuration directory and wallet location.
    #[cfg(feature = "oracle")]
    #[arg(long, env = "RINE_WALLET_DIR")]
    wallet_dir: Option<PathBuf>,
    #[cfg(feature = "oracle")]
    #[arg(long, default_value = "", env = "RINE_ORACLE_USER")]
    user: String,
    #[cfg(feature = "oracle")]
    #[arg(long, default_value = "", env = "RINE_ORACLE_PASSWORD")]
    password: String,
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

    let backend: Arc<dyn Backend> = match args.backend.as_str() {
        "memory" => Arc::new(rine_etcd::MemoryBackend::new()),
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            let db = rine_store_sqlite::SqliteDb::open(&args.sqlite_path)?;
            Arc::new(rine_store_sqlite::SqliteBackend::new(Arc::new(db)))
        }
        #[cfg(feature = "oracle")]
        "oracle" => {
            let connect = args.oracle_connect.clone().ok_or_else(|| {
                anyhow::anyhow!("oracle backend needs --oracle-connect (or RINE_ORACLE_CONNECT)")
            })?;
            let mut config =
                oracledb::Config::default().set_credentials(&args.user, &args.password);
            if let Some(dir) = &args.wallet_dir {
                let dir = dir
                    .to_str()
                    .ok_or_else(|| anyhow::anyhow!("wallet dir is not UTF-8"))?;
                config = config.set_config_dir(dir).set_wallet_location(dir);
            }
            let config = config.set_connect_string(&connect)?;
            let db = rine_store_oracle::OracleDb::connect_with_config(config)?;
            Arc::new(rine_store_oracle::OracleBackend::new(Arc::new(db)))
        }
        other => anyhow::bail!("unknown backend: {other} (expected memory|sqlite|oracle)"),
    };
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
