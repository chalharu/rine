use std::{net::SocketAddr, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use rine_etcd::backend::BackendFactory;

mod listen;

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
    // Load `.env` first so `RINE_*` / `--*_FILE`-style env bindings below
    // (declared via clap `env`) also pick up file-based configuration.
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    // Backend implementations register themselves here; `--backend` selects
    // one by the name its factory reports.
    let factories = build_factories(&args);
    let factory = select_factory(&factories, &args.backend)?;
    let backend = factory.build()?;
    backend.initialize().await?;

    // Transport setup (URL parsing, TLS validation) lives with the binary;
    // the server library only consumes addresses plus loaded identity bytes.
    let listeners = listen::ListenConfig {
        listen: args.listen,
        listen_client_urls: args.listen_client_urls,
        cert_file: args.cert_file,
        key_file: args.key_file,
        client_cert_auth: args.client_cert_auth,
        trusted_ca_file: args.trusted_ca_file,
    }
    .into_listeners()
    .await?;
    rine_etcd::server::EtcdServer::new(backend)
        .serve_many(listeners)
        .await?;
    Ok(())
}

/// Register every compiled-in backend factory.
#[allow(unused_mut)]
fn build_factories(args: &Args) -> Vec<Box<dyn BackendFactory>> {
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
}

/// Pick the factory named by `--backend`, listing the compiled-in names on
/// a miss so flag values can never drift from the implementations.
fn select_factory<'a>(
    factories: &'a [Box<dyn BackendFactory>],
    name: &str,
) -> Result<&'a dyn BackendFactory> {
    factories
        .iter()
        .find(|f| f.name() == name)
        .map(AsRef::as_ref)
        .ok_or_else(|| {
            let names = factories
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
                .join("|");
            anyhow::anyhow!("unknown backend: {name} (expected {names})")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn factories() -> Vec<Box<dyn BackendFactory>> {
        vec![Box::new(rine_etcd::MemoryFactory)]
    }

    #[test]
    fn select_factory_finds_memory_and_rejects_unknown() {
        let factories = factories();
        assert!(matches!(
            select_factory(&factories, "memory"),
            Ok(f) if f.name() == "memory"
        ));
        assert!(matches!(
            select_factory(&factories, "bogus"),
            Err(e) if e.to_string().contains("unknown backend: bogus (expected memory)")
        ));
    }

    #[test]
    fn build_factories_registers_backends() {
        let args = Args::try_parse_from(["rine"]).unwrap();
        let names: Vec<_> = build_factories(&args).iter().map(|f| f.name()).collect();
        assert!(names.contains(&"memory"));
        #[cfg(feature = "sqlite")]
        assert!(names.contains(&"sqlite"));
        #[cfg(feature = "oracle")]
        assert!(names.contains(&"oracle"));
    }

    #[test]
    fn args_parse_from_flags() {
        let args = Args::try_parse_from([
            "rine",
            "--backend",
            "sqlite",
            "--listen",
            "127.0.0.1:2380",
            "--sqlite-path",
            "/tmp/x.db",
        ])
        .unwrap();
        assert_eq!(args.backend, "sqlite");
        assert_eq!(args.listen, "127.0.0.1:2380".parse().unwrap());
        let defaults = Args::try_parse_from(["rine"]).unwrap();
        assert_eq!(defaults.backend, "memory");
    }
}
