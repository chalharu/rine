use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::Parser;

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

    // One backend per `--backend` name; unknown names list the compiled-in
    // options so flag values can never drift from the implementations.
    let backend: Arc<dyn rine_etcd::Backend> = match args.backend.as_str() {
        "memory" => Arc::new(rine_etcd::MemoryBackend::new()),
        #[cfg(feature = "sqlite")]
        "sqlite" => args.sqlite.open_backend()?,
        #[cfg(feature = "oracle")]
        "oracle" => args.oracle.open_backend()?,
        other => {
            let mut names = vec!["memory"];
            #[cfg(feature = "sqlite")]
            names.push("sqlite");
            #[cfg(feature = "oracle")]
            names.push("oracle");
            anyhow::bail!("unknown backend: {other} (expected {})", names.join("|"));
        }
    };
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

#[cfg(test)]
mod tests {
    use super::*;

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
