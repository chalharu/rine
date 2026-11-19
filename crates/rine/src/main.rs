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
    let tls = match (&args.cert_file, &args.key_file) {
        (None, None) => None,
        (Some(cert), Some(key)) => {
            let identity =
                tonic::transport::Identity::from_pem(std::fs::read(cert)?, std::fs::read(key)?);
            let mut config = tonic::transport::ServerTlsConfig::new().identity(identity);
            if args.client_cert_auth {
                let ca = args.trusted_ca_file.clone().ok_or_else(|| {
                    anyhow::anyhow!(
                        "--client-cert-auth needs --trusted-ca-file (or RINE_TRUSTED_CA_FILE)"
                    )
                })?;
                config = config
                    .client_ca_root(tonic::transport::Certificate::from_pem(std::fs::read(ca)?));
            }
            Some(config)
        }
        _ => anyhow::bail!("--cert-file and --key-file must be given together"),
    };
    let server = rine_etcd::server::EtcdServer::new(backend);
    match tls {
        Some(tls) => server.serve_tls(args.listen, tls).await?,
        None => server.serve(args.listen).await?,
    }
    Ok(())
}
