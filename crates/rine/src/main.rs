use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::Parser;
use rine_etcd::Backend;

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
    rine_etcd::server::EtcdServer::new(backend)
        .serve(args.listen)
        .await?;
    Ok(())
}
