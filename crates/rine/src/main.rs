use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "rine", version)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:2379", env = "RINE_LISTEN")]
    listen: SocketAddr,
    #[arg(long, default_value = "memory", env = "RINE_BACKEND")]
    backend: String,
    /// Oracle connect string: Easy Connect (`host/service`), a full connect
    /// descriptor, or a TNS alias (resolved via TNS_ADMIN or --wallet-dir).
    #[arg(long, env = "RINE_ORACLE_CONNECT")]
    oracle_connect: Option<String>,
    /// Wallet directory (`tnsnames.ora` + `ewallet.pem`): used as the Oracle
    /// Net configuration directory and wallet location.
    #[arg(long, env = "RINE_WALLET_DIR")]
    wallet_dir: Option<PathBuf>,
    #[arg(long, default_value = "", env = "RINE_ORACLE_USER")]
    user: String,
    #[arg(long, default_value = "", env = "RINE_ORACLE_PASSWORD")]
    password: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();

    match args.backend.as_str() {
        "memory" => {
            let mem = Arc::new(rine_etcd::MemoryBackend::new());
            rine_etcd::server::EtcdServer::new(mem)
                .serve(args.listen)
                .await?;
        }
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
            let backend = Arc::new(rine_store_oracle::OracleBackend::new(Arc::new(db)));
            rine_etcd::server::EtcdServer::new(backend)
                .serve(args.listen)
                .await?;
        }
        other => anyhow::bail!("unknown backend: {other} (expected memory|oracle)"),
    }
    Ok(())
}
