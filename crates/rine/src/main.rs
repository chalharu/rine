use anyhow::Result;
use clap::Parser;
use rine_etcd::backend::BackendFactory;

use crate::listen::ListenConfig;

mod listen;

#[derive(Parser, Debug)]
#[command(
    name = "rine",
    version,
    about = "etcd-compatible KV store with pluggable backends"
)]
struct Args {
    #[command(flatten)]
    listen: ListenConfig,
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

    // Backend implementations register themselves here; `--backend`
    // selects one by the name its factory reports, so a new backend crate
    // only adds a factory here plus its own `BackendFactory` impl.
    let factories = build_factories(&args);
    let factory = select_factory(&factories, &args.backend)?;
    let backend = factory.build()?;
    backend.initialize().await?;

    // Transport setup (URL parsing, TLS validation) lives with the binary;
    // the server library only consumes addresses plus loaded identity bytes.
    let listeners = args.listen.into_listeners().await?;
    rine_etcd::server::EtcdServer::new(backend)
        .serve_many(listeners)
        .await?;
    Ok(())
}

/// Register every compiled-in backend factory. Adding a backend is this
/// list plus the new crate's own `BackendFactory` impl and CLI config;
/// `--backend` selection and the unknown-name error follow automatically.
#[allow(unused_mut)]
fn build_factories(_args: &Args) -> Vec<Box<dyn BackendFactory>> {
    let mut factories: Vec<Box<dyn BackendFactory>> = vec![Box::new(rine_etcd::MemoryFactory)];
    #[cfg(feature = "sqlite")]
    factories.push(Box::new(rine_store_sqlite::SqliteFactory::new(
        _args.sqlite.clone(),
    )));
    #[cfg(feature = "oracle")]
    factories.push(Box::new(rine_store_oracle::OracleFactory::new(
        _args.oracle.clone(),
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
        assert_eq!(args.listen.listen, "127.0.0.1:2380".parse().unwrap());
        let defaults = Args::try_parse_from(["rine"]).unwrap();
        assert_eq!(defaults.backend, "memory");
    }
}
