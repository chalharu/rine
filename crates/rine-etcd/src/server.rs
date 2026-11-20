//! gRPC server wiring.
//!
//! Transport *mechanism* lives here: applying a [`TlsIdentity`] to tonic,
//! serving plaintext and TLS listeners on one shared backend. Transport
//! *configuration* (flag parsing, file reads, URL interpretation) lives with
//! the binary (`rine/src/listen.rs`); this module only consumes an address
//! plus already-loaded identity bytes.

use std::{net::SocketAddr, sync::Arc};

use crate::{
    backend::Backend,
    cluster::{ClusterService, MaintenanceService},
    etcdserverpb::{
        cluster_server::ClusterServer, kv_server::KvServer, lease_server::LeaseServer,
        maintenance_server::MaintenanceServer, watch_server::WatchServer,
    },
    kv::KvService,
    lease::{LeaseManager, LeaseService},
    watch::WatchService,
};

pub struct EtcdServer {
    pub backend: Arc<dyn Backend>,
}

/// Loaded TLS identity for one `https://` listener: PEM-encoded server
/// certificate chain, private key, and the optional client CA root (mTLS).
/// Bytes only — no file paths: loading them is the binary's job.
#[derive(Debug, Clone)]
pub struct TlsIdentity {
    pub cert: Vec<u8>,
    pub key: Vec<u8>,
    pub client_ca: Option<Vec<u8>>,
}

/// One client listener: address plus optional TLS identity. `https://`
/// listeners require an identity; `http://` listeners stay plaintext.
pub struct ListenAddr {
    pub addr: SocketAddr,
    pub tls: Option<TlsIdentity>,
}

impl EtcdServer {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    /// Serve several listeners (mixed `http://` plaintext and `https://` TLS)
    /// on one shared backend and one shared lease manager.
    pub async fn serve_many(self, listeners: Vec<ListenAddr>) -> anyhow::Result<()> {
        if listeners.is_empty() {
            anyhow::bail!("no listen addresses");
        }
        // rustls 0.23 leaves process-wide provider selection to the
        // application; without this every TLS handshake panics.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let writes = crate::backend::new_commit_lock();
        let leases = LeaseManager::new(self.backend.clone(), writes.clone());
        let mut set = tokio::task::JoinSet::new();
        for l in listeners {
            let (backend, leases, writes) = (self.backend.clone(), leases.clone(), writes.clone());
            let addr = l.addr;
            set.spawn(async move {
                Self::serve_on(l.addr, l.tls, backend, leases, writes)
                    .await
                    .map_err(|e| anyhow::anyhow!("listener {addr}: {e:#}"))
            });
        }
        while let Some(res) = set.join_next().await {
            res??;
        }
        Ok(())
    }

    async fn serve_on(
        addr: SocketAddr,
        tls: Option<TlsIdentity>,
        backend: Arc<dyn Backend>,
        leases: LeaseManager,
        writes: crate::backend::CommitLock,
    ) -> anyhow::Result<()> {
        let scheme = if tls.is_some() { "https" } else { "http" };
        let client_url = format!("{scheme}://{addr}");
        tracing::info!(%addr, tls = tls.is_some(), "serving etcd-compatible gRPC");
        let mut builder = tonic::transport::Server::builder();
        if let Some(identity) = tls {
            let mut config = tonic::transport::ServerTlsConfig::new().identity(
                tonic::transport::Identity::from_pem(identity.cert, identity.key),
            );
            if let Some(ca) = identity.client_ca {
                config = config.client_ca_root(tonic::transport::Certificate::from_pem(ca));
            }
            builder = builder.tls_config(config)?;
        }
        builder
            .add_service(KvServer::new(KvService::new(
                backend.clone(),
                leases.clone(),
                writes,
            )))
            .add_service(WatchServer::new(WatchService::new(backend.clone())))
            .add_service(LeaseServer::new(LeaseService::new(backend.clone(), leases)))
            .add_service(ClusterServer::new(ClusterService::new(
                backend.clone(),
                client_url,
            )))
            .add_service(MaintenanceServer::new(MaintenanceService::new(backend)))
            .serve(addr)
            .await?;
        Ok(())
    }
}
