//! gRPC server wiring.

use std::{net::SocketAddr, sync::Arc};

use tonic::transport::ServerTlsConfig;

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

/// One client listener: address plus optional TLS. `https://` listeners
/// require a TLS config; `http://` listeners stay plaintext.
pub struct ListenAddr {
    pub addr: SocketAddr,
    pub tls: Option<ServerTlsConfig>,
}

impl EtcdServer {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    pub async fn serve(self, addr: SocketAddr) -> anyhow::Result<()> {
        self.serve_inner(addr, None).await
    }

    /// Serve with TLS. When the config carries a client CA root, clients must
    /// present a certificate signed by that CA (mTLS).
    pub async fn serve_tls(self, addr: SocketAddr, tls: ServerTlsConfig) -> anyhow::Result<()> {
        self.serve_inner(addr, Some(tls)).await
    }

    async fn serve_inner(
        self,
        addr: SocketAddr,
        tls: Option<ServerTlsConfig>,
    ) -> anyhow::Result<()> {
        let leases = LeaseManager::new(self.backend.clone());
        Self::serve_on(addr, tls, self.backend, leases).await
    }

    /// Serve several listeners (mixed `http://` plaintext and `https://` TLS)
    /// on one shared backend and one shared lease manager.
    pub async fn serve_many(self, listeners: Vec<ListenAddr>) -> anyhow::Result<()> {
        if listeners.is_empty() {
            anyhow::bail!("no listen addresses");
        }
        let leases = LeaseManager::new(self.backend.clone());
        let mut set = tokio::task::JoinSet::new();
        for l in listeners {
            set.spawn(Self::serve_on(
                l.addr,
                l.tls,
                self.backend.clone(),
                leases.clone(),
            ));
        }
        while let Some(res) = set.join_next().await {
            res??;
        }
        Ok(())
    }

    async fn serve_on(
        addr: SocketAddr,
        tls: Option<ServerTlsConfig>,
        backend: Arc<dyn Backend>,
        leases: LeaseManager,
    ) -> anyhow::Result<()> {
        // rustls 0.23 leaves process-wide provider selection to the
        // application; without this every TLS handshake panics.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let scheme = if tls.is_some() { "https" } else { "http" };
        let client_url = format!("{scheme}://{addr}");
        tracing::info!(%addr, tls = tls.is_some(), "serving etcd-compatible gRPC");
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = tls {
            builder = builder.tls_config(tls)?;
        }
        builder
            .add_service(KvServer::new(KvService::new(
                backend.clone(),
                leases.clone(),
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
