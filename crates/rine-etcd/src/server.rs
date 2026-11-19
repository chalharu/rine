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
        // rustls 0.23 leaves process-wide provider selection to the
        // application; without this every TLS handshake panics.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let leases = LeaseManager::new(self.backend.clone());
        let scheme = if tls.is_some() { "https" } else { "http" };
        let client_url = format!("{scheme}://{addr}");
        tracing::info!(%addr, tls = tls.is_some(), "serving etcd-compatible gRPC");
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = tls {
            builder = builder.tls_config(tls)?;
        }
        builder
            .add_service(KvServer::new(KvService::new(
                self.backend.clone(),
                leases.clone(),
            )))
            .add_service(WatchServer::new(WatchService::new(self.backend.clone())))
            .add_service(LeaseServer::new(LeaseService::new(
                self.backend.clone(),
                leases,
            )))
            .add_service(ClusterServer::new(ClusterService::new(
                self.backend.clone(),
                client_url,
            )))
            .add_service(MaintenanceServer::new(MaintenanceService::new(
                self.backend,
            )))
            .serve(addr)
            .await?;
        Ok(())
    }
}
