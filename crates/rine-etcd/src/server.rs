//! gRPC server wiring.

use crate::backend::Backend;
use crate::cluster::{ClusterService, MaintenanceService};
use crate::etcdserverpb::{
    cluster_server::ClusterServer, kv_server::KvServer, lease_server::LeaseServer,
    maintenance_server::MaintenanceServer, watch_server::WatchServer,
};
use crate::kv::KvService;
use crate::lease::{LeaseManager, LeaseService};
use crate::watch::WatchService;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct EtcdServer<B: Backend> {
    pub backend: Arc<B>,
}

impl<B: Backend> EtcdServer<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }

    pub async fn serve(self, addr: SocketAddr) -> anyhow::Result<()> {
        let leases = LeaseManager::new(self.backend.clone());
        let client_url = format!("http://{addr}");
        tracing::info!(%addr, "serving etcd-compatible gRPC");
        tonic::transport::Server::builder()
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
