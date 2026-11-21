//! Cluster::MemberList and Maintenance::Status.
//!
//! Single-member responses sufficient for k8s apiserver health/discovery.

use std::sync::Arc;

use crate::{
    backend::{Backend, MEMBER_ID, VERSION, header},
    etcdserverpb::{
        Member, MemberListRequest, MemberListResponse, StatusRequest, StatusResponse,
        cluster_server::Cluster, maintenance_server::Maintenance,
    },
};

pub struct ClusterService {
    backend: Arc<dyn Backend>,
    client_url: String,
}

impl ClusterService {
    pub fn new(backend: Arc<dyn Backend>, client_url: String) -> Self {
        Self {
            backend,
            client_url,
        }
    }
}

#[tonic::async_trait]
impl Cluster for ClusterService {
    async fn member_list(
        &self,
        _request: tonic::Request<MemberListRequest>,
    ) -> Result<tonic::Response<MemberListResponse>, tonic::Status> {
        let rev = self.backend.current_revision().await;
        Ok(tonic::Response::new(MemberListResponse {
            header: Some(header(rev)),
            members: vec![Member {
                id: MEMBER_ID,
                name: "rine".into(),
                peer_ur_ls: vec![self.client_url.clone()],
                client_ur_ls: vec![self.client_url.clone()],
                is_learner: false,
            }],
        }))
    }
}

pub struct MaintenanceService {
    backend: Arc<dyn Backend>,
}

impl MaintenanceService {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }
}

#[tonic::async_trait]
impl Maintenance for MaintenanceService {
    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let rev = self.backend.current_revision().await;
        let size = self.backend.db_size().await;
        Ok(tonic::Response::new(StatusResponse {
            header: Some(header(rev)),
            version: VERSION.into(),
            db_size: size,
            leader: MEMBER_ID,
            raft_index: rev.max(0) as u64,
            raft_term: 0,
            raft_applied_index: rev.max(0) as u64,
            errors: Vec::new(),
            db_size_in_use: size,
            is_learner: false,
        }))
    }
}
