//! Cluster::MemberList and Maintenance::Status.
//!
//! Single-member responses sufficient for k8s apiserver health/discovery.

use crate::backend::Backend;
use crate::etcdserverpb::cluster_server::Cluster;
use crate::etcdserverpb::maintenance_server::Maintenance;
use crate::etcdserverpb::{
    Member, MemberListRequest, MemberListResponse, ResponseHeader, StatusRequest, StatusResponse,
};
use std::sync::Arc;

pub struct ClusterService<B: Backend> {
    backend: Arc<B>,
    member_id: u64,
    member_name: String,
    client_url: String,
}

impl<B: Backend> ClusterService<B> {
    pub fn new(backend: Arc<B>, client_url: String) -> Self {
        Self {
            backend,
            member_id: 0x7269_6e65_0000_0002,
            member_name: "rine".into(),
            client_url,
        }
    }
}

#[tonic::async_trait]
impl<B: Backend> Cluster for ClusterService<B> {
    async fn member_list(
        &self,
        _request: tonic::Request<MemberListRequest>,
    ) -> Result<tonic::Response<MemberListResponse>, tonic::Status> {
        let rev = self.backend.current_revision().await;
        Ok(tonic::Response::new(MemberListResponse {
            header: Some(ResponseHeader {
                cluster_id: 0x7269_6e65_0000_0001,
                member_id: self.member_id,
                revision: rev,
                raft_term: 0,
            }),
            members: vec![Member {
                id: self.member_id,
                name: self.member_name.clone(),
                peer_ur_ls: vec![self.client_url.clone()],
                client_ur_ls: vec![self.client_url.clone()],
                is_learner: false,
            }],
        }))
    }
}

pub struct MaintenanceService<B: Backend> {
    backend: Arc<B>,
    member_id: u64,
}

impl<B: Backend> MaintenanceService<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            member_id: 0x7269_6e65_0000_0002,
        }
    }
}

#[tonic::async_trait]
impl<B: Backend> Maintenance for MaintenanceService<B> {
    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        let rev = self.backend.current_revision().await;
        let size = self.backend.db_size().await;
        Ok(tonic::Response::new(StatusResponse {
            header: Some(ResponseHeader {
                cluster_id: 0x7269_6e65_0000_0001,
                member_id: self.member_id,
                revision: rev,
                raft_term: 0,
            }),
            version: "3.5.0".into(),
            db_size: size,
            leader: self.member_id,
            raft_index: rev.max(0) as u64,
            raft_term: 0,
            raft_applied_index: rev.max(0) as u64,
            errors: Vec::new(),
            db_size_in_use: size,
            is_learner: false,
        }))
    }
}
