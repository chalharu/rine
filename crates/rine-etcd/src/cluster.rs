//! Cluster::MemberList and Maintenance::Status.
//!
//! Single-member responses sufficient for k8s apiserver health/discovery.

use std::sync::Arc;

use crate::{
    backend::{Backend, MEMBER_ID, VERSION, header},
    etcdserverpb::{
        AlarmRequest, AlarmResponse, Member, MemberListRequest, MemberListResponse,
        SnapshotRequest, SnapshotResponse, StatusRequest, StatusResponse,
        alarm_request::AlarmAction, cluster_server::Cluster, maintenance_server::Maintenance,
    },
    http::Metrics,
};

pub(crate) struct ClusterService {
    backend: Arc<dyn Backend>,
    client_url: String,
    metrics: Arc<Metrics>,
}

impl ClusterService {
    pub fn new(backend: Arc<dyn Backend>, client_url: String, metrics: Arc<Metrics>) -> Self {
        Self {
            backend,
            client_url,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl Cluster for ClusterService {
    async fn member_list(
        &self,
        _request: tonic::Request<MemberListRequest>,
    ) -> Result<tonic::Response<MemberListResponse>, tonic::Status> {
        self.metrics
            .observe("etcdserverpb.Cluster", "MemberList", async {
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
            })
            .await
    }
}

pub(crate) struct MaintenanceService {
    backend: Arc<dyn Backend>,
    metrics: Arc<Metrics>,
}

impl MaintenanceService {
    pub fn new(backend: Arc<dyn Backend>, metrics: Arc<Metrics>) -> Self {
        Self { backend, metrics }
    }
}

#[tonic::async_trait]
impl Maintenance for MaintenanceService {
    type SnapshotStream =
        tokio_stream::wrappers::ReceiverStream<Result<SnapshotResponse, tonic::Status>>;

    async fn status(
        &self,
        _request: tonic::Request<StatusRequest>,
    ) -> Result<tonic::Response<StatusResponse>, tonic::Status> {
        self.metrics
            .observe("etcdserverpb.Maintenance", "Status", async {
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
            })
            .await
    }

    /// Alarm is a read-only stub: rine runs no quota or corruption watcher,
    /// so GET always reports an empty list (which is also what
    /// `endpoint health` needs). Raising or clearing alarms is rejected.
    async fn alarm(
        &self,
        request: tonic::Request<AlarmRequest>,
    ) -> Result<tonic::Response<AlarmResponse>, tonic::Status> {
        self.metrics
            .observe("etcdserverpb.Maintenance", "Alarm", async {
                let req = request.into_inner();
                if req.action != AlarmAction::Get as i32 {
                    return Err(tonic::Status::invalid_argument(
                        "rine: alarms cannot be raised or cleared",
                    ));
                }
                Ok(tonic::Response::new(AlarmResponse {
                    header: Some(header(self.backend.current_revision().await)),
                    alarms: Vec::new(),
                }))
            })
            .await
    }

    /// Snapshot is intentionally unsupported: rine stores data in SQLite /
    /// Oracle / memory, never in bbolt, so no byte stream here could satisfy
    /// `snapshot status` / `snapshot restore`. Registered explicitly so the
    /// client fails fast with this message instead of an empty
    /// unknown-service UNIMPLEMENTED.
    async fn snapshot(
        &self,
        _request: tonic::Request<SnapshotRequest>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status> {
        self.metrics
            .observe("etcdserverpb.Maintenance", "Snapshot", async {
                Err(tonic::Status::unimplemented(
                    "rine: snapshot save is not supported; back up the backend directly \
                     (SQLite: copy the database file; Oracle: RMAN or Data Pump)",
                ))
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryBackend;

    #[tokio::test]
    async fn alarm_get_is_empty_and_mutations_rejected() {
        use crate::etcdserverpb::AlarmRequest;
        let svc = MaintenanceService::new(
            Arc::new(MemoryBackend::new()),
            Arc::new(crate::http::Metrics::new()),
        );
        let ok = svc
            .alarm(tonic::Request::new(AlarmRequest {
                action: AlarmAction::Get as i32,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(ok.alarms.is_empty());
        for action in [AlarmAction::Activate, AlarmAction::Deactivate] {
            let err = svc
                .alarm(tonic::Request::new(AlarmRequest {
                    action: action as i32,
                    ..Default::default()
                }))
                .await
                .unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn snapshot_fails_fast_with_guidance() {
        let svc = MaintenanceService::new(
            Arc::new(MemoryBackend::new()),
            Arc::new(crate::http::Metrics::new()),
        );
        let err = svc
            .snapshot(tonic::Request::new(SnapshotRequest {}))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        let msg = err.message();
        assert!(msg.contains("snapshot save is not supported"), "{msg}");
        assert!(msg.contains("SQLite"), "{msg}");
    }
}
