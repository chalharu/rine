//! Lease manager + Lease service.
//!
//! rine grants dummy leases; here leases are tracked and expired keys are
//! deleted by a background task, which is what the k8s apiserver relies on.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    backend::Backend,
    etcdserverpb::{
        LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
        LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
        LeaseStatus, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, ResponseHeader,
        lease_server::Lease,
    },
};

struct LeaseEntry {
    ttl: i64,
    granted_ttl: i64,
    deadline: Instant,
    keys: Vec<Vec<u8>>,
}

pub struct LeaseManager {
    inner: Arc<RwLock<HashMap<i64, LeaseEntry>>>,
    backend: Arc<dyn Backend>,
    next_id: Arc<AtomicI64>,
}

impl Clone for LeaseManager {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            backend: self.backend.clone(),
            next_id: self.next_id.clone(),
        }
    }
}

impl LeaseManager {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        let mgr = Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            backend,
            next_id: Arc::new(AtomicI64::new(1)),
        };
        // Expiry loop: delete keys whose lease deadline passed.
        let inner = mgr.inner.clone();
        let backend = mgr.backend.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                let expired: Vec<(i64, Vec<Vec<u8>>)> = {
                    let now = Instant::now();
                    let mut g = inner.write().await;
                    let mut out = Vec::new();
                    g.retain(|id, e| {
                        if e.deadline <= now {
                            out.push((*id, std::mem::take(&mut e.keys)));
                            false
                        } else {
                            true
                        }
                    });
                    out
                };
                for (id, keys) in expired {
                    for k in keys {
                        // Only delete while the key still belongs to this lease.
                        if let Ok((_, Some(kv))) = backend.get(&k, 0).await
                            && kv.lease == id
                        {
                            let _ = backend.delete(&k).await;
                        }
                    }
                }
            }
        });
        mgr
    }

    pub async fn grant(&self, ttl: i64, requested: i64) -> i64 {
        let id = if requested != 0 {
            requested
        } else {
            self.next_id.fetch_add(1, Ordering::Relaxed)
        };
        let mut g = self.inner.write().await;
        g.insert(
            id,
            LeaseEntry {
                ttl,
                granted_ttl: ttl,
                deadline: Instant::now() + Duration::from_secs(ttl.max(0) as u64),
                keys: Vec::new(),
            },
        );
        id
    }

    pub async fn exists(&self, id: i64) -> bool {
        self.inner.read().await.contains_key(&id)
    }

    pub async fn attach(&self, id: i64, key: Vec<u8>) {
        if let Some(e) = self.inner.write().await.get_mut(&id)
            && !e.keys.contains(&key)
        {
            e.keys.push(key);
        }
    }

    pub async fn revoke(&self, id: i64) {
        let keys = self
            .inner
            .write()
            .await
            .remove(&id)
            .map(|e| e.keys)
            .unwrap_or_default();
        for k in keys {
            if let Ok((_, Some(kv))) = self.backend.get(&k, 0).await
                && kv.lease == id
            {
                let _ = self.backend.delete(&k).await;
            }
        }
    }

    pub async fn keepalive(&self, id: i64) -> i64 {
        match self.inner.write().await.get_mut(&id) {
            Some(e) => {
                e.deadline = Instant::now() + Duration::from_secs(e.ttl.max(0) as u64);
                e.ttl
            }
            None => 0,
        }
    }

    pub async fn ttl_info(&self, id: i64) -> Option<(i64, i64, Vec<Vec<u8>>)> {
        self.inner.read().await.get(&id).map(|e| {
            let remain = e
                .deadline
                .saturating_duration_since(Instant::now())
                .as_secs() as i64;
            (remain, e.granted_ttl, e.keys.clone())
        })
    }

    pub async fn all_ids(&self) -> Vec<i64> {
        self.inner.read().await.keys().copied().collect()
    }
}

pub struct LeaseService {
    backend: Arc<dyn Backend>,
    leases: LeaseManager,
    cluster_id: u64,
    member_id: u64,
}

impl LeaseService {
    pub fn new(backend: Arc<dyn Backend>, leases: LeaseManager) -> Self {
        Self {
            backend,
            leases,
            cluster_id: 0x7269_6e65_0000_0001,
            member_id: 0x7269_6e65_0000_0002,
        }
    }

    fn header(&self, revision: i64) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision,
            raft_term: 0,
        }
    }
}

#[tonic::async_trait]
impl Lease for LeaseService {
    async fn lease_grant(
        &self,
        request: tonic::Request<LeaseGrantRequest>,
    ) -> Result<tonic::Response<LeaseGrantResponse>, tonic::Status> {
        let req = request.into_inner();
        if req.ttl <= 0 {
            return Err(tonic::Status::invalid_argument(
                "etcdserver: lease TTL must be positive",
            ));
        }
        let id = self.leases.grant(req.ttl, req.id).await;
        let rev = self.backend.current_revision().await;
        Ok(tonic::Response::new(LeaseGrantResponse {
            header: Some(self.header(rev)),
            id,
            ttl: req.ttl,
            error: String::new(),
        }))
    }

    async fn lease_revoke(
        &self,
        request: tonic::Request<LeaseRevokeRequest>,
    ) -> Result<tonic::Response<LeaseRevokeResponse>, tonic::Status> {
        let req = request.into_inner();
        self.leases.revoke(req.id).await;
        let rev = self.backend.current_revision().await;
        Ok(tonic::Response::new(LeaseRevokeResponse {
            header: Some(self.header(rev)),
        }))
    }

    type LeaseKeepAliveStream = ReceiverStream<Result<LeaseKeepAliveResponse, tonic::Status>>;

    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let leases = self.leases.clone();
        let backend = self.backend.clone();
        let (tx, rx) = mpsc::channel(32);
        let cluster_id = self.cluster_id;
        let member_id = self.member_id;
        tokio::spawn(async move {
            while let Ok(Some(req)) = inbound.message().await {
                let ttl = leases.keepalive(req.id).await;
                let rev = backend.current_revision().await;
                let resp = LeaseKeepAliveResponse {
                    header: Some(ResponseHeader {
                        cluster_id,
                        member_id,
                        revision: rev,
                        raft_term: 0,
                    }),
                    id: req.id,
                    ttl,
                };
                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn lease_time_to_live(
        &self,
        request: tonic::Request<LeaseTimeToLiveRequest>,
    ) -> Result<tonic::Response<LeaseTimeToLiveResponse>, tonic::Status> {
        let req = request.into_inner();
        let rev = self.backend.current_revision().await;
        match self.leases.ttl_info(req.id).await {
            None => Err(tonic::Status::not_found(
                "etcdserver: requested lease not found",
            )),
            Some((ttl, granted, keys)) => Ok(tonic::Response::new(LeaseTimeToLiveResponse {
                header: Some(self.header(rev)),
                id: req.id,
                ttl,
                granted_ttl: granted,
                keys: if req.keys { keys } else { Vec::new() },
            })),
        }
    }

    async fn lease_leases(
        &self,
        _request: tonic::Request<LeaseLeasesRequest>,
    ) -> Result<tonic::Response<LeaseLeasesResponse>, tonic::Status> {
        let rev = self.backend.current_revision().await;
        let ids = self.leases.all_ids().await;
        Ok(tonic::Response::new(LeaseLeasesResponse {
            header: Some(self.header(rev)),
            leases: ids.into_iter().map(|id| LeaseStatus { id }).collect(),
        }))
    }
}
