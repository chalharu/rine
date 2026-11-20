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

use bytes::Bytes;
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    backend::{Backend, CommitLock, Event, KeyValue, ListOptions, ReadSet, header},
    error::Error,
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
    writes: CommitLock,
    inner: Arc<RwLock<HashMap<i64, LeaseEntry>>>,
    backend: Arc<dyn Backend>,
    next_id: Arc<AtomicI64>,
}

impl Clone for LeaseManager {
    fn clone(&self) -> Self {
        Self {
            writes: self.writes.clone(),
            inner: self.inner.clone(),
            backend: self.backend.clone(),
            next_id: self.next_id.clone(),
        }
    }
}

impl LeaseManager {
    /// Delete many expired keys in one guarded commit instead of one commit
    /// per key: a mass expiry (node loss, TTL sync) must not stall all
    /// writers for seconds. Keys that moved under us abort the batch with a
    /// conflict; those fall back to per-key best effort on the snapshot we
    /// already hold (no re-list).
    async fn expire_keys(backend: &Arc<dyn Backend>, pairs: Vec<(i64, Vec<u8>)>) {
        if pairs.is_empty() {
            return;
        }
        let mut live: Vec<(i64, Vec<u8>, KeyValue)> = Vec::new();
        for (id, key) in &pairs {
            if let Ok((_, rows, _)) = backend.list(key, None, ListOptions::default()).await
                && let Some(kv) = rows.into_iter().next()
                && kv.lease == *id
            {
                live.push((*id, key.clone(), kv));
            }
        }
        if live.is_empty() {
            return;
        }
        let base = backend.current_revision().await;
        let reads = ReadSet {
            points: live
                .iter()
                .map(|(_, key, kv)| (Bytes::copy_from_slice(key), Some(kv.mod_revision)))
                .collect(),
            ranges: Vec::new(),
        };
        let events = live
            .iter()
            .map(|(_, key, kv)| Event {
                revision: base + 1,
                key: Bytes::copy_from_slice(key),
                deleted: true,
                kv: None,
                prev_kv: Some(kv.clone()),
            })
            .collect();
        match backend.commit(reads, events).await {
            Ok(_) => {}
            Err(Error::Conflict) => {
                // Fall back key by key on the snapshot above, without
                // re-listing: each delete is still guarded, so a key that
                // moved again simply conflicts away.
                for (_, key, prev) in &live {
                    let reads = ReadSet {
                        points: vec![(Bytes::copy_from_slice(key), Some(prev.mod_revision))],
                        ranges: Vec::new(),
                    };
                    let _ = backend
                        .commit(
                            reads,
                            vec![Event {
                                revision: base + 1,
                                key: Bytes::copy_from_slice(key),
                                deleted: true,
                                kv: None,
                                prev_kv: Some(prev.clone()),
                            }],
                        )
                        .await;
                }
            }
            Err(_) => {}
        }
    }

    pub fn new(backend: Arc<dyn Backend>, writes: CommitLock) -> Self {
        let mgr = Self {
            writes,
            inner: Arc::new(RwLock::new(HashMap::new())),
            backend,
            next_id: Arc::new(AtomicI64::new(1)),
        };
        // Expiry loop: delete keys whose lease deadline passed. The 500ms
        // poll matches upstream etcd's lessor `runLoop`, bounding the extra
        // lifetime of an expired lease the same way.
        let inner = mgr.inner.clone();
        let backend = mgr.backend.clone();
        let writes = mgr.writes.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(500));
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
                if expired.is_empty() {
                    // No global write lock on empty ticks.
                    continue;
                }
                let _guard = writes.lock().await;
                Self::expire_keys(
                    &backend,
                    expired
                        .into_iter()
                        .flat_map(|(id, keys)| keys.into_iter().map(move |k| (id, k)))
                        .collect(),
                )
                .await;
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

    pub(crate) async fn detach(&self, id: i64, key: &[u8]) {
        if let Some(e) = self.inner.write().await.get_mut(&id) {
            e.keys.retain(|k| k != key);
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
        if keys.is_empty() {
            return;
        }
        let _guard = self.writes.lock().await;
        Self::expire_keys(&self.backend, keys.into_iter().map(|k| (id, k)).collect()).await;
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
}

impl LeaseService {
    pub fn new(backend: Arc<dyn Backend>, leases: LeaseManager) -> Self {
        Self { backend, leases }
    }

    fn header(&self, revision: i64) -> ResponseHeader {
        header(revision)
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
        tokio::spawn(async move {
            while let Ok(Some(req)) = inbound.message().await {
                let ttl = leases.keepalive(req.id).await;
                let rev = backend.current_revision().await;
                let resp = LeaseKeepAliveResponse {
                    header: Some(header(rev)),
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
