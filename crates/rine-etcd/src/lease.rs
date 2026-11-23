//! Lease manager + Lease service: leases are tracked and expired keys are
//! deleted by a background task, which is what the k8s apiserver relies on.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    backend::{Backend, CommitLock, Event, KeyValue, ListOptions, ReadSet, header},
    error::{Error, status_err},
    etcdserverpb::{
        LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
        LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest, LeaseRevokeResponse,
        LeaseStatus, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, lease_server::Lease,
    },
};

/// Wall-clock milliseconds. Lease deadlines live in the backend, so every
/// node and every restart compares the same clock base.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone)]
pub(crate) struct LeaseManager {
    writes: CommitLock,
    backend: Arc<dyn Backend>,
}

impl LeaseManager {
    /// Delete many expired keys in one guarded commit instead of one commit
    /// per key: a mass expiry (node loss, TTL sync) must not stall all
    /// writers for seconds.
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
        // A key that moved under us conflicts away and is retried on the
        // next expiry tick.
        let _ = backend.commit(reads, events).await;
    }

    pub fn new(backend: Arc<dyn Backend>, writes: CommitLock) -> Self {
        Self { writes, backend }
    }

    /// Poll for past-due leases and delete their keys. The 500ms cadence
    /// matches upstream etcd's lessor `runLoop`. Lease state lives in the
    /// backend, so every node (and every restart) sees the same deadlines;
    /// `take_expired` is atomic, so rival loops never work the same lease
    /// twice. Call once per process (the server does); tests that need
    /// expiry call it explicitly instead of paying a task per manager.
    pub fn spawn_expiry_loop(&self) {
        let backend = self.backend.clone();
        let writes = self.writes.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(500));
            loop {
                tick.tick().await;
                let expired = backend
                    .lease_take_expired(now_ms())
                    .await
                    .unwrap_or_default();
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
    }

    pub async fn revoke(&self, id: i64) -> Result<(), Error> {
        let keys = self.backend.lease_revoke(id).await?;
        if keys.is_empty() {
            return Ok(());
        }
        let _guard = self.writes.lock().await;
        Self::expire_keys(&self.backend, keys.into_iter().map(|k| (id, k)).collect()).await;
        Ok(())
    }
}

pub(crate) struct LeaseService {
    backend: Arc<dyn Backend>,
    leases: LeaseManager,
}

impl LeaseService {
    pub fn new(backend: Arc<dyn Backend>, leases: LeaseManager) -> Self {
        Self { backend, leases }
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
        let id = self
            .backend
            .lease_grant(req.ttl, req.id, now_ms())
            .await
            .map_err(status_err)?;
        let rev = self.backend.current_revision().await;
        Ok(tonic::Response::new(LeaseGrantResponse {
            header: Some(header(rev)),
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
        self.leases.revoke(req.id).await.map_err(status_err)?;
        let rev = self.backend.current_revision().await;
        Ok(tonic::Response::new(LeaseRevokeResponse {
            header: Some(header(rev)),
        }))
    }

    type LeaseKeepAliveStream = ReceiverStream<Result<LeaseKeepAliveResponse, tonic::Status>>;

    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<tonic::Response<Self::LeaseKeepAliveStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let backend = self.backend.clone();
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            while let Ok(Some(req)) = inbound.message().await {
                // A transient storage error surfaces as TTL 0 (unknown
                // lease), exactly like a missing lease; the stream stays
                // open so the next response can still succeed.
                let ttl = backend
                    .lease_keepalive(req.id, now_ms())
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(0);
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
        match self
            .backend
            .lease_lookup(req.id)
            .await
            .map_err(status_err)?
        {
            None => Err(tonic::Status::not_found(
                "etcdserver: requested lease not found",
            )),
            Some(state) => {
                let remain = state.expires_at_ms.saturating_sub(now_ms()) / 1000;
                let (ttl, granted, keys) = (remain as i64, state.granted_ttl, state.keys);
                Ok(tonic::Response::new(LeaseTimeToLiveResponse {
                    header: Some(header(rev)),
                    id: req.id,
                    ttl,
                    granted_ttl: granted,
                    keys: if req.keys { keys } else { Vec::new() },
                }))
            }
        }
    }

    async fn lease_leases(
        &self,
        _request: tonic::Request<LeaseLeasesRequest>,
    ) -> Result<tonic::Response<LeaseLeasesResponse>, tonic::Status> {
        let rev = self.backend.current_revision().await;
        let ids = self.backend.lease_ids().await.map_err(status_err)?;
        Ok(tonic::Response::new(LeaseLeasesResponse {
            header: Some(header(rev)),
            leases: ids.into_iter().map(|id| LeaseStatus { id }).collect(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryBackend,
        backend::{Event, ReadSet},
        etcdserverpb::{LeaseGrantRequest, LeaseRevokeRequest, LeaseTimeToLiveRequest},
    };

    async fn manager() -> (LeaseManager, Arc<dyn Backend>) {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let writes: CommitLock = Arc::new(tokio::sync::Mutex::new(()));
        let mgr = LeaseManager::new(backend.clone(), writes);
        (mgr, backend)
    }

    pub(super) async fn grant(backend: &Arc<dyn Backend>, ttl: i64, requested: i64) -> i64 {
        backend.lease_grant(ttl, requested, now_ms()).await.unwrap()
    }

    pub(super) async fn exists(backend: &Arc<dyn Backend>, id: i64) -> bool {
        backend.lease_lookup(id).await.unwrap().is_some()
    }

    async fn keepalive(backend: &Arc<dyn Backend>, id: i64) -> i64 {
        backend
            .lease_keepalive(id, now_ms())
            .await
            .unwrap()
            .unwrap_or(0)
    }

    async fn put_key(backend: &Arc<dyn Backend>, key: &[u8], lease: i64) {
        let key = Bytes::copy_from_slice(key);
        let revision = backend.current_revision().await + 1;
        backend
            .commit(
                ReadSet {
                    points: vec![(key.clone(), None)],
                    ranges: Vec::new(),
                },
                vec![Event {
                    revision,
                    key: key.clone(),
                    deleted: false,
                    kv: Some(KeyValue {
                        key,
                        value: Bytes::from("v"),
                        create_revision: revision,
                        mod_revision: revision,
                        version: 1,
                        lease,
                    }),
                    prev_kv: None,
                }],
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn grant_assigns_ids_and_tracks_state() {
        let (_, backend) = manager().await;
        let auto = grant(&backend, 60, 0).await;
        assert!(auto > 0);
        assert_eq!(grant(&backend, 60, 42).await, 42);
        assert!(exists(&backend, auto).await);
        assert!(exists(&backend, 42).await);
        assert!(!exists(&backend, 43).await);
        assert_eq!(keepalive(&backend, auto).await, 60);
        assert_eq!(keepalive(&backend, 999).await, 0);
        let ids = backend.lease_ids().await.unwrap();
        assert!(ids.contains(&auto) && ids.contains(&42));
        let state = backend.lease_lookup(auto).await.unwrap().unwrap();
        assert_eq!(state.granted_ttl, 60);
        assert!(state.keys.is_empty());
        assert!(backend.lease_lookup(999).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn attach_detach_revoke_lifecycle() {
        let (mgr, backend) = manager().await;
        let id = grant(&backend, 60, 0).await;
        backend.lease_attach(id, b"k".to_vec()).await.unwrap();
        // Attaching twice does not duplicate.
        backend.lease_attach(id, b"k".to_vec()).await.unwrap();
        // Detaching an unknown lease or key is a no-op.
        backend.lease_detach(999, b"k").await.unwrap();
        backend.lease_detach(id, b"missing").await.unwrap();
        put_key(&backend, b"k", id).await;
        mgr.revoke(id).await.unwrap();
        let (_, rows, _) = backend
            .list(b"k", None, ListOptions::default())
            .await
            .unwrap();
        assert!(rows.is_empty());
        assert!(!exists(&backend, id).await);
        // Revoking an empty or unknown lease is a no-op.
        mgr.revoke(grant(&backend, 60, 0).await).await.unwrap();
        mgr.revoke(999).await.unwrap();
    }

    #[tokio::test]
    async fn service_grant_revoke_timetolive_leases() {
        let (mgr, backend) = manager().await;
        let svc = LeaseService::new(backend.clone(), mgr.clone());
        let err = svc
            .lease_grant(tonic::Request::new(LeaseGrantRequest { ttl: 0, id: 0 }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        let g = svc
            .lease_grant(tonic::Request::new(LeaseGrantRequest { ttl: 60, id: 7 }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!((g.id, g.ttl), (7, 60));
        put_key(&backend, b"sk", 7).await;
        backend.lease_attach(7, b"sk".to_vec()).await.unwrap();
        let ttl = svc
            .lease_time_to_live(tonic::Request::new(LeaseTimeToLiveRequest {
                id: 7,
                keys: true,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(ttl.keys, vec![b"sk".to_vec()]);
        let err = svc
            .lease_time_to_live(tonic::Request::new(LeaseTimeToLiveRequest {
                id: 999,
                keys: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        let list = svc
            .lease_leases(tonic::Request::new(LeaseLeasesRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert!(list.leases.iter().any(|l| l.id == 7));
        svc.lease_revoke(tonic::Request::new(LeaseRevokeRequest { id: 7 }))
            .await
            .unwrap();
        let (_, rows, _) = backend
            .list(b"sk", None, ListOptions::default())
            .await
            .unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn expiry_deletes_keys_and_ignores_missing() {
        let (mgr, backend) = manager().await;
        mgr.spawn_expiry_loop();
        // Expires on the first tick: deadline is already past.
        let quick = grant(&backend, 0, 0).await;
        put_key(&backend, b"q", quick).await;
        backend.lease_attach(quick, b"q".to_vec()).await.unwrap();
        // Vanished before expiry: nothing to delete.
        let gone = grant(&backend, 0, 0).await;
        backend
            .lease_attach(gone, b"absent".to_vec())
            .await
            .unwrap();
        // Idle lease with no keys at all.
        grant(&backend, 0, 0).await;
        tokio::time::sleep(Duration::from_millis(1200)).await;
        let (_, rows, _) = backend
            .list(b"q", None, ListOptions::default())
            .await
            .unwrap();
        assert!(rows.is_empty());
        assert!(!exists(&backend, quick).await);
        assert!(!exists(&backend, gone).await);
    }
}

#[cfg(test)]
mod expire_tests {
    use super::*;
    use crate::{MemoryBackend, backend::Event};

    #[tokio::test]
    async fn expire_keys_handles_empty_and_missing() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        // Nothing expired: no work, no commit.
        LeaseManager::expire_keys(&backend, Vec::new()).await;
        // Listed but gone: observed empty, nothing to delete.
        LeaseManager::expire_keys(&backend, vec![(7, b"absent".to_vec())]).await;
        assert_eq!(backend.current_revision().await, 1);
        // A live key is deleted at a fresh revision.
        let key = Bytes::from("gone");
        backend
            .commit(
                ReadSet::default(),
                vec![Event {
                    revision: 2,
                    key: key.clone(),
                    deleted: false,
                    kv: Some(KeyValue {
                        key: key.clone(),
                        value: Bytes::from("v"),
                        create_revision: 2,
                        mod_revision: 2,
                        version: 1,
                        lease: 7,
                    }),
                    prev_kv: None,
                }],
            )
            .await
            .unwrap();
        LeaseManager::expire_keys(&backend, vec![(7, b"gone".to_vec())]).await;
        let (_, rows, _) = backend
            .list(b"gone", None, ListOptions::default())
            .await
            .unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn mixed_expiry_keeps_live_leases() {
        use super::tests::{exists, grant};
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let writes: CommitLock = Arc::new(tokio::sync::Mutex::new(()));
        let mgr = LeaseManager::new(backend.clone(), writes);
        mgr.spawn_expiry_loop();
        // Expires immediately; the survivor keeps its lease entry.
        let quick = grant(&backend, 0, 0).await;
        let survivor = grant(&backend, 600, 0).await;
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(!exists(&backend, quick).await);
        assert!(exists(&backend, survivor).await);
    }
}
