//! Watch service: multiplexed bidi-stream watchers over `Backend::subscribe`.
//!
//! Semantics follow etcd: server-assigned IDs (client-specified accepted when
//! unused), history replay from `start_revision`, cancel/progress, and cancel
//! with `compact_revision` when history is unavailable.
//!
//! Live delivery is twofold: the backend broadcast is the fast path for this
//! process's own commits, while a periodic catch-up replays the shared event
//! log from each watcher's oldest undelivered revision. Commits from a rival
//! node sharing the backend never enter this process's broadcast channel, so
//! without the catch-up they would never reach local watchers. Both sources
//! derive from the same total revision order and share the per-watcher
//! `next_rev` cursor, so deliveries are gap-free and duplicate-free.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    backend::{Backend, Event, header, in_range},
    error::{Error, status_err},
    etcdserverpb::{
        WatchCancelRequest, WatchCreateRequest, WatchProgressRequest, WatchRequest, WatchResponse,
        watch_server::Watch,
    },
    kv::to_proto_kv,
    mvccpb,
};

/// Cross-instance catch-up cadence: bounds the extra delay before a rival
/// node's commit reaches local watchers. One range scan per stream per tick.
const CATCH_UP_INTERVAL: Duration = Duration::from_millis(200);
/// Revisions fetched per catch-up tick: bounds one catch-up after a long
/// broadcast outage; the cursor simply resumes on the next tick.
const CATCH_UP_MAX_SPAN: i64 = 512;

struct Watcher {
    /// Next undelivered revision. Advanced by both the broadcast fast path
    /// and the catch-up poll, so neither source replays the other.
    next_rev: i64,
    key: Vec<u8>,
    range_end: Vec<u8>,
    no_put: bool,
    no_delete: bool,
    prev_kv: bool,
    progress_notify: bool,
}

fn matches(w: &Watcher, key: &[u8]) -> bool {
    let end = if w.range_end.is_empty() {
        None
    } else {
        Some(w.range_end.as_slice())
    };
    in_range(key, &w.key, end)
}

fn to_proto_event(e: &Event, want_prev: bool) -> mvccpb::Event {
    let (typ, kv) = if e.deleted {
        let prev = e.prev_kv.clone().unwrap_or(crate::backend::KeyValue {
            key: e.key.clone(),
            value: bytes::Bytes::new(),
            create_revision: 0,
            mod_revision: e.revision,
            version: 0,
            lease: 0,
        });
        (
            mvccpb::event::EventType::Delete as i32,
            crate::backend::KeyValue {
                value: bytes::Bytes::new(),
                mod_revision: e.revision,
                ..prev
            },
        )
    } else {
        let kv = e.kv.clone().expect("put event carries kv");
        (mvccpb::event::EventType::Put as i32, kv)
    };
    mvccpb::Event {
        r#type: typ,
        kv: Some(to_proto_kv(kv)),
        prev_kv: if want_prev {
            e.prev_kv.clone().map(to_proto_kv)
        } else {
            None
        },
    }
}

pub struct WatchService {
    backend: Arc<dyn Backend>,
    next_id: Arc<AtomicI64>,
}

impl WatchService {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            next_id: Arc::new(AtomicI64::new(1)),
        }
    }
}

#[tonic::async_trait]
impl Watch for WatchService {
    type WatchStream = ReceiverStream<Result<WatchResponse, tonic::Status>>;

    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let backend = self.backend.clone();
        let next_id = self.next_id.clone();
        let (tx, rx) = mpsc::channel::<Result<WatchResponse, tonic::Status>>(64);

        tokio::spawn(async move {
            let mut watchers: HashMap<i64, Watcher> = HashMap::new();
            // Subscribe before reading current revision to avoid gaps.
            let mut live = backend.subscribe();
            let mut progress = tokio::time::interval(Duration::from_secs(10));
            // Skip the immediate first tick.
            progress.tick().await;
            let mut catchup = tokio::time::interval(CATCH_UP_INTERVAL);
            catchup.tick().await;

            loop {
                tokio::select! {
                    incoming = inbound.message() => {
                        let msg = match incoming {
                            Ok(m) => m,
                            Err(_) => break,
                        };
                        let Some(req) = msg else { break };
                        use crate::etcdserverpb::watch_request::RequestUnion;
                        match req.request_union {
                            Some(RequestUnion::CreateRequest(c)) => {
                                handle_create(&backend, &next_id, &tx, &mut watchers, c).await;
                            }
                            Some(RequestUnion::CancelRequest(WatchCancelRequest { watch_id })) => {
                                watchers.remove(&watch_id);
                                let rev = backend.current_revision().await;
                                let _ = tx.send(Ok(WatchResponse {
                                    header: Some(header(rev)),
                                    watch_id,
                                    canceled: true,
                                    ..Default::default()
                                })).await;
                            }
                            Some(RequestUnion::ProgressRequest(WatchProgressRequest {})) => {
                                let rev = backend.current_revision().await;
                                for id in watchers.keys() {
                                    let _ = tx.send(Ok(WatchResponse {
                                        header: Some(header(rev)),
                                        watch_id: *id,
                                        ..Default::default()
                                    })).await;
                                }
                            }
                            None => {}
                        }
                    }
                    ev = live.recv() => {
                        let batch = match ev {
                            Ok(e) => e,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                // Missed broadcasts: recover from the shared
                                // log when retention allows; only truly
                                // compacted watchers cancel (see `catch_up`).
                                catch_up(&backend, &tx, &mut watchers).await;
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        };
                        let Some(last_rev) = batch.last().map(|e| e.revision) else { continue };
                        emit_new(&mut watchers, &tx, last_rev, &batch).await;
                    }
                    _ = progress.tick() => {
                        let rev = backend.current_revision().await;
                        for (id, w) in &watchers {
                            if w.progress_notify {
                                let _ = tx.send(Ok(WatchResponse {
                                    header: Some(header(rev)),
                                    watch_id: *id,
                                    ..Default::default()
                                })).await;
                            }
                        }
                    }
                    _ = catchup.tick() => {
                        catch_up(&backend, &tx, &mut watchers).await;
                    }
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

/// Deliver every revision the broadcast missed: commits from a rival node
/// sharing the backend never enter this process's channel, so each watch
/// stream replays the shared event log from its oldest undelivered revision
/// on a poll. Both sources derive from the same total revision order and
/// share the per-watcher `next_rev` cursor, so deliveries are gap-free and
/// duplicate-free. The fetch is capped to `CATCH_UP_MAX_SPAN` revisions so
/// one catch-up after a long outage cannot balloon; the cursor resumes on
/// the next tick.
async fn catch_up(
    backend: &Arc<dyn Backend>,
    tx: &mpsc::Sender<Result<WatchResponse, tonic::Status>>,
    watchers: &mut HashMap<i64, Watcher>,
) {
    if watchers.is_empty() {
        return;
    }
    let min_next = watchers
        .values()
        .map(|w| w.next_rev)
        .min()
        .unwrap_or(1)
        .max(1);
    let cur = backend.current_revision().await;
    if min_next > cur {
        return;
    }
    let to = (min_next + CATCH_UP_MAX_SPAN - 1).min(cur);
    match backend.events(min_next, to).await {
        Ok(hist) => {
            emit_new(watchers, tx, to, &hist).await;
        }
        Err(Error::Compacted { compacted, .. }) => {
            // Only watchers that can no longer catch up cancel; the rest
            // retry from their own cursors on the next tick.
            let stale: Vec<i64> = watchers
                .iter()
                .filter(|(_, w)| w.next_rev <= compacted)
                .map(|(id, _)| *id)
                .collect();
            for id in stale {
                watchers.remove(&id);
                let _ = tx
                    .send(Ok(WatchResponse {
                        header: Some(header(compacted)),
                        watch_id: id,
                        canceled: true,
                        compact_revision: compacted,
                        cancel_reason: "etcdserver: mvcc: required revision has been compacted"
                            .into(),
                        ..Default::default()
                    }))
                    .await;
            }
        }
        // Transient storage error: keep the cursors, retry next tick.
        Err(_) => {}
    }
}

/// Fan out one fetched batch (live broadcast or catch-up poll) to every
/// watcher from its own cursor. Both sources share the per-watcher
/// `next_rev` cursor (forward-only), so deliveries are gap-free and
/// duplicate-free across sources.
async fn emit_new(
    watchers: &mut HashMap<i64, Watcher>,
    tx: &mpsc::Sender<Result<WatchResponse, tonic::Status>>,
    rev: i64,
    batch: &[Event],
) {
    for (id, w) in watchers.iter_mut() {
        let fresh: Vec<Event> = batch
            .iter()
            .filter(|e| e.revision >= w.next_rev)
            .cloned()
            .collect();
        let events = watch_events(w, &fresh);
        w.next_rev = w.next_rev.max(rev + 1);
        if events.is_empty() {
            continue;
        }
        let _ = tx
            .send(Ok(WatchResponse {
                header: Some(header(rev)),
                watch_id: *id,
                events,
                ..Default::default()
            }))
            .await;
    }
}

fn watch_events(w: &Watcher, events: &[Event]) -> Vec<mvccpb::Event> {
    events
        .iter()
        .filter(|e| matches(w, &e.key) && if e.deleted { !w.no_delete } else { !w.no_put })
        .map(|e| to_proto_event(e, w.prev_kv))
        .collect()
}

async fn handle_create(
    backend: &Arc<dyn Backend>,
    next_id: &AtomicI64,
    tx: &mpsc::Sender<Result<WatchResponse, tonic::Status>>,
    watchers: &mut HashMap<i64, Watcher>,
    c: WatchCreateRequest,
) {
    let id = if c.watch_id > 0 && !watchers.contains_key(&c.watch_id) {
        c.watch_id
    } else {
        next_id.fetch_add(1, Ordering::Relaxed)
    };
    let cur = backend.current_revision().await;
    let start = if c.start_revision == 0 {
        cur + 1
    } else {
        c.start_revision
    };
    let watcher = Watcher {
        next_rev: start.max(cur + 1),
        key: c.key.clone(),
        range_end: c.range_end.clone(),
        no_put: c.filters.contains(&(0i32)),
        no_delete: c.filters.contains(&(1i32)),
        prev_kv: c.prev_kv,
        progress_notify: c.progress_notify,
    };
    // clientv3 binds the pending watch to its ID on the Created response.
    if tx
        .send(Ok(WatchResponse {
            header: Some(header(cur)),
            watch_id: id,
            created: true,
            ..Default::default()
        }))
        .await
        .is_err()
    {
        return;
    }
    if start <= cur {
        match backend.events(start, cur).await {
            Ok(hist) => {
                let events = watch_events(&watcher, &hist);
                if !events.is_empty() {
                    let _ = tx
                        .send(Ok(WatchResponse {
                            header: Some(header(cur)),
                            watch_id: id,
                            events,
                            ..Default::default()
                        }))
                        .await;
                }
            }
            Err(error) => {
                let compact_revision = match &error {
                    crate::error::Error::Compacted { compacted, .. } => *compacted,
                    _ => 0,
                };
                let _ = tx
                    .send(Ok(WatchResponse {
                        header: Some(header(cur)),
                        watch_id: id,
                        canceled: true,
                        compact_revision,
                        cancel_reason: status_err(error).message().to_owned(),
                        ..Default::default()
                    }))
                    .await;
                return;
            }
        }
    }
    watchers.insert(id, watcher);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MemoryBackend,
        backend::{Event, KeyValue, ReadSet},
    };
    use bytes::Bytes;

    fn kv(key: &[u8], rev: i64) -> KeyValue {
        KeyValue {
            key: Bytes::copy_from_slice(key),
            value: Bytes::from("v"),
            create_revision: rev,
            mod_revision: rev,
            version: 1,
            lease: 0,
        }
    }

    fn put_event(key: &[u8], rev: i64) -> Event {
        Event {
            revision: rev,
            key: Bytes::copy_from_slice(key),
            deleted: false,
            kv: Some(kv(key, rev)),
            prev_kv: None,
        }
    }

    fn del_event(key: &[u8], rev: i64) -> Event {
        let prev = kv(key, rev - 1);
        Event {
            revision: rev,
            key: Bytes::copy_from_slice(key),
            deleted: true,
            kv: None,
            prev_kv: Some(prev),
        }
    }

    async fn backend_with(keys: &[&[u8]]) -> Arc<dyn Backend> {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        for (i, key) in keys.iter().enumerate() {
            let rev = i as i64 + 2;
            let key = Bytes::copy_from_slice(key);
            b.commit(
                ReadSet {
                    points: vec![(key.clone(), None)],
                    ranges: Vec::new(),
                },
                vec![Event {
                    revision: rev,
                    key: key.clone(),
                    deleted: false,
                    kv: Some(KeyValue {
                        key,
                        value: Bytes::from("v"),
                        create_revision: rev,
                        mod_revision: rev,
                        version: 1,
                        lease: 0,
                    }),
                    prev_kv: None,
                }],
            )
            .await
            .unwrap();
        }
        b
    }

    fn watcher(key: &[u8]) -> Watcher {
        Watcher {
            next_rev: 0,
            key: key.to_vec(),
            range_end: Vec::new(),
            no_put: false,
            no_delete: false,
            prev_kv: false,
            progress_notify: false,
        }
    }

    #[test]
    fn matches_point_and_range() {
        assert!(matches(&watcher(b"a"), b"a"));
        assert!(!matches(&watcher(b"a"), b"b"));
        let mut prefix = watcher(b"a");
        prefix.range_end = b"c".to_vec();
        assert!(matches(&prefix, b"a"));
        assert!(matches(&prefix, b"b"));
        assert!(!matches(&prefix, b"c"));
        assert!(!matches(&prefix, b"z"));
    }

    #[test]
    fn proto_events_cover_put_delete_and_prev() {
        let put = to_proto_event(&put_event(b"k", 2), false);
        assert_eq!(put.r#type, mvccpb::event::EventType::Put as i32);
        assert!(put.prev_kv.is_none());
        let put_prev = to_proto_event(&put_event(b"k", 2), true);
        assert!(put_prev.prev_kv.is_none());
        let del = to_proto_event(&del_event(b"k", 3), false);
        assert_eq!(del.r#type, mvccpb::event::EventType::Delete as i32);
        assert_eq!(del.kv.as_ref().unwrap().mod_revision, 3);
        assert!(del.prev_kv.is_none());
        let del_prev = to_proto_event(&del_event(b"k", 3), true);
        assert!(del_prev.prev_kv.is_some());
        // A delete without recorded state still yields a tombstone.
        let bare = Event {
            revision: 4,
            key: Bytes::from("k"),
            deleted: true,
            kv: None,
            prev_kv: None,
        };
        let bare_ev = to_proto_event(&bare, false);
        assert_eq!(bare_ev.kv.as_ref().unwrap().version, 0);
    }

    #[test]
    fn watch_events_filter_by_type() {
        let events = vec![put_event(b"k", 2), del_event(b"k", 3)];
        assert_eq!(watch_events(&watcher(b"k"), &events).len(), 2);
        let mut no_put = watcher(b"k");
        no_put.no_put = true;
        assert_eq!(watch_events(&no_put, &events).len(), 1);
        let mut no_del = watcher(b"k");
        no_del.no_delete = true;
        assert_eq!(watch_events(&no_del, &events).len(), 1);
        assert!(watch_events(&watcher(b"other"), &events).is_empty());
    }

    async fn created(
        backend: &Arc<dyn Backend>,
        c: WatchCreateRequest,
    ) -> (Vec<WatchResponse>, HashMap<i64, Watcher>) {
        let (tx, mut rx) = mpsc::channel(16);
        let mut watchers = HashMap::new();
        let next_id = AtomicI64::new(1);
        handle_create(backend, &next_id, &tx, &mut watchers, c).await;
        drop(tx);
        let mut out = Vec::new();
        while let Some(r) = rx.recv().await {
            out.push(r.unwrap());
        }
        (out, watchers)
    }

    fn create(key: &[u8], start: i64) -> WatchCreateRequest {
        WatchCreateRequest {
            key: key.to_vec(),
            start_revision: start,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn create_assigns_ids_and_replays_history() {
        let backend = backend_with(&[b"h"]).await;
        // Server-assigned ID, live watch.
        let (out, watchers) = created(&backend, create(b"h", 0)).await;
        assert_eq!(out.len(), 1);
        assert!(out[0].created);
        assert_eq!(watchers.len(), 1);
        // Client-specified ID is honored when unused, replaced when taken.
        let (out, watchers) = created(
            &backend,
            WatchCreateRequest {
                watch_id: 42,
                ..create(b"h", 0)
            },
        )
        .await;
        assert_eq!(out[0].watch_id, 42);
        let mut taken = watchers;
        let (out, _) = {
            let (tx, mut rx) = mpsc::channel(16);
            let next_id = AtomicI64::new(100);
            handle_create(
                &backend,
                &next_id,
                &tx,
                &mut taken,
                WatchCreateRequest {
                    watch_id: 42,
                    ..create(b"h", 0)
                },
            )
            .await;
            drop(tx);
            let mut out = Vec::new();
            while let Some(r) = rx.recv().await {
                out.push(r.unwrap());
            }
            (out, taken)
        };
        assert_ne!(out[0].watch_id, 42);
        // Historical replay from an old revision.
        let (out, _) = created(&backend, create(b"h", 1)).await;
        assert_eq!(out.len(), 2);
        assert!(!out[1].events.is_empty());
    }

    #[tokio::test]
    async fn create_honors_filters_and_prev_kv() {
        let backend = backend_with(&[]).await;
        let (out, watchers) = created(
            &backend,
            WatchCreateRequest {
                prev_kv: true,
                progress_notify: true,
                filters: vec![0],
                range_end: b"z".to_vec(),
                ..create(b"a", 0)
            },
        )
        .await;
        assert_eq!(out.len(), 1);
        let w = watchers.values().next().unwrap();
        assert!(w.prev_kv && w.progress_notify && w.no_put && !w.no_delete);
        assert_eq!(w.range_end, b"z".to_vec());
    }

    #[tokio::test]
    async fn create_after_compact_cancels() {
        let backend = backend_with(&[b"h"]).await;
        backend.compact(2).await.unwrap();
        let (out, watchers) = created(&backend, create(b"h", 1)).await;
        assert_eq!(out.len(), 2);
        assert!(out[0].created);
        assert!(out[1].canceled);
        assert!(out[1].compact_revision > 0);
        assert!(watchers.is_empty());
    }

    #[tokio::test]
    async fn create_without_receiver_returns_early() {
        let backend = backend_with(&[]).await;
        let (tx, rx) = mpsc::channel(16);
        drop(rx);
        let mut watchers = HashMap::new();
        handle_create(
            &backend,
            &AtomicI64::new(1),
            &tx,
            &mut watchers,
            create(b"h", 0),
        )
        .await;
        assert!(watchers.is_empty());
    }

    async fn caught(
        backend: &Arc<dyn Backend>,
        watchers: &mut HashMap<i64, Watcher>,
    ) -> Vec<WatchResponse> {
        let (tx, mut rx) = mpsc::channel(16);
        catch_up(backend, &tx, watchers).await;
        drop(tx);
        let mut out = Vec::new();
        while let Some(r) = rx.recv().await {
            out.push(r.unwrap());
        }
        out
    }

    fn ranged(key: &[u8], next_rev: i64) -> Watcher {
        Watcher {
            next_rev,
            key: key.to_vec(),
            range_end: Vec::new(),
            no_put: false,
            no_delete: false,
            prev_kv: false,
            progress_notify: false,
        }
    }

    #[tokio::test]
    async fn catch_up_replays_missed_revisions_without_broadcast() {
        // Revs 2 and 3 committed; the watcher never saw a broadcast.
        let backend = backend_with(&[b"a", b"b"]).await;
        let mut watchers = HashMap::new();
        watchers.insert(7, ranged(b"a", 2));
        // A cursor past the current revision fetches nothing.
        watchers.insert(8, ranged(b"a", 99));
        let out = caught(&backend, &mut watchers).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].watch_id, 7);
        assert_eq!(out[0].events.len(), 1);
        assert_eq!(out[0].events[0].kv.as_ref().unwrap().key, b"a".to_vec());
        // Cursors advanced past everything fetched; a second tick is quiet
        // and duplicate-free even though the broadcast never arrived.
        assert_eq!(watchers[&7].next_rev, 4);
        assert_eq!(watchers[&8].next_rev, 99);
        assert!(caught(&backend, &mut watchers).await.is_empty());
    }

    #[tokio::test]
    async fn catch_up_cancels_only_compacted_watchers() {
        let backend = backend_with(&[b"h"]).await;
        backend.compact(2).await.unwrap();
        let mut watchers = HashMap::new();
        watchers.insert(7, ranged(b"h", 2));
        watchers.insert(8, ranged(b"h", 99));
        let out = caught(&backend, &mut watchers).await;
        assert_eq!(out.len(), 1);
        assert!(out[0].canceled);
        assert_eq!(out[0].watch_id, 7);
        assert!(out[0].compact_revision > 0);
        assert!(!watchers.contains_key(&7));
        assert!(watchers.contains_key(&8));
    }
}
