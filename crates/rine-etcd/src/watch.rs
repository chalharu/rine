//! Watch service: multiplexed bidi-stream watchers over `Backend::subscribe`.
//!
//! Semantics follow etcd: server-assigned IDs (client-specified accepted when
//! unused), history replay from `start_revision`, cancel/progress, and cancel
//! with `compact_revision` when history is unavailable.

use crate::backend::{Backend, Event};
use crate::etcdserverpb::watch_server::Watch;
use crate::etcdserverpb::{
    ResponseHeader, WatchCancelRequest, WatchCreateRequest, WatchProgressRequest, WatchRequest,
    WatchResponse,
};
use crate::kv::to_proto_kv;
use crate::mvccpb;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

struct Watcher {
    key: Vec<u8>,
    range_end: Vec<u8>,
    no_put: bool,
    no_delete: bool,
    prev_kv: bool,
    progress_notify: bool,
}

fn matches(w: &Watcher, key: &[u8]) -> bool {
    if w.range_end.is_empty() {
        key == w.key.as_slice()
    } else if w.range_end == [0] {
        key >= w.key.as_slice()
    } else {
        key >= w.key.as_slice() && key < w.range_end.as_slice()
    }
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

pub struct WatchService<B: Backend> {
    backend: Arc<B>,
    next_id: Arc<AtomicI64>,
    cluster_id: u64,
    member_id: u64,
}

impl<B: Backend> WatchService<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self {
            backend,
            next_id: Arc::new(AtomicI64::new(1)),
            cluster_id: 0x7269_6e65_0000_0001,
            member_id: 0x7269_6e65_0000_0002,
        }
    }
}

#[tonic::async_trait]
impl<B: Backend> Watch for WatchService<B> {
    type WatchStream = ReceiverStream<Result<WatchResponse, tonic::Status>>;

    async fn watch(
        &self,
        request: tonic::Request<tonic::Streaming<WatchRequest>>,
    ) -> Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let mut inbound = request.into_inner();
        let backend = self.backend.clone();
        let next_id = self.next_id.clone();
        let cluster_id = self.cluster_id;
        let member_id = self.member_id;
        let (tx, rx) = mpsc::channel::<Result<WatchResponse, tonic::Status>>(64);
        let header = move |revision: i64| ResponseHeader {
            cluster_id,
            member_id,
            revision,
            raft_term: 0,
        };

        tokio::spawn(async move {
            let mut watchers: HashMap<i64, Watcher> = HashMap::new();
            // Subscribe before reading current revision to avoid gaps.
            let mut live = backend.subscribe();
            let mut progress = tokio::time::interval(Duration::from_secs(10));
            // Skip the immediate first tick.
            progress.tick().await;

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
                                handle_create(&backend, &next_id, &header, &tx, &mut watchers, &mut live, c).await;
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
                        let ev = match ev {
                            Ok(e) => e,
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                // Missed events: cancel watchers that can no longer catch up.
                                let compacted = backend.compacted_revision().await;
                                for (id, _) in watchers.drain() {
                                    let _ = tx.send(Ok(WatchResponse {
                                        header: Some(header(compacted)),
                                        watch_id: id,
                                        canceled: true,
                                        compact_revision: compacted,
                                        cancel_reason: "etcdserver: mvcc: required revision has been compacted".into(),
                                        ..Default::default()
                                    })).await;
                                }
                                continue;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        };
                        for (id, w) in &watchers {
                            if !matches(w, &ev.key) { continue; }
                            if ev.deleted && w.no_delete { continue; }
                            if !ev.deleted && w.no_put { continue; }
                            let _ = tx.send(Ok(WatchResponse {
                                header: Some(header(ev.revision)),
                                watch_id: *id,
                                events: vec![to_proto_event(&ev, w.prev_kv)],
                                ..Default::default()
                            })).await;
                        }
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
                }
            }
        });
        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_create<B: Backend>(
    backend: &Arc<B>,
    next_id: &AtomicI64,
    header: &impl Fn(i64) -> ResponseHeader,
    tx: &mpsc::Sender<Result<WatchResponse, tonic::Status>>,
    watchers: &mut HashMap<i64, Watcher>,
    _live: &mut tokio::sync::broadcast::Receiver<Event>,
    c: WatchCreateRequest,
) {
    let id = if c.watch_id > 0 && !watchers.contains_key(&c.watch_id) {
        c.watch_id
    } else {
        next_id.fetch_add(1, Ordering::Relaxed)
    };
    let watcher = Watcher {
        key: c.key.clone(),
        range_end: c.range_end.clone(),
        no_put: c.filters.contains(&(0i32)),
        no_delete: c.filters.contains(&(1i32)),
        prev_kv: c.prev_kv,
        progress_notify: c.progress_notify,
    };
    let cur = backend.current_revision().await;
    let start = if c.start_revision == 0 {
        cur + 1 // live only
    } else {
        c.start_revision
    };
    // Pre-check compaction before registering (avoids leaking dead watchers).
    if start <= cur {
        match backend.events(start, cur).await {
            Err(_) => {
                let compacted = backend.compacted_revision().await;
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
                return;
            }
            Ok(_) => {}
        }
    }
    watchers.insert(id, watcher);
    let _ = tx
        .send(Ok(WatchResponse {
            header: Some(header(cur)),
            watch_id: id,
            created: true,
            ..Default::default()
        }))
        .await;
    if start <= cur {
        // Replay (re-fetch; cheap and race-free enough behind the live feed
        // since live events with rev > cur are still queued in broadcast).
        if let Ok(hist) = backend.events(start, cur).await {
            let w = &watchers[&id];
            let evs: Vec<mvccpb::Event> = hist
                .iter()
                .filter(|e| matches(w, &e.key))
                .filter(|e| !(e.deleted && w.no_delete) && !(!e.deleted && w.no_put))
                .map(|e| to_proto_event(e, w.prev_kv))
                .collect();
            if !evs.is_empty() {
                let _ = tx
                    .send(Ok(WatchResponse {
                        header: Some(header(cur)),
                        watch_id: id,
                        events: evs,
                        ..Default::default()
                    }))
                    .await;
            }
        }
    }
}
