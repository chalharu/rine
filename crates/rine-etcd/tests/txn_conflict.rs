//! A commit conflict inside `Txn` surfaces as `succeeded=false` with the
//! failure-branch responses (etcd txn-conflict semantics), and the staged
//! success-branch writes are discarded.
//!
//! `FlakyBackend` fails the first commit with `Error::Conflict`, then behaves
//! like a normal `MemoryBackend`.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use rine_etcd::{
    backend::{Backend, Event, KeyValue, ListOptions, ReadSet, Revision},
    error::{Error, Result},
    etcdserverpb::{
        Compare, PutRequest, RangeRequest, RequestOp, TxnRequest,
        compare::{CompareResult, CompareTarget, TargetUnion},
        kv_client::KvClient,
        request_op::Request,
        response_op::Response,
    },
    server::{EtcdServer, ListenAddr},
};

struct FlakyBackend {
    inner: rine_etcd::MemoryBackend,
    fail_once: AtomicBool,
}

impl FlakyBackend {
    fn new() -> Self {
        Self {
            inner: rine_etcd::MemoryBackend::new(),
            fail_once: AtomicBool::new(true),
        }
    }
}

#[async_trait]
impl Backend for FlakyBackend {
    async fn current_revision(&self) -> Revision {
        self.inner.current_revision().await
    }

    async fn compacted_revision(&self) -> Revision {
        self.inner.compacted_revision().await
    }

    async fn initialize(&self) -> Result<()> {
        self.inner.initialize().await
    }

    async fn commit(&self, reads: ReadSet, events: Vec<Event>) -> Result<Revision> {
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(Error::Conflict);
        }
        self.inner.commit(reads, events).await
    }

    async fn list(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        self.inner.list(prefix, range_end, opts).await
    }

    async fn compact(&self, revision: Revision) -> Result<Revision> {
        self.inner.compact(revision).await
    }

    async fn db_size(&self) -> i64 {
        self.inner.db_size().await
    }

    async fn events(&self, from: Revision, to: Revision) -> Result<Vec<Event>> {
        self.inner.events(from, to).await
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Vec<Event>> {
        self.inner.subscribe()
    }
}

async fn spawn() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let endpoint = format!("http://{addr}");
    let backend = Arc::new(FlakyBackend::new());
    tokio::spawn(async move {
        EtcdServer::new(backend)
            .serve_many(vec![ListenAddr { addr, tls: None }])
            .await
            .unwrap();
    });
    for _ in 0..50 {
        if let Ok(mut c) = KvClient::connect(endpoint.clone()).await
            && c.range(RangeRequest {
                key: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    endpoint
}

#[tokio::test]
async fn txn_conflict_reports_not_succeeded() {
    let ep = spawn().await;
    let mut kv = KvClient::connect(ep).await.unwrap();

    // Compare passes (missing key has version 0), but the commit of the
    // staged put is forced to conflict: the client must see succeeded=false
    // with the failure-branch get response, exactly like etcd.
    let r = kv
        .txn(TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal as i32,
                target: CompareTarget::Version as i32,
                key: b"k".to_vec(),
                target_union: Some(TargetUnion::Version(0)),
                ..Default::default()
            }],
            success: vec![RequestOp {
                request: Some(Request::RequestPut(PutRequest {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    ..Default::default()
                })),
            }],
            failure: vec![RequestOp {
                request: Some(Request::RequestRange(RangeRequest {
                    key: b"k".to_vec(),
                    ..Default::default()
                })),
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!r.succeeded);
    assert_eq!(r.responses.len(), 1);
    match &r.responses[0].response {
        Some(Response::ResponseRange(got)) => assert!(got.kvs.is_empty()),
        other => panic!("expected failure-branch range response, got {other:?}"),
    }

    // The staged put was discarded: the key must not exist.
    let got = kv
        .range(RangeRequest {
            key: b"k".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(got.kvs.is_empty());

    // The backend recovered: a plain put works after the conflict.
    kv.put(PutRequest {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let got = kv
        .range(RangeRequest {
            key: b"k".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.kvs[0].value, b"v".to_vec());
}
