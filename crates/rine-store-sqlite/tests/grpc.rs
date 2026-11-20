//! gRPC over the SQLite-backed store: proves the full stack serves the
//! etcd API against a real (in-memory) database.

use std::sync::Arc;

use rine_etcd::Backend as _;
use rine_etcd::etcdserverpb::{
    Compare, PutRequest, RangeRequest, RequestOp, TxnRequest, WatchCreateRequest, WatchRequest,
    compare::{CompareResult, CompareTarget, TargetUnion},
    kv_client::KvClient,
    request_op::Request,
    watch_client::WatchClient,
    watch_request,
};
use rine_etcd::server::serve_ephemeral;
use rine_store_sqlite::{SqliteBackend, SqliteDb};

async fn spawn() -> String {
    let db = SqliteDb::open_in_memory().unwrap();
    let backend = Arc::new(SqliteBackend::new(Arc::new(db)));
    backend.initialize().await.unwrap();
    serve_ephemeral(backend).await
}

#[tokio::test]
async fn sqlite_backend_serves_kv_and_txn() {
    let ep = spawn().await;
    let mut kv = KvClient::connect(ep).await.unwrap();

    // create via txn (MOD == 0)
    let r = kv
        .txn(TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal as i32,
                target: CompareTarget::Mod as i32,
                key: b"ok".to_vec(),
                target_union: Some(TargetUnion::ModRevision(0)),
                ..Default::default()
            }],
            success: vec![RequestOp {
                request: Some(Request::RequestPut(PutRequest {
                    key: b"ok".to_vec(),
                    value: b"v1".to_vec(),
                    ..Default::default()
                })),
            }],
            failure: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.succeeded);

    let got = kv
        .range(RangeRequest {
            key: b"ok".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.kvs.len(), 1);
    assert_eq!(got.kvs[0].value, b"v1".to_vec());
    assert_eq!(got.kvs[0].version, 1);

    // direct put bumps version, preserves create_revision
    kv.put(PutRequest {
        key: b"ok".to_vec(),
        value: b"v2".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let got = kv
        .range(RangeRequest {
            key: b"ok".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.kvs[0].value, b"v2".to_vec());
    assert_eq!(got.kvs[0].version, 2);
    assert_eq!(got.kvs[0].mod_revision, got.kvs[0].create_revision + 1);
    assert_eq!(got.kvs[0].mod_revision, got.header.unwrap().revision);
}

/// Two servers sharing one SQLite file: a commit through instance A must
/// reach a watcher on instance B via the catch-up poll, since A's commit
/// never enters B's broadcast channel.
#[tokio::test]
async fn sqlite_watch_sees_cross_instance_commits() {
    use std::time::Duration;

    let path = std::env::temp_dir().join(format!(
        "rine-watch-xinstance-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_file(&path);
    let backend = |path: &std::path::Path| {
        let db = SqliteDb::open(path).unwrap();
        Arc::new(SqliteBackend::new(Arc::new(db)))
    };
    let backend_a = backend(&path);
    let backend_b = backend(&path);
    // Both converge on the same shared schema.
    backend_a.initialize().await.unwrap();
    backend_b.initialize().await.unwrap();
    let ep_a = serve_ephemeral(backend_a).await;
    let ep_b = serve_ephemeral(backend_b).await;

    // Watch on B first: the put that follows only exists on A.
    let mut watch = WatchClient::connect(ep_b).await.unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let mut stream = watch
        .watch(tonic::Request::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .await
        .unwrap()
        .into_inner();
    tx.send(WatchRequest {
        request_union: Some(watch_request::RequestUnion::CreateRequest(
            WatchCreateRequest {
                key: b"xk".to_vec(),
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();
    let created = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(created.created);

    let mut kv_a = KvClient::connect(ep_a).await.unwrap();
    kv_a.put(PutRequest {
        key: b"xk".to_vec(),
        value: b"hello".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();

    // No shared broadcast: delivery waits for B's catch-up poll (200ms).
    let ev = tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ev.events.len(), 1);
    assert_eq!(ev.events[0].kv.as_ref().unwrap().value, b"hello".to_vec());

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
