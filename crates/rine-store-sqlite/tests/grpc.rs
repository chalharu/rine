//! gRPC over the SQLite-backed store: proves the full stack serves the
//! etcd API against a real (in-memory) database.

use std::sync::Arc;

use rine_etcd::etcdserverpb::{
    Compare, PutRequest, RangeRequest, RequestOp, TxnRequest,
    compare::{CompareResult, CompareTarget, TargetUnion},
    kv_client::KvClient,
    request_op,
};
use rine_etcd::server::serve_ephemeral;
use rine_store_sqlite::{SqliteBackend, SqliteDb};

async fn spawn() -> String {
    let db = SqliteDb::open_in_memory().unwrap();
    let backend = Arc::new(SqliteBackend::new(Arc::new(db)));
    backend.ensure_schema().await.unwrap();
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
                request: Some(request_op::Request::RequestPut(PutRequest {
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
