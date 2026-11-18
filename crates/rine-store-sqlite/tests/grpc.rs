//! gRPC over the SQLite-backed store: proves the full stack serves the
//! etcd API against a real (in-memory) database.

use rine_etcd::etcdserverpb::compare::{CompareResult, CompareTarget, TargetUnion};
use rine_etcd::etcdserverpb::kv_client::KvClient;
use rine_etcd::etcdserverpb::{
    Compare, PutRequest, RangeRequest, RequestOp, TxnRequest, request_op,
};
use rine_etcd::server::EtcdServer;
use rine_store_sqlite::{SqliteBackend, SqliteDb};
use std::sync::Arc;
use std::time::Duration;

async fn spawn() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let endpoint = format!("http://{addr}");
    let db = SqliteDb::open_in_memory().unwrap();
    let backend = Arc::new(SqliteBackend::new(Arc::new(db)));
    backend.ensure_schema().await.unwrap();
    tokio::spawn(async move {
        EtcdServer::new(backend).serve(addr).await.unwrap();
    });
    for _ in 0..50 {
        if let Ok(mut c) = KvClient::connect(endpoint.clone()).await {
            if c.range(RangeRequest {
                key: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .is_ok()
            {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    endpoint
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
    assert_eq!(got.kvs[0].create_revision, 1);
    assert_eq!(got.kvs[0].mod_revision, 2);
}
