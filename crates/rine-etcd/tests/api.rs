//! End-to-end tests: real gRPC client against an in-process server.

use std::{sync::Arc, time::Duration};

use rine_etcd::{
    MemoryBackend,
    etcdserverpb::{
        CompactionRequest, Compare, DeleteRangeRequest, LeaseGrantRequest, MemberListRequest,
        PutRequest, RangeRequest, RequestOp, StatusRequest, TxnRequest, WatchCancelRequest,
        WatchCreateRequest, WatchRequest,
        cluster_client::ClusterClient,
        compare::{CompareResult, CompareTarget, TargetUnion},
        kv_client::KvClient,
        lease_client::LeaseClient,
        maintenance_client::MaintenanceClient,
        range_request::{SortOrder, SortTarget},
        request_op,
        watch_client::WatchClient,
        watch_request,
    },
    server::serve_ephemeral,
};

fn put_op(key: &[u8], value: &[u8]) -> RequestOp {
    RequestOp {
        request: Some(request_op::Request::RequestPut(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        })),
    }
}

#[tokio::test]
async fn kv_put_range_sort() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    for (k, v) in [
        (b"b".as_slice(), b"2".as_slice()),
        (b"a", b"1"),
        (b"c", b"3"),
    ] {
        kv.put(PutRequest {
            key: k.to_vec(),
            value: v.to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    let r = kv
        .range(RangeRequest {
            key: b"a".to_vec(),
            range_end: b"d".to_vec(),
            sort_order: SortOrder::Ascend as i32,
            sort_target: SortTarget::Key as i32,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let keys: Vec<_> = r.kvs.iter().map(|k| k.key.clone()).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    assert_eq!(r.count, 3);
}

#[tokio::test]
async fn txn_create_cas_delete() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();

    // create: MOD == 0
    let txn = TxnRequest {
        compare: vec![Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Mod as i32,
            key: b"cas".to_vec(),
            target_union: Some(TargetUnion::ModRevision(0)),
            ..Default::default()
        }],
        success: vec![put_op(b"cas", b"v1")],
        failure: vec![],
    };
    let r = kv.txn(txn).await.unwrap().into_inner();
    assert!(r.succeeded);
    assert!(r.header.unwrap().revision > 1);

    // cas: MOD == 2 (create bumped rev to 2)
    let txn = TxnRequest {
        compare: vec![Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Mod as i32,
            key: b"cas".to_vec(),
            target_union: Some(TargetUnion::ModRevision(2)),
            ..Default::default()
        }],
        success: vec![put_op(b"cas", b"v2")],
        failure: vec![],
    };
    let r = kv.txn(txn).await.unwrap().into_inner();
    assert!(r.succeeded);

    // stale cas must fail
    let txn = TxnRequest {
        compare: vec![Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Mod as i32,
            key: b"cas".to_vec(),
            target_union: Some(TargetUnion::ModRevision(2)),
            ..Default::default()
        }],
        success: vec![put_op(b"cas", b"v3")],
        failure: vec![],
    };
    let r = kv.txn(txn).await.unwrap().into_inner();
    assert!(!r.succeeded);

    // delete via txn
    let txn = TxnRequest {
        compare: vec![],
        success: vec![RequestOp {
            request: Some(request_op::Request::RequestDeleteRange(
                DeleteRangeRequest {
                    key: b"cas".to_vec(),
                    ..Default::default()
                },
            )),
        }],
        failure: vec![],
    };
    let r = kv.txn(txn).await.unwrap().into_inner();
    assert!(r.succeeded);
    let g = kv
        .range(RangeRequest {
            key: b"cas".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(g.kvs.is_empty());
}

#[tokio::test]
async fn compact_and_history() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    kv.put(PutRequest {
        key: b"h".to_vec(),
        value: b"1".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let rev = kv
        .put(PutRequest {
            key: b"h".to_vec(),
            value: b"2".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .header
        .unwrap()
        .revision;
    kv.compact(CompactionRequest {
        revision: rev,
        ..Default::default()
    })
    .await
    .unwrap();
    let err = kv
        .range(RangeRequest {
            key: b"h".to_vec(),
            revision: rev - 1,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::OutOfRange);
}

#[tokio::test]
async fn status_member_list() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut m = MaintenanceClient::connect(ep.clone()).await.unwrap();
    let s = m.status(StatusRequest {}).await.unwrap().into_inner();
    assert_eq!(s.version, "3.5.0");
    let mut c = ClusterClient::connect(ep).await.unwrap();
    let l = c
        .member_list(MemberListRequest { linearizable: true })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(l.members.len(), 1);
    assert_eq!(l.members[0].name, "rine");
}

#[tokio::test]
async fn lease_grant_put_ttl() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut lease = LeaseClient::connect(ep.clone()).await.unwrap();
    let g = lease
        .lease_grant(LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap()
        .into_inner();
    assert!(g.id > 0);
    assert_eq!(g.ttl, 60);

    let mut kv = KvClient::connect(ep).await.unwrap();
    kv.put(PutRequest {
        key: b"lk".to_vec(),
        value: b"v".to_vec(),
        lease: g.id,
        ..Default::default()
    })
    .await
    .unwrap();

    // unknown lease must be rejected
    let err = kv
        .put(PutRequest {
            key: b"bad".to_vec(),
            value: b"v".to_vec(),
            lease: 999999,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn watch_receives_put() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut watch = WatchClient::connect(ep.clone()).await.unwrap();
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
                key: b"wk".to_vec(),
                ..Default::default()
            },
        )),
    })
    .await
    .unwrap();

    // created
    let created = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(created.created);

    let mut kv = KvClient::connect(ep).await.unwrap();
    kv.put(PutRequest {
        key: b"wk".to_vec(),
        value: b"hello".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();

    let ev = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ev.events.len(), 1);
    assert_eq!(ev.events[0].kv.as_ref().unwrap().value, b"hello".to_vec());

    // cancel
    tx.send(WatchRequest {
        request_union: Some(watch_request::RequestUnion::CancelRequest(
            WatchCancelRequest {
                watch_id: ev.watch_id,
            },
        )),
    })
    .await
    .unwrap();
    let canceled = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(canceled.canceled);
}
