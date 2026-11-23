//! End-to-end tests: real gRPC client against an in-process server.

use std::{sync::Arc, time::Duration};

use rine_etcd::{
    MemoryBackend,
    etcdserverpb::{
        CompactionRequest, Compare, DeleteRangeRequest, LeaseGrantRequest, MemberListRequest,
        PutRequest, RangeRequest, RequestOp, StatusRequest, TxnRequest, WatchCancelRequest,
        WatchCreateRequest, WatchProgressRequest, WatchRequest,
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

#[tokio::test]
async fn range_validation_errors() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    let err = kv
        .range(RangeRequest {
            key: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let err = kv
        .range(RangeRequest {
            key: b"a".to_vec(),
            sort_order: 999,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let err = kv
        .range(RangeRequest {
            key: b"a".to_vec(),
            sort_target: 999,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    for (op, code) in [
        (
            RequestOp {
                request: Some(request_op::Request::RequestPut(PutRequest {
                    key: Vec::new(),
                    value: b"v".to_vec(),
                    ..Default::default()
                })),
            },
            tonic::Code::InvalidArgument,
        ),
        (RequestOp { request: None }, tonic::Code::InvalidArgument),
    ] {
        let txn = TxnRequest {
            compare: vec![],
            success: vec![op],
            failure: vec![],
        };
        let err = kv.txn(txn).await.unwrap_err();
        assert_eq!(err.code(), code);
    }
    let err = kv
        .delete_range(DeleteRangeRequest {
            key: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn range_slow_path_options() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    for (k, v) in [(b"o/a".as_slice(), b"2"), (b"o/b", b"1"), (b"o/c", b"3")] {
        kv.put(PutRequest {
            key: k.to_vec(),
            value: v.to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    // Update one key so mod/create revisions diverge.
    kv.put(PutRequest {
        key: b"o/a".to_vec(),
        value: b"4".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    // Slow path: min_mod_revision forces per-key filtering.
    let r = kv
        .range(RangeRequest {
            key: b"o/".to_vec(),
            range_end: b"o0".to_vec(),
            min_mod_revision: 5,
            sort_order: SortOrder::Descend as i32,
            sort_target: SortTarget::Version as i32,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.count, 1);
    assert_eq!(r.kvs[0].key, b"o/a".to_vec());
    // Revision windows, keys-only, limit with more, count-only.
    let r = kv
        .range(RangeRequest {
            key: b"o/".to_vec(),
            range_end: b"o0".to_vec(),
            max_mod_revision: 3,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.count, 1);
    assert_eq!(r.kvs[0].key, b"o/b".to_vec());
    let r = kv
        .range(RangeRequest {
            key: b"o/".to_vec(),
            range_end: b"o0".to_vec(),
            min_create_revision: 1,
            max_create_revision: 4,
            keys_only: true,
            limit: 1,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.more);
    assert_eq!(r.count, 3);
    assert!(r.kvs.iter().all(|k| k.value.is_empty()));
    let r = kv
        .range(RangeRequest {
            key: b"o/".to_vec(),
            range_end: b"o0".to_vec(),
            min_mod_revision: 1,
            count_only: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.kvs.is_empty());
    assert_eq!(r.count, 3);
    // Sort by value and mod revision on the slow path.
    let r = kv
        .range(RangeRequest {
            key: b"o/".to_vec(),
            range_end: b"o0".to_vec(),
            min_mod_revision: 1,
            sort_order: SortOrder::Ascend as i32,
            sort_target: SortTarget::Value as i32,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let values: Vec<_> = r.kvs.iter().map(|k| k.value.clone()).collect();
    assert_eq!(values, vec![b"1".to_vec(), b"3".to_vec(), b"4".to_vec()]);
}

#[tokio::test]
async fn put_ignore_value_and_lease() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    // ignore_value on a missing key is rejected.
    let err = kv
        .put(PutRequest {
            key: b"ig".to_vec(),
            value: b"v".to_vec(),
            ignore_value: true,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    kv.put(PutRequest {
        key: b"ig".to_vec(),
        value: b"v1".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    // ignore_value keeps the stored value; prev_kv is returned on request.
    let r = kv
        .put(PutRequest {
            key: b"ig".to_vec(),
            value: b"ignored".to_vec(),
            ignore_value: true,
            prev_kv: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.prev_kv.unwrap().value, b"v1".to_vec());
    let got = kv
        .range(RangeRequest {
            key: b"ig".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.kvs[0].value, b"v1".to_vec());
    // ignore_lease on a missing key is rejected too.
    let err = kv
        .put(PutRequest {
            key: b"ig2".to_vec(),
            value: b"v".to_vec(),
            ignore_lease: true,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn delete_prev_kv() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    kv.put(PutRequest {
        key: b"dp".to_vec(),
        value: b"v".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let r = kv
        .delete_range(DeleteRangeRequest {
            key: b"dp".to_vec(),
            prev_kv: true,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.deleted, 1);
    assert_eq!(r.prev_kvs[0].value, b"v".to_vec());
}

#[tokio::test]
async fn txn_duplicate_key_rejected() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    let err = kv
        .txn(TxnRequest {
            compare: vec![],
            success: vec![put_op(b"dup", b"1"), put_op(b"dup", b"2")],
            failure: vec![],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn txn_read_your_write_and_nested() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    kv.put(PutRequest {
        key: b"n/del".to_vec(),
        value: b"x".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    // Read-your-write: a range inside the txn sees the staged put.
    let r = kv
        .txn(TxnRequest {
            compare: vec![],
            success: vec![
                put_op(b"n/a", b"1"),
                RequestOp {
                    request: Some(request_op::Request::RequestRange(RangeRequest {
                        key: b"n/".to_vec(),
                        range_end: b"n0".to_vec(),
                        ..Default::default()
                    })),
                },
            ],
            failure: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.succeeded);
    // Nested txn plus a delete op exercise the remaining apply arms.
    let r = kv
        .txn(TxnRequest {
            compare: vec![],
            success: vec![
                put_op(b"n/b", b"2"),
                RequestOp {
                    request: Some(request_op::Request::RequestTxn(TxnRequest {
                        compare: vec![],
                        success: vec![put_op(b"n/c", b"3")],
                        failure: vec![],
                    })),
                },
                RequestOp {
                    request: Some(request_op::Request::RequestDeleteRange(
                        DeleteRangeRequest {
                            key: b"n/del".to_vec(),
                            ..Default::default()
                        },
                    )),
                },
            ],
            failure: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.succeeded);
    assert_eq!(r.responses.len(), 3);
    let got = kv
        .range(RangeRequest {
            key: b"n/del".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(got.kvs.is_empty());
}

#[tokio::test]
async fn compare_all_targets() {
    use rine_etcd::etcdserverpb::compare::CompareTarget;
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    kv.put(PutRequest {
        key: b"cmp".to_vec(),
        value: b"vv".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let cmp = |target, union, result| Compare {
        result,
        target,
        key: b"cmp".to_vec(),
        target_union: Some(union),
        ..Default::default()
    };
    async fn check(
        kv: &mut rine_etcd::etcdserverpb::kv_client::KvClient<tonic::transport::Channel>,
        c: Compare,
    ) -> bool {
        kv.txn(TxnRequest {
            compare: vec![c],
            success: vec![],
            failure: vec![],
        })
        .await
        .unwrap()
        .into_inner()
        .succeeded
    }
    // Version 1, create rev 2, mod rev 2, value "vv", no lease.
    assert!(
        check(
            &mut kv,
            cmp(
                CompareTarget::Version as i32,
                TargetUnion::Version(0),
                CompareResult::Greater as i32,
            )
        )
        .await
    );
    assert!(
        check(
            &mut kv,
            cmp(
                CompareTarget::Create as i32,
                TargetUnion::CreateRevision(3),
                CompareResult::Less as i32,
            )
        )
        .await
    );
    assert!(
        check(
            &mut kv,
            cmp(
                CompareTarget::Lease as i32,
                TargetUnion::Lease(0),
                CompareResult::Equal as i32,
            )
        )
        .await
    );
    assert!(
        !check(
            &mut kv,
            cmp(
                CompareTarget::Value as i32,
                TargetUnion::Value(b"other".to_vec()),
                CompareResult::Equal as i32,
            )
        )
        .await
    );
    let missing_version = Compare {
        result: CompareResult::Equal as i32,
        target: CompareTarget::Version as i32,
        key: b"missing".to_vec(),
        target_union: Some(TargetUnion::Version(0)),
        ..Default::default()
    };
    assert!(check(&mut kv, missing_version).await);
    // Target/type mismatch and bad enums are rejected.
    for bad in [
        Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Mod as i32,
            key: b"cmp".to_vec(),
            target_union: Some(TargetUnion::Version(2)),
            ..Default::default()
        },
        Compare {
            result: CompareResult::Equal as i32,
            target: CompareTarget::Mod as i32,
            key: b"cmp".to_vec(),
            target_union: None,
            ..Default::default()
        },
        Compare {
            result: 999,
            target: CompareTarget::Mod as i32,
            key: b"cmp".to_vec(),
            target_union: Some(TargetUnion::ModRevision(2)),
            ..Default::default()
        },
        Compare {
            result: CompareResult::Equal as i32,
            target: 999,
            key: b"cmp".to_vec(),
            target_union: Some(TargetUnion::ModRevision(2)),
            ..Default::default()
        },
    ] {
        let err = kv
            .txn(TxnRequest {
                compare: vec![bad],
                success: vec![],
                failure: vec![],
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}

#[tokio::test]
async fn compact_future_rejected() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    let err = kv
        .compact(CompactionRequest {
            revision: 99999,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::OutOfRange);
}

#[tokio::test]
async fn lease_keep_alive() {
    use rine_etcd::etcdserverpb::{LeaseKeepAliveRequest, lease_client::LeaseClient};
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut lease = LeaseClient::connect(ep).await.unwrap();
    let g = lease
        .lease_grant(LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap()
        .into_inner();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let mut stream = lease
        .lease_keep_alive(tonic::Request::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .await
        .unwrap()
        .into_inner();
    tx.send(LeaseKeepAliveRequest { id: g.id }).await.unwrap();
    let resp = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(resp.id, g.id);
    assert!(resp.ttl > 0);
    // Unknown leases report TTL 0 instead of failing.
    tx.send(LeaseKeepAliveRequest { id: 999999 }).await.unwrap();
    let resp = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(resp.ttl, 0);
}

#[tokio::test]
async fn watch_progress_and_history() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep.clone()).await.unwrap();
    for (k, v) in [(b"wh/a".as_slice(), b"1"), (b"wh/b", b"2")] {
        kv.put(PutRequest {
            key: k.to_vec(),
            value: v.to_vec(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    let mut watch = WatchClient::connect(ep.clone()).await.unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let mut stream = watch
        .watch(tonic::Request::new(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .await
        .unwrap()
        .into_inner();
    // History replay from revision 1.
    tx.send(WatchRequest {
        request_union: Some(watch_request::RequestUnion::CreateRequest(
            WatchCreateRequest {
                key: b"wh/".to_vec(),
                range_end: b"wh0".to_vec(),
                start_revision: 1,
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
    let replay = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(replay.events.len(), 2);
    // An empty request is ignored.
    tx.send(WatchRequest {
        request_union: None,
    })
    .await
    .unwrap();
    // Progress request echoes per watcher.
    tx.send(WatchRequest {
        request_union: Some(watch_request::RequestUnion::ProgressRequest(
            WatchProgressRequest {},
        )),
    })
    .await
    .unwrap();
    let progress = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(progress.watch_id, created.watch_id);
    assert!(!progress.created);
}

#[tokio::test]
async fn commit_detaches_old_lease_and_attaches_new() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep.clone()).await.unwrap();
    let mut lease = rine_etcd::etcdserverpb::lease_client::LeaseClient::connect(ep)
        .await
        .unwrap();
    let g = lease
        .lease_grant(rine_etcd::etcdserverpb::LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap()
        .into_inner();
    // Fresh put with a lease attaches the key.
    kv.put(PutRequest {
        key: b"lk1".to_vec(),
        value: b"v".to_vec(),
        lease: g.id,
        ..Default::default()
    })
    .await
    .unwrap();
    // Overwriting without the lease detaches it.
    kv.put(PutRequest {
        key: b"lk1".to_vec(),
        value: b"v2".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    // Deleting a leased key detaches it as well.
    kv.put(PutRequest {
        key: b"lk2".to_vec(),
        value: b"v".to_vec(),
        lease: g.id,
        ..Default::default()
    })
    .await
    .unwrap();
    kv.delete_range(DeleteRangeRequest {
        key: b"lk2".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn txn_overlay_applies_delete_and_rejects_depth() {
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep).await.unwrap();
    kv.put(PutRequest {
        key: b"ov".to_vec(),
        value: b"v".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    // A range staged after a delete in the same txn observes the removal.
    let r = kv
        .txn(TxnRequest {
            compare: vec![],
            success: vec![
                RequestOp {
                    request: Some(request_op::Request::RequestDeleteRange(
                        DeleteRangeRequest {
                            key: b"ov".to_vec(),
                            ..Default::default()
                        },
                    )),
                },
                RequestOp {
                    request: Some(request_op::Request::RequestRange(RangeRequest {
                        key: b"ov".to_vec(),
                        ..Default::default()
                    })),
                },
            ],
            failure: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.succeeded);
    // Nesting past the limit is rejected.
    let mut inner = TxnRequest {
        compare: vec![],
        success: vec![put_op(b"deep", b"v")],
        failure: vec![],
    };
    for _ in 0..9 {
        inner = TxnRequest {
            compare: vec![],
            success: vec![RequestOp {
                request: Some(request_op::Request::RequestTxn(inner)),
            }],
            failure: vec![],
        };
    }
    let err = kv.txn(inner).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn compare_notequal_and_put_ignore_lease() {
    use rine_etcd::etcdserverpb::compare::{CompareResult, CompareTarget, TargetUnion};
    let ep = serve_ephemeral(Arc::new(MemoryBackend::new())).await;
    let mut kv = KvClient::connect(ep.clone()).await.unwrap();
    kv.put(PutRequest {
        key: b"neq".to_vec(),
        value: b"v".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let r = kv
        .txn(TxnRequest {
            compare: vec![Compare {
                result: CompareResult::NotEqual as i32,
                target: CompareTarget::Mod as i32,
                key: b"neq".to_vec(),
                target_union: Some(TargetUnion::ModRevision(2)),
                ..Default::default()
            }],
            success: vec![],
            failure: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!r.succeeded);
    // ignore_lease keeps the stored lease while replacing the value.
    let mut lease = rine_etcd::etcdserverpb::lease_client::LeaseClient::connect(ep)
        .await
        .unwrap();
    let g = lease
        .lease_grant(rine_etcd::etcdserverpb::LeaseGrantRequest { ttl: 60, id: 0 })
        .await
        .unwrap()
        .into_inner();
    kv.put(PutRequest {
        key: b"neq".to_vec(),
        value: b"v2".to_vec(),
        lease: g.id,
        ..Default::default()
    })
    .await
    .unwrap();
    // ignore_lease replaces the value while keeping the stored lease.
    kv.put(PutRequest {
        key: b"neq".to_vec(),
        value: b"v3".to_vec(),
        ignore_lease: true,
        ..Default::default()
    })
    .await
    .unwrap();
    let got = kv
        .range(RangeRequest {
            key: b"neq".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.kvs[0].value, b"v3".to_vec());
    assert_eq!(got.kvs[0].lease, g.id);
}
