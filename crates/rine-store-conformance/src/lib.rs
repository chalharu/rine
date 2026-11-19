//! Shared conformance suite for [`Backend`](rine_etcd::Backend) implementations.
//!
//! Every storage backend (memory, SQLite, Oracle, mocks) upholds the same
//! MVCC contract; these scenarios are generic over the [`Backend`] trait, so
//! each backend crate only wires construction (plus its backend-specific
//! behavior) and gets the contract coverage for free. `ns` namespaces all
//! keys, keeping runs against a shared database (the live Oracle test)
//! parallel-safe.

use bytes::Bytes;
use rine_etcd::{
    backend::{Backend, Event, KeyValue, ListOptions, ReadSet},
    error::Error,
};

fn key(ns: &str, name: &str) -> Bytes {
    Bytes::from(format!("{ns}/{name}"))
}

fn val(s: &str) -> Bytes {
    Bytes::from(s.to_owned())
}

/// CRUD lifecycle: create, duplicate-create rejection, get, update (version
/// and create-revision bookkeeping), list, delete, tombstone reads.
pub async fn crud<B: Backend>(b: &B, ns: &str) {
    let a = key(ns, "a");
    let r1 = b.create(a.clone(), val("1"), 0).await.unwrap();
    assert!(b.create(a.clone(), val("x"), 0).await.is_err());
    let (_, kv) = b.get(&a, 0).await.unwrap();
    assert_eq!(kv.unwrap().value, val("1"));
    b.update(a.clone(), val("2"), 0, 0).await.unwrap();
    let (_, kvs, count) = b.list(&a, None, ListOptions::default()).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(kvs[0].version, 2);
    assert_eq!(kvs[0].create_revision, r1);
    b.delete(&a).await.unwrap();
    assert!(b.get(&a, 0).await.unwrap().1.is_none());
}

/// History reads, event log, compaction, and post-compaction behavior: the
/// compacted revision errors while the live row survives as the read base.
pub async fn history_compact_events<B: Backend>(b: &B, ns: &str) {
    let k = key(ns, "k");
    b.create(k.clone(), val("v1"), 0).await.unwrap();
    let r2 = b.update(k.clone(), val("v2"), 0, 0).await.unwrap();
    let (_, kv) = b.get(&k, r2 - 1).await.unwrap();
    assert_eq!(kv.unwrap().value, val("v1"));
    let evs = b.events(r2 - 1, r2).await.unwrap();
    assert_eq!(evs.len(), 2);
    b.compact(r2).await.unwrap();
    assert!(b.get(&k, r2 - 1).await.is_err());
    let (_, kv) = b.get(&k, 0).await.unwrap();
    assert_eq!(kv.unwrap().value, val("v2"));
    assert!(b.events(r2 - 1, r2).await.is_err());
}

/// Committed events reach subscribers.
pub async fn watch_stream<B: Backend>(b: &B, ns: &str) {
    let mut rx = b.subscribe();
    b.create(key(ns, "w"), val("1"), 0).await.unwrap();
    let ev = rx.recv().await.unwrap().pop().unwrap();
    assert!(!ev.deleted);
    assert_eq!(ev.kv.unwrap().version, 1);
}

/// Per-key guard validation: unrelated writes never conflict, while a moved
/// guarded key — present or absent — is a conflict.
pub async fn commit_guards_per_key<B: Backend>(b: &B, ns: &str) {
    let other = key(ns, "other");
    // Unrelated writes never conflict: an empty read set commits cleanly
    // no matter how far the revision moved.
    b.commit(ReadSet::default(), vec![]).await.unwrap();
    b.create(other.clone(), val("v"), 0).await.unwrap();
    b.commit(ReadSet::default(), vec![]).await.unwrap();

    // A guarded key that moved is a conflict.
    let (_, kv) = b.get(&other, 0).await.unwrap();
    let guard = ReadSet {
        points: vec![(other.clone(), kv.map(|kv| kv.mod_revision))],
        ranges: Vec::new(),
    };
    b.commit(guard.clone(), vec![]).await.unwrap();
    b.update(other.clone(), val("v2"), 0, 0).await.unwrap();
    let err = b.commit(guard, vec![]).await.unwrap_err();
    assert!(matches!(err, Error::Conflict));
    // Absent-key guards work the same way.
    let gone = key(ns, "gone");
    let missing = ReadSet {
        points: vec![(gone.clone(), None)],
        ranges: Vec::new(),
    };
    b.commit(missing.clone(), vec![]).await.unwrap();
    b.create(gone.clone(), val("v"), 0).await.unwrap();
    let err = b.commit(missing, vec![]).await.unwrap_err();
    assert!(matches!(err, Error::Conflict));
}

/// A batch violating the unique backstop fails atomically: conflict error,
/// unchanged revision, no visible rows, no published events.
pub async fn commit_duplicate_events_conflict_atomically<B: Backend>(b: &B, ns: &str) {
    let k = key(ns, "k");
    let cur = b.current_revision().await;
    let put = || Event {
        revision: cur + 1,
        key: k.clone(),
        deleted: false,
        kv: Some(KeyValue {
            key: k.clone(),
            value: val("v"),
            create_revision: cur + 1,
            mod_revision: cur + 1,
            version: 1,
            lease: 0,
        }),
        prev_kv: None,
    };
    // Same key twice in one batch violates the (name, prev_revision)
    // unique index on the second row.
    let mut watch = b.subscribe();
    let err = b
        .commit(ReadSet::default(), vec![put(), put()])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict));
    // The failed batch left nothing behind.
    assert_eq!(b.current_revision().await, cur);
    assert!(b.get(&k, 0).await.unwrap().1.is_none());
    assert!(matches!(
        watch.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}
