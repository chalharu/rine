//! `SqliteBackend` contract tests on an in-memory database.
//!
//! Mirrors the MockExec-backed unit tests and the live Oracle test, so the
//! shared `SqlBackend` logic is validated against a real database locally.

use bytes::Bytes;
use rine_etcd::backend::{Backend, ListOptions};
use rine_store_sqlite::{SqliteBackend, SqliteDb};
use std::sync::Arc;

async fn backend() -> SqliteBackend {
    let db = SqliteDb::open_in_memory().unwrap();
    let b = SqliteBackend::new(Arc::new(db));
    b.ensure_schema().await.unwrap();
    b
}

#[tokio::test]
async fn sqlite_crud() {
    let b = backend().await;
    let r1 = b
        .create(Bytes::from("a"), Bytes::from("1"), 0)
        .await
        .unwrap();
    assert!(
        b.create(Bytes::from("a"), Bytes::from("x"), 0)
            .await
            .is_err()
    );
    let (_, kv) = b.get(b"a", 0).await.unwrap();
    assert_eq!(kv.unwrap().value, Bytes::from("1"));
    b.update(Bytes::from("a"), Bytes::from("2"), 0, 0)
        .await
        .unwrap();
    let (_, kvs, count) = b.list(b"a", None, ListOptions::default()).await.unwrap();
    assert_eq!(count, 1);
    assert_eq!(kvs[0].version, 2);
    assert_eq!(kvs[0].create_revision, r1);
    b.delete(b"a").await.unwrap();
    assert!(b.get(b"a", 0).await.unwrap().1.is_none());
}

#[tokio::test]
async fn sqlite_history_compact_events() {
    let b = backend().await;
    b.create(Bytes::from("k"), Bytes::from("v1"), 0)
        .await
        .unwrap();
    let r2 = b
        .update(Bytes::from("k"), Bytes::from("v2"), 0, 0)
        .await
        .unwrap();
    let (_, kv) = b.get(b"k", r2 - 1).await.unwrap();
    assert_eq!(kv.unwrap().value, Bytes::from("v1"));
    let evs = b.events(r2 - 1, r2).await.unwrap();
    assert_eq!(evs.len(), 2);
    b.compact(r2).await.unwrap();
    assert!(b.get(b"k", r2 - 1).await.is_err());
    let (_, kv) = b.get(b"k", 0).await.unwrap();
    assert_eq!(kv.unwrap().value, Bytes::from("v2"));
    assert!(b.events(r2 - 1, r2).await.is_err());
    assert!(b.db_size().await >= 0);
}

#[tokio::test]
async fn sqlite_ensure_schema_is_idempotent() {
    let db = SqliteDb::open_in_memory().unwrap();
    let b = SqliteBackend::new(Arc::new(db));
    b.ensure_schema().await.unwrap();
    b.ensure_schema().await.unwrap();
    b.create(Bytes::from("a"), Bytes::from("1"), 0)
        .await
        .unwrap();
    let (_, kv) = b.get(b"a", 0).await.unwrap();
    assert_eq!(kv.unwrap().value, Bytes::from("1"));
}
