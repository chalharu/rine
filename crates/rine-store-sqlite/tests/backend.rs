//! `SqliteBackend` contract tests on an in-memory database.
//!
//! Mirrors the MockExec-backed unit tests and the live Oracle test, so the
//! shared `SqlBackend` logic is validated against a real database locally.

use std::sync::Arc;

use bytes::Bytes;
use rine_etcd::backend::{Backend, Event, KeyValue, ListOptions, ReadSet};
use rine_store_executor::{SqlExecutor, SqlValue};
use rine_store_sqlite::{SqliteBackend, SqliteDb};

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

#[tokio::test]
async fn sqlite_commit_rolls_back_after_database_error() {
    let b = backend().await;
    let base = b.current_revision().await;
    let a = Bytes::from_static(b"a");
    let event = Event {
        revision: base + 1,
        key: a.clone(),
        deleted: false,
        kv: Some(KeyValue {
            key: a,
            value: Bytes::from_static(b"value"),
            create_revision: base + 1,
            mod_revision: base + 1,
            version: 1,
            lease: 0,
        }),
        prev_kv: None,
    };
    // Both rows use the same (name, predecessor) unique key.
    let mut watch = b.subscribe();
    assert!(
        b.commit(ReadSet::default(), vec![event.clone(), event])
            .await
            .is_err()
    );
    assert_eq!(b.current_revision().await, base);
    assert!(b.get(b"a", 0).await.unwrap().1.is_none());
    assert!(matches!(
        watch.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn sqlite_legacy_row_revisions_remain_readable() {
    let db = Arc::new(SqliteDb::open_in_memory().unwrap());
    let b = SqliteBackend::new(db.clone());
    b.ensure_schema().await.unwrap();
    db.execute(
        "INSERT INTO rine (name, created, deleted, create_revision, prev_revision, lease, version, value) \
         VALUES (?, 0, 0, 1, 0, 0, 1, ?)",
        vec![
            SqlValue::Str("legacy".into()),
            SqlValue::Bytes(b"before".to_vec()),
        ],
    )
    .await
    .unwrap();
    let r = b
        .update(
            Bytes::from_static(b"legacy"),
            Bytes::from_static(b"after"),
            1,
            0,
        )
        .await
        .unwrap();
    assert_eq!(r, 2);
    let old = b.get(b"legacy", 1).await.unwrap().1.unwrap();
    assert_eq!(old.value, b"before".as_slice());
    assert_eq!(old.mod_revision, 1);
    let event = b.events(r, r).await.unwrap().pop().unwrap();
    assert_eq!(event.prev_kv.unwrap().mod_revision, 1);
    assert_eq!(event.kv.unwrap().mod_revision, r);
}

#[tokio::test]
async fn sqlite_live_range_counts_only_visible_rows_before_limiting() {
    let b = backend().await;
    for key in ["a", "b", "c"] {
        b.create(
            Bytes::from_static(key.as_bytes()),
            Bytes::from_static(b"value"),
            0,
        )
        .await
        .unwrap();
    }
    let (old_revision, _) = b.delete(b"b").await.unwrap();
    let current = b
        .update(Bytes::from_static(b"a"), Bytes::from_static(b"new"), 0, 0)
        .await
        .unwrap();
    b.compact(old_revision).await.unwrap();

    for revision in [0, old_revision] {
        let opts = ListOptions {
            revision,
            limit: 1,
            keys_only: true,
            ..Default::default()
        };
        let (header, rows, count) = b.list(b"a", Some(&[0]), opts.clone()).await.unwrap();
        assert_eq!((header, rows.len(), count), (current, 1, 2));
        assert_eq!(rows[0].key, b"a".as_slice());
        assert!(rows[0].value.is_empty());

        let (header, rows, count) = b
            .list(
                b"a",
                Some(&[0]),
                ListOptions {
                    count_only: true,
                    ..opts
                },
            )
            .await
            .unwrap();
        assert_eq!((header, rows.len(), count), (current, 0, 2));
    }
    let (_, rows, count) = b
        .list(b"b", Some(b"c"), ListOptions::default())
        .await
        .unwrap();
    assert!(rows.is_empty());
    assert_eq!(count, 0);
}

#[tokio::test]
async fn sqlite_unique_violation_maps_to_conflict() {
    use rine_etcd::error::Error;
    use rine_store_executor::{Dialect, SqlExecutor, SqlValue};
    use rine_store_sqlite::SqliteDialect;

    let db = SqliteDb::open_in_memory().unwrap();
    let b = SqliteBackend::new(Arc::new(db.clone()));
    b.ensure_schema().await.unwrap();
    // (id, name, revision, deleted, create_rev, prev_rev, lease, version, value):
    // the second identical insert violates the (name, prev_revision) index.
    let row = || {
        vec![
            SqlValue::Int(1),
            SqlValue::Str("k".into()),
            SqlValue::Int(1),
            SqlValue::Int(0),
            SqlValue::Int(1),
            SqlValue::Int(0),
            SqlValue::Int(0),
            SqlValue::Int(1),
            SqlValue::Bytes(b"v".to_vec()),
        ]
    };
    db.execute(&SqliteDialect::insert_event_sql(), row())
        .await
        .unwrap();
    let err = db
        .execute(&SqliteDialect::insert_event_sql(), row())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict), "got {err:?}");
}
