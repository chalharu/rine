//! `SqliteBackend` integration tests on a real (in-memory) database:
//! backend-specific behavior plus the shared conformance suite.

use std::sync::Arc;

use bytes::Bytes;
use rine_etcd::backend::{Backend, ListOptions};
use rine_store_conformance::{
    commit_duplicate_events_conflict_atomically, commit_guards_per_key, crud,
    history_compact_events, kv_create, kv_delete, kv_get, kv_update, watch_stream,
};
use rine_store_executor::SqlExecutor;
use rine_store_sqlite::{SqliteBackend, SqliteDb};

async fn backend() -> SqliteBackend {
    let db = SqliteDb::open_in_memory().unwrap();
    let b = SqliteBackend::new(Arc::new(db));
    b.ensure_schema().await.unwrap();
    b
}

#[tokio::test]
async fn conformance_crud() {
    crud(&backend().await, "t").await;
}

#[tokio::test]
async fn conformance_history_compact_events() {
    let b = backend().await;
    history_compact_events(&b, "t").await;
    assert!(b.db_size().await >= 0);
}

#[tokio::test]
async fn conformance_watch_stream() {
    watch_stream(&backend().await, "t").await;
}

#[tokio::test]
async fn conformance_commit_guards_per_key() {
    commit_guards_per_key(&backend().await, "t").await;
}

#[tokio::test]
async fn conformance_commit_duplicate_events_conflict_atomically() {
    commit_duplicate_events_conflict_atomically(&backend().await, "t").await;
}

#[tokio::test]
async fn sqlite_ensure_schema_is_idempotent() {
    let db = SqliteDb::open_in_memory().unwrap();
    let b = SqliteBackend::new(Arc::new(db));
    b.ensure_schema().await.unwrap();
    b.ensure_schema().await.unwrap();
    kv_create(&b, Bytes::from("a"), Bytes::from("1"), 0)
        .await
        .unwrap();
    let (_, kv) = kv_get(&b, b"a", 0).await.unwrap();
    assert_eq!(kv.unwrap().value, Bytes::from("1"));
}

#[tokio::test]
async fn sqlite_live_range_counts_only_visible_rows_before_limiting() {
    let b = backend().await;
    for key in ["a", "b", "c"] {
        kv_create(
            &b,
            Bytes::from_static(key.as_bytes()),
            Bytes::from_static(b"value"),
            0,
        )
        .await
        .unwrap();
    }
    let (old_revision, _) = kv_delete(&b, b"b").await.unwrap();
    let current = kv_update(
        &b,
        Bytes::from_static(b"a"),
        Bytes::from_static(b"new"),
        0,
        0,
    )
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
async fn sqlite_driver_errors_are_typed() {
    use rine_etcd::error::Error;

    let db = SqliteDb::open_in_memory().unwrap();
    // Missing table surfaces as TableMissing, not a plain Backend string.
    let err = db
        .query_row("SELECT 1 FROM no_such_table_xyz", vec![])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::TableMissing(_)), "got {err:?}");
    // A concurrent creator's DDL surfaces as AlreadyExists.
    db.execute("CREATE TABLE dup (id INTEGER PRIMARY KEY)", vec![])
        .await
        .unwrap();
    let err = db
        .execute("CREATE TABLE dup (id INTEGER PRIMARY KEY)", vec![])
        .await
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)), "got {err:?}");
}

#[tokio::test]
async fn sqlite_ensure_schema_adds_missing_unique_index() {
    // A DBA-prepared table without the backstop index: startup must add the
    // index itself instead of failing or running degraded.
    let db = SqliteDb::open_in_memory().unwrap();
    db.execute(
        "CREATE TABLE rine (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(630), created INTEGER DEFAULT 0, deleted INTEGER DEFAULT 0, create_revision INTEGER DEFAULT 0, prev_revision INTEGER DEFAULT 0, lease INTEGER DEFAULT 0, version INTEGER DEFAULT 1, value BLOB)",
        vec![],
    )
    .await
    .unwrap();
    let before = db
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'rine_name_prev_revision_uindex'",
            vec![],
        )
        .await
        .unwrap();
    assert!(before.is_none());
    let b = SqliteBackend::new(Arc::new(db.clone()));
    b.ensure_schema().await.unwrap();
    let after = db
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'rine_name_prev_revision_uindex'",
            vec![],
        )
        .await
        .unwrap();
    assert!(after.is_some());
    // The backend is fully functional on the adopted table.
    kv_create(&b, Bytes::from("a"), Bytes::from("1"), 0)
        .await
        .unwrap();
    assert!(
        kv_create(&b, Bytes::from("a"), Bytes::from("x"), 0)
            .await
            .is_err()
    );
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
