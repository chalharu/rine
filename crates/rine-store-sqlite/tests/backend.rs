//! `SqliteBackend` integration tests on a real (in-memory) database:
//! backend-specific behavior plus the shared conformance suite.

use std::sync::Arc;

use bytes::Bytes;
use rine_etcd::backend::{Backend, ListOptions};
use rine_store_conformance::{
    commit_duplicate_events_conflict_atomically, commit_guards_per_key, crud,
    history_compact_events, watch_stream,
};
use rine_store_executor::{SqlExecutor, SqlValue};
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
    b.create(Bytes::from("a"), Bytes::from("1"), 0)
        .await
        .unwrap();
    let (_, kv) = b.get(b"a", 0).await.unwrap();
    assert_eq!(kv.unwrap().value, Bytes::from("1"));
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
    b.create(Bytes::from("a"), Bytes::from("1"), 0)
        .await
        .unwrap();
    assert!(
        b.create(Bytes::from("a"), Bytes::from("x"), 0)
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
