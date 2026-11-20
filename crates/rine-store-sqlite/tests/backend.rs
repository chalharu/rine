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
    use rine_store_executor::Dialect as _;
    use rine_store_sqlite::SqliteDialect;

    // A DBA-prepared table without the backstop index: startup must add the
    // index itself instead of failing or running degraded.
    let db = SqliteDb::open_in_memory().unwrap();
    db.execute(SqliteDialect::create_table_sql(), vec![])
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

#[tokio::test]
async fn sqlite_historical_reads_cover_snapshot_paths() {
    let b = backend().await;
    kv_create(&b, Bytes::from("h"), Bytes::from("v1"), 0)
        .await
        .unwrap();
    kv_create(&b, Bytes::from("h2"), Bytes::from("v1"), 0)
        .await
        .unwrap();
    let r2 = kv_update(&b, Bytes::from("h"), Bytes::from("v2"), 0, 0)
        .await
        .unwrap();
    // Point read at an old revision.
    let (_, kv) = kv_get(&b, b"h", r2 - 1).await.unwrap();
    assert_eq!(kv.unwrap().value, Bytes::from("v1"));
    // Historical range with an explicit end exercises the snapshot query.
    let (_, rows, count) = b
        .list(
            b"h",
            Some(b"h3"),
            ListOptions {
                revision: r2 - 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(count, 2);
    assert!(rows.iter().any(|kv| kv.value == "v1"));
    // Historical count-only, limit, and keys-only variants.
    for opts in [
        ListOptions {
            revision: r2 - 1,
            count_only: true,
            ..Default::default()
        },
        ListOptions {
            revision: r2 - 1,
            limit: 1,
            ..Default::default()
        },
        ListOptions {
            revision: r2 - 1,
            keys_only: true,
            ..Default::default()
        },
    ] {
        let (cur, _, _) = b.list(b"h", Some(b"h3"), opts).await.unwrap();
        assert!(cur >= r2);
    }
    // A future revision is rejected.
    let err = b
        .list(
            b"h",
            None,
            ListOptions {
                revision: r2 + 1000,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, rine_etcd::error::Error::FutureRev { .. }));
}

#[tokio::test]
async fn sqlite_commit_guards_ranges_and_reserved_keys() {
    use rine_etcd::backend::{Event, ObservedRange, ReadSet};
    use rine_etcd::error::Error;

    let b = backend().await;
    kv_create(&b, Bytes::from("g/a"), Bytes::from("1"), 0)
        .await
        .unwrap();
    kv_create(&b, Bytes::from("g/b"), Bytes::from("2"), 0)
        .await
        .unwrap();
    let (_, kvs, count) = b
        .list(b"g/", Some(&[0u8]), ListOptions::default())
        .await
        .unwrap();
    let observed = ObservedRange {
        prefix: Bytes::from("g/"),
        range_end: Some(Bytes::copy_from_slice(&[0u8])),
        opts: ListOptions::default(),
        observed: kvs
            .iter()
            .map(|kv| (kv.key.clone(), kv.mod_revision))
            .collect(),
        count,
    };
    // A matching snapshot commits cleanly.
    b.commit(
        ReadSet {
            points: Vec::new(),
            ranges: vec![observed.clone()],
        },
        vec![],
    )
    .await
    .unwrap();
    // Moving a key under the range conflicts.
    kv_update(&b, Bytes::from("g/a"), Bytes::from("3"), 0, 0)
        .await
        .unwrap();
    let err = b
        .commit(
            ReadSet {
                points: Vec::new(),
                ranges: vec![observed],
            },
            vec![],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict));
    // A guard on a deleted key observes no live revision.
    kv_delete(&b, b"g/a").await.unwrap();
    b.commit(
        ReadSet {
            points: vec![(Bytes::from("g/a"), None)],
            ranges: Vec::new(),
        },
        vec![],
    )
    .await
    .unwrap();
    // The compaction watermark key is reserved.
    let err = b
        .commit(
            ReadSet::default(),
            vec![Event {
                revision: 1,
                key: Bytes::from("compact_rev_key"),
                deleted: false,
                kv: None,
                prev_kv: None,
            }],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Backend(_)));
}

#[tokio::test]
async fn sqlite_executor_edges_and_lifecycle() {
    use rine_store_executor::SqlExecutor as _;

    let db = SqliteDb::open_in_memory().unwrap();
    let b = SqliteBackend::new(Arc::new(db.clone()));
    b.initialize().await.unwrap();
    assert!(b.compacted_revision().await >= 0);
    // Batch writes outside a commit unit run in their own transaction.
    db.execute_batch(vec![(
        "CREATE TABLE t (id INTEGER PRIMARY KEY)".to_string(),
        vec![],
    )])
    .await
    .unwrap();
    // A REAL value cannot be read as a row cell.
    let err = db.query_all("SELECT 1.5", vec![]).await.unwrap_err();
    assert!(matches!(err, rine_etcd::error::Error::Backend(_)));
    assert!(!format!("{err:?}").is_empty());
    // Batch reads inside a unit bypass the extra transaction; errors surface.
    db.begin_unit().await.unwrap();
    let rows = db
        .query_batch(vec![("SELECT 1".to_string(), vec![])])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    db.end_unit(true).await.unwrap();
    let err = db
        .query_batch(vec![(
            "SELECT * FROM no_such_table_xyz".to_string(),
            vec![],
        )])
        .await
        .unwrap_err();
    assert!(matches!(err, rine_etcd::error::Error::TableMissing(_)));
    // File open plus the factory path.
    let dir = std::env::temp_dir().join(format!("rine-test-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&dir);
    let file_db = SqliteDb::open(&dir).unwrap();
    use rine_etcd::backend::BackendFactory as _;
    let factory = rine_store_sqlite::SqliteFactory::new(rine_store_sqlite::SqliteConfig {
        path: dir.clone(),
    });
    assert_eq!(factory.name(), "sqlite");
    let backend = factory.build().unwrap();
    backend.initialize().await.unwrap();
    drop(file_db);
    let _ = std::fs::remove_file(&dir);
}

#[tokio::test]
async fn sqlite_conformance_helper_edges() {
    use rine_etcd::error::Error;

    let b = backend().await;
    // Duplicate create, conflicting update, and missing-key delete paths.
    kv_create(&b, Bytes::from("e"), Bytes::from("1"), 0)
        .await
        .unwrap();
    let err = kv_create(&b, Bytes::from("e"), Bytes::from("x"), 0)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict), "got {err:?}");
    let err = kv_update(&b, Bytes::from("gone"), Bytes::from("x"), 0, 0)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::KeyExists(_)), "got {err:?}");
    let err = kv_update(&b, Bytes::from("e"), Bytes::from("x"), 1, 999)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Backend(_)), "got {err:?}");
    let (_, prev) = kv_delete(&b, b"missing").await.unwrap();
    assert!(prev.is_none());
}

#[tokio::test]
async fn sqlite_leases_live_in_the_backend() {
    use rine_etcd::backend::LeaseState;

    const NOW: u64 = 1_700_000_000_000;
    let b = backend().await;
    // Auto ids are backend-unique and positive; requested ids are honored.
    let auto = b.lease_grant(60, 0, NOW).await.unwrap();
    assert!(auto > 0);
    assert_eq!(b.lease_grant(60, 42, NOW).await.unwrap(), 42);
    assert_ne!(auto, 42);
    // Re-granting an id replaces it: deadline and keys reset.
    b.lease_attach(auto, b"k".to_vec()).await.unwrap();
    b.lease_grant(60, auto, NOW + 1).await.unwrap();
    assert_eq!(
        b.lease_lookup(auto).await.unwrap().unwrap(),
        LeaseState {
            granted_ttl: 60,
            expires_at_ms: NOW + 1 + 60_000,
            keys: Vec::new(),
        }
    );
    // Keepalive moves the deadline out; unknown leases stay unknown.
    assert_eq!(b.lease_keepalive(999_999, NOW + 2).await.unwrap(), None);
    assert_eq!(b.lease_keepalive(auto, NOW + 2).await.unwrap(), Some(60));
    assert_eq!(
        b.lease_lookup(auto).await.unwrap().unwrap().expires_at_ms,
        NOW + 2 + 60_000
    );
    // Attach dedups; unknown leases refuse; detach tolerates the unknown.
    assert!(b.lease_attach(auto, b"k1".to_vec()).await.unwrap());
    assert!(b.lease_attach(auto, b"k1".to_vec()).await.unwrap());
    assert!(!b.lease_attach(999_999, b"k".to_vec()).await.unwrap());
    b.lease_detach(auto, b"missing").await.unwrap();
    b.lease_detach(999_999, b"k").await.unwrap();
    b.lease_detach(auto, b"k1").await.unwrap();
    assert!(b.lease_lookup(auto).await.unwrap().unwrap().keys.is_empty());
    let mut ids = b.lease_ids().await.unwrap();
    ids.sort_unstable();
    assert_eq!(ids, vec![auto.min(42), auto.max(42)]);
    // Expiry takes only the past-due leases with their keys (binary names
    // round-trip through the BLOB codec); the second take is empty, so
    // rival expiry loops never work the same lease twice.
    let short = b.lease_grant(0, 0, 1_000).await.unwrap();
    b.lease_attach(short, b"gone".to_vec()).await.unwrap();
    b.lease_attach(short, vec![0, 255, 1, 2]).await.unwrap();
    let taken = b.lease_take_expired(1_001).await.unwrap();
    assert_eq!(taken.len(), 1);
    assert_eq!(taken[0].0, short);
    let mut keys = taken[0].1.clone();
    keys.sort();
    assert_eq!(keys, vec![vec![0, 255, 1, 2], b"gone".to_vec()]);
    assert!(b.lease_take_expired(1_001).await.unwrap().is_empty());
    assert!(b.lease_lookup(auto).await.unwrap().is_some());
    // Revoke hands back keys and drops the lease; unknown revokes are empty.
    b.lease_attach(auto, b"victim".to_vec()).await.unwrap();
    assert_eq!(
        b.lease_revoke(auto).await.unwrap(),
        vec![b"victim".to_vec()]
    );
    assert!(b.lease_lookup(auto).await.unwrap().is_none());
    assert!(b.lease_revoke(auto).await.unwrap().is_empty());
}

#[tokio::test]
async fn sqlite_leases_survive_reopen() {
    let path = std::env::temp_dir().join(format!(
        "rine-lease-reopen-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_file(&path);
    const NOW: u64 = 1_700_000_000_000;
    let id = {
        let db = SqliteDb::open(&path).unwrap();
        let b = SqliteBackend::new(Arc::new(db));
        b.initialize().await.unwrap();
        let id = b.lease_grant(3600, 0, NOW).await.unwrap();
        b.lease_attach(id, b"keep".to_vec()).await.unwrap();
        id
    };
    {
        let db = SqliteDb::open(&path).unwrap();
        let b = SqliteBackend::new(Arc::new(db));
        b.initialize().await.unwrap();
        let state = b.lease_lookup(id).await.unwrap().unwrap();
        assert_eq!(state.granted_ttl, 3600);
        assert_eq!(state.expires_at_ms, NOW + 3_600_000);
        assert_eq!(state.keys, vec![b"keep".to_vec()]);
        // Keepalive works after reopen: the deadline recomputes from the
        // stored TTL, and expiry still collects the lease afterwards.
        assert_eq!(
            b.lease_keepalive(id, NOW + 1_000).await.unwrap(),
            Some(3600)
        );
        assert_eq!(
            b.lease_take_expired(NOW + 3_600_001 + 1_000)
                .await
                .unwrap()
                .len(),
            1
        );
    }
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
