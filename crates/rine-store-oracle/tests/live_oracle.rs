//! Live Oracle XE integration test for `OracleBackend` over the official driver.
//!
//! Runs only when `RINE_LIVE_ORACLE=1` (the CI `oracle-xe` job starts a
//! `gvenzl/oracle-xe` container and sets this). Without it the test skips,
//! so plain `cargo test` never needs a database.
//!
//! Connection parameters come from the environment (defaults match CI):
//! `ORACLE_HOST`, `ORACLE_PORT`, `ORACLE_SERVICE`, `ORACLE_USER`,
//! `ORACLE_PASSWORD`.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use rine_etcd::{
    backend::{Backend, ListOptions},
    error::Error,
};
use rine_store_executor::{SqlExecutor as _, SqlValue};
use rine_store_oracle::{OracleBackend, OracleDb};

fn live_enabled() -> bool {
    std::env::var("RINE_LIVE_ORACLE").as_deref() == Ok("1")
}

fn connect_parts() -> (String, String, String) {
    let host = std::env::var("ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("ORACLE_PORT").unwrap_or_else(|_| "1521".into());
    let service = std::env::var("ORACLE_SERVICE").unwrap_or_else(|_| "XEPDB1".into());
    let user = std::env::var("ORACLE_USER").unwrap_or_else(|_| "rine".into());
    let password = std::env::var("ORACLE_PASSWORD").unwrap_or_else(|_| "RinePass123_".into());
    (user, password, format!("{host}:{port}/{service}"))
}

/// The XE container opens port 1521 before the PDB accepts logons, so retry.
async fn connect_retry() -> OracleDb {
    let (user, password, connect_string) = connect_parts();
    let mut last = String::new();
    for attempt in 1..=60 {
        let (u, p, cs) = (user.clone(), password.clone(), connect_string.clone());
        let r = tokio::task::spawn_blocking(move || OracleDb::connect(&u, &p, &cs)).await;
        match r {
            Ok(Ok(db)) => {
                eprintln!("connected to Oracle XE on attempt {attempt}");
                return db;
            }
            Ok(Err(e)) => {
                last = e.to_string();
            }
            Err(e) => {
                last = format!("task failed: {e}");
            }
        }
        eprintln!("connect attempt {attempt}/60 failed: {last}");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    panic!("could not connect to Oracle XE after 60 attempts: {last}");
}

#[tokio::test]
async fn live_backend_crud_and_compact() {
    if !live_enabled() {
        eprintln!("skipping live OracleBackend test (RINE_LIVE_ORACLE != 1)");
        return;
    }
    let db = connect_retry().await;
    // Fresh schema on every run (ignore ORA-00942 when the table is absent).
    let _ = db.execute("DROP TABLE rine", vec![]).await;

    let backend = OracleBackend::new(Arc::new(db));
    // Creation path: table was just dropped, so this builds the schema.
    backend.ensure_schema().await.expect("ensure_schema");

    // --- CRUD (mirrors the MockExec sql_crud test) ---
    let r1 = backend
        .create(Bytes::from("live/a"), Bytes::from("1"), 0)
        .await
        .expect("create");
    assert!(
        backend
            .create(Bytes::from("live/a"), Bytes::from("x"), 0)
            .await
            .is_err()
    );
    let (_, kv) = backend.get(b"live/a", 0).await.expect("get");
    assert_eq!(kv.expect("kv present").value, Bytes::from("1"));

    backend
        .update(Bytes::from("live/a"), Bytes::from("2"), 0, 0)
        .await
        .expect("update");
    let (_, kvs, count) = backend
        .list(b"live/a", None, ListOptions::default())
        .await
        .expect("list");
    assert_eq!(count, 1);
    assert_eq!(kvs[0].version, 2);
    assert_eq!(kvs[0].create_revision, r1);

    backend.delete(b"live/a").await.expect("delete");
    assert!(
        backend
            .get(b"live/a", 0)
            .await
            .expect("get after delete")
            .1
            .is_none()
    );

    // --- history + compact ---
    backend
        .create(Bytes::from("live/k"), Bytes::from("v1"), 0)
        .await
        .expect("create k");
    let r2 = backend
        .update(Bytes::from("live/k"), Bytes::from("v2"), 0, 0)
        .await
        .expect("update k");
    let (_, kv) = backend
        .get(b"live/k", r2 - 1)
        .await
        .expect("historical get");
    assert_eq!(kv.expect("old kv").value, Bytes::from("v1"));
    let evs = backend.events(r2 - 1, r2).await.expect("events");
    assert_eq!(evs.len(), 2);
    backend.compact(r2).await.expect("compact");
    assert!(
        backend.get(b"live/k", r2 - 1).await.is_err(),
        "compacted rev must error"
    );
    let (_, kv) = backend
        .get(b"live/k", 0)
        .await
        .expect("live get after compact");
    assert_eq!(kv.expect("live kv").value, Bytes::from("v2"));

    assert!(backend.db_size().await >= 0);
}

/// Reads must stay correct across DROP + CREATE of the table on one connection.
///
/// The driver reuses cached SELECT statements; a cached SELECT re-executed
/// after the table was dropped and recreated could return a stale empty
/// result (or ORA-01002) instead of re-parsing. `OracleDb` reads bypass the
/// statement cache, so both generations must be visible. Uses an isolated
/// table (`rine_regen`) to avoid racing other tests on `rine`.
#[tokio::test]
async fn live_read_after_recreate() {
    if !live_enabled() {
        eprintln!("skipping live OracleBackend test (RINE_LIVE_ORACLE != 1)");
        return;
    }

    let db = connect_retry().await;
    const CREATE: &str = "CREATE TABLE rine_regen (id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY, name VARCHAR2(630), created NUMBER DEFAULT 0, deleted NUMBER DEFAULT 0, create_revision NUMBER DEFAULT 0, prev_revision NUMBER DEFAULT 0, lease NUMBER DEFAULT 0, version NUMBER DEFAULT 1, value BLOB)";
    const INSERT: &str = "INSERT INTO rine_regen (name, created, deleted, create_revision, prev_revision, lease, version, value) VALUES (:1, :2, :3, :4, :5, :6, :7, :8)";
    const LIVE: &str = "SELECT id, name, created, deleted, create_revision, prev_revision, lease, version, value FROM rine_regen WHERE name = :1 AND deleted = 0";

    let _ = db.execute("DROP TABLE rine_regen", vec![]).await;
    for generation in 1i64..=2 {
        db.execute(CREATE, vec![]).await.expect("create");
        db.execute(
            INSERT,
            vec![
                SqlValue::Str("foo".into()),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(0),
                SqlValue::Int(1),
                SqlValue::Bytes(b"bar".to_vec()),
            ],
        )
        .await
        .expect("insert");
        let row = db
            .query_row(LIVE, vec![SqlValue::Str("foo".into())])
            .await
            .expect("query_row");
        assert!(
            row.is_some(),
            "gen {generation}: row must be visible via query_row"
        );
        let rows = db
            .query_all(LIVE, vec![SqlValue::Str("foo".into())])
            .await
            .expect("query_all");
        assert_eq!(
            rows.len(),
            1,
            "gen {generation}: query_all must see the row"
        );
        db.execute("DROP TABLE rine_regen", vec![])
            .await
            .expect("drop");
    }
}

/// A commit against a base the storage has moved past is an optimistic
/// concurrency conflict, even when the batch itself is empty. Zero writes:
/// safe to run in parallel with the other live tests on the same table.
#[tokio::test]
async fn live_stale_base_commit_is_conflict() {
    if !live_enabled() {
        eprintln!("skipping live OracleBackend test (RINE_LIVE_ORACLE != 1)");
        return;
    }
    let db = connect_retry().await;
    let backend = OracleBackend::new(Arc::new(db));
    backend.ensure_schema().await.expect("ensure_schema");
    let cur = backend.current_revision().await;
    assert!(cur >= 1);
    let err = backend.commit(0, vec![]).await.unwrap_err();
    assert!(matches!(err, Error::Conflict));
    backend.commit(cur, vec![]).await.expect("fresh base");
}
