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
    backend::{Backend, Event, ListOptions, ReadSet},
    error::Error,
};
use rine_store_conformance::{crud, history_compact_events, kv_create, kv_get, kv_update};
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

/// Nanos since the epoch: with the process id, a unique keyspace per run.
fn run_nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[tokio::test]
async fn live_backend_crud_and_compact() {
    if !live_enabled() {
        eprintln!("skipping live OracleBackend test (RINE_LIVE_ORACLE != 1)");
        return;
    }
    let db = connect_retry().await;
    // Fresh schema on every run (ignore ORA-00942 when a table is absent).
    // Leases are global to the table, not the keyspace, so `rine_leases`
    // resets here too instead of leaking ids across runs.
    let _ = db.execute("DROP TABLE rine", vec![]).await;
    let _ = db.execute("DROP TABLE rine_leases", vec![]).await;

    let backend = OracleBackend::new(Arc::new(db));
    // Creation path: table was just dropped, so this builds the schema.
    backend.ensure_schema().await.expect("ensure_schema");

    // --- CRUD + history + compact (shared conformance suite) ---
    // Unique keyspace per run: this binary runs twice per CI job against
    // the same XE, and the DROP above fails silently when a parallel test
    // holds locks — so fixed namespaces would collide with leftover live
    // keys and flake absent-guard creates with Conflict.
    let ns = format!("live-p{}-{}", std::process::id(), run_nanos());
    crud(&backend, &ns).await;
    history_compact_events(&backend, &ns).await;

    let range = format!("{ns}/range");
    let first = Bytes::from(format!("{range}/a"));
    let second = Bytes::from(format!("{range}/b"));
    kv_create(&backend, first.clone(), Bytes::from_static(b"value"), 0)
        .await
        .unwrap();
    let revision = kv_create(&backend, second, Bytes::from_static(b"second"), 0)
        .await
        .unwrap();
    let prefix = format!("{range}/");
    let end = format!("{range}0");
    let (header, rows, count) = backend
        .list(
            prefix.as_bytes(),
            Some(end.as_bytes()),
            ListOptions {
                limit: 1,
                keys_only: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!((header, rows.len(), count), (revision, 1, 2));
    assert_eq!(rows[0].key, first);
    assert!(rows[0].value.is_empty());
    let missing = format!("{range}/missing");
    let (header, rows, count) = backend
        .list(missing.as_bytes(), None, ListOptions::default())
        .await
        .unwrap();
    assert_eq!((header, rows.len(), count), (revision, 0, 0));

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

/// A commit guarding a key that moved is an optimistic concurrency conflict,
/// even when the batch itself is empty. Uses an isolated key; safe to run in
/// parallel with the other live tests on the same table.
#[tokio::test]
async fn live_stale_point_read_is_conflict() {
    if !live_enabled() {
        eprintln!("skipping live OracleBackend test (RINE_LIVE_ORACLE != 1)");
        return;
    }
    let db = connect_retry().await;
    let backend = OracleBackend::new(Arc::new(db));
    backend.ensure_schema().await.expect("ensure_schema");
    let key = Bytes::from(format!("rine-live-guard-{}", std::process::id()));
    // Absent-key guard passes, then fails once the key is created elsewhere.
    let missing = ReadSet {
        points: vec![(key.clone(), None)],
        ranges: Vec::new(),
    };
    backend
        .commit(missing.clone(), vec![])
        .await
        .expect("absent guard");
    kv_create(&backend, key.clone(), Bytes::from("v"), 0)
        .await
        .expect("create");
    let err = backend.commit(missing, vec![]).await.unwrap_err();
    assert!(matches!(err, Error::Conflict));
    // The live row's guard passes, then fails after an update.
    let (_, kv) = kv_get(&backend, &key, 0).await.expect("get");
    let prev = kv.expect("key exists");
    let live = ReadSet {
        points: vec![(key.clone(), Some(prev.mod_revision))],
        ranges: Vec::new(),
    };
    backend
        .commit(live.clone(), vec![])
        .await
        .expect("live guard");
    kv_update(&backend, key.clone(), Bytes::from("v2"), 0, 0)
        .await
        .expect("update");
    let err = backend.commit(live, vec![]).await.unwrap_err();
    assert!(matches!(err, Error::Conflict));

    let revision = prev.mod_revision + 1;
    let mut stale = prev.clone();
    stale.value = Bytes::from_static(b"stale");
    stale.mod_revision = revision;
    stale.version += 1;
    let mut watch = backend.subscribe();
    let err = backend
        .commit(
            ReadSet::default(),
            vec![Event {
                revision,
                key: key.clone(),
                deleted: false,
                kv: Some(stale),
                prev_kv: Some(prev),
            }],
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Conflict));
    assert!(matches!(
        watch.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let (_, current) = kv_get(&backend, &key, 0).await.unwrap();
    assert_eq!(current.unwrap().value, Bytes::from_static(b"v2"));
}

/// Lease state is a backend row, not process memory: grant, attach,
/// keepalive, expiry-take and revoke all run against Oracle (BLOB key set
/// included), so a restarted or rival node sees the same leases.
#[tokio::test]
async fn live_leases_live_in_the_backend() {
    if !live_enabled() {
        eprintln!("skipping live OracleBackend test (RINE_LIVE_ORACLE != 1)");
        return;
    }
    let db = connect_retry().await;
    let backend = OracleBackend::new(Arc::new(db));
    backend.ensure_schema().await.expect("ensure_schema");

    const NOW: u64 = 1_700_000_000_000;
    let id = backend.lease_grant(3600, 0, NOW).await.expect("grant");
    assert!(id > 0);
    assert!(backend.lease_attach(id, b"live-k".to_vec()).await.unwrap());
    let state = backend
        .lease_lookup(id)
        .await
        .unwrap()
        .expect("lease present");
    assert_eq!(state.granted_ttl, 3600);
    assert_eq!(state.expires_at_ms, NOW + 3_600_000);
    assert_eq!(state.keys, vec![b"live-k".to_vec()]);
    assert_eq!(
        backend.lease_keepalive(id, NOW + 1_000).await.unwrap(),
        Some(3600)
    );
    // A past-due grant is collected exactly once with its keys.
    let short = backend.lease_grant(0, 0, 1_000).await.expect("short grant");
    backend
        .lease_attach(short, b"short-k".to_vec())
        .await
        .unwrap();
    let taken = backend.lease_take_expired(1_001).await.unwrap();
    assert_eq!(taken.len(), 1);
    assert_eq!(taken[0].0, short);
    assert!(backend.lease_take_expired(1_001).await.unwrap().is_empty());
    assert!(backend.lease_lookup(id).await.unwrap().is_some());
    backend.lease_detach(id, b"live-k").await.unwrap();
    assert!(backend.lease_revoke(id).await.unwrap().is_empty());
    assert!(backend.lease_lookup(id).await.unwrap().is_none());
}
