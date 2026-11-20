//! `OracleBackendMock` tests: the shared conformance suite over `MockExec`
//! (no database) plus mock-only schema coverage.

use std::sync::Arc;

use bytes::Bytes;
use rine_store_conformance::{
    commit_duplicate_events_conflict_atomically, commit_guards_per_key, crud,
    history_compact_events, kv_create, watch_stream,
};
use rine_store_oracle::{MockExec, OracleBackendMock};

async fn backend() -> OracleBackendMock {
    let b = OracleBackendMock::new(Arc::new(MockExec::new()));
    b.ensure_schema().await.unwrap();
    b
}

#[tokio::test]
async fn conformance_crud() {
    crud(&backend().await, "t").await;
}

#[tokio::test]
async fn conformance_history_compact_events() {
    history_compact_events(&backend().await, "t").await;
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
async fn ensure_schema_is_noop_when_table_exists() {
    let b = backend().await;
    // Still functional afterwards.
    kv_create(&b, Bytes::from("a"), Bytes::from("1"), 0)
        .await
        .unwrap();
}
