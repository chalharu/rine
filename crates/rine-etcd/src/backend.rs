//! Storage contract for the etcd-compatible MVCC layer.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::broadcast;

use crate::error::Result;
pub type Revision = i64;
pub type LeaseId = i64;

/// Mutual exclusion between user writes and lease expiry/revoke.
/// Owned by the server and shared by `KvService` and `LeaseManager`;
/// storage-level serialization additionally lives in each `Backend`.
pub type CommitLock = Arc<tokio::sync::Mutex<()>>;

/// Capacity of each backend's watch broadcast channel.
pub const BROADCAST_CAP: usize = 1024;

/// Single-member cluster identity (k8s apiserver health/discovery only).
pub const CLUSTER_ID: u64 = 0x7269_6e65_0000_0001;
pub const MEMBER_ID: u64 = 0x7269_6e65_0000_0002;
/// Pinned so k8s apiserver health checks accept `Status.version`.
pub const VERSION: &str = "3.5.0";

pub fn header(revision: Revision) -> crate::etcdserverpb::ResponseHeader {
    crate::etcdserverpb::ResponseHeader {
        cluster_id: CLUSTER_ID,
        member_id: MEMBER_ID,
        revision,
        raft_term: 0,
    }
}

/// Etcd range predicate: empty end = single key, `[0]` = `>= key`, else `[key, end)`.
pub fn in_range(key: &[u8], start: &[u8], end: Option<&[u8]>) -> bool {
    match end {
        None | Some([]) => key == start,
        Some([0]) => key >= start,
        Some(end) => key >= start && key < end,
    }
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: Bytes,
    pub value: Bytes,
    pub create_revision: Revision,
    pub mod_revision: Revision,
    pub version: i64,
    pub lease: LeaseId,
}

#[derive(Debug, Clone)]
pub struct Event {
    pub revision: Revision,
    pub key: Bytes,
    /// true = delete event
    pub deleted: bool,
    /// State after the event (None for deletes).
    pub kv: Option<KeyValue>,
    /// State before the event.
    pub prev_kv: Option<KeyValue>,
}

#[derive(Debug, Clone, Default)]
pub struct ListOptions {
    pub revision: Revision,
    pub limit: i64,
    pub keys_only: bool,
    pub count_only: bool,
}

/// Point and range reads a transaction observed at its base state.
///
/// `commit` revalidates these under the database commit lock and rejects the
/// batch with [`crate::error::Error::Conflict`] when any of them moved.
/// Reads performed to fetch a write predecessor (unconditional `Put`/`Delete`)
/// are deliberately *not* recorded: they are rebased onto the newest state
/// instead of conflicting.
#[derive(Debug, Clone, Default)]
pub struct ReadSet {
    /// Single-key reads: key plus the observed live `mod_revision`
    /// (`None` when the key was absent). First observation wins.
    pub points: Vec<(Bytes, Option<Revision>)>,
    /// Range reads with the backend-level result snapshot.
    pub ranges: Vec<ObservedRange>,
}

/// One backend-level range read inside a transaction.
#[derive(Debug, Clone)]
pub struct ObservedRange {
    pub prefix: Bytes,
    pub range_end: Option<Bytes>,
    pub opts: ListOptions,
    /// Backend-level rows before any overlay, in returned order.
    pub observed: Vec<(Bytes, Revision)>,
    /// Full count before limiting.
    pub count: i64,
}

impl ObservedRange {
    /// True when a fresh backend-level read still shows the same snapshot.
    pub fn matches_snapshot(&self, kvs: &[(Bytes, Revision)], count: i64) -> bool {
        count == self.count
            && kvs.len() == self.observed.len()
            && kvs
                .iter()
                .zip(self.observed.iter())
                .all(|((k, r), (ek, er))| k == ek && r == er)
    }
}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    async fn current_revision(&self) -> Revision;
    async fn compacted_revision(&self) -> Revision;

    async fn initialize(&self) -> Result<()>;

    /// Validate `reads` under the commit lock, then store and publish `events`
    /// at a freshly assigned revision (`MAX(revision) + 1`). Returns that new
    /// revision so callers can rewrite response headers staged at `base + 1`.
    /// A moved read is an optimistic-concurrency conflict, never a backend
    /// error. An empty `events` batch still validates `reads` and returns the
    /// current revision. Dropping the returned future can never leave storage
    /// or watch publication incomplete.
    async fn commit(&self, reads: ReadSet, events: Vec<Event>) -> Result<Revision>;

    /// Return keys in ascending key order and the full count before limiting.
    async fn list(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)>;
    async fn compact(&self, revision: Revision) -> Result<Revision>;
    async fn db_size(&self) -> i64;

    /// Events in [from, to] for watch replay.
    /// Returns `Error::Compacted` when `from` is at or below the compaction watermark.
    async fn events(&self, from: Revision, to: Revision) -> Result<Vec<Event>>;

    /// Committed events, grouped by revision.
    fn subscribe(&self) -> broadcast::Receiver<Vec<Event>>;
}

/// Construction of one named [`Backend`] implementation.
///
/// Each storage crate provides a factory over its own CLI config struct
/// (a `clap::Args` derive); the server binary registers every compiled-in
/// factory and selects one via `--backend`. The [`name`](Self::name) is
/// both the selector and the unknown-backend error text, so flag values
/// can never drift from the implementations.
pub trait BackendFactory: Send + Sync {
    /// Backend identifier used with `--backend`.
    fn name(&self) -> &'static str;
    /// Build the backend (open files, connect). Does not provision the
    /// schema; call [`Backend::initialize`] afterwards.
    fn build(&self) -> anyhow::Result<Arc<dyn Backend>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_predicate_covers_bounds() {
        assert!(in_range(b"a", b"a", None));
        assert!(!in_range(b"b", b"a", None));
        assert!(in_range(b"b", b"a", Some(&[0])));
        assert!(!in_range(b"a", b"b", Some(&[0])));
        assert!(in_range(b"a", b"a", Some(b"c".as_slice())));
        assert!(!in_range(b"c", b"a", Some(b"c".as_slice())));
    }

    #[test]
    fn snapshot_match_rejects_moved_reads() {
        let observed = ObservedRange {
            prefix: Bytes::from("k"),
            range_end: None,
            opts: ListOptions::default(),
            observed: vec![(Bytes::from("k"), 2)],
            count: 1,
        };
        assert!(observed.matches_snapshot(&[(Bytes::from("k"), 2)], 1));
        assert!(!observed.matches_snapshot(&[(Bytes::from("k"), 3)], 1));
        assert!(!observed.matches_snapshot(&[(Bytes::from("k"), 2), (Bytes::from("x"), 1)], 2));
        assert!(!observed.matches_snapshot(&[], 0));
    }
}
