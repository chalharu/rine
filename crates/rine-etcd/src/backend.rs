//! Storage contract for the etcd-compatible MVCC layer.

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::broadcast;

use crate::error::Result;

pub type Revision = i64;
pub type LeaseId = i64;

pub(crate) fn in_range(key: &[u8], start: &[u8], end: Option<&[u8]>) -> bool {
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

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    async fn current_revision(&self) -> Revision;
    async fn compacted_revision(&self) -> Revision;

    async fn initialize(&self) -> Result<()>;

    async fn get(&self, key: &[u8], revision: Revision) -> Result<(Revision, Option<KeyValue>)>;
    async fn create(&self, key: Bytes, value: Bytes, lease: LeaseId) -> Result<Revision>;
    async fn update(
        &self,
        key: Bytes,
        value: Bytes,
        expected_mod_revision: Revision,
        lease: LeaseId,
    ) -> Result<Revision>;
    async fn delete(&self, key: &[u8]) -> Result<(Revision, Option<KeyValue>)>;
    /// Validate `reads` under the commit lock, then store and publish `events`
    /// at a freshly assigned revision (`MAX(revision) + 1`). Returns that new
    /// revision so callers can rewrite response headers staged at `base + 1`.
    /// A moved read is an optimistic-concurrency conflict, never a backend
    /// error. An empty `events` batch still validates `reads` and returns the
    /// current revision.
    async fn commit(&self, reads: ReadSet, events: Vec<Event>) -> Result<Revision>;
    /// Dropping `commit` can never leave storage or watch publication incomplete.
    fn commit_is_cancel_safe(&self) -> bool {
        false
    }

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
