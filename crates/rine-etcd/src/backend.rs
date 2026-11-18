//! Storage contract. `server.Backend` + `logstructured` MVCC rules.

use crate::error::Result;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::broadcast;

pub type Revision = i64;
pub type LeaseId = i64;

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

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    async fn current_revision(&self) -> Revision;
    async fn compacted_revision(&self) -> Revision;

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
    async fn list(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)>;
    async fn compact(&self, revision: Revision) -> Result<Revision>;
    async fn db_size(&self) -> i64;

    /// Events in (from, to] — actually [from, to] inclusive — for watch replay.
    /// Returns `Error::Compacted` when `from` is below the compacted revision.
    async fn events(&self, from: Revision, to: Revision) -> Result<Vec<Event>>;

    fn subscribe(&self) -> broadcast::Receiver<Event>;
}
