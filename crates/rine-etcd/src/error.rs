use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("key exists: {0:?}")]
    KeyExists(Vec<u8>),
    #[error("revision compacted: requested {requested}, compacted {compacted}")]
    Compacted { requested: i64, compacted: i64 },
    #[error("future revision: requested {requested}, current {current}")]
    FutureRev { requested: i64, current: i64 },
    #[error("storage revision changed during transaction")]
    Conflict,
    #[error("not supported: {0}")]
    NotSupported(String),
    #[error("backend: {0}")]
    Backend(String),
    /// Schema probe hit a database without the `rine` table (fresh database).
    /// Carries the driver message for logs; match on the variant, not the text.
    #[error("table missing: {0}")]
    TableMissing(String),
    /// DDL raced a concurrent creator; the object is already there.
    #[error("already exists: {0}")]
    AlreadyExists(String),
    /// Transient lock conflict with a concurrent creator or writer
    /// (worth a bounded retry).
    #[error("lock contention: {0}")]
    LockContention(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
