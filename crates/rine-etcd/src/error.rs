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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
