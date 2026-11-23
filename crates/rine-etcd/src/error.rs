use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("revision compacted: requested {requested}, compacted {compacted}")]
    Compacted { requested: i64, compacted: i64 },
    #[error("future revision: requested {requested}, current {current}")]
    FutureRev { requested: i64, current: i64 },
    #[error("storage revision changed during transaction")]
    Conflict,
    /// Pass-through for errors the backend does not classify: the original
    /// driver error is boxed verbatim (message, chain and downcastability
    /// preserved), never re-stringified and re-parsed.
    #[error("backend: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),
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

/// Map storage errors to etcd API statuses. Lives here (not in `kv`) so
/// `watch` does not depend on `kv` for error text.
pub fn status_err(e: Error) -> tonic::Status {
    match e {
        Error::Compacted { .. } => {
            tonic::Status::out_of_range("etcdserver: mvcc: required revision has been compacted")
        }
        Error::FutureRev { .. } => {
            tonic::Status::out_of_range("etcdserver: mvcc: required revision is a future revision")
        }
        Error::Conflict => tonic::Status::aborted("storage revision changed during transaction"),
        Error::Backend(s) => tonic::Status::internal(s.to_string()),
        Error::TableMissing(s) | Error::AlreadyExists(s) | Error::LockContention(s) => {
            tonic::Status::internal(s)
        }
    }
}

#[cfg(test)]
mod status_tests {
    use super::*;

    #[test]
    fn storage_errors_map_to_status_codes() {
        assert_eq!(
            status_err(Error::FutureRev {
                requested: 9,
                current: 2
            })
            .code(),
            tonic::Code::OutOfRange
        );
        assert_eq!(status_err(Error::Conflict).code(), tonic::Code::Aborted);
        assert_eq!(
            status_err(Error::Backend("x".into())).code(),
            tonic::Code::Internal
        );
        assert_eq!(
            status_err(Error::TableMissing("t".into())).code(),
            tonic::Code::Internal
        );
        assert_eq!(
            status_err(Error::AlreadyExists("i".into())).code(),
            tonic::Code::Internal
        );
        assert_eq!(
            status_err(Error::LockContention("l".into())).code(),
            tonic::Code::Internal
        );
    }
}
