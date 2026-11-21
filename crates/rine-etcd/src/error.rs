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

/// A `spawn_blocking` join failure with the operation that spawned it.
/// Keeps the original error (e.g. [`tokio::task::JoinError`]) as the
/// [`source`](std::error::Error::source) instead of stringifying it, so
/// `is_cancelled` / `is_panic` / downcasting still work downstream.
#[derive(Debug, thiserror::Error)]
#[error("{what}: {error}")]
struct SpawnFailed {
    what: &'static str,
    #[source]
    error: Box<dyn std::error::Error + Send + Sync>,
}

impl Error {
    /// Box a task-join failure with an operation label, preserving the
    /// original error in the source chain.
    pub fn task(what: &'static str, error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Error::Backend(Box::new(SpawnFailed {
            what,
            error: Box::new(error),
        }))
    }
}

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
        Error::KeyExists(_) => tonic::Status::already_exists("key already exists"),
        Error::Backend(s) => tonic::Status::internal(s.to_string()),
        Error::TableMissing(s) | Error::AlreadyExists(s) | Error::LockContention(s) => {
            tonic::Status::internal(s)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Join failures keep their context label in the message and the
    /// original `JoinError` reachable through the source chain.
    #[tokio::test]
    async fn task_failure_keeps_context_and_source() {
        let join = tokio::task::spawn_blocking(|| panic!("boom"))
            .await
            .unwrap_err();
        assert!(join.is_panic());
        let err = Error::task("db task failed", join);
        let text = err.to_string();
        assert!(text.contains("db task failed"), "{text}");
        assert!(text.contains("panicked"), "{text}");
        let source = std::error::Error::source(&err).expect("source");
        let inner = std::error::Error::source(source).expect("inner source");
        assert!(
            inner
                .downcast_ref::<tokio::task::JoinError>()
                .is_some_and(|j| j.is_panic()),
            "{inner:?}"
        );
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
            status_err(Error::KeyExists(vec![1])).code(),
            tonic::Code::AlreadyExists
        );
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
