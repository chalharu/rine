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
#[derive(Debug)]
struct SpawnFailed {
    what: &'static str,
    error: Box<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Display for SpawnFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.what, self.error)
    }
}

impl std::error::Error for SpawnFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.error)
    }
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
