use crate::dialect::OracleDialect;
use rine_store_executor::SqlBackend;

/// Oracle-flavored [`SqlBackend`].
pub type OracleBackend<E> = SqlBackend<E, OracleDialect>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use rine_etcd::{Backend as _, backend::ListOptions};

    use super::*;
    use crate::mock::MockExec;

    fn backend() -> OracleBackend<MockExec> {
        OracleBackend::new(Arc::new(MockExec::new()))
    }

    #[tokio::test]
    async fn ensure_schema_is_noop_when_table_exists() {
        let b = backend();
        b.ensure_schema().await.unwrap();
        // Still functional afterwards.
        b.create(Bytes::from("a"), Bytes::from("1"), 0)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn sql_crud() {
        let b = backend();
        let r1 = b
            .create(Bytes::from("a"), Bytes::from("1"), 0)
            .await
            .unwrap();
        assert!(
            b.create(Bytes::from("a"), Bytes::from("x"), 0)
                .await
                .is_err()
        );
        let (_, kv) = b.get(b"a", 0).await.unwrap();
        assert_eq!(kv.unwrap().value, Bytes::from("1"));
        b.update(Bytes::from("a"), Bytes::from("2"), 0, 0)
            .await
            .unwrap();
        let (_, kvs, count) = b.list(b"a", None, ListOptions::default()).await.unwrap();
        assert_eq!(count, 1);
        assert_eq!(kvs[0].version, 2);
        assert_eq!(kvs[0].create_revision, r1);
        let _ = r1;
        b.delete(b"a").await.unwrap();
        assert!(b.get(b"a", 0).await.unwrap().1.is_none());
    }

    #[tokio::test]
    async fn sql_history_compact_events() {
        let b = backend();
        b.create(Bytes::from("k"), Bytes::from("v1"), 0)
            .await
            .unwrap();
        let r2 = b
            .update(Bytes::from("k"), Bytes::from("v2"), 0, 0)
            .await
            .unwrap();
        let (_, kv) = b.get(b"k", r2 - 1).await.unwrap();
        assert_eq!(kv.unwrap().value, Bytes::from("v1"));
        let evs = b.events(r2 - 1, r2).await.unwrap();
        assert_eq!(evs.len(), 2);
        b.compact(r2).await.unwrap();
        assert!(b.get(b"k", r2 - 1).await.is_err());
        // live row survives compaction as the historical base
        let (_, kv) = b.get(b"k", 0).await.unwrap();
        assert_eq!(kv.unwrap().value, Bytes::from("v2"));
        assert!(b.events(r2 - 1, r2).await.is_err());
    }

    #[tokio::test]
    async fn sql_watch_stream() {
        let b = backend();
        let mut rx = b.subscribe();
        b.create(Bytes::from("w"), Bytes::from("1"), 0)
            .await
            .unwrap();
        let ev = rx.recv().await.unwrap();
        assert!(!ev.deleted);
        assert_eq!(ev.kv.unwrap().version, 1);
    }
}
