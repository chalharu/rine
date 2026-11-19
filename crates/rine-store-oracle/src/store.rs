#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use rine_etcd::{Backend as _, backend::ListOptions};

    use crate::{OracleBackendMock, mock::MockExec};

    fn backend() -> OracleBackendMock {
        OracleBackendMock::new(Arc::new(MockExec::new()))
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
        let ev = rx.recv().await.unwrap().pop().unwrap();
        assert!(!ev.deleted);
        assert_eq!(ev.kv.unwrap().version, 1);
    }

    #[tokio::test]
    async fn commit_validates_point_reads_per_key() {
        use rine_etcd::{backend::ReadSet, error::Error};

        let b = backend();
        b.ensure_schema().await.unwrap();
        // Unrelated writes never conflict: an empty read set commits cleanly
        // no matter how far the revision moved.
        let r1 = b.commit(ReadSet::default(), vec![]).await.unwrap();
        b.create(Bytes::from("other"), Bytes::from("v"), 0)
            .await
            .unwrap();
        b.commit(ReadSet::default(), vec![]).await.unwrap();

        // A guarded key that moved is a conflict.
        let (_, kv) = b.get(b"other", 0).await.unwrap();
        let guard = ReadSet {
            points: vec![(Bytes::from("other"), kv.map(|kv| kv.mod_revision))],
            ranges: Vec::new(),
        };
        b.commit(guard.clone(), vec![]).await.unwrap();
        b.update(Bytes::from("other"), Bytes::from("v2"), 0, 0)
            .await
            .unwrap();
        let err = b.commit(guard, vec![]).await.unwrap_err();
        assert!(matches!(err, Error::Conflict));
        // Absent-key guards work the same way.
        let missing = ReadSet {
            points: vec![(Bytes::from("gone"), None)],
            ranges: Vec::new(),
        };
        b.commit(missing.clone(), vec![]).await.unwrap();
        b.create(Bytes::from("gone"), Bytes::from("v"), 0)
            .await
            .unwrap();
        let err = b.commit(missing, vec![]).await.unwrap_err();
        assert!(matches!(err, Error::Conflict));
        let _ = r1;
    }

    #[tokio::test]
    async fn commit_duplicate_events_conflict_atomically() {
        use rine_etcd::{
            backend::{Event, KeyValue, ReadSet},
            error::Error,
        };

        let b = backend();
        b.ensure_schema().await.unwrap();
        let cur = b.current_revision().await;
        let put = || Event {
            revision: cur + 1,
            key: Bytes::from("k"),
            deleted: false,
            kv: Some(KeyValue {
                key: Bytes::from("k"),
                value: Bytes::from("v"),
                create_revision: cur + 1,
                mod_revision: cur + 1,
                version: 1,
                lease: 0,
            }),
            prev_kv: None,
        };
        // Same key twice in one batch violates the (name, prev_revision)
        // unique index on the second row.
        let err = b
            .commit(ReadSet::default(), vec![put(), put()])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Conflict));
        // The failed batch left nothing behind.
        assert!(b.get(b"k", 0).await.unwrap().1.is_none());
    }
}
