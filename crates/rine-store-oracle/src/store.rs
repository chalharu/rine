use crate::dialect::{Dialect, OracleDialect};
use crate::executor::{Row, SqlExecutor, SqlValue};
use async_trait::async_trait;
use bytes::Bytes;
use rine_etcd::backend::{Backend, Event, KeyValue, LeaseId, ListOptions, Revision};
use rine_etcd::error::{Error, Result};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::broadcast;

/// kine-style append-only `kine` table over any SQL database.
///
/// Assumes a single writer (like kine on SQLite): ids come from an
/// auto-increment column, so the newest row for a key resolves via the
/// `(name, prev_revision)` unique index. Multi-writer leader election is
/// future work; concurrent writers surface as `Backend` errors, never as
/// silent corruption.
pub struct SqlBackend<E: SqlExecutor, D: Dialect> {
    exec: Arc<E>,
    tx: broadcast::Sender<Event>,
    _dialect: PhantomData<D>,
}

/// Oracle-flavored [`SqlBackend`].
pub type OracleBackend<E> = SqlBackend<E, OracleDialect>;

impl<E: SqlExecutor, D: Dialect> SqlBackend<E, D> {
    pub fn new(exec: Arc<E>) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            exec,
            tx,
            _dialect: PhantomData,
        }
    }

    /// Create the schema if the `kine` table is missing (server startup path).
    /// A concurrent creator racing us surfaces an already-exists error,
    /// which is tolerated.
    pub async fn ensure_schema(&self) -> Result<()> {
        match self.exec.query_row(D::probe_sql(), vec![]).await {
            Ok(_) => Ok(()),
            Err(Error::Backend(m)) if D::is_missing_table(&m) => match self.create_schema().await {
                Err(Error::Backend(m)) if D::is_already_exists(&m) => Ok(()),
                r => r,
            },
            Err(e) => Err(e),
        }
    }

    /// Run the DDL. Idempotent only when the table does not exist yet;
    /// migrations are out of scope for now.
    pub async fn create_schema(&self) -> Result<()> {
        for stmt in
            std::iter::once(D::create_table_sql()).chain(D::create_indexes_sql().iter().copied())
        {
            self.exec.execute(stmt, vec![]).await?;
        }
        Ok(())
    }

    async fn current_rev_inner(&self) -> Result<Revision> {
        let row = self
            .exec
            .query_row(D::current_revision_sql(), vec![])
            .await?;
        Ok(row.map(|r| r.get_int(0).unwrap_or(1)).unwrap_or(1))
    }

    async fn compacted_inner(&self) -> Result<Revision> {
        let row = self.exec.query_row(D::compacted_rev_sql(), vec![]).await?;
        Ok(match row {
            Some(r) if !r.is_null(0) => r.get_int(0).unwrap_or(0),
            _ => 0,
        })
    }

    async fn check_rev(&self, rev: Revision) -> Result<Revision> {
        let cur = self.current_rev_inner().await?;
        if rev != 0 {
            let c = self.compacted_inner().await?;
            if rev < c {
                return Err(Error::Compacted {
                    requested: rev,
                    compacted: c,
                });
            }
            if rev > cur {
                return Err(Error::FutureRev {
                    requested: rev,
                    current: cur,
                });
            }
        }
        Ok(cur)
    }

    async fn live_row(&self, key: &[u8]) -> Result<Option<Row>> {
        self.exec
            .query_row(&D::live_by_name_sql(), vec![SqlValue::Str(key_str(key))])
            .await
    }

    async fn resolve_id(&self, key: &[u8], prev_revision: Revision) -> Result<Revision> {
        let row = self
            .exec
            .query_row(
                D::resolve_id_sql(),
                vec![SqlValue::Str(key_str(key)), SqlValue::Int(prev_revision)],
            )
            .await?;
        row.map(|r| r.get_int(0)).transpose()?.ok_or_else(|| {
            Error::Backend(format!(
                "inserted row for {key:?} not found via (name, prev_revision)"
            ))
        })
    }

    fn emit(&self, ev: Event) {
        let _ = self.tx.send(ev);
    }
}

fn key_str(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

fn row_to_kv(row: &Row) -> Result<KeyValue> {
    Ok(KeyValue {
        key: Bytes::from(row.get_str(1)?.into_bytes()),
        value: Bytes::from(row.get_bytes(8)?),
        create_revision: row.get_int(4)?,
        mod_revision: row.get_int(0)?,
        version: row.get_int(7)?,
        lease: row.get_int(6)?,
    })
}

fn row_to_event(row: &Row) -> Result<Event> {
    let deleted = row.get_int(3)? != 0;
    let key = Bytes::from(row.get_str(1)?.into_bytes());
    Ok(Event {
        revision: row.get_int(0)?,
        key,
        deleted,
        kv: if deleted { None } else { Some(row_to_kv(row)?) },
        prev_kv: None,
    })
}

#[async_trait]
impl<E: SqlExecutor + 'static, D: Dialect> Backend for SqlBackend<E, D> {
    async fn current_revision(&self) -> Revision {
        self.current_rev_inner().await.unwrap_or(1)
    }

    async fn compacted_revision(&self) -> Revision {
        self.compacted_inner().await.unwrap_or(0)
    }

    async fn get(&self, key: &[u8], revision: Revision) -> Result<(Revision, Option<KeyValue>)> {
        let cur = self.check_rev(revision).await?;
        if revision == 0 || revision == cur {
            let kv = match self.live_row(key).await? {
                Some(r) => Some(row_to_kv(&r)?),
                None => None,
            };
            return Ok((cur, kv));
        }
        let rows = self
            .exec
            .query_all(
                &D::history_by_name_sql(),
                vec![SqlValue::Str(key_str(key)), SqlValue::Int(revision)],
            )
            .await?;
        let kv = match rows.first() {
            Some(r) => {
                let deleted = r.get_int(3)?;
                if deleted == 0 || deleted > revision {
                    Some(row_to_kv(r)?)
                } else {
                    None
                }
            }
            None => None,
        };
        Ok((cur, kv))
    }

    async fn create(&self, key: Bytes, value: Bytes, lease: LeaseId) -> Result<Revision> {
        if self.live_row(&key).await?.is_some() {
            return Err(Error::KeyExists(key.to_vec()));
        }
        self.exec
            .execute(
                D::insert_sql(),
                vec![
                    SqlValue::Str(key_str(&key)),
                    SqlValue::Int(0),
                    SqlValue::Int(0),
                    SqlValue::Int(0),
                    SqlValue::Int(0),
                    SqlValue::Int(lease),
                    SqlValue::Int(1),
                    SqlValue::Bytes(value.to_vec()),
                ],
            )
            .await?;
        let id = self.resolve_id(&key, 0).await?;
        self.exec
            .execute(
                D::set_create_rev_sql(),
                vec![SqlValue::Int(id), SqlValue::Int(id)],
            )
            .await?;
        let kv = KeyValue {
            key: key.clone(),
            value,
            create_revision: id,
            mod_revision: id,
            version: 1,
            lease,
        };
        self.emit(Event {
            revision: id,
            key,
            deleted: false,
            kv: Some(kv),
            prev_kv: None,
        });
        Ok(id)
    }

    async fn update(
        &self,
        key: Bytes,
        value: Bytes,
        expected_mod_revision: Revision,
        lease: LeaseId,
    ) -> Result<Revision> {
        let live = self.live_row(&key).await?;
        let Some(prev_row) = live else {
            return Err(Error::KeyExists(key.to_vec()));
        };
        let prev = row_to_kv(&prev_row)?;
        if expected_mod_revision != 0 && prev.mod_revision != expected_mod_revision {
            return Err(Error::Backend("mod revision mismatch".into()));
        }
        self.exec
            .execute(
                D::insert_sql(),
                vec![
                    SqlValue::Str(key_str(&key)),
                    SqlValue::Int(0),
                    SqlValue::Int(0),
                    SqlValue::Int(prev.create_revision),
                    SqlValue::Int(prev.mod_revision),
                    SqlValue::Int(lease),
                    SqlValue::Int(prev.version + 1),
                    SqlValue::Bytes(value.to_vec()),
                ],
            )
            .await?;
        let id = self.resolve_id(&key, prev.mod_revision).await?;
        let n = self
            .exec
            .execute(
                D::set_deleted_by_id_sql(),
                vec![SqlValue::Int(id), SqlValue::Int(prev.mod_revision)],
            )
            .await?;
        if n == 0 {
            return Err(Error::Backend("concurrent modification".into()));
        }
        let kv = KeyValue {
            key: key.clone(),
            value,
            create_revision: prev.create_revision,
            mod_revision: id,
            version: prev.version + 1,
            lease,
        };
        self.emit(Event {
            revision: id,
            key,
            deleted: false,
            kv: Some(kv),
            prev_kv: Some(prev),
        });
        Ok(id)
    }

    async fn delete(&self, key: &[u8]) -> Result<(Revision, Option<KeyValue>)> {
        let prev = match self.live_row(key).await? {
            Some(r) => Some(row_to_kv(&r)?),
            None => None,
        };
        let (parent, create, version) = match &prev {
            Some(p) => (p.mod_revision, p.create_revision, p.version + 1),
            None => (-1, 0, 1),
        };
        self.exec
            .execute(
                D::insert_sql(),
                vec![
                    SqlValue::Str(key_str(key)),
                    SqlValue::Int(0),
                    SqlValue::Int(1),
                    SqlValue::Int(create),
                    SqlValue::Int(parent),
                    SqlValue::Int(0),
                    SqlValue::Int(version),
                    SqlValue::Null,
                ],
            )
            .await?;
        let tomb_id = self.resolve_id(key, parent).await?;
        if let Some(p) = &prev {
            self.exec
                .execute(
                    D::set_deleted_by_id_sql(),
                    vec![SqlValue::Int(tomb_id), SqlValue::Int(p.mod_revision)],
                )
                .await?;
        }
        self.emit(Event {
            revision: tomb_id,
            key: Bytes::copy_from_slice(key),
            deleted: true,
            kv: None,
            prev_kv: prev.clone(),
        });
        Ok((tomb_id, prev))
    }

    async fn list(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        let cur = self.check_rev(opts.revision).await?;
        let (sql, params) = match range_end {
            None | Some([]) => (D::list_exact_sql(), vec![SqlValue::Str(key_str(prefix))]),
            Some([0]) => (D::list_ge_sql(), vec![SqlValue::Str(key_str(prefix))]),
            Some(end) => (
                D::list_range_sql(),
                vec![SqlValue::Str(key_str(prefix)), SqlValue::Str(key_str(end))],
            ),
        };
        let rows = self.exec.query_all(&sql, params).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            out.push(row_to_kv(r)?);
        }
        let count = out.len() as i64;
        if opts.count_only {
            return Ok((cur, Vec::new(), count));
        }
        if opts.limit > 0 && (out.len() as i64) > opts.limit {
            out.truncate(opts.limit as usize);
        }
        if opts.keys_only {
            for kv in &mut out {
                kv.value = Bytes::new();
            }
        }
        Ok((cur, out, count))
    }

    async fn compact(&self, revision: Revision) -> Result<Revision> {
        let cur = self.current_rev_inner().await?;
        if revision > cur {
            return Err(Error::FutureRev {
                requested: revision,
                current: cur,
            });
        }
        self.exec
            .execute(
                D::set_compact_rev_sql(),
                vec![SqlValue::Int(revision), SqlValue::Int(revision)],
            )
            .await?;
        self.exec
            .execute(
                D::compact_delete_sql(),
                vec![SqlValue::Int(revision), SqlValue::Int(revision)],
            )
            .await?;
        Ok(revision)
    }

    async fn db_size(&self) -> i64 {
        self.exec
            .query_row(D::db_size_sql(), vec![])
            .await
            .ok()
            .flatten()
            .and_then(|r| r.get_int(0).ok())
            .unwrap_or(0)
    }

    async fn events(&self, from: Revision, to: Revision) -> Result<Vec<Event>> {
        let c = self.compacted_inner().await?;
        if from <= c {
            return Err(Error::Compacted {
                requested: from,
                compacted: c,
            });
        }
        let rows = self
            .exec
            .query_all(
                &D::events_sql(),
                vec![SqlValue::Int(from), SqlValue::Int(to)],
            )
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            if r.get_str(1)
                .map(|s| s.as_str() == crate::ddl::COMPACT_REV_KEY)
                .unwrap_or(false)
            {
                continue;
            }
            let mut ev = row_to_event(r)?;
            if ev.deleted {
                // Attach predecessor snapshot for prev_kv fidelity.
                if r.get_int(0).unwrap_or(0) > 1 {
                    let pred = self
                        .exec
                        .query_all(
                            &D::history_by_name_sql(),
                            vec![SqlValue::Str(r.get_str(1)?), SqlValue::Int(ev.revision - 1)],
                        )
                        .await?;
                    if let Some(p) = pred.first() {
                        if p.get_int(3).unwrap_or(1) == 0 {
                            ev.prev_kv = Some(row_to_kv(p)?);
                        }
                    }
                }
            }
            out.push(ev);
        }
        Ok(out)
    }

    fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.tx.subscribe()
    }
}

impl<E: SqlExecutor, D: Dialect> std::fmt::Debug for SqlBackend<E, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqlBackend")
    }
}

#[cfg(test)]
mod tests {
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
        assert!(b
            .create(Bytes::from("a"), Bytes::from("x"), 0)
            .await
            .is_err());
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
