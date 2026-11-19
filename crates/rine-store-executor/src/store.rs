use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use rine_etcd::{
    backend::{Backend, Event, KeyValue, LeaseId, ListOptions, Revision},
    error::{Error, Result},
};
use tokio::sync::{Mutex, broadcast};

use crate::{
    dialect::Dialect,
    executor::{Row, SqlExecutor, SqlValue},
};

/// MVCC event log over the `rine` table.
///
/// Assumes a single writer process. Physical row IDs link per-key history;
/// a batch can give several rows the same logical revision.
pub struct SqlBackend<E: SqlExecutor, D: Dialect> {
    exec: Arc<E>,
    tx: broadcast::Sender<Vec<Event>>,
    writes: Mutex<()>,
    _dialect: PhantomData<D>,
}

impl<E: SqlExecutor, D: Dialect> SqlBackend<E, D> {
    pub fn new(exec: Arc<E>) -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self {
            exec,
            tx,
            writes: Mutex::new(()),
            _dialect: PhantomData,
        }
    }

    /// Create the schema if the `rine` table is missing (server startup path).
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
            .query_row(&D::current_revision_sql(), vec![])
            .await?;
        match row {
            Some(r) if !r.is_null(0) => Ok(r.get_int(0)?.max(1)),
            _ => Ok(1),
        }
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
        if rev > 0 {
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

    async fn commit_inner(&self, base: Revision, events: Vec<Event>) -> Result<()> {
        if self.current_rev_inner().await? != base {
            return Err(Error::Conflict);
        }
        if events.is_empty() {
            return Ok(());
        }
        let last = self.exec.query_row(D::last_row_id_sql(), vec![]).await?;
        let mut id = match last {
            Some(row) if !row.is_null(0) => row.get_int(0)?.max(1),
            _ => 1,
        };
        let mut statements = Vec::new();
        for event in &events {
            let name = key_str(&event.key);
            if name == D::compacted_rev_key() {
                return Err(Error::Backend(
                    "key is reserved for compaction metadata".into(),
                ));
            }
            let row = self
                .exec
                .query_row(&D::latest_by_name_sql(), vec![SqlValue::Str(name.clone())])
                .await?;
            let previous = row.map(|row| row.get_int(0)).transpose()?.unwrap_or(0);
            id += 1;
            if event.prev_kv.is_some() {
                statements.push((
                    D::set_deleted_by_id_sql().into(),
                    vec![SqlValue::Int(id), SqlValue::Int(previous)],
                ));
            }
            let (create, lease, version, value) = match &event.kv {
                Some(kv) => (
                    kv.create_revision,
                    kv.lease,
                    kv.version,
                    SqlValue::Bytes(kv.value.to_vec()),
                ),
                None => (0, 0, 0, SqlValue::Null),
            };
            statements.push((
                D::insert_event_sql(),
                vec![
                    SqlValue::Int(id),
                    SqlValue::Str(name),
                    SqlValue::Int(event.revision),
                    SqlValue::Int(i64::from(event.deleted)),
                    SqlValue::Int(create),
                    SqlValue::Int(previous),
                    SqlValue::Int(lease),
                    SqlValue::Int(version),
                    value,
                ],
            ));
        }
        self.exec.execute_batch(statements).await?;
        let _ = self.tx.send(events);
        Ok(())
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
        mod_revision: logical_revision(row)?,
        version: row.get_int(7)?,
        lease: row.get_int(6)?,
    })
}

fn logical_revision(row: &Row) -> Result<Revision> {
    let revision = row.get_int(2)?;
    if revision > 0 {
        Ok(revision)
    } else {
        row.get_int(0)
    }
}

fn row_to_event(row: &Row) -> Result<Event> {
    // Only 1 is a tombstone; larger values link to the superseding row.
    let deleted = row.get_int(3)? == 1;
    let key = Bytes::from(row.get_str(1)?.into_bytes());
    Ok(Event {
        revision: logical_revision(row)?,
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

    async fn initialize(&self) -> Result<()> {
        self.ensure_schema().await
    }

    async fn get(&self, key: &[u8], revision: Revision) -> Result<(Revision, Option<KeyValue>)> {
        let (cur, rows, _) = self
            .list(
                key,
                None,
                ListOptions {
                    revision,
                    ..Default::default()
                },
            )
            .await?;
        Ok((cur, rows.into_iter().next()))
    }

    async fn create(&self, key: Bytes, value: Bytes, lease: LeaseId) -> Result<Revision> {
        let _guard = self.writes.lock().await;
        if self.live_row(&key).await?.is_some() {
            return Err(Error::KeyExists(key.to_vec()));
        }
        let base = self.current_rev_inner().await?;
        let revision = base + 1;
        let kv = KeyValue {
            key: key.clone(),
            value,
            create_revision: revision,
            mod_revision: revision,
            version: 1,
            lease,
        };
        self.commit_inner(
            base,
            vec![Event {
                revision,
                key,
                deleted: false,
                kv: Some(kv),
                prev_kv: None,
            }],
        )
        .await?;
        Ok(revision)
    }

    async fn update(
        &self,
        key: Bytes,
        value: Bytes,
        expected_mod_revision: Revision,
        lease: LeaseId,
    ) -> Result<Revision> {
        let _guard = self.writes.lock().await;
        let live = self.live_row(&key).await?;
        let Some(prev_row) = live else {
            return Err(Error::KeyExists(key.to_vec()));
        };
        let prev = row_to_kv(&prev_row)?;
        if expected_mod_revision != 0 && prev.mod_revision != expected_mod_revision {
            return Err(Error::Backend("mod revision mismatch".into()));
        }
        let base = self.current_rev_inner().await?;
        let revision = base + 1;
        let kv = KeyValue {
            key: key.clone(),
            value,
            create_revision: prev.create_revision,
            mod_revision: revision,
            version: prev.version + 1,
            lease,
        };
        self.commit_inner(
            base,
            vec![Event {
                revision,
                key,
                deleted: false,
                kv: Some(kv),
                prev_kv: Some(prev),
            }],
        )
        .await?;
        Ok(revision)
    }

    async fn delete(&self, key: &[u8]) -> Result<(Revision, Option<KeyValue>)> {
        let _guard = self.writes.lock().await;
        let base = self.current_rev_inner().await?;
        let prev = match self.live_row(key).await? {
            Some(r) => Some(row_to_kv(&r)?),
            None => None,
        };
        if prev.is_none() {
            return Ok((base, None));
        }
        let revision = base + 1;
        self.commit_inner(
            base,
            vec![Event {
                revision,
                key: Bytes::copy_from_slice(key),
                deleted: true,
                kv: None,
                prev_kv: prev.clone(),
            }],
        )
        .await?;
        Ok((revision, prev))
    }

    async fn commit(&self, base: Revision, events: Vec<Event>) -> Result<()> {
        let _guard = self.writes.lock().await;
        self.commit_inner(base, events).await
    }

    async fn list(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        let _guard = self.writes.lock().await;
        let cur = self.check_rev(opts.revision).await?;
        let (mut sql, mut params) = match range_end {
            None | Some([]) => (D::list_exact_sql(), vec![SqlValue::Str(key_str(prefix))]),
            Some([0]) => (D::list_ge_sql(), vec![SqlValue::Str(key_str(prefix))]),
            Some(end) => (
                D::list_range_sql(),
                vec![SqlValue::Str(key_str(prefix)), SqlValue::Str(key_str(end))],
            ),
        };
        if opts.revision > 0 && opts.revision < cur {
            sql = D::snapshot_sql(range_end);
            params.push(SqlValue::Int(opts.revision));
        }
        let rows = self.exec.query_all(&sql, params).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            if r.get_str(1)? != D::compacted_rev_key() {
                out.push(row_to_kv(r)?);
            }
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
        let _guard = self.writes.lock().await;
        let cur = self.current_rev_inner().await?;
        if revision > cur {
            return Err(Error::FutureRev {
                requested: revision,
                current: cur,
            });
        }
        let compacted = self.compacted_inner().await?;
        if revision <= compacted {
            return Err(Error::Compacted {
                requested: revision,
                compacted,
            });
        }
        let watermark = if compacted == 0 {
            D::set_compact_rev_insert_sql()
        } else {
            D::set_compact_rev_sql()
        };
        self.exec
            .execute_batch(vec![
                (watermark.into(), vec![SqlValue::Int(revision)]),
                (
                    D::compact_delete_sql().into(),
                    vec![SqlValue::Int(revision), SqlValue::Int(revision)],
                ),
            ])
            .await?;
        Ok(cur)
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
        let _guard = self.writes.lock().await;
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
            let mut ev = row_to_event(r)?;
            let parent = r.get_int(5)?;
            if parent > 0
                && let Some(prev) = self
                    .exec
                    .query_row(&D::predecessor_sql(), vec![SqlValue::Int(parent)])
                    .await?
                && prev.get_int(3)? != 1
            {
                ev.prev_kv = Some(row_to_kv(&prev)?);
            }
            out.push(ev);
        }
        Ok(out)
    }

    fn subscribe(&self) -> broadcast::Receiver<Vec<Event>> {
        self.tx.subscribe()
    }
}

impl<E: SqlExecutor, D: Dialect> std::fmt::Debug for SqlBackend<E, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SqlBackend")
    }
}
