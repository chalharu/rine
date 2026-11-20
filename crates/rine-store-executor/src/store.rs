use std::{
    collections::{HashMap, HashSet},
    future::Future,
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use rine_etcd::{
    backend::{
        BROADCAST_CAP, Backend, Event, KeyValue, LeaseId, LeaseState, ListOptions, ObservedRange,
        ReadSet, Revision,
    },
    error::{Error, Result},
};
use tokio::sync::{Mutex, broadcast};

use crate::{
    dialect::Dialect,
    executor::{Row, SqlExecutor, SqlValue},
};

/// MVCC event log over the `rine` table.
///
/// Physical row IDs link per-key history; a batch can give several rows the
/// same logical revision. Commits validate the transaction's read set under a
/// database commit lock, then assign `MAX(revision) + 1`, so writers on any
/// node serialize without false conflicts on untouched keys.
pub struct SqlBackend<E: SqlExecutor, D: Dialect> {
    exec: Arc<E>,
    tx: broadcast::Sender<Vec<Event>>,
    writes: Arc<Mutex<()>>,
    _dialect: PhantomData<D>,
}

impl<E: SqlExecutor, D: Dialect> Clone for SqlBackend<E, D> {
    fn clone(&self) -> Self {
        Self {
            exec: self.exec.clone(),
            tx: self.tx.clone(),
            writes: self.writes.clone(),
            _dialect: PhantomData,
        }
    }
}

/// Maximum keys per commit-lookup statement (Oracle caps `IN` lists at 1000;
/// larger batches run the same statement once per chunk and merge).
const COMMIT_NAMES_PER_QUERY: usize = 1000;

impl<E: SqlExecutor, D: Dialect> SqlBackend<E, D> {
    pub fn new(exec: Arc<E>) -> Self {
        let (tx, _) = broadcast::channel(BROADCAST_CAP);
        Self {
            exec,
            tx,
            writes: Arc::new(Mutex::new(())),
            _dialect: PhantomData,
        }
    }

    /// Bring the schema to the state the backend needs (server startup path).
    /// A concurrent creator racing us surfaces `AlreadyExists`, which is
    /// tolerated. Classification happens in the driver (`cvt_error`), so the
    /// generic code matches typed variants instead of parsing messages.
    ///
    /// A concurrent DROP can pull the table from under a single pass,
    /// surfacing `TableMissing` from an index statement or probe. That is
    /// transient by definition — someone else is converging the schema — so
    /// the whole pass restarts on `TableMissing` (bounded). Anything else
    /// fails fast.
    pub async fn ensure_schema(&self) -> Result<()> {
        let mut delay = Duration::from_millis(50);
        for _ in 0..10 {
            match self.ensure_schema_once().await {
                Ok(()) => return Ok(()),
                Err(Error::TableMissing(_)) => {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
                Err(e) => return Err(e),
            }
        }
        self.ensure_schema_once().await
    }

    /// Probe one table, creating it when absent. A concurrent creator's
    /// `AlreadyExists` converges; anything else fails fast.
    async fn ensure_table(&self, probe: &str, create: &str) -> Result<()> {
        match self.exec.query_row(probe, vec![]).await {
            Ok(_) => Ok(()),
            Err(Error::TableMissing(_)) => self.create_statement_with_retry(create).await,
            Err(e) => Err(e),
        }
    }

    async fn ensure_schema_once(&self) -> Result<()> {
        self.ensure_table(D::probe_sql(), D::create_table_sql())
            .await?;
        // One index list for fresh and pre-existing tables alike: re-running
        // tolerates a concurrent creator via `AlreadyExists`, so tables
        // created before an index existed converge here instead of failing
        // or running degraded.
        for stmt in D::create_indexes_sql() {
            self.create_statement_with_retry(stmt).await?;
        }
        // Durable leases live beside the event log; a pre-existing database
        // from before leases converges here the same way.
        self.ensure_table(D::leases_probe_sql(), D::create_leases_table_sql())
            .await
    }

    /// Run the DDL. Each statement tolerates a concurrent creator
    /// (already-exists) and retries a transient lock conflict with a
    /// concurrent writer (e.g. `CREATE INDEX` racing DML on a fresh database),
    /// so simultaneous startups on any number of nodes converge.
    /// Migrations are out of scope for now.
    async fn create_statement_with_retry(&self, stmt: &str) -> Result<()> {
        let mut delay = Duration::from_millis(50);
        for _ in 0..10 {
            match self.exec.execute(stmt, vec![]).await {
                Ok(_) => return Ok(()),
                Err(Error::AlreadyExists(_)) => return Ok(()),
                Err(Error::LockContention(_)) => {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
                Err(e) => return Err(e),
            }
        }
        self.exec.execute(stmt, vec![]).await.map(|_| ())
    }

    /// Single-integer watermark (`current_revision`, `compacted_revision`):
    /// missing or NULL reads as `default` (fresh table, absent watermark row).
    async fn watermark(&self, sql: &str, default: Revision) -> Result<Revision> {
        match self.exec.query_row(sql, vec![]).await? {
            Some(r) if !r.is_null(0) => Ok(r.get_int(0)?),
            _ => Ok(default),
        }
    }

    async fn current_rev_inner(&self) -> Result<Revision> {
        self.watermark(&D::current_revision_sql(), 1)
            .await
            .map(|v| v.max(1))
    }

    async fn compacted_inner(&self) -> Result<Revision> {
        self.watermark(D::compacted_rev_sql(), 0).await
    }

    /// First column of the first row, or `default` when the query is empty
    /// (fresh table). A present but NULL cell is a schema bug, not emptiness.
    fn cell(rows: Option<Vec<Row>>, default: i64) -> Result<i64> {
        match rows.and_then(|r| r.into_iter().next()) {
            Some(r) => r.get_int(0),
            None => Ok(default),
        }
    }

    async fn check_rev(&self, rev: Revision) -> Result<Revision> {
        // Both watermarks in one trip (a single blocking call on Oracle).
        let mut results = self
            .exec
            .query_batch(vec![
                (D::current_revision_sql(), vec![]),
                (D::compacted_rev_sql().into(), vec![]),
            ])
            .await?
            .into_iter();
        let cur = Self::cell(results.next(), 1)?.max(1);
        if rev > 0 {
            let c = Self::cell(results.next(), 0)?;
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

    /// Backend-level range/list read without taking the write mutex. Reads the
    /// committed state; inside a commit unit it observes the unit's snapshot.
    async fn list_inner(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: &ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        if opts.revision <= 0 {
            return self.list_latest(prefix, range_end, opts).await;
        }
        self.list_history(prefix, range_end, opts).await
    }

    /// Latest reads ride one read transaction (revision + rows + count
    /// in a single `query_batch`): rows come back in index order with
    /// LIMIT, the count (when needed) from a covering index — no
    /// `COUNT(*) OVER ()` sort over the full range.
    async fn list_latest(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: &ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        let (rows_sql, count_sql) = D::live_range_queries(range_end, opts);
        let mut params = vec![SqlValue::Str(key_str(prefix))];
        if let Some(end) = range_end.filter(|end| !end.is_empty() && *end != [0]) {
            params.push(SqlValue::Str(key_str(end)));
        }
        let has_rows = rows_sql.is_some();
        let has_count = count_sql.is_some();
        let mut queries = vec![(D::current_revision_sql(), vec![])];
        if let Some(sql) = rows_sql {
            let mut binds = params.clone();
            if opts.limit > 0 {
                binds.push(SqlValue::Int(opts.limit));
            }
            queries.push((sql, binds));
        }
        if let Some(sql) = count_sql {
            queries.push((sql, params));
        }
        let mut results = self.exec.query_batch(queries).await?.into_iter();
        let revision = Self::cell(results.next(), 1)?.max(1);
        let mut kvs = Vec::new();
        if has_rows {
            let rows = results
                .next()
                .ok_or_else(|| Error::Backend("missing range result".into()))?;
            kvs.reserve(rows.len());
            for r in &rows {
                kvs.push(row_to_kv(r)?);
            }
        }
        let total = if has_count {
            results
                .next()
                .and_then(|rows| rows.into_iter().next())
                .ok_or_else(|| Error::Backend("missing range count".into()))?
                .get_int(0)?
        } else {
            kvs.len() as i64
        };
        Ok((revision, kvs, total))
    }

    async fn list_history(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: &ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        let cur = self.check_rev(opts.revision).await?;
        let mut sql = D::list_history_sql(range_end);
        let mut params = vec![SqlValue::Str(key_str(prefix))];
        if let Some(end) = range_end.filter(|end| !end.is_empty() && *end != [0]) {
            params.push(SqlValue::Str(key_str(end)));
        }
        if opts.revision > 0 && opts.revision < cur {
            sql = D::snapshot_sql(range_end);
            params.push(SqlValue::Int(opts.revision));
        }
        let rows = self.exec.query_all(&sql, params).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            if r.get_str(1)? != "compact_rev_key" {
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

    /// Revalidate one observed range against the current committed state.
    async fn check_range(&self, observed: &ObservedRange) -> Result<()> {
        let (_, kvs, count) = self
            .list_inner(
                &observed.prefix,
                observed.range_end.as_deref(),
                &observed.opts,
            )
            .await?;
        let snapshot: Vec<(bytes::Bytes, Revision)> = kvs
            .iter()
            .map(|kv| (kv.key.clone(), kv.mod_revision))
            .collect();
        if !observed.matches_snapshot(&snapshot, count) {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// Validate `reads`, assign `MAX(revision) + 1` to `events`, and write
    /// them. The caller holds the commit unit (cross-node lock) across this.
    ///
    /// Guard reads (including keys this batch also writes, the k8s
    /// compare-and-write-same-key pattern) are validated against one batched
    /// per-key snapshot, so no key costs an extra round trip.
    async fn commit_guarded(
        &self,
        reads: &ReadSet,
        events: Vec<Event>,
    ) -> Result<(Revision, Vec<Event>)> {
        // First observation wins, mirroring the transaction's own recording.
        let mut expected: HashMap<&[u8], Option<Revision>> = HashMap::new();
        for (key, rev) in &reads.points {
            expected.entry(key.as_ref()).or_insert(*rev);
        }
        let written: HashSet<&[u8]> = events.iter().map(|e| e.key.as_ref()).collect();
        let names: Vec<String> = expected
            .keys()
            .copied()
            .chain(written.iter().copied())
            .collect::<HashSet<_>>()
            .into_iter()
            .map(key_str)
            .collect();
        // One lookup per chunk (empty commits still read the metadata row).
        // The newest physical row carries the highest logical revision.
        let mut chunks: Vec<&[String]> = names.chunks(COMMIT_NAMES_PER_QUERY).collect();
        if chunks.is_empty() {
            chunks.push(&[]);
        }
        let mut latest = HashMap::new();
        let (mut fresh, mut id) = (0, 0);
        for chunk in chunks {
            let rows = self
                .exec
                .query_all(
                    &D::commit_state_sql(chunk.len()),
                    chunk
                        .iter()
                        .map(|name| SqlValue::Str(name.clone()))
                        .collect(),
                )
                .await?;
            let meta = rows
                .first()
                .ok_or_else(|| Error::Backend("missing commit metadata".into()))?;
            fresh = meta.get_int(0)? + 1;
            id = meta.get_int(1)?;
            for row in &rows {
                if !row.is_null(2) {
                    let live = if row.is_null(4) {
                        None
                    } else {
                        Some(row.get_int(4)?)
                    };
                    latest.insert(row.get_str(2)?, (row.get_int(3)?, live));
                }
            }
        }
        for (key, rev) in &expected {
            if latest.get(&key_str(key)).and_then(|(_, live)| *live) != *rev {
                return Err(Error::Conflict);
            }
        }
        for range in &reads.ranges {
            self.check_range(range).await?;
        }
        if events.is_empty() {
            return Ok((fresh - 1, events));
        }
        let mut statements = Vec::new();
        for event in &events {
            let name = key_str(&event.key);
            if name == "compact_rev_key" {
                return Err(Error::Backend(
                    "key is reserved for compaction metadata".into(),
                ));
            }
            let previous = latest.get(&name).map_or(0, |(id, _)| *id);
            id += 1;
            if event.prev_kv.is_some() {
                statements.push((
                    D::set_deleted_by_id_sql().into(),
                    vec![SqlValue::Int(id), SqlValue::Int(previous)],
                ));
            }
            let (create, lease, version, value) = match &event.kv {
                Some(kv) => (
                    if kv.create_revision == event.revision {
                        fresh
                    } else {
                        kv.create_revision
                    },
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
                    SqlValue::Int(fresh),
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
        let rebased = events
            .into_iter()
            .map(|event| rebase(event, fresh))
            .collect();
        Ok((fresh, rebased))
    }

    /// Run one commit unit to completion: the writes lock, `begin_unit`,
    /// validation, writes and `end_unit` always pair up, even if the caller
    /// drops the [`commit`](Backend::commit) future that spawned this.
    async fn commit_inner(&self, reads: ReadSet, events: Vec<Event>) -> Result<Revision> {
        let _guard = self.writes.lock().await;
        if let [event] = events.as_slice()
            && !event.deleted
            && key_str(&event.key) != "compact_rev_key"
            && reads.ranges.is_empty()
            && reads.points.iter().all(|(key, revision)| {
                *key == event.key && *revision == event.prev_kv.as_ref().map(|kv| kv.mod_revision)
            })
            && let Some(fresh) = self.exec.try_commit_put(event).await?
        {
            let _ = self.tx.send(vec![rebase(event.clone(), fresh)]);
            return Ok(fresh);
        }
        self.exec.begin_unit().await?;
        match self.commit_guarded(&reads, events).await {
            Ok((fresh, rebased)) => {
                self.exec.end_unit(true).await?;
                if !rebased.is_empty() {
                    let _ = self.tx.send(rebased);
                }
                Ok(fresh)
            }
            Err(e) => {
                let _ = self.exec.end_unit(false).await;
                Err(e)
            }
        }
    }

    /// Run `op` inside the cross-node commit unit, pairing `begin_unit`
    /// with `end_unit` on every path (mirrors `commit_inner`). Lease
    /// read-modify-write cycles ride the same writer serialization as
    /// commits, so rival nodes never interleave them. The writes mutex is
    /// held throughout: units share one connection (SQLite) or one table
    /// lock (Oracle), so two open units at once fail or interleave.
    async fn lease_unit<T>(&self, op: impl Future<Output = Result<T>>) -> Result<T> {
        let _guard = self.writes.lock().await;
        self.exec.begin_unit().await?;
        match op.await {
            Ok(value) => {
                self.exec.end_unit(true).await?;
                Ok(value)
            }
            Err(e) => {
                let _ = self.exec.end_unit(false).await;
                Err(e)
            }
        }
    }

    async fn lease_row(&self, id: LeaseId) -> Result<Option<LeaseState>> {
        Ok(
            match self
                .exec
                .query_row(&D::lease_select_sql(), vec![SqlValue::Int(id)])
                .await?
            {
                Some(row) => Some(row_to_lease(&row)?),
                None => None,
            },
        )
    }

    async fn store_lease_row(&self, id: LeaseId, state: &LeaseState) -> Result<()> {
        let blob = SqlValue::Bytes(encode_lease_keys(&state.keys));
        let updated = self
            .exec
            .execute(
                &D::lease_update_sql(),
                vec![
                    SqlValue::Int(state.granted_ttl),
                    SqlValue::Int(state.expires_at_ms as i64),
                    blob.clone(),
                    SqlValue::Int(id),
                ],
            )
            .await?;
        if updated == 0 {
            self.exec
                .execute(
                    &D::lease_insert_sql(),
                    vec![
                        SqlValue::Int(id),
                        SqlValue::Int(state.granted_ttl),
                        SqlValue::Int(state.expires_at_ms as i64),
                        blob,
                    ],
                )
                .await?;
        }
        Ok(())
    }
}

fn rebase(mut event: Event, fresh: Revision) -> Event {
    let staged = event.revision;
    event.revision = fresh;
    if let Some(kv) = event.kv.as_mut() {
        kv.mod_revision = fresh;
        if kv.create_revision == staged {
            kv.create_revision = fresh;
        }
    }
    event
}

fn key_str(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

fn row_to_kv(row: &Row) -> Result<KeyValue> {
    Ok(KeyValue {
        key: Bytes::from(row.get_str(1)?.into_bytes()),
        value: Bytes::from(row.get_bytes(8)?),
        create_revision: row.get_int(4)?,
        mod_revision: row.get_int(2)?,
        version: row.get_int(7)?,
        lease: row.get_int(6)?,
    })
}

fn row_to_lease(row: &Row) -> Result<LeaseState> {
    let raw_expiry = row.get_int(1)?;
    Ok(LeaseState {
        granted_ttl: row.get_int(0)?,
        expires_at_ms: u64::try_from(raw_expiry)
            .map_err(|_| Error::Backend(format!("negative lease expiry: {raw_expiry}").into()))?,
        keys: decode_lease_keys(&row.get_bytes(2)?)?,
    })
}

/// Length-prefixed key names (`u32 BE length` + bytes, concatenated):
/// one BLOB column holds the attached set on both dialects.
fn encode_lease_keys(keys: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for key in keys {
        out.extend_from_slice(&(key.len() as u32).to_be_bytes());
        out.extend_from_slice(key);
    }
    out
}

fn decode_lease_keys(mut blob: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut keys = Vec::new();
    while !blob.is_empty() {
        if blob.len() < 4 {
            return Err(Error::Backend("truncated lease keys".into()));
        }
        let len = u32::from_be_bytes(blob[..4].try_into().expect("sliced to 4")) as usize;
        blob = &blob[4..];
        if blob.len() < len {
            return Err(Error::Backend("truncated lease keys".into()));
        }
        keys.push(blob[..len].to_vec());
        blob = &blob[len..];
    }
    Ok(keys)
}

fn row_to_event(row: &Row) -> Result<Event> {
    // Only 1 is a tombstone; larger values link to the superseding row.
    let deleted = row.get_int(3)? == 1;
    let key = Bytes::from(row.get_str(1)?.into_bytes());
    Ok(Event {
        revision: row.get_int(2)?,
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

    async fn commit(&self, reads: ReadSet, events: Vec<Event>) -> Result<Revision> {
        // Run the commit unit detached: dropping this future (e.g. a
        // cancelled RPC) never leaves the database transaction open — the
        // unit always runs to completion with `end_unit` pairing
        // `begin_unit`. `commit` is therefore cancel-safe; callers must not
        // add their own spawn for that purpose.
        let this = self.clone();
        tokio::spawn(async move { this.commit_inner(reads, events).await })
            .await
            .map_err(|error| Error::task("commit task failed", error))?
    }

    async fn list(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        // Latest reads run lock-free in one `query_batch` read transaction;
        // historical reads take the write mutex so a concurrent compact
        // cannot delete the history under the read.
        if opts.revision <= 0 {
            return self.list_inner(prefix, range_end, &opts).await;
        }
        let _guard = self.writes.lock().await;
        self.list_inner(prefix, range_end, &opts).await
    }

    async fn compact(&self, revision: Revision) -> Result<Revision> {
        // Delete bound per statement so writers and readers interleave
        // between chunks instead of stalling on one full-table delete.
        const CHUNK: i64 = 1000;
        let cur = {
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
                .execute(watermark, vec![SqlValue::Int(revision)])
                .await?;
            cur
        };
        // Chunked deletes are idempotent, so a same-revision double simply
        // converges; a newer compact subsumes our range, so we stop.
        loop {
            {
                let _guard = self.writes.lock().await;
                if self.compacted_inner().await? > revision {
                    break;
                }
            }
            let n = self
                .exec
                .execute(
                    D::compact_delete_sql(),
                    vec![
                        SqlValue::Int(revision),
                        SqlValue::Int(revision),
                        SqlValue::Int(CHUNK),
                    ],
                )
                .await?;
            if n == 0 {
                break;
            }
        }
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
        // One batched predecessor lookup instead of one query per event.
        let parents: Vec<i64> = rows
            .iter()
            .map(|r| r.get_int(5))
            .collect::<Result<Vec<_>>>()?;
        let mut prev_map: HashMap<i64, Row> = HashMap::new();
        if !parents.is_empty() {
            for row in self
                .exec
                .query_all(
                    &D::predecessor_set_sql(parents.len()),
                    parents.iter().map(|id| SqlValue::Int(*id)).collect(),
                )
                .await?
            {
                prev_map.insert(row.get_int(0)?, row);
            }
        }
        let mut out = Vec::with_capacity(rows.len());
        for r in &rows {
            let mut ev = row_to_event(r)?;
            let parent = r.get_int(5)?;
            if parent > 0
                && let Some(prev) = prev_map.get(&parent)
                && prev.get_int(3)? != 1
            {
                ev.prev_kv = Some(row_to_kv(prev)?);
            }
            out.push(ev);
        }
        Ok(out)
    }

    fn subscribe(&self) -> broadcast::Receiver<Vec<Event>> {
        self.tx.subscribe()
    }

    async fn lease_grant(&self, ttl: i64, requested: LeaseId, now_ms: u64) -> Result<LeaseId> {
        let expires_at_ms = LeaseState::deadline(ttl, now_ms);
        self.lease_unit(async {
            let id = if requested != 0 {
                requested
            } else {
                let row = self
                    .exec
                    .query_row(&D::lease_max_id_sql(), vec![])
                    .await?
                    .ok_or_else(|| Error::Backend("missing lease id".into()))?;
                row.get_int(0)? + 1
            };
            self.store_lease_row(
                id,
                &LeaseState {
                    granted_ttl: ttl,
                    expires_at_ms,
                    keys: Vec::new(),
                },
            )
            .await?;
            Ok(id)
        })
        .await
    }

    async fn lease_lookup(&self, id: LeaseId) -> Result<Option<LeaseState>> {
        self.lease_row(id).await
    }

    async fn lease_keepalive(&self, id: LeaseId, now_ms: u64) -> Result<Option<i64>> {
        self.lease_unit(async {
            let Some(mut state) = self.lease_row(id).await? else {
                return Ok(None);
            };
            state.expires_at_ms = LeaseState::deadline(state.granted_ttl, now_ms);
            self.store_lease_row(id, &state).await?;
            Ok(Some(state.granted_ttl))
        })
        .await
    }

    async fn lease_attach(&self, id: LeaseId, key: Vec<u8>) -> Result<bool> {
        self.lease_unit(async {
            let Some(mut state) = self.lease_row(id).await? else {
                return Ok(false);
            };
            if !state.keys.contains(&key) {
                state.keys.push(key);
                self.store_lease_row(id, &state).await?;
            }
            Ok(true)
        })
        .await
    }

    async fn lease_detach(&self, id: LeaseId, key: &[u8]) -> Result<()> {
        self.lease_unit(async {
            let Some(mut state) = self.lease_row(id).await? else {
                return Ok(());
            };
            if state.keys.iter().any(|k| k == key) {
                state.keys.retain(|k| k != key);
                self.store_lease_row(id, &state).await?;
            }
            Ok(())
        })
        .await
    }

    async fn lease_take_expired(&self, now_ms: u64) -> Result<Vec<(LeaseId, Vec<Vec<u8>>)>> {
        self.lease_unit(async {
            let params = vec![SqlValue::Int(now_ms as i64)];
            let rows = self
                .exec
                .query_all(&D::lease_expired_select_sql(), params.clone())
                .await?;
            let mut out = Vec::with_capacity(rows.len());
            for row in &rows {
                out.push((row.get_int(0)?, decode_lease_keys(&row.get_bytes(1)?)?));
            }
            self.exec
                .execute(&D::lease_expired_delete_sql(), params)
                .await?;
            Ok(out)
        })
        .await
    }

    async fn lease_revoke(&self, id: LeaseId) -> Result<Vec<Vec<u8>>> {
        self.lease_unit(async {
            let Some(state) = self.lease_row(id).await? else {
                return Ok(Vec::new());
            };
            self.exec
                .execute(&D::lease_delete_sql(), vec![SqlValue::Int(id)])
                .await?;
            Ok(state.keys)
        })
        .await
    }

    async fn lease_ids(&self) -> Result<Vec<LeaseId>> {
        Ok(self
            .exec
            .query_all(&D::lease_ids_sql(), vec![])
            .await?
            .iter()
            .map(|row| row.get_int(0))
            .collect::<Result<_>>()?)
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Minimal dialect: only the schema path is real.
    struct TestDialect;
    impl Dialect for TestDialect {
        fn create_table_sql() -> &'static str {
            "CREATE TABLE rine"
        }
        fn create_indexes_sql() -> &'static [&'static str] {
            &["CREATE INDEX idx_test"]
        }
        fn probe_sql() -> &'static str {
            "PROBE TABLE"
        }
        fn current_revision_sql() -> String {
            String::new()
        }
        fn bind(_index: usize) -> String {
            "?".into()
        }
        fn compacted_rev_sql() -> &'static str {
            ""
        }
        fn events_sql() -> String {
            String::new()
        }
        fn live_range_queries(
            _end: Option<&[u8]>,
            _opts: &ListOptions,
        ) -> (Option<String>, Option<String>) {
            (Some(String::new()), Some(String::new()))
        }
        fn commit_state_sql(_names: usize) -> String {
            String::new()
        }
        fn set_deleted_by_id_sql() -> &'static str {
            ""
        }
        fn compact_delete_sql() -> &'static str {
            ""
        }
        fn set_compact_rev_sql() -> &'static str {
            ""
        }
        fn set_compact_rev_insert_sql() -> &'static str {
            ""
        }
        fn db_size_sql() -> &'static str {
            ""
        }
        fn create_leases_table_sql() -> &'static str {
            "CREATE TABLE rine_leases"
        }
        fn leases_probe_sql() -> &'static str {
            "PROBE LEASES"
        }
    }

    /// Injects `drops` transient `TableMissing` failures on index creation,
    /// then converges — a parallel DROP TABLE landing mid-ensure.
    struct RacyExec {
        drops: AtomicUsize,
        ddl_calls: AtomicUsize,
        probe_drops: AtomicUsize,
        contention: AtomicUsize,
        probe_err: bool,
        fast: bool,
    }

    impl RacyExec {
        fn new(drops: usize) -> Self {
            Self {
                drops: AtomicUsize::new(drops),
                ddl_calls: AtomicUsize::new(0),
                probe_drops: AtomicUsize::new(0),
                contention: AtomicUsize::new(0),
                probe_err: false,
                fast: false,
            }
        }
    }

    #[async_trait]
    impl SqlExecutor for RacyExec {
        async fn try_commit_put(&self, _event: &Event) -> Result<Option<Revision>> {
            Ok(self.fast.then_some(7))
        }
        async fn execute(&self, sql: &str, _params: Vec<SqlValue>) -> Result<u64> {
            self.ddl_calls.fetch_add(1, Ordering::SeqCst);
            if self.contention.load(Ordering::SeqCst) > 0 {
                self.contention.fetch_sub(1, Ordering::SeqCst);
                return Err(Error::LockContention("writer raced DDL".into()));
            }
            if sql == "CREATE INDEX idx_test" && self.drops.load(Ordering::SeqCst) > 0 {
                self.drops.fetch_sub(1, Ordering::SeqCst);
                return Err(Error::TableMissing("table dropped mid-ensure".into()));
            }
            Ok(1)
        }
        async fn execute_batch(&self, _statements: Vec<(String, Vec<SqlValue>)>) -> Result<()> {
            Ok(())
        }
        async fn query_row(&self, sql: &str, _params: Vec<SqlValue>) -> Result<Option<Row>> {
            if self.probe_err {
                return Err(Error::Backend("boom".into()));
            }
            if sql == "PROBE TABLE" && self.probe_drops.load(Ordering::SeqCst) > 0 {
                self.probe_drops.fetch_sub(1, Ordering::SeqCst);
                return Err(Error::TableMissing("table dropped mid-probe".into()));
            }
            match sql {
                "PROBE TABLE" => Ok(Some(Row {
                    values: vec![SqlValue::Int(1)],
                })),
                // Index never present: forces the CREATE INDEX path.
                _ => Ok(None),
            }
        }
        async fn query_all(&self, _sql: &str, _params: Vec<SqlValue>) -> Result<Vec<Row>> {
            Ok(Vec::new())
        }
    }

    /// Short reads: fewer result sets than the latest-range lookup expects.
    struct ShortExec {
        results: usize,
    }

    #[async_trait]
    impl SqlExecutor for ShortExec {
        async fn execute(&self, _sql: &str, _params: Vec<SqlValue>) -> Result<u64> {
            Ok(1)
        }
        async fn execute_batch(&self, _statements: Vec<(String, Vec<SqlValue>)>) -> Result<()> {
            Ok(())
        }
        async fn query_row(&self, _sql: &str, _params: Vec<SqlValue>) -> Result<Option<Row>> {
            Ok(None)
        }
        async fn query_all(&self, _sql: &str, _params: Vec<SqlValue>) -> Result<Vec<Row>> {
            Ok(Vec::new())
        }
        async fn query_batch(
            &self,
            _statements: Vec<(String, Vec<SqlValue>)>,
        ) -> Result<Vec<Vec<Row>>> {
            Ok((0..self.results)
                .map(|_| {
                    vec![Row {
                        values: vec![SqlValue::Int(1)],
                    }]
                })
                .collect())
        }
    }

    /// Denies all DDL with a privilege error: DBA-prepared schema missing a
    /// required index plus no DDL grant must keep failing fast.
    struct DenyExec;

    #[async_trait]
    impl SqlExecutor for DenyExec {
        async fn execute(&self, _sql: &str, _params: Vec<SqlValue>) -> Result<u64> {
            Err(Error::Backend("ORA-01031: insufficient privileges".into()))
        }
        async fn execute_batch(&self, _statements: Vec<(String, Vec<SqlValue>)>) -> Result<()> {
            Ok(())
        }
        async fn query_row(&self, sql: &str, _params: Vec<SqlValue>) -> Result<Option<Row>> {
            match sql {
                "PROBE TABLE" => Ok(Some(Row {
                    values: vec![SqlValue::Int(1)],
                })),
                _ => Ok(None),
            }
        }
        async fn query_all(&self, _sql: &str, _params: Vec<SqlValue>) -> Result<Vec<Row>> {
            Ok(Vec::new())
        }
    }

    /// Regression test for the CI failure in `live_stale_point_read_is_conflict`:
    /// a parallel DROP TABLE between probe and CREATE INDEX (ORA-00942) must
    /// restart the pass and converge, not abort startup.
    #[tokio::test]
    async fn ensure_schema_survives_concurrent_drop() {
        let exec = Arc::new(RacyExec::new(1));
        let backend = SqlBackend::<RacyExec, TestDialect>::new(exec.clone());
        backend
            .ensure_schema()
            .await
            .expect("must converge after concurrent DROP");
        assert!(
            exec.ddl_calls.load(Ordering::SeqCst) >= 2,
            "expected a retried CREATE INDEX"
        );
    }

    /// A persistent privilege failure must still abort instead of being
    /// retried away.
    #[tokio::test]
    async fn ensure_schema_still_aborts_without_ddl_privilege() {
        let exec = Arc::new(DenyExec);
        let backend = SqlBackend::<DenyExec, TestDialect>::new(exec.clone());
        let err = backend.ensure_schema().await.unwrap_err();
        assert!(
            err.to_string().contains("insufficient privileges"),
            "{err:?}"
        );
        // Mock helper paths stay covered.
        exec.execute_batch(vec![]).await.unwrap();
        exec.query_all("SELECT 1", vec![]).await.unwrap();
    }

    /// A concurrent DROP between probe and DDL restarts the whole pass.
    #[tokio::test]
    async fn ensure_schema_restarts_after_concurrent_drop() {
        let exec = Arc::new(RacyExec::new(0));
        exec.probe_drops.store(1, Ordering::SeqCst);
        let backend = SqlBackend::<RacyExec, TestDialect>::new(exec.clone());
        backend.ensure_schema().await.expect("must converge");
        exec.execute_batch(vec![]).await.unwrap();
        exec.query_all("SELECT 1", vec![]).await.unwrap();
    }

    /// A non-retryable probe failure aborts startup immediately.
    #[tokio::test]
    async fn ensure_schema_aborts_on_probe_error() {
        let mut exec = RacyExec::new(0);
        exec.probe_err = true;
        let backend = SqlBackend::<RacyExec, TestDialect>::new(Arc::new(exec));
        let err = backend.ensure_schema().await.unwrap_err();
        assert!(err.to_string().contains("boom"), "{err:?}");
    }

    /// A transient lock conflict on DDL is retried, then converges.
    #[tokio::test]
    async fn ensure_schema_retries_lock_contention() {
        let exec = Arc::new(RacyExec::new(0));
        exec.contention.store(1, Ordering::SeqCst);
        let backend = SqlBackend::<RacyExec, TestDialect>::new(exec);
        backend.ensure_schema().await.expect("must converge");
    }

    /// Every dialect hook has a callable body (the schema path only needs
    /// a few of them; the rest must still link and run).
    #[test]
    fn test_dialect_covers_all_hooks() {
        assert_eq!(TestDialect::create_table_sql(), "CREATE TABLE rine");
        assert_eq!(
            TestDialect::create_indexes_sql(),
            &["CREATE INDEX idx_test"]
        );
        assert_eq!(TestDialect::probe_sql(), "PROBE TABLE");
        assert_eq!(TestDialect::bind(3), "?");
        let _ = TestDialect::current_revision_sql();
        let _ = TestDialect::compacted_rev_sql();
        let _ = TestDialect::events_sql();
        let _ = TestDialect::live_range_queries(None, &ListOptions::default());
        let _ = TestDialect::commit_state_sql(1);
        let _ = TestDialect::set_deleted_by_id_sql();
        let _ = TestDialect::compact_delete_sql();
        let _ = TestDialect::set_compact_rev_sql();
        let _ = TestDialect::set_compact_rev_insert_sql();
        let _ = TestDialect::db_size_sql();
        assert_eq!(
            TestDialect::create_leases_table_sql(),
            "CREATE TABLE rine_leases"
        );
        assert_eq!(TestDialect::leases_probe_sql(), "PROBE LEASES");
        let _ = TestDialect::lease_max_id_sql();
        let _ = TestDialect::lease_update_sql();
        let _ = TestDialect::lease_insert_sql();
        let _ = TestDialect::lease_select_sql();
        let _ = TestDialect::lease_keepalive_sql();
        let _ = TestDialect::lease_delete_sql();
        let _ = TestDialect::lease_expired_select_sql();
        let _ = TestDialect::lease_expired_delete_sql();
        let _ = TestDialect::lease_ids_sql();
    }

    /// The lease key set round-trips through one BLOB column, including
    /// empty and binary names; truncated blobs are rejected, not sliced.
    #[test]
    fn lease_key_codec_round_trips() {
        let keys = vec![b"a".to_vec(), Vec::new(), vec![0, 255, 1, 2, 3]];
        let blob = encode_lease_keys(&keys);
        assert_eq!(decode_lease_keys(&blob).unwrap(), keys);
        assert_eq!(decode_lease_keys(&[]).unwrap(), Vec::<Vec<u8>>::new());
        assert!(decode_lease_keys(&blob[..blob.len() - 1]).is_err());
        assert!(decode_lease_keys(&[0, 0]).is_err());
        assert!(decode_lease_keys(&[0, 0, 0, 5, 1]).is_err());
    }

    /// A short read transaction surfaces a backend error, not a panic.
    #[tokio::test]
    async fn list_latest_rejects_short_results() {
        use crate::SqlExecutor as _;
        for results in [1, 2] {
            let exec = Arc::new(ShortExec { results });
            let backend = SqlBackend::<ShortExec, TestDialect>::new(exec.clone());
            let err = backend
                .list(b"k", None, ListOptions::default())
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Backend(_)), "{err:?}");
            // Remaining executor surface stays covered.
            assert_eq!(exec.execute("SELECT 1", vec![]).await.unwrap(), 1);
            exec.execute_batch(vec![]).await.unwrap();
            assert!(exec.query_row("SELECT 1", vec![]).await.unwrap().is_none());
            assert!(exec.query_all("SELECT 1", vec![]).await.unwrap().is_empty());
        }
    }

    /// A commit lookup with no rows surfaces a backend error.
    #[tokio::test]
    async fn commit_rejects_missing_metadata() {
        let backend = SqlBackend::<ShortExec, TestDialect>::new(Arc::new(ShortExec { results: 0 }));
        let err = backend
            .commit(ReadSet::default(), vec![])
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Backend(_)), "{err:?}");
    }

    /// The single-Put fast path assigns the executor's revision directly.
    #[tokio::test]
    async fn commit_takes_executor_fast_put() {
        let mut exec = RacyExec::new(0);
        exec.fast = true;
        let backend = SqlBackend::<RacyExec, TestDialect>::new(Arc::new(exec));
        let key = Bytes::from("k");
        let rev = backend
            .commit(
                ReadSet::default(),
                vec![Event {
                    revision: 2,
                    key: key.clone(),
                    deleted: false,
                    kv: Some(KeyValue {
                        key,
                        value: Bytes::from("v"),
                        create_revision: 2,
                        mod_revision: 2,
                        version: 1,
                        lease: 0,
                    }),
                    prev_kv: None,
                }],
            )
            .await
            .unwrap();
        assert_eq!(rev, 7);
    }
}
