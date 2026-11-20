use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
use rine_etcd::{
    backend::{
        BROADCAST_CAP, Backend, Event, KeyValue, ListOptions, ObservedRange, ReadSet, Revision,
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

    async fn ensure_schema_once(&self) -> Result<()> {
        match self.exec.query_row(D::probe_sql(), vec![]).await {
            Ok(_) => Ok(()),
            Err(Error::TableMissing(_)) => self.create_schema().await,
            Err(e) => Err(e),
        }?;
        // `create_indexes_sql` covers fresh tables; `ensure_indexes_sql`
        // converges pre-existing tables missing an index. Re-running either
        // tolerates a concurrent creator via `AlreadyExists`.
        for stmt in D::create_indexes_sql()
            .iter()
            .chain(D::ensure_indexes_sql().iter())
        {
            self.create_statement_with_retry(stmt).await?;
        }
        Ok(())
    }

    /// Run the DDL. Each statement tolerates a concurrent creator
    /// (already-exists) and retries a transient lock conflict with a
    /// concurrent writer (e.g. `CREATE INDEX` racing DML on a fresh database),
    /// so simultaneous startups on any number of nodes converge.
    /// Migrations are out of scope for now.
    async fn create_schema(&self) -> Result<()> {
        for stmt in
            std::iter::once(D::create_table_sql()).chain(D::create_indexes_sql().iter().copied())
        {
            self.create_statement_with_retry(stmt).await?;
        }
        Ok(())
    }

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
        // Both watermarks in one trip (a single blocking call on Oracle).
        let mut rows = self
            .exec
            .query_batch(vec![
                (D::current_revision_sql(), vec![]),
                (D::compacted_rev_sql().into(), vec![]),
            ])
            .await?
            .into_iter();
        let cur = rows
            .next()
            .and_then(|rows| rows.into_iter().next())
            .map(|r| r.get_int(0))
            .transpose()?
            .unwrap_or(1)
            .max(1);
        if rev > 0 {
            let c = rows
                .next()
                .and_then(|rows| rows.into_iter().next())
                .map(|r| r.get_int(0))
                .transpose()?
                .unwrap_or(0);
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

    async fn list_live(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        opts: &ListOptions,
        rows_sql: Option<String>,
        count_sql: Option<String>,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        let mut params = vec![SqlValue::Str(key_str(start))];
        if let Some(end) = end.filter(|end| !end.is_empty() && *end != [0]) {
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
        let cur = results
            .next()
            .and_then(|rows| rows.into_iter().next())
            .map(|r| r.get_int(0))
            .transpose()?
            .unwrap_or(1)
            .max(1);
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
        let count = if has_count {
            results
                .next()
                .and_then(|rows| rows.into_iter().next())
                .ok_or_else(|| Error::Backend("missing range count".into()))?
                .get_int(0)?
        } else {
            kvs.len() as i64
        };
        Ok((cur, kvs, count))
    }

    /// Backend-level range/list read without taking the write mutex. Reads the
    /// committed state; inside a commit unit it observes the unit's snapshot.
    async fn list_inner(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: &ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        if opts.revision <= 0
            && let Some(sql) = D::live_range_sql(range_end, opts)
        {
            let mut params = vec![SqlValue::Str(key_str(prefix))];
            if let Some(end) = range_end.filter(|end| !end.is_empty() && *end != [0]) {
                params.push(SqlValue::Str(key_str(end)));
            }
            if opts.limit > 0 && !opts.count_only {
                params.push(SqlValue::Int(opts.limit));
            }
            let rows = self.exec.query_all(&sql, params).await?;
            let meta = rows
                .first()
                .ok_or_else(|| Error::Backend("missing range metadata".into()))?;
            let revision = meta.get_int(0)?;
            let mut kvs = Vec::new();
            for row in &rows {
                if !row.is_null(2) {
                    kvs.push(row_to_kv_at(row, 2)?);
                }
            }
            let total = if meta.is_null(1) {
                kvs.len() as i64
            } else {
                meta.get_int(1)?
            };
            return Ok((revision, kvs, total));
        }
        if opts.revision <= 0
            && let Some((rows_sql, count_sql)) = D::live_range_queries(range_end, opts)
        {
            return self
                .list_live(prefix, range_end, opts, rows_sql, count_sql)
                .await;
        }
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
    /// Guard reads on keys this batch also writes are folded into the
    /// predecessor lookup below instead of costing an extra round trip (the
    /// k8s compare-and-write-same-key pattern); guard-only keys share one
    /// batched read with the revision assignment.
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
        let guard_only: Vec<&[u8]> = expected
            .keys()
            .copied()
            .filter(|key| !written.contains(*key))
            .collect();
        let names: Vec<_> = expected
            .keys()
            .copied()
            .chain(written.iter().copied())
            .collect::<HashSet<_>>()
            .into_iter()
            .map(key_str)
            .collect();
        let optimized = if let Some(sql) = D::commit_state_sql(names.len()) {
            let params = names.into_iter().map(SqlValue::Str).collect();
            let rows = self.exec.query_all(&sql, params).await?;
            let meta = rows
                .first()
                .ok_or_else(|| Error::Backend("missing commit metadata".into()))?;
            let mut state = HashMap::new();
            for row in &rows {
                if !row.is_null(2) {
                    let live = if row.is_null(4) {
                        None
                    } else {
                        Some(row.get_int(4)?)
                    };
                    state.insert(row.get_str(2)?, (row.get_int(3)?, live));
                }
            }
            for (key, rev) in &expected {
                if state.get(&key_str(key)).and_then(|(_, live)| *live) != *rev {
                    return Err(Error::Conflict);
                }
            }
            Some((meta.get_int(0)? + 1, meta.get_int(1)?, state))
        } else {
            None
        };
        // The newest physical row carries the highest logical revision.
        let (fresh, mut id, latest) = if let Some(state) = optimized {
            state
        } else {
            let mut batch: Vec<(String, Vec<SqlValue>)> = Vec::new();
            if !guard_only.is_empty() {
                batch.push((
                    D::live_by_names_sql(guard_only.len()),
                    guard_only
                        .iter()
                        .map(|key| SqlValue::Str(key_str(key)))
                        .collect(),
                ));
            }
            batch.push((D::current_revision_sql(), vec![]));
            let mut results = self.exec.query_batch(batch).await?.into_iter();
            if !guard_only.is_empty() {
                let rows = results
                    .next()
                    .ok_or_else(|| Error::Backend("missing guard read".into()))?;
                let mut live: HashMap<Vec<u8>, Revision> = HashMap::new();
                for row in &rows {
                    let kv = row_to_kv(row)?;
                    live.entry(kv.key.to_vec()).or_insert(kv.mod_revision);
                }
                for key in &guard_only {
                    if live.get(*key).copied() != expected[key] {
                        return Err(Error::Conflict);
                    }
                }
            }
            let fresh = match results.next().as_ref().and_then(|rows| rows.first()) {
                Some(r) if !r.is_null(0) => r.get_int(0)?.max(1),
                _ => 1,
            } + 1;
            if events.is_empty() {
                for range in &reads.ranges {
                    self.check_range(range).await?;
                }
                return Ok((fresh - 1, events));
            }
            // One batch for every written key's live row and latest id plus the
            // id assignment, no matter how many keys the commit carries.
            let mut names: Vec<String> = Vec::new();
            {
                let mut seen_names: HashSet<&[u8]> = HashSet::new();
                for event in &events {
                    if seen_names.insert(event.key.as_ref()) {
                        names.push(key_str(&event.key));
                    }
                }
            }
            let name_params: Vec<SqlValue> =
                names.iter().map(|n| SqlValue::Str(n.clone())).collect();
            let mut results = self
                .exec
                .query_batch(vec![
                    (D::live_by_names_sql(names.len()), name_params.clone()),
                    (D::latest_ids_by_names_sql(names.len()), name_params),
                    (D::last_row_id_sql().into(), vec![]),
                ])
                .await?
                .into_iter();
            let mut live: HashMap<String, Row> = HashMap::new();
            for row in results
                .next()
                .ok_or_else(|| Error::Backend("missing live rows".into()))?
            {
                live.entry(row.get_str(1)?).or_insert(row);
            }
            let mut latest = HashMap::new();
            for row in results
                .next()
                .ok_or_else(|| Error::Backend("missing latest ids".into()))?
            {
                latest.insert(row.get_str(0)?, (row.get_int(1)?, None));
            }
            let id = match results.next().as_ref().and_then(|rows| rows.first()) {
                Some(row) if !row.is_null(0) => row.get_int(0)?.max(1),
                _ => 1,
            };
            for (name, row) in live {
                latest.insert(name, (row.get_int(0)?, Some(row.get_int(2)?)));
            }
            for event in &events {
                if let Some(expected) = expected.get(event.key.as_ref())
                    && latest.get(&key_str(&event.key)).and_then(|(_, live)| *live) != *expected
                {
                    return Err(Error::Conflict);
                }
            }
            (fresh, id, latest)
        };
        for range in &reads.ranges {
            self.check_range(range).await?;
        }
        if events.is_empty() {
            return Ok((fresh - 1, events));
        }
        let mut statements = Vec::new();
        for event in &events {
            let name = key_str(&event.key);
            if name == D::compacted_rev_key() {
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
            && key_str(&event.key) != D::compacted_rev_key()
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
    row_to_kv_at(row, 0)
}

fn row_to_kv_at(row: &Row, offset: usize) -> Result<KeyValue> {
    Ok(KeyValue {
        key: Bytes::from(row.get_str(offset + 1)?.into_bytes()),
        value: Bytes::from(row.get_bytes(offset + 8)?),
        create_revision: row.get_int(offset + 4)?,
        mod_revision: row.get_int(offset + 2)?,
        version: row.get_int(offset + 7)?,
        lease: row.get_int(offset + 6)?,
    })
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
        if opts.revision <= 0
            && (D::live_range_sql(range_end, &opts).is_some()
                || D::live_range_queries(range_end, &opts).is_some())
        {
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
            &[]
        }
        fn ensure_indexes_sql() -> &'static [&'static str] {
            &["CREATE INDEX idx_test"]
        }
        fn probe_sql() -> &'static str {
            "PROBE TABLE"
        }
        fn bind(_index: usize) -> String {
            "?".into()
        }
        fn first_row() -> &'static str {
            ""
        }
        fn compacted_rev_sql() -> &'static str {
            unimplemented!()
        }
        fn events_sql() -> String {
            unimplemented!()
        }
        fn list_exact_sql() -> String {
            unimplemented!()
        }
        fn list_ge_sql() -> String {
            unimplemented!()
        }
        fn list_range_sql() -> String {
            unimplemented!()
        }
        fn set_deleted_by_id_sql() -> &'static str {
            unimplemented!()
        }
        fn compact_delete_sql() -> &'static str {
            unimplemented!()
        }
        fn set_compact_rev_sql() -> &'static str {
            unimplemented!()
        }
        fn set_compact_rev_insert_sql() -> &'static str {
            unimplemented!()
        }
        fn db_size_sql() -> &'static str {
            unimplemented!()
        }
    }

    /// Injects `drops` transient `TableMissing` failures on index creation,
    /// then converges — a parallel DROP TABLE landing mid-ensure.
    struct RacyExec {
        drops: AtomicUsize,
        ddl_calls: AtomicUsize,
    }

    impl RacyExec {
        fn new(drops: usize) -> Self {
            Self {
                drops: AtomicUsize::new(drops),
                ddl_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl SqlExecutor for RacyExec {
        async fn execute(&self, sql: &str, _params: Vec<SqlValue>) -> Result<u64> {
            self.ddl_calls.fetch_add(1, Ordering::SeqCst);
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
        let backend = SqlBackend::<DenyExec, TestDialect>::new(Arc::new(DenyExec));
        let err = backend.ensure_schema().await.unwrap_err();
        assert!(
            err.to_string().contains("insufficient privileges"),
            "{err:?}"
        );
    }
}
