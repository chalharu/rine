use std::{collections::HashSet, marker::PhantomData, sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use rine_etcd::{
    backend::{Backend, Event, KeyValue, ListOptions, ObservedRange, ReadSet, Revision},
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

    /// Bring the schema to the state the backend needs (server startup path),
    /// creating only what is missing:
    /// - table absent → create table plus all indexes;
    /// - table present but a required index absent → create just that index;
    /// - everything present (e.g. DBA-prepared, no DDL privilege on our
    ///   side) → do nothing.
    ///
    /// A concurrent creator racing us surfaces `AlreadyExists`, which is
    /// tolerated. Classification happens in the driver (`cvt_error`), so the
    /// generic code matches typed variants instead of parsing messages.
    ///
    /// When creation fails (e.g. insufficient privileges) the schema is
    /// re-probed: if another node or the DBA converged it meanwhile we
    /// proceed; otherwise startup aborts with an actionable error naming the
    /// missing objects instead of running degraded.
    ///
    /// A concurrent DROP (parallel live tests sharing one schema, a DBA
    /// migration) can pull the table from under a single pass, surfacing
    /// `TableMissing` from an index statement or probe. That is transient by
    /// definition — someone else is converging the schema — so the whole
    /// pass restarts on `TableMissing` (bounded). Anything else, including
    /// the converged-or-explain abort, fails fast.
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
        for stmt in D::ensure_indexes_sql() {
            self.create_statement_with_retry(stmt).await?;
        }
        self.ensure_required_indexes().await
    }

    /// DDL statements that fully provision the schema, for operators who
    /// pre-create objects with a privileged user (no DDL privilege needed at
    /// runtime as long as table plus [`Dialect::required_indexes`] exist).
    pub fn schema_statements() -> Vec<&'static str> {
        std::iter::once(D::create_table_sql())
            .chain(D::create_indexes_sql().iter().copied())
            .chain(D::ensure_indexes_sql().iter().copied())
            .collect()
    }

    /// Run the DDL. Each statement tolerates a concurrent creator
    /// (already-exists) and retries a transient lock conflict with a
    /// concurrent writer (e.g. `CREATE INDEX` racing DML on a fresh database),
    /// so simultaneous startups on any number of nodes converge. If a
    /// statement still fails (e.g. no DDL privilege), re-probe: a schema
    /// converged by someone else is accepted, otherwise the error names the
    /// missing objects so the operator knows what to pre-create.
    /// Migrations are out of scope for now.
    pub async fn create_schema(&self) -> Result<()> {
        for stmt in
            std::iter::once(D::create_table_sql()).chain(D::create_indexes_sql().iter().copied())
        {
            if let Err(e) = self.create_statement_with_retry(stmt).await {
                return self.converged_or_err(e).await;
            }
        }
        Ok(())
    }

    /// Create required indexes absent from a pre-existing table. Same
    /// converge-or-explain rule as [`Self::create_schema`].
    async fn ensure_required_indexes(&self) -> Result<()> {
        for (name, ddl) in D::required_indexes() {
            if self.index_present(name).await? {
                continue;
            }
            if let Err(e) = self.create_statement_with_retry(ddl).await {
                return self.converged_or_err(e).await;
            }
        }
        Ok(())
    }

    /// True when the required index `name` already exists.
    async fn index_present(&self, name: &str) -> Result<bool> {
        Ok(self
            .exec
            .query_row(&D::index_probe_sql(), vec![SqlValue::Str(name.to_string())])
            .await?
            .is_some())
    }

    /// After a DDL failure, accept a schema someone else converged and
    /// otherwise explain exactly what is missing (plus the original error).
    /// A `TableMissing` failure is passed through untouched: the table
    /// vanished under us (concurrent DROP), which the [`Self::ensure_schema`]
    /// retry loop treats as "restart the pass", not as a broken schema.
    /// (A genuinely absent table with no DDL privilege fails table creation
    /// with a privilege error, never `TableMissing`, so the actionable abort
    /// below still fires for that case.)
    async fn converged_or_err(&self, err: Error) -> Result<()> {
        if matches!(err, Error::TableMissing(_)) {
            return Err(err);
        }
        let missing = self.missing_objects().await?;
        if missing.is_empty() {
            return Ok(());
        }
        Err(Error::Backend(
            format!(
                "rine schema is incomplete (missing: {}) and automatic creation failed ({err}); \
             pre-create the table and required indexes with a privileged user, then restart",
                missing.join(", ")
            )
            .into(),
        ))
    }

    /// Names of absent objects: the table and/or required index names.
    /// Best-effort: probe errors here are reported as missing, since the
    /// caller already holds the original failure.
    async fn missing_objects(&self) -> Result<Vec<String>> {
        let mut out = Vec::new();
        match self.exec.query_row(D::probe_sql(), vec![]).await {
            Ok(_) => {}
            Err(_) => return Ok(vec!["table `rine`".into()]),
        }
        for (name, _) in D::required_indexes() {
            if !self.index_present(name).await.unwrap_or(true) {
                out.push(format!("index `{name}`"));
            }
        }
        Ok(out)
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
        if count != observed.count
            || kvs.len() != observed.observed.len()
            || kvs
                .iter()
                .zip(observed.observed.iter())
                .any(|(kv, (key, rev))| kv.key.as_ref() != key.as_ref() || kv.mod_revision != *rev)
        {
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
        let written: HashSet<&[u8]> = events.iter().map(|e| e.key.as_ref()).collect();
        let mut seen: HashSet<&[u8]> = HashSet::new();
        let mut guard_only: Vec<&[u8]> = Vec::new();
        for (key, _) in &reads.points {
            let key: &[u8] = key.as_ref();
            if written.contains(key) || !seen.insert(key) {
                continue;
            }
            guard_only.push(key);
        }
        // One trip for all guard-only reads plus the revision assignment.
        // The newest physical row carries the highest logical revision:
        // revisions are assigned strictly increasing, so ordering by id and
        // taking the first row is an indexed lookup, unlike MAX() which would
        // scan the whole table on every commit.
        let mut batch: Vec<(String, Vec<SqlValue>)> = guard_only
            .iter()
            .map(|key| (D::live_by_name_sql(), vec![SqlValue::Str(key_str(key))]))
            .collect();
        batch.push((D::current_revision_sql(), vec![]));
        let mut results = self.exec.query_batch(batch).await?.into_iter();
        for key in guard_only {
            let expected = reads
                .points
                .iter()
                .find(|(k, _)| k.as_ref() == key)
                .and_then(|(_, rev)| *rev);
            let rows = results
                .next()
                .ok_or_else(|| Error::Backend("missing guard read".into()))?;
            let current = rows
                .into_iter()
                .next()
                .map(|row| row_to_kv(&row).map(|kv| kv.mod_revision))
                .transpose()?;
            if current != expected {
                return Err(Error::Conflict);
            }
        }
        let fresh = match results.next().as_ref().and_then(|rows| rows.first()) {
            Some(r) if !r.is_null(0) => r.get_int(0)?.max(1),
            _ => 1,
        } + 1;
        for range in &reads.ranges {
            self.check_range(range).await?;
        }
        if events.is_empty() {
            return Ok((fresh - 1, events));
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
            // The predecessor lookup doubles as the guard validation for keys
            // this batch also writes: same lock, same state, no extra query.
            let live = self.live_row(&event.key).await?;
            if let Some(expected) = reads
                .points
                .iter()
                .find(|(k, _)| k.as_ref() == event.key.as_ref())
                .map(|(_, rev)| *rev)
            {
                let current = live
                    .as_ref()
                    .map(|row| row_to_kv(row).map(|kv| kv.mod_revision))
                    .transpose()?;
                if current != expected {
                    return Err(Error::Conflict);
                }
            }
            let previous = match live {
                Some(row) => row.get_int(0)?,
                None => self
                    .exec
                    .query_row(&D::latest_by_name_sql(), vec![SqlValue::Str(name.clone())])
                    .await?
                    .map(|row| row.get_int(0))
                    .transpose()?
                    .unwrap_or(0),
            };
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
            .map(|mut event| {
                let staged = event.revision;
                event.revision = fresh;
                if let Some(kv) = event.kv.as_mut() {
                    kv.mod_revision = fresh;
                    if kv.create_revision == staged {
                        kv.create_revision = fresh;
                    }
                }
                event
            })
            .collect();
        Ok((fresh, rebased))
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
        mod_revision: row.get_int(2)?,
        version: row.get_int(7)?,
        lease: row.get_int(6)?,
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
        let _guard = self.writes.lock().await;
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

    async fn list(
        &self,
        prefix: &[u8],
        range_end: Option<&[u8]>,
        opts: ListOptions,
    ) -> Result<(Revision, Vec<KeyValue>, i64)> {
        if opts.revision <= 0
            && let Some((rows_sql, count_sql)) = D::live_range_queries(range_end, &opts)
        {
            return self
                .list_live(prefix, range_end, &opts, rows_sql, count_sql)
                .await;
        }
        let _guard = self.writes.lock().await;
        self.list_inner(prefix, range_end, &opts).await
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
        fn probe_sql() -> &'static str {
            "PROBE TABLE"
        }
        fn required_indexes() -> &'static [(&'static str, &'static str)] {
            &[("idx_test", "CREATE INDEX idx_test")]
        }
        fn index_probe_sql() -> String {
            "PROBE INDEX".into()
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
        fn live_by_name_sql() -> String {
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

    /// A persistent privilege failure must still abort with the actionable
    /// missing-object error instead of being retried away.
    #[tokio::test]
    async fn ensure_schema_still_aborts_without_ddl_privilege() {
        let backend = SqlBackend::<DenyExec, TestDialect>::new(Arc::new(DenyExec));
        let err = backend.ensure_schema().await.unwrap_err();
        assert!(err.to_string().contains("idx_test"), "{err:?}");
    }
}
