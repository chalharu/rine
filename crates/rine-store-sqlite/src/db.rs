//! Live [`SqlExecutor`] over rusqlite.
//!
//! The driver API is blocking, so every call runs on Tokio's blocking pool
//! via `spawn_blocking`. The connection is acquired asynchronously so queued
//! requests don't occupy blocking threads waiting for it.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use rine_etcd::{
    backend::BackendFactory,
    error::{Error, Result},
};
use rine_store_executor::{Row, SqlExecutor, SqlValue};
use rusqlite::{ToSql, types::ToSqlOutput};
use tokio::sync::Mutex;

/// Live SQLite connection. `Clone` shares the same underlying connection.
pub struct SqliteDb {
    conn: Arc<Mutex<rusqlite::Connection>>,
    /// Serializes transaction scopes on the shared connection: an open
    /// unit and an implicit per-batch transaction must never overlap, or
    /// the second BEGIN fails with "cannot start a transaction within a
    /// transaction". Held across `begin_unit`..`end_unit` (via `unit_guard`)
    /// and across each implicit-transaction batch. Statement-level ops are
    /// already exclusive through `conn`; only scopes need this.
    tx: Arc<tokio::sync::Mutex<()>>,
    /// The open unit's hold on `tx` (`begin_unit` sets it, `end_unit`
    /// releases it). Separate calls, so the guard lives here instead of
    /// on the stack.
    unit_guard: Arc<std::sync::Mutex<Option<tokio::sync::OwnedMutexGuard<()>>>>,
}

impl Clone for SqliteDb {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            tx: self.tx.clone(),
            unit_guard: self.unit_guard.clone(),
        }
    }
}

impl std::fmt::Debug for SqliteDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteDb")
            .field("in_unit", &self.unit_guard.lock().unwrap().is_some())
            .finish_non_exhaustive()
    }
}

impl SqliteDb {
    fn new(conn: rusqlite::Connection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
            tx: Arc::new(tokio::sync::Mutex::new(())),
            unit_guard: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Open (or create) a SQLite database file.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        // A failover peer may be creating or converting the same file right
        // now — notably the WAL switch, which needs EXCLUSIVE and does not
        // reliably honor the busy timeout. Retry transient locks (10s); a
        // rival startup finishes in milliseconds, so anything else failing
        // here is real and fails fast.
        let mut waits = 0;
        loop {
            let conn = rusqlite::Connection::open(&path)?;
            match Self::tune(&conn) {
                Ok(()) => return Ok(Self::new(conn)),
                Err(e)
                    if matches!(
                        e.sqlite_error_code(),
                        Some(
                            rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
                        )
                    ) && waits < 100 =>
                {
                    waits += 1;
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Open a private in-memory database (tests).
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;
        Self::tune(&conn)?;
        Ok(Self::new(conn))
    }

    /// WAL for multi-process visibility, `FULL` sync per commit, and a busy
    /// timeout so a commit unit waits for a rival writer instead of failing.
    fn tune(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
        // First: a failover peer or rolling restart may be opening the same
        // file right now, and every statement below (including the WAL
        // switch itself, which needs EXCLUSIVE) can block on its locks.
        conn.busy_timeout(Duration::from_secs(30))?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        // Use a single WAL sync per commit without sacrificing crash durability.
        let journal: String =
            conn.pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))?;
        if journal != "wal" && journal != "memory" {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                Some(format!("failed to enable WAL: journal_mode is {journal}")),
            ));
        }
        Ok(())
    }

    async fn run_blocking<T, F>(&self, op: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&rusqlite::Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let guard = self.conn.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || op(&guard).map_err(cvt_error))
            .await
            .map_err(|e| Error::task("db task failed", e))?
    }
}

/// Classify driver errors at the boundary, where the typed `rusqlite::Error`
/// is still in scope. Generic code above this layer matches the typed
/// [`Error`] variants and never parses messages:
/// - `ConstraintViolation`: another writer committed first → [`Error::Conflict`].
/// - `DatabaseBusy`/`DatabaseLocked`: transient lock conflict.
/// - Missing table / already-exists have no distinct SQLite codes, so the
///   message is inspected here (once, at the boundary) and exposed upwards
///   only as [`Error::TableMissing`] / [`Error::AlreadyExists`].
fn cvt_error(e: rusqlite::Error) -> Error {
    match e.sqlite_error_code() {
        Some(rusqlite::ErrorCode::ConstraintViolation) => return Error::Conflict,
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked) => {
            return Error::LockContention(e.to_string());
        }
        _ => {}
    }
    let msg = e.to_string();
    if msg.contains("no such table") {
        return Error::TableMissing(msg);
    }
    if msg.contains("already exists") {
        return Error::AlreadyExists(msg);
    }
    // Unknown errors keep the original value verbatim: no re-stringify.
    Error::Backend(e.into())
}

struct Bind<'a>(&'a SqlValue);

impl ToSql for Bind<'_> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self.0 {
            SqlValue::Int(v) => v.to_sql(),
            SqlValue::Str(v) => v.to_sql(),
            SqlValue::Bytes(v) => v.to_sql(),
            SqlValue::Null => Ok(ToSqlOutput::Borrowed(rusqlite::types::ValueRef::Null)),
        }
    }
}

fn apply_batch(
    conn: &rusqlite::Connection,
    statements: Vec<(String, Vec<SqlValue>)>,
) -> rusqlite::Result<()> {
    for (sql, params) in statements {
        conn.prepare_cached(&sql)?
            .execute(rusqlite::params_from_iter(params.iter().map(Bind)))?;
    }
    Ok(())
}

fn query(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqlValue],
) -> rusqlite::Result<Vec<Row>> {
    let mut stmt = conn.prepare_cached(sql)?;
    let ncols = stmt.column_count();
    let rows = stmt.query_map(rusqlite::params_from_iter(params.iter().map(Bind)), |row| {
        read_row(row, ncols)
    })?;
    rows.collect()
}

fn read_row(row: &rusqlite::Row, ncols: usize) -> Result<Row, rusqlite::Error> {
    let mut values = Vec::with_capacity(ncols);
    for i in 0..ncols {
        values.push(read_col(row, i)?);
    }
    Ok(Row { values })
}

fn read_col(row: &rusqlite::Row, idx: usize) -> Result<SqlValue, rusqlite::Error> {
    match row.get_ref(idx)? {
        rusqlite::types::ValueRef::Null => Ok(SqlValue::Null),
        rusqlite::types::ValueRef::Integer(i) => Ok(SqlValue::Int(i)),
        rusqlite::types::ValueRef::Real(f) => Err(rusqlite::Error::FromSqlConversionFailure(
            idx,
            rusqlite::types::Type::Real,
            format!("unexpected REAL value {f}").into(),
        )),
        rusqlite::types::ValueRef::Text(t) => {
            Ok(SqlValue::Str(String::from_utf8_lossy(t).into_owned()))
        }
        rusqlite::types::ValueRef::Blob(b) => Ok(SqlValue::Bytes(b.to_vec())),
    }
}

#[async_trait]
impl SqlExecutor for SqliteDb {
    async fn execute_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<()> {
        // Inside a commit unit the connection already holds BEGIN IMMEDIATE;
        // run the statements directly instead of nesting a second transaction.
        if self.unit_guard.lock().unwrap().is_some() {
            return self
                .run_blocking(move |conn| apply_batch(conn, statements))
                .await;
        }
        // Implicit transaction: claim the scope first, so a rival unit
        // cannot open between our BEGIN and COMMIT.
        let _scope = self.tx.clone().lock_owned().await;
        self.run_blocking(move |conn| {
            let tx = conn.unchecked_transaction()?;
            apply_batch(&tx, statements)?;
            tx.commit()
        })
        .await
    }

    async fn execute(&self, sql: &str, params: Vec<SqlValue>) -> Result<u64> {
        let sql = sql.to_string();
        self.run_blocking(move |conn| {
            let n = conn
                .prepare_cached(&sql)?
                .execute(rusqlite::params_from_iter(params.iter().map(Bind)))?;
            Ok(n as u64)
        })
        .await
    }

    async fn query_row(&self, sql: &str, params: Vec<SqlValue>) -> Result<Option<Row>> {
        let sql = sql.to_string();
        self.run_blocking(move |conn| {
            let mut stmt = conn.prepare_cached(&sql)?;
            let ncols = stmt.column_count();
            let mut rows = stmt.query(rusqlite::params_from_iter(params.iter().map(Bind)))?;
            match rows.next()? {
                Some(row) => Ok(Some(read_row(row, ncols)?)),
                None => Ok(None),
            }
        })
        .await
    }

    async fn query_all(&self, sql: &str, params: Vec<SqlValue>) -> Result<Vec<Row>> {
        let sql = sql.to_string();
        self.run_blocking(move |conn| query(conn, &sql, &params))
            .await
    }

    async fn query_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<Vec<Vec<Row>>> {
        if self.unit_guard.lock().unwrap().is_some() {
            return self
                .run_blocking(move |conn| {
                    statements
                        .iter()
                        .map(|(sql, params)| query(conn, sql, params))
                        .collect()
                })
                .await;
        }
        // Implicit read transaction: claim the scope first, so a rival unit
        // cannot open between our BEGIN and COMMIT.
        let _scope = self.tx.clone().lock_owned().await;
        self.run_blocking(move |conn| {
            let tx = conn.unchecked_transaction()?;
            let out = statements
                .iter()
                .map(|(sql, params)| query(&tx, sql, params))
                .collect::<rusqlite::Result<_>>()?;
            tx.commit()?;
            Ok(out)
        })
        .await
    }

    /// Serialize writers across processes: `BEGIN IMMEDIATE` takes the
    /// RESERVED lock up front (waiting up to the busy timeout), so validation
    /// reads and the following writes run atomically. Readers keep working.
    ///
    /// The `tx` scope is claimed before BEGIN and released by `end_unit`,
    /// so an implicit per-batch transaction can never slip in between and
    /// trip on a nested BEGIN.
    async fn begin_unit(&self) -> Result<()> {
        let guard = self.tx.clone().lock_owned().await;
        let r = self
            .run_blocking(|conn| conn.execute_batch("BEGIN IMMEDIATE"))
            .await;
        match r {
            Ok(()) => {
                *self.unit_guard.lock().unwrap() = Some(guard);
                Ok(())
            }
            // The guard drops here: the scope is released on failure.
            Err(e) => Err(e),
        }
    }

    async fn end_unit(&self, commit: bool) -> Result<()> {
        let sql = if commit { "COMMIT" } else { "ROLLBACK" };
        let r = self.run_blocking(move |conn| conn.execute_batch(sql)).await;
        // Release the scope after the transaction is closed.
        self.unit_guard.lock().unwrap().take();
        r
    }
}

/// CLI configuration for the SQLite backend (`--backend sqlite`).
#[derive(clap::Args, Clone, Debug)]
pub struct SqliteConfig {
    /// SQLite database file path.
    #[arg(
        long = "sqlite-path",
        env = "RINE_SQLITE_PATH",
        default_value = "rine.db"
    )]
    pub path: PathBuf,
}

/// Factory that opens the configured SQLite file.
pub struct SqliteFactory {
    config: SqliteConfig,
}

impl SqliteFactory {
    pub fn new(config: SqliteConfig) -> Self {
        Self { config }
    }
}

impl BackendFactory for SqliteFactory {
    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn build(&self) -> anyhow::Result<Arc<dyn rine_etcd::Backend>> {
        let db = SqliteDb::open(&self.config.path)?;
        Ok(Arc::new(crate::SqliteBackend::new(Arc::new(db))))
    }
}
