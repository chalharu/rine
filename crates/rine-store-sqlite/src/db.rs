//! Live [`SqlExecutor`] over rusqlite.
//!
//! The driver API is blocking, so every call runs on Tokio's blocking pool
//! via `spawn_blocking`. The connection is acquired asynchronously so queued
//! requests don't occupy blocking threads waiting for it.

use std::{
    path::Path,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use rine_etcd::error::{Error, Result};
use rine_store_executor::{Dialect, Row, SqlExecutor, SqlValue};
use rusqlite::{ToSql, types::ToSqlOutput};
use tokio::sync::Mutex;

/// Live SQLite connection. `Clone` shares the same underlying connection.
#[derive(Clone, Debug)]
pub struct SqliteDb {
    conn: Arc<Mutex<rusqlite::Connection>>,
    in_unit: Arc<AtomicBool>,
}

impl SqliteDb {
    /// Open (or create) a SQLite database file.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        Self::tune(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            in_unit: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Open a private in-memory database (tests).
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;
        Self::tune(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            in_unit: Arc::new(AtomicBool::new(false)),
        })
    }

    /// WAL for multi-process visibility, `FULL` sync per commit, and a busy
    /// timeout so a commit unit waits for a rival writer instead of failing.
    fn tune(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
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
        conn.busy_timeout(Duration::from_secs(30))?;
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
            .map_err(|e| Error::Backend(format!("db task failed: {e}")))?
    }
}

/// A unique-constraint violation means another writer committed a conflicting
/// row first: report it as an optimistic-concurrency conflict, never as a
/// backend failure.
fn cvt_error(e: rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(failure, _) = &e
        && failure.code == rusqlite::ErrorCode::ConstraintViolation
    {
        return Error::Conflict;
    }
    let msg = e.to_string();
    if crate::dialect::SqliteDialect::is_unique_violation(&msg) {
        return Error::Conflict;
    }
    Error::Backend(msg)
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
            Box::new(SqliteRealUnexpected(f)),
        )),
        rusqlite::types::ValueRef::Text(t) => {
            Ok(SqlValue::Str(String::from_utf8_lossy(t).into_owned()))
        }
        rusqlite::types::ValueRef::Blob(b) => Ok(SqlValue::Bytes(b.to_vec())),
    }
}

#[derive(Debug)]
struct SqliteRealUnexpected(f64);

impl std::fmt::Display for SqliteRealUnexpected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unexpected REAL value {}", self.0)
    }
}

impl std::error::Error for SqliteRealUnexpected {}

#[async_trait]
impl SqlExecutor for SqliteDb {
    async fn execute_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<()> {
        if self.in_unit.load(Ordering::SeqCst) {
            // Inside a commit unit the connection already holds BEGIN
            // IMMEDIATE; run the statements directly instead of nesting a
            // second transaction.
            return self
                .run_blocking(move |conn| {
                    for (sql, params) in statements {
                        conn.prepare_cached(&sql)?
                            .execute(rusqlite::params_from_iter(params.iter().map(Bind)))?;
                    }
                    Ok(())
                })
                .await;
        }
        self.run_blocking(move |conn| {
            let tx = conn.unchecked_transaction()?;
            for (sql, params) in statements {
                tx.prepare_cached(&sql)?
                    .execute(rusqlite::params_from_iter(params.iter().map(Bind)))?;
            }
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
        if self.in_unit.load(Ordering::SeqCst) {
            return self
                .run_blocking(move |conn| {
                    statements
                        .iter()
                        .map(|(sql, params)| query(conn, sql, params))
                        .collect()
                })
                .await;
        }
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
    async fn begin_unit(&self) -> Result<()> {
        self.run_blocking(|conn| conn.execute_batch("BEGIN IMMEDIATE"))
            .await?;
        self.in_unit.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn end_unit(&self, commit: bool) -> Result<()> {
        let sql = if commit { "COMMIT" } else { "ROLLBACK" };
        let r = self.run_blocking(move |conn| conn.execute_batch(sql)).await;
        self.in_unit.store(false, Ordering::SeqCst);
        r
    }
}
