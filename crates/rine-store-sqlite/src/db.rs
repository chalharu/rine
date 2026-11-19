//! Live [`SqlExecutor`] over rusqlite.
//!
//! The driver API is blocking, so every call runs on Tokio's blocking pool
//! via `spawn_blocking`. One connection is shared behind a mutex: the backend
//! assumes a single writer, and serializing access keeps concurrent `&self`
//! use sound. Statements autocommit (no explicit transaction handling,
//! matching the Oracle executor's commit-per-`execute`).

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use rine_etcd::error::{Error, Result};
use rine_store_executor::{Row, SqlExecutor, SqlValue};
use rusqlite::ToSql;

/// Live SQLite connection. `Clone` shares the same underlying connection.
#[derive(Clone, Debug)]
pub struct SqliteDb {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl SqliteDb {
    /// Open (or create) a SQLite database file.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Open a private in-memory database (tests).
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = rusqlite::Connection::open_in_memory()?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    async fn run_blocking<T, F>(&self, op: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&rusqlite::Connection) -> Result<T, rusqlite::Error> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|_| Error::Backend("db mutex poisoned".into()))?;
            op(&guard).map_err(|e| Error::Backend(e.to_string()))
        })
        .await
        .map_err(|e| Error::Backend(format!("db task failed: {e}")))?
    }
}

/// Owned bind values; [`as_bind`] re-exports them as `&dyn ToSql`.
enum OwnedParam {
    Int(i64),
    Str(String),
    Bytes(Vec<u8>),
    Null(Option<Vec<u8>>),
}

impl From<&SqlValue> for OwnedParam {
    fn from(v: &SqlValue) -> Self {
        match v {
            SqlValue::Int(i) => Self::Int(*i),
            SqlValue::Str(s) => Self::Str(s.clone()),
            SqlValue::Bytes(b) => Self::Bytes(b.clone()),
            SqlValue::Null => Self::Null(None),
        }
    }
}

fn as_bind(p: &OwnedParam) -> &dyn ToSql {
    match p {
        OwnedParam::Int(v) => v,
        OwnedParam::Str(s) => s,
        OwnedParam::Bytes(b) => b,
        OwnedParam::Null(v) => v,
    }
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
    async fn execute(&self, sql: &str, params: Vec<SqlValue>) -> Result<u64> {
        let sql = sql.to_string();
        self.run_blocking(move |conn| {
            let owned: Vec<OwnedParam> = params.iter().map(OwnedParam::from).collect();
            let refs: Vec<&dyn ToSql> = owned.iter().map(as_bind).collect();
            let n = conn.execute(&sql, rusqlite::params_from_iter(refs))?;
            Ok(n as u64)
        })
        .await
    }

    async fn query_row(&self, sql: &str, params: Vec<SqlValue>) -> Result<Option<Row>> {
        let sql = sql.to_string();
        self.run_blocking(move |conn| {
            let owned: Vec<OwnedParam> = params.iter().map(OwnedParam::from).collect();
            let refs: Vec<&dyn ToSql> = owned.iter().map(as_bind).collect();
            let mut stmt = conn.prepare(&sql)?;
            let ncols = stmt.column_count();
            let mut rows = stmt.query(rusqlite::params_from_iter(refs))?;
            match rows.next()? {
                Some(row) => Ok(Some(read_row(row, ncols)?)),
                None => Ok(None),
            }
        })
        .await
    }

    async fn query_all(&self, sql: &str, params: Vec<SqlValue>) -> Result<Vec<Row>> {
        let sql = sql.to_string();
        self.run_blocking(move |conn| {
            let owned: Vec<OwnedParam> = params.iter().map(OwnedParam::from).collect();
            let refs: Vec<&dyn ToSql> = owned.iter().map(as_bind).collect();
            let mut stmt = conn.prepare(&sql)?;
            let ncols = stmt.column_count();
            let mut rows = stmt.query(rusqlite::params_from_iter(refs))?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                out.push(read_row(row, ncols)?);
            }
            Ok(out)
        })
        .await
    }
}
