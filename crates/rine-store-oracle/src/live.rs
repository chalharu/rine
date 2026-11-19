//! Live [`SqlExecutor`] over the official `oracledb` driver.
//!
//! The driver API is blocking, so every call runs on Tokio's blocking pool
//! via `spawn_blocking`. One connection is shared behind a mutex: the backend
//! assumes a single writer, and serializing access also
//! keeps concurrent `&self` use sound.
//!
//! [`SqlValue`] mapping: `Int` -> NUMBER, `Str` -> VARCHAR2,
//! `Bytes` -> BLOB, `Null` -> typed BLOB NULL (the only NULL bind the backend
//! emits is the tombstone `value`).

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use oracledb::ToDbValue;
use rine_etcd::error::{Error, Result};
use rine_store_executor::{Dialect, Row, SqlExecutor, SqlValue};

/// Live Oracle connection. `Clone` shares the same underlying session.
///
/// Reads bypass the driver's statement cache (`exclude_from_cache`).
/// rine synchronizes multiple nodes through the database's ACID
/// guarantees, so every read must reflect committed state; serving reads
/// from a client-side cache risks stale results. Writes keep using the
/// cache: they always reach the server and surface hard errors, never
/// stale results.
#[derive(Clone, Debug)]
pub struct OracleDb {
    conn: Arc<Mutex<oracledb::Connection>>,
    in_unit: Arc<AtomicBool>,
}

impl OracleDb {
    /// Connect with an Easy Connect string (`host/service`), a full connect
    /// descriptor, or a TNS alias (with `TNS_ADMIN` set or `set_config_dir`).
    pub fn connect(
        user: &str,
        password: &str,
        connect_string: &str,
    ) -> std::result::Result<Self, oracledb::Error> {
        let config = oracledb::Config::default()
            .set_credentials(user, password)
            .set_connect_string(connect_string)?;
        Self::connect_with_config(config)
    }

    /// Connect with a prebuilt [`oracledb::Config`] (wallets, timeouts, ...).
    pub fn connect_with_config(
        config: oracledb::Config,
    ) -> std::result::Result<Self, oracledb::Error> {
        let conn = oracledb::connect(config)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            in_unit: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn run_blocking<T, F>(&self, op: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&oracledb::Connection) -> std::result::Result<T, oracledb::Error>
            + Send
            + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = conn
                .lock()
                .map_err(|_| Error::Backend("db mutex poisoned".into()))?;
            op(&guard).map_err(cvt_error)
        })
        .await
        .map_err(|e| Error::Backend(format!("db task failed: {e}")))?
    }
}

/// A unique-constraint violation means another writer committed a conflicting
/// row first: report it as an optimistic-concurrency conflict, never as a
/// backend failure.
fn cvt_error(e: oracledb::Error) -> Error {
    if e.db_error_code() == Some(1) {
        return Error::Conflict;
    }
    let msg = e.to_string();
    if crate::dialect::OracleDialect::is_unique_violation(&msg) {
        return Error::Conflict;
    }
    Error::Backend(msg)
}

/// Owned bind values; [`as_bind`] re-exports them as `&dyn ToDbValue`.
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

fn as_bind(p: &OwnedParam) -> &dyn ToDbValue {
    match p {
        OwnedParam::Int(v) => v,
        OwnedParam::Str(s) => s,
        OwnedParam::Bytes(b) => b,
        OwnedParam::Null(v) => v,
    }
}

fn read_col(row: &oracledb::Row, idx: usize) -> Result<SqlValue, oracledb::Error> {
    let db_type = row.columns()[idx].db_type();
    if *db_type == *oracledb::DB_TYPE_NUMBER {
        let v: Option<i64> = row.get(idx)?;
        Ok(v.map(SqlValue::Int).unwrap_or(SqlValue::Null))
    } else if db_type.is_binary_type() {
        let v: Option<Vec<u8>> = row.get(idx)?;
        Ok(v.map(SqlValue::Bytes).unwrap_or(SqlValue::Null))
    } else {
        let v: Option<String> = row.get(idx)?;
        Ok(v.map(SqlValue::Str).unwrap_or(SqlValue::Null))
    }
}

fn read_row(row: &oracledb::Row) -> Result<Row, oracledb::Error> {
    let mut values = Vec::with_capacity(row.columns().len());
    for i in 0..row.columns().len() {
        values.push(read_col(row, i)?);
    }
    Ok(Row { values })
}

fn is_no_data(e: &oracledb::Error) -> bool {
    e.kind() == &oracledb::ErrorKind::NoDataFound || e.db_error_code() == Some(1403)
}

#[async_trait]
impl SqlExecutor for OracleDb {
    async fn execute_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<()> {
        if self.in_unit.load(Ordering::SeqCst) {
            // Inside a commit unit the table lock serializes writers; the
            // unit boundary commits, so statements must not commit one by one.
            return self
                .run_blocking(move |conn| {
                    for (sql, params) in statements {
                        let owned: Vec<_> = params.iter().map(OwnedParam::from).collect();
                        let refs: Vec<_> = owned.iter().map(as_bind).collect();
                        conn.execute(&sql, &refs)?;
                    }
                    Ok(())
                })
                .await;
        }
        self.run_blocking(move |conn| {
            let result = (|| {
                for (sql, params) in statements {
                    let owned: Vec<_> = params.iter().map(OwnedParam::from).collect();
                    let refs: Vec<_> = owned.iter().map(as_bind).collect();
                    conn.execute(&sql, &refs)?;
                }
                conn.commit()
            })();
            if result.is_err() {
                conn.rollback()?;
            }
            result
        })
        .await
    }

    async fn execute(&self, sql: &str, params: Vec<SqlValue>) -> Result<u64> {
        if self.in_unit.load(Ordering::SeqCst) {
            let sql = sql.to_string();
            return self
                .run_blocking(move |conn| {
                    let owned: Vec<OwnedParam> = params.iter().map(OwnedParam::from).collect();
                    let refs: Vec<&dyn ToDbValue> = owned.iter().map(as_bind).collect();
                    Ok(conn.execute(&sql, &refs)?.rows_affected())
                })
                .await;
        }
        let sql = sql.to_string();
        self.run_blocking(move |conn| {
            let owned: Vec<OwnedParam> = params.iter().map(OwnedParam::from).collect();
            let refs: Vec<&dyn ToDbValue> = owned.iter().map(as_bind).collect();
            let n = conn.execute(&sql, &refs)?.rows_affected();
            conn.commit()?;
            Ok(n)
        })
        .await
    }

    async fn query_row(&self, sql: &str, params: Vec<SqlValue>) -> Result<Option<Row>> {
        let sql = sql.to_string();
        self.run_blocking(move |conn| {
            let owned: Vec<OwnedParam> = params.iter().map(OwnedParam::from).collect();
            let refs: Vec<&dyn ToDbValue> = owned.iter().map(as_bind).collect();
            let stmt = conn.statement(&sql)?.exclude_from_cache().build()?;
            match stmt.query_row(&refs) {
                Ok(row) => read_row(&row).map(Some),
                Err(e) if is_no_data(&e) => Ok(None),
                Err(e) => Err(e),
            }
        })
        .await
    }

    async fn query_all(&self, sql: &str, params: Vec<SqlValue>) -> Result<Vec<Row>> {
        let sql = sql.to_string();
        self.run_blocking(move |conn| {
            let owned: Vec<OwnedParam> = params.iter().map(OwnedParam::from).collect();
            let refs: Vec<&dyn ToDbValue> = owned.iter().map(as_bind).collect();
            let stmt = conn.statement(&sql)?.exclude_from_cache().build()?;
            let cursor = stmt.query(&refs)?;
            let mut out = Vec::new();
            for row in cursor {
                out.push(read_row(&row?)?);
            }
            Ok(out)
        })
        .await
    }

    /// Serialize writers across nodes: `LOCK TABLE ... IN EXCLUSIVE MODE`
    /// blocks rival commits (readers keep working) until the unit commits or
    /// rolls back. The flag also suspends per-statement auto-commit above.
    async fn begin_unit(&self) -> Result<()> {
        self.in_unit.store(true, Ordering::SeqCst);
        let r = self
            .run_blocking(|conn| {
                let refs: Vec<&dyn ToDbValue> = Vec::new();
                conn.execute("LOCK TABLE rine IN EXCLUSIVE MODE", &refs)?;
                Ok(())
            })
            .await;
        if r.is_err() {
            self.in_unit.store(false, Ordering::SeqCst);
        }
        r
    }

    async fn end_unit(&self, commit: bool) -> Result<()> {
        let r = self
            .run_blocking(move |conn| {
                if commit {
                    conn.commit()
                } else {
                    conn.rollback()
                }
            })
            .await;
        self.in_unit.store(false, Ordering::SeqCst);
        r
    }
}
