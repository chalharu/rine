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

use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use oracledb::ToDbValue;
use rine_etcd::{
    backend::{Event, Revision},
    error::{Error, Result},
};
use rine_store_executor::{Row, SqlExecutor, SqlValue};

/// Live Oracle connection. `Clone` shares the same underlying session.
///
/// Bypass the driver's cursor cache on reads so a DROP/CREATE performed by
/// another node cannot leave this session with an invalid cursor.
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
        .map_err(|e| Error::task("db task failed", e))?
    }
}

/// Classify driver errors at the boundary, where the typed `oracledb::Error`
/// (including its numeric ORA code) is still in scope. Generic code above
/// this layer matches the typed [`Error`] variants and never parses messages:
/// - ORA-00001 (unique violation): another writer committed first →
///   optimistic-concurrency [`Error::Conflict`], never a backend failure.
/// - ORA-00942: fresh database without the `rine` table.
/// - ORA-00955: a concurrent creator already ran the DDL.
/// - ORA-00054: transient lock conflict with a concurrent creator/writer.
fn cvt_error(e: oracledb::Error) -> Error {
    let msg = e.to_string();
    match e.db_error_code() {
        Some(1) => Error::Conflict,
        Some(942) => Error::TableMissing(msg),
        Some(955) => Error::AlreadyExists(msg),
        Some(54) => Error::LockContention(msg),
        // Unknown errors keep the original value verbatim: no re-stringify.
        _ => Error::Backend(e.into()),
    }
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

/// Build owned binds and project them to `&dyn ToDbValue` for one call.
fn with_binds<T>(
    params: &[SqlValue],
    f: impl FnOnce(&[&dyn ToDbValue]) -> Result<T, oracledb::Error>,
) -> Result<T, oracledb::Error> {
    let owned: Vec<OwnedParam> = params.iter().map(OwnedParam::from).collect();
    let refs: Vec<&dyn ToDbValue> = owned.iter().map(as_bind).collect();
    f(&refs)
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

fn query_all_on(
    conn: &oracledb::Connection,
    sql: &str,
    params: &[SqlValue],
) -> Result<Vec<Row>, oracledb::Error> {
    let stmt = conn
        .statement(sql)?
        .exclude_from_cache()
        .prefetch_rows(64)
        .build()?;
    let cursor = with_binds(params, |refs| stmt.query(refs))?;
    let mut out = Vec::new();
    for row in cursor {
        out.push(read_row(&row?)?);
    }
    Ok(out)
}

fn apply_batch(
    conn: &oracledb::Connection,
    statements: Vec<(String, Vec<SqlValue>)>,
) -> std::result::Result<(), oracledb::Error> {
    for (sql, params) in statements {
        with_binds(&params, |refs| conn.execute(&sql, refs))?;
    }
    Ok(())
}

fn is_no_data(e: &oracledb::Error) -> bool {
    e.kind() == &oracledb::ErrorKind::NoDataFound || e.db_error_code() == Some(1403)
}

// RAW binds avoid a temporary LOB round trip for small values (1..=2000
// bytes: empty values stay on the generic path so they are stored as empty
// BLOBs rather than NULL, and larger values exceed RAW(2000)). TO_BLOB runs
// on Oracle and the predecessor revision is checked while holding the table
// lock. Revisions are non-negative, so NVL(..., -1) is a null-safe compare.
const COMMIT_PUT: &str = r"
DECLARE
    k rine.name%TYPE := :key;
    v RAW(2000) := :value;
    expected NUMBER := :expected;
    cr NUMBER := :create_rev;
    lease_id NUMBER := :lease_id;
    ver NUMBER := :version;
    head_id NUMBER := 1;
    head_rev NUMBER := 1;
    prev_id NUMBER := 0;
    live_rev NUMBER;
BEGIN
    LOCK TABLE rine IN EXCLUSIVE MODE;
    BEGIN
        SELECT id, created INTO head_id, head_rev
        FROM rine ORDER BY id DESC FETCH FIRST 1 ROWS ONLY;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        head_id := 1;
        head_rev := 1;
    END;
    BEGIN
        SELECT id, CASE WHEN deleted = 0 THEN created END
        INTO prev_id, live_rev
        FROM rine WHERE name = k ORDER BY id DESC FETCH FIRST 1 ROWS ONLY;
    EXCEPTION WHEN NO_DATA_FOUND THEN
        prev_id := 0;
        live_rev := NULL;
    END;
    IF NVL(live_rev, -1) != NVL(expected, -1) THEN
        ROLLBACK;
        :fresh := 0;
    ELSE
        head_id := GREATEST(head_id, 1) + 1;
        head_rev := GREATEST(head_rev, 1) + 1;
        IF live_rev IS NOT NULL THEN
            UPDATE rine SET deleted = head_id WHERE id = prev_id AND deleted = 0;
        END IF;
        INSERT INTO rine (id, name, created, deleted, create_revision, prev_revision,
                          lease, version, value)
        VALUES (head_id, k, head_rev, 0, NVL(cr, head_rev), prev_id, lease_id, ver, TO_BLOB(v));
        :fresh := head_rev;
        COMMIT WRITE IMMEDIATE WAIT;
    END IF;
END;";

#[async_trait]
impl SqlExecutor for OracleDb {
    async fn try_commit_put(&self, event: &Event) -> Result<Option<Revision>> {
        let Some(kv) = event
            .kv
            .as_ref()
            .filter(|kv| (1..=2000).contains(&kv.value.len()))
        else {
            return Ok(None);
        };
        let key = String::from_utf8_lossy(&event.key).into_owned();
        let value = kv.value.clone();
        let expected = event.prev_kv.as_ref().map(|kv| kv.mod_revision);
        let create = (kv.create_revision != event.revision).then_some(kv.create_revision);
        let (lease, version) = (kv.lease, kv.version);
        let fresh = self
            .run_blocking(move |conn| {
                let result = conn
                    .execute(
                        COMMIT_PUT,
                        &[
                            &key,
                            &value.as_ref(),
                            &expected,
                            &create,
                            &lease,
                            &version,
                            &oracledb::DB_TYPE_NUMBER,
                        ],
                    )
                    .and_then(|mut result| result.out_bind_data().get::<i64>(0));
                if result.is_err() {
                    conn.rollback()?;
                }
                result
            })
            .await?;
        if fresh == 0 {
            return Err(Error::Conflict);
        }
        Ok(Some(fresh))
    }

    async fn execute_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<()> {
        // Inside a commit unit the table lock serializes writers and the unit
        // boundary commits, so statements must not commit one by one.
        let unit = self.in_unit.load(Ordering::SeqCst);
        self.run_blocking(move |conn| {
            let r = (|| {
                apply_batch(conn, statements)?;
                if !unit {
                    conn.commit()?;
                }
                Ok(())
            })();
            if !unit && r.is_err() {
                conn.rollback()?;
            }
            r
        })
        .await
    }

    async fn execute(&self, sql: &str, params: Vec<SqlValue>) -> Result<u64> {
        let unit = self.in_unit.load(Ordering::SeqCst);
        let sql = sql.to_string();
        self.run_blocking(move |conn| {
            let n = with_binds(&params, |refs| {
                Ok(conn.execute(&sql, refs)?.rows_affected())
            })?;
            if !unit {
                conn.commit()?;
            }
            Ok(n)
        })
        .await
    }

    async fn query_row(&self, sql: &str, params: Vec<SqlValue>) -> Result<Option<Row>> {
        let sql = sql.to_string();
        self.run_blocking(move |conn| {
            let stmt = conn.statement(&sql)?.exclude_from_cache().build()?;
            with_binds(&params, |refs| match stmt.query_row(refs) {
                Ok(row) => read_row(&row).map(Some),
                Err(e) if is_no_data(&e) => Ok(None),
                Err(e) => Err(e),
            })
        })
        .await
    }

    async fn query_all(&self, sql: &str, params: Vec<SqlValue>) -> Result<Vec<Row>> {
        let sql = sql.to_string();
        self.run_blocking(move |conn| query_all_on(conn, &sql, &params))
            .await
    }

    /// Run related reads in one blocking call on one connection instead of
    /// one runtime round trip per statement (the default `query_batch`).
    async fn query_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<Vec<Vec<Row>>> {
        self.run_blocking(move |conn| {
            statements
                .iter()
                .map(|(sql, params)| query_all_on(conn, sql, params))
                .collect()
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

/// CLI configuration for the Oracle backend (`--backend oracle`).
#[derive(clap::Args, Clone, Debug)]
pub struct OracleConfig {
    /// Oracle connect string: Easy Connect (`host/service`), a full connect
    /// descriptor, or a TNS alias (resolved via TNS_ADMIN or --wallet-dir).
    #[arg(long = "oracle-connect", env = "RINE_ORACLE_CONNECT")]
    pub connect: Option<String>,
    /// Wallet directory (`tnsnames.ora` + `ewallet.pem`): used as the Oracle
    /// Net configuration directory and wallet location.
    #[arg(long = "wallet-dir", env = "RINE_WALLET_DIR")]
    pub wallet_dir: Option<PathBuf>,
    #[arg(long = "user", default_value = "", env = "RINE_ORACLE_USER")]
    pub user: String,
    #[arg(long = "password", default_value = "", env = "RINE_ORACLE_PASSWORD")]
    pub password: String,
}

impl OracleConfig {
    /// Connect with the configured credentials as a shared backend.
    pub fn open_backend(&self) -> anyhow::Result<Arc<dyn rine_etcd::Backend>> {
        let connect = self.connect.clone().ok_or_else(|| {
            anyhow::anyhow!("oracle backend needs --oracle-connect (or RINE_ORACLE_CONNECT)")
        })?;
        let mut config = oracledb::Config::default().set_credentials(&self.user, &self.password);
        if let Some(dir) = &self.wallet_dir {
            let dir = dir
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("wallet dir is not UTF-8"))?;
            config = config.set_config_dir(dir).set_wallet_location(dir);
        }
        let config = config.set_connect_string(&connect)?;
        let db = OracleDb::connect_with_config(config)?;
        Ok(Arc::new(crate::OracleBackend::new(Arc::new(db))))
    }
}
