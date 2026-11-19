//! Oracle-backed `Backend` for rine-etcd.
//!
//! The `rine` table uses `(id, name, created, deleted,
//! create_revision, prev_revision, lease, version, value)` where `id` is the
//! revision. The value column is a BLOB; the official `oracledb` driver speaks
//! the temporary-LOB protocol natively, so no LONG RAW workaround is needed.
//!
//! [`OracleDb`] is the live [`SqlExecutor`] over a real database connection.

mod ddl;
mod dialect;
mod live;
#[cfg(any(feature = "mock", test))]
mod mock;
mod store;

pub use live::OracleDb;
#[cfg(feature = "mock")]
pub use mock::MockExec;
use rine_store_executor::SqlBackend;

pub type OracleBackend = SqlBackend<OracleDb, dialect::OracleDialect>;
#[cfg(any(feature = "mock", test))]
pub type OracleBackendMock = SqlBackend<mock::MockExec, dialect::OracleDialect>;
