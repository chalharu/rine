//! Oracle-backed `Backend` for rine-etcd.
//!
//! Schema mirrors kine's `rine` table: `(id, name, created, deleted,
//! create_revision, prev_revision, lease, version, value)` where `id` is the
//! revision. The value column is a BLOB; the official `oracledb` driver speaks
//! the temporary-LOB protocol natively, so no LONG RAW workaround is needed.
//!
//! [`OracleDb`] is the live [`SqlExecutor`] over a real database connection.
//! [`MockExec`] is the in-memory executor used by unit tests.

mod ddl;
mod dialect;
mod live;
mod mock;
mod store;

pub use dialect::OracleDialect;
pub use live::OracleDb;
pub use mock::MockExec;
pub use store::OracleBackend;
