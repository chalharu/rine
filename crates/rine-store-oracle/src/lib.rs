//! Oracle-backed `Backend` for rine-etcd.
//!
//! Schema mirrors kine's `kine` table: `(id, name, created, deleted,
//! create_revision, prev_revision, lease, version, value)` where `id` is the
//! revision. The value column is a BLOB; the official `oracledb` driver speaks
//! the temporary-LOB protocol natively, so no LONG RAW workaround is needed.
//!
//! [`OracleDb`] is the live [`SqlExecutor`] over a real database connection.
//! [`MockExec`] is the in-memory executor used by unit tests.

pub mod ddl;
pub mod dialect;
pub mod executor;
pub mod live;
pub mod mock;
pub mod store;

pub use dialect::{Dialect, OracleDialect, COLS};

pub use executor::{Row, SqlExecutor, SqlValue};
pub use live::OracleDb;
pub use mock::MockExec;
pub use store::{OracleBackend, SqlBackend};
