//! Oracle-backed `Backend` for rine-etcd.
//!
//! The `rine` table uses `(id, name, created, deleted,
//! create_revision, prev_revision, lease, version, value)` where `id` is the
//! revision. The value column is a BLOB; the official `oracledb` driver speaks
//! the temporary-LOB protocol natively, so no LONG RAW workaround is needed.
//!
//! [`OracleDb`] is the live [`SqlExecutor`] over a real database connection.
//! Oracle SQL strings are covered by the dialect unit tests plus the live XE
//! test in CI; the shared MVCC logic is covered by the SQLite backend tests.

mod db;
mod dialect;

pub use db::{OracleConfig, OracleDb};
use rine_store_executor::SqlBackend;

pub type OracleBackend = SqlBackend<OracleDb, dialect::OracleDialect>;
