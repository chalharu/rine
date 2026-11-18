//! SQLite-backed `Backend` for rine-etcd.
//!
//! Same kine-style `kine` table and MVCC rules as the Oracle backend,
//! expressed in the [`SqliteDialect`]. [`SqliteDb`] is the live
//! [`SqlExecutor`] over rusqlite (blocking calls run on Tokio's blocking
//! pool behind a mutex, like the Oracle executor).

mod db;
mod dialect;

pub use db::SqliteDb;
pub use dialect::SqliteDialect;
pub use rine_store_executor::{Row, SqlBackend, SqlExecutor, SqlValue};

/// SQLite-flavored [`SqlBackend`].
pub type SqliteBackend = SqlBackend<SqliteDb, SqliteDialect>;
