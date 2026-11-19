//! SQLite-backed `Backend` for rine-etcd.
//!
//! Same `rine` table and MVCC rules as the Oracle backend,
//! expressed in the [`SqliteDialect`]. [`SqliteDb`] is the live
//! [`SqlExecutor`] over rusqlite (blocking calls run on Tokio's blocking
//! pool behind a mutex, like the Oracle executor).

mod db;
mod dialect;

pub use db::{SqliteConfig, SqliteDb, SqliteFactory};
pub use dialect::SqliteDialect;

pub type SqliteBackend = rine_store_executor::SqlBackend<SqliteDb, dialect::SqliteDialect>;
