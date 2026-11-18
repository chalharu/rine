//! SQL dialects for kine-style `kine` backends.
//!
//! [`Dialect`] abstracts the per-database SQL (`:1` vs `?` placeholders,
//! DDL, a few functions); the MVCC logic in `SqlBackend` is shared. The
//! strings are covered by unit tests (via `MockExec`) so SQL regressions are
//! caught without a database; the live XE test in CI covers Oracle against a
//! real database.

use rine_etcd::error::{Error, Result};

/// Shared column list: id, name, created, deleted, create_revision,
/// prev_revision, lease, version, value.
pub const COLS: &str =
    "id, name, created, deleted, create_revision, prev_revision, lease, version, value";

/// SQL dialect for one kine-style database.
pub trait Dialect: Send + Sync + 'static {
    fn create_table_sql() -> &'static str;
    fn create_indexes_sql() -> &'static [&'static str];

    /// Schema probe for `ensure_schema`.
    fn probe_sql() -> &'static str;

    fn current_revision_sql() -> &'static str;
    fn compact_revision_sql() -> &'static str;
    fn compacted_rev_sql() -> &'static str;

    /// Latest live row for one key.
    fn live_by_name_sql() -> String;

    /// Resolve the id of a just-inserted row via the (name, prev_revision)
    /// unique index.
    fn resolve_id_sql() -> &'static str;

    /// History for one key at or below a revision, newest first.
    fn history_by_name_sql() -> String;

    /// Event log slice for watch replay.
    fn events_sql() -> String;

    /// Single-key read.
    fn get_sql() -> String;

    /// Range reads. Caller picks the predicate by end-of-range convention:
    /// empty = single key, `x00` = `>= key`, else `[key, end)`.
    fn list_exact_sql() -> String;
    fn list_ge_sql() -> String;
    fn list_range_sql() -> String;

    /// Optimistic insert: the new revision row, keyed by
    /// `(name, prev_revision = parent id)`.
    fn insert_sql() -> &'static str;

    /// Delete marks the live row deleted and appends a tombstone revision.
    fn mark_deleted_sql() -> &'static str;
    fn set_deleted_by_id_sql() -> &'static str;
    fn set_create_rev_sql() -> &'static str;

    /// Drop history below `rev`, keeping the newest row at or below `rev`
    /// per key (that row is the historical read base).
    fn compact_delete_sql() -> &'static str;
    /// Upsert of the compaction watermark, expressed as update ...
    fn set_compact_rev_sql() -> &'static str;
    /// ... followed by insert-if-missing (for dialects without `MERGE`).
    fn set_compact_rev_insert_sql() -> &'static str;

    fn db_size_sql() -> &'static str;

    /// True when `msg` reports a missing table (fresh database).
    fn is_missing_table(msg: &str) -> bool;
    /// True when `msg` reports a name clash from a concurrent creator.
    fn is_already_exists(msg: &str) -> bool;
}

/// Oracle SQL dialect. Named bind style (`:1`, `:2`, ...).
pub struct OracleDialect;

impl Dialect for OracleDialect {
    fn create_table_sql() -> &'static str {
        crate::ddl::CREATE_TABLE
    }

    fn create_indexes_sql() -> &'static [&'static str] {
        crate::ddl::CREATE_INDEXES
    }

    fn probe_sql() -> &'static str {
        "SELECT 1 FROM kine WHERE ROWNUM = 1"
    }

    fn current_revision_sql() -> &'static str {
        "SELECT MAX(id) FROM kine"
    }

    fn compact_revision_sql() -> &'static str {
        "SELECT MAX(prev_revision) FROM kine WHERE name = 'compact_rev_key'"
    }

    fn compacted_rev_sql() -> &'static str {
        "SELECT prev_revision FROM kine WHERE name = 'compact_rev_key'"
    }

    fn live_by_name_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name = :1 AND deleted = 0")
    }

    fn resolve_id_sql() -> &'static str {
        "SELECT id FROM kine WHERE name = :1 AND prev_revision = :2"
    }

    fn history_by_name_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name = :1 AND id <= :2 ORDER BY id DESC")
    }

    fn events_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE id >= :1 AND id <= :2 ORDER BY id")
    }

    fn get_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name = :1 AND deleted = 0 AND ROWNUM = 1 ORDER BY id DESC")
    }

    fn list_exact_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name = :1 AND deleted = 0 ORDER BY name, id")
    }

    fn list_ge_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name >= :1 AND deleted = 0 ORDER BY name, id")
    }

    fn list_range_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name >= :1 AND name < :2 AND deleted = 0 ORDER BY name, id")
    }

    fn insert_sql() -> &'static str {
        "INSERT INTO kine (name, created, deleted, create_revision, prev_revision, lease, version, value) \
         VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"
    }

    fn mark_deleted_sql() -> &'static str {
        "UPDATE kine SET deleted = :1 WHERE name = :2 AND deleted = 0"
    }

    fn set_deleted_by_id_sql() -> &'static str {
        "UPDATE kine SET deleted = :1 WHERE id = :2 AND deleted = 0"
    }

    fn set_create_rev_sql() -> &'static str {
        "UPDATE kine SET create_revision = :1 WHERE id = :2"
    }

    fn compact_delete_sql() -> &'static str {
        "DELETE FROM kine WHERE id < :1 AND name <> 'compact_rev_key' \
         AND id NOT IN (SELECT MAX(id) FROM kine WHERE id <= :2 GROUP BY name)"
    }

    fn set_compact_rev_sql() -> &'static str {
        "UPDATE kine SET prev_revision = :1 WHERE name = 'compact_rev_key'"
    }

    fn set_compact_rev_insert_sql() -> &'static str {
        "INSERT INTO kine (name, created, deleted, create_revision, prev_revision, lease) \
         VALUES ('compact_rev_key', 0, 0, 0, :1, 0)"
    }

    fn db_size_sql() -> &'static str {
        "SELECT NVL(SUM(LENGTH(name) + NVL(DBMS_LOB.GETLENGTH(value), 0)), 0) FROM kine"
    }

    fn is_missing_table(msg: &str) -> bool {
        msg.contains("ORA-00942")
    }

    fn is_already_exists(msg: &str) -> bool {
        msg.contains("ORA-00955")
    }
}

impl OracleDialect {
    /// Translate Oracle error codes to backend errors.
    /// `code` is the numeric ORA code (e.g. 1 for ORA-00001).
    pub fn translate_error(code: u32, key: Vec<u8>) -> Error {
        match code {
            1 => Error::KeyExists(key),
            1403 => Error::Backend("ORA-01403: no data found".into()),
            _ => Error::Backend(format!("ORA-{code:05}")),
        }
    }
}

/// End of the `[key, end)` range for a prefix scan: increment last byte,
/// appending `0x00` when the prefix ends in `0xFF` (etcd `GetPrefixRangeEnd`).
pub fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] != 0xFF {
            end[i] += 1;
            end.truncate(i + 1);
            return end;
        }
    }
    end.push(0);
    end
}

pub fn check_result<E>(r: Result<E>) -> Result<E> {
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_are_oracle_style() {
        let owned = [
            OracleDialect::list_range_sql(),
            OracleDialect::live_by_name_sql(),
            OracleDialect::history_by_name_sql(),
            OracleDialect::events_sql(),
        ];
        for sql in owned.iter().map(String::as_str).chain([
            OracleDialect::insert_sql(),
            OracleDialect::mark_deleted_sql(),
            OracleDialect::set_compact_rev_sql(),
            OracleDialect::set_compact_rev_insert_sql(),
            OracleDialect::compact_delete_sql(),
            OracleDialect::set_deleted_by_id_sql(),
            OracleDialect::set_create_rev_sql(),
            OracleDialect::resolve_id_sql(),
        ]) {
            assert!(sql.contains(":1"), "{sql}");
            assert!(!sql.contains('?'), "{sql}");
        }
    }

    #[test]
    fn error_mapping() {
        assert!(matches!(
            OracleDialect::translate_error(1, b"k".to_vec()),
            Error::KeyExists(_)
        ));
        assert!(matches!(
            OracleDialect::translate_error(1403, vec![]),
            Error::Backend(_)
        ));
        assert!(matches!(
            OracleDialect::translate_error(942, vec![]),
            Error::Backend(_)
        ));
    }

    #[test]
    fn prefix_end_cases() {
        assert_eq!(prefix_end(b"/registry/a"), b"/registry/b".to_vec());
        assert_eq!(prefix_end(b"a\xff"), b"b".to_vec());
        assert!(prefix_end(b"\xff").ends_with(&[0u8]));
    }
}
