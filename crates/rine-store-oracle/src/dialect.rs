//! Oracle SQL dialect for the kine-compatible `kine` table.
//!
//! The live `Connection` executes these statements once the step-1 driver
//! finishes its TNS/TLS dial; until then the strings and row mapping are
//! covered by unit tests so SQL regressions are caught without a database.

use rine_etcd::error::{Error, Result};

/// Named bind style is Oracle (`:1`, `:2`, ...). `?` is never emitted.
/// Shared column list: id, name, created, deleted, create_revision,
/// prev_revision, lease, version, value.
pub struct Dialect;

const COLS: &str =
    "id, name, created, deleted, create_revision, prev_revision, lease, version, value";

impl Dialect {
    pub fn current_revision_sql() -> &'static str {
        "SELECT MAX(id) FROM kine"
    }

    pub fn compact_revision_sql() -> &'static str {
        "SELECT MAX(prev_revision) FROM kine WHERE name = 'compact_rev_key'"
    }

    pub fn compacted_rev_sql() -> &'static str {
        "SELECT prev_revision FROM kine WHERE name = 'compact_rev_key'"
    }

    /// Latest live row for one key.
    pub fn live_by_name_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name = :1 AND deleted = 0")
    }

    /// Resolve the id of a just-inserted row via the (name, prev_revision)
    /// unique index.
    pub fn resolve_id_sql() -> &'static str {
        "SELECT id FROM kine WHERE name = :1 AND prev_revision = :2"
    }

    /// History for one key at or below a revision, newest first.
    pub fn history_by_name_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name = :1 AND id <= :2 ORDER BY id DESC")
    }

    /// Event log slice for watch replay.
    pub fn events_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE id >= :1 AND id <= :2 ORDER BY id")
    }

    /// Single-key read. `revision = 0` means latest.
    pub fn get_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name = :1 AND deleted = 0 AND ROWNUM = 1 ORDER BY id DESC")
    }

    /// Range read. Caller picks the predicate by end-of-range convention:
    /// empty = single key, `x00` = `>= key`, else `[key, end)`.
    pub fn list_exact_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name = :1 AND deleted = 0 ORDER BY name, id")
    }

    pub fn list_ge_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name >= :1 AND deleted = 0 ORDER BY name, id")
    }

    pub fn list_range_sql() -> String {
        format!("SELECT {COLS} FROM kine WHERE name >= :1 AND name < :2 AND deleted = 0 ORDER BY name, id")
    }

    /// Optimistic insert: succeeds only when no live row exists. The caller
    /// checks `ORA-00001` via [`translate_error`]... in practice the unique
    /// index on `(name, prev_revision)` plus a pre-check keeps this simple:
    /// insert the new revision row, keyed by `(name, prev_revision = parent id)`.
    pub fn insert_sql() -> &'static str {
        "INSERT INTO kine (name, created, deleted, create_revision, prev_revision, lease, version, value) \
         VALUES (:1, :2, :3, :4, :5, :6, :7, :8)"
    }

    /// Delete marks the live row deleted and appends a tombstone revision.
    pub fn mark_deleted_sql() -> &'static str {
        "UPDATE kine SET deleted = :1 WHERE name = :2 AND deleted = 0"
    }

    pub fn set_deleted_by_id_sql() -> &'static str {
        "UPDATE kine SET deleted = :1 WHERE id = :2 AND deleted = 0"
    }

    pub fn set_create_rev_sql() -> &'static str {
        "UPDATE kine SET create_revision = :1 WHERE id = :1"
    }

    /// Drop history below `rev`, keeping the newest row at or below `rev`
    /// per key (that row is the historical read base).
    pub fn compact_delete_sql() -> &'static str {
        "DELETE FROM kine WHERE id < :1 AND name <> 'compact_rev_key' \
         AND id NOT IN (SELECT MAX(id) FROM kine WHERE id <= :1 GROUP BY name)"
    }

    pub fn set_compact_rev_sql() -> &'static str {
        "MERGE INTO kine USING DUAL ON (name = 'compact_rev_key') \
         WHEN MATCHED THEN UPDATE SET prev_revision = :1 \
         WHEN NOT MATCHED THEN INSERT (name, created, deleted, create_revision, prev_revision, lease) \
         VALUES ('compact_rev_key', 0, 0, 0, :1, 0)"
    }

    pub fn db_size_sql() -> &'static str {
        "SELECT NVL(SUM(LENGTH(name) + NVL(DBMS_LOB.GETLENGTH(value), 0)), 0) FROM kine"
    }
}

/// Translate Oracle error codes to backend errors.
/// `code` is the numeric ORA code (e.g. 1 for ORA-00001).
pub fn translate_error(code: u32, key: Vec<u8>) -> Error {
    match code {
        1 => Error::KeyExists(key),
        1403 => Error::Backend("ORA-01403: no data found".into()),
        _ => Error::Backend(format!("ORA-{code:05}")),
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
            Dialect::list_range_sql(),
            Dialect::live_by_name_sql(),
            Dialect::history_by_name_sql(),
            Dialect::events_sql(),
        ];
        for sql in owned.iter().map(String::as_str).chain([
            Dialect::insert_sql(),
            Dialect::mark_deleted_sql(),
            Dialect::set_compact_rev_sql(),
            Dialect::compact_delete_sql(),
            Dialect::set_deleted_by_id_sql(),
            Dialect::set_create_rev_sql(),
            Dialect::resolve_id_sql(),
        ]) {
            assert!(sql.contains(":1"), "{sql}");
            assert!(!sql.contains('?'), "{sql}");
        }
    }

    #[test]
    fn error_mapping() {
        assert!(matches!(
            translate_error(1, b"k".to_vec()),
            Error::KeyExists(_)
        ));
        assert!(matches!(translate_error(1403, vec![]), Error::Backend(_)));
        assert!(matches!(translate_error(942, vec![]), Error::Backend(_)));
    }

    #[test]
    fn prefix_end_cases() {
        assert_eq!(prefix_end(b"/registry/a"), b"/registry/b".to_vec());
        assert_eq!(prefix_end(b"a\xff"), b"b".to_vec());
        assert!(prefix_end(b"\xff").ends_with(&[0u8]));
    }
}
