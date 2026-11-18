//! SQLite SQL dialect for the kine-style `kine` table.
//!
//! Positional `?` binds; `AUTOINCREMENT` ids; `COALESCE` instead of `NVL`;
//! `LIMIT 1` instead of `ROWNUM = 1`. The compact-revision bookkeeping is an
//! update-then-insert pair (SQLite has no `MERGE`).

use rine_store_executor::Dialect;

/// SQLite SQL dialect.
pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn create_table_sql() -> &'static str {
        "CREATE TABLE rine (\
            id INTEGER PRIMARY KEY AUTOINCREMENT, \
            name VARCHAR(630), \
            created INTEGER DEFAULT 0, \
            deleted INTEGER DEFAULT 0, \
            create_revision INTEGER DEFAULT 0, \
            prev_revision INTEGER DEFAULT 0, \
            lease INTEGER DEFAULT 0, \
            version INTEGER DEFAULT 1, \
            value BLOB\
        )"
    }

    fn create_indexes_sql() -> &'static [&'static str] {
        &[
            "CREATE INDEX kine_name_index ON kine(name)",
            "CREATE INDEX kine_name_id_index ON kine(name, id)",
            "CREATE INDEX kine_id_deleted_index ON kine(id, deleted)",
            "CREATE INDEX kine_prev_revision_index ON kine(prev_revision)",
            "CREATE UNIQUE INDEX kine_name_prev_revision_uindex ON kine(name, prev_revision)",
        ]
    }

    fn probe_sql() -> &'static str {
        "SELECT 1 FROM rine LIMIT 1"
    }

    fn current_revision_sql() -> &'static str {
        "SELECT MAX(id) FROM kine"
    }

    fn compact_revision_sql() -> &'static str {
        "SELECT MAX(prev_revision) FROM rine WHERE name = 'compact_rev_key'"
    }

    fn compacted_rev_sql() -> &'static str {
        "SELECT prev_revision FROM rine WHERE name = 'compact_rev_key'"
    }

    fn live_by_name_sql() -> String {
        format!(
            "SELECT {COLS} FROM rine WHERE name = ? AND deleted = 0",
            COLS = Self::cols()
        )
    }

    fn resolve_id_sql() -> &'static str {
        "SELECT id FROM rine WHERE name = ? AND prev_revision = ?"
    }

    fn history_by_name_sql() -> String {
        format!(
            "SELECT {COLS} FROM rine WHERE name = ? AND id <= ? ORDER BY id DESC",
            COLS = Self::cols()
        )
    }

    fn events_sql() -> String {
        format!(
            "SELECT {COLS} FROM rine WHERE id >= ? AND id <= ? ORDER BY id",
            COLS = Self::cols()
        )
    }

    fn get_sql() -> String {
        format!(
            "SELECT {COLS} FROM rine WHERE name = ? AND deleted = 0 ORDER BY id DESC LIMIT 1",
            COLS = Self::cols()
        )
    }

    fn list_exact_sql() -> String {
        format!(
            "SELECT {COLS} FROM rine WHERE name = ? AND deleted = 0 ORDER BY name, id",
            COLS = Self::cols()
        )
    }

    fn list_ge_sql() -> String {
        format!(
            "SELECT {COLS} FROM rine WHERE name >= ? AND deleted = 0 ORDER BY name, id",
            COLS = Self::cols()
        )
    }

    fn list_range_sql() -> String {
        format!(
            "SELECT {COLS} FROM rine WHERE name >= ? AND name < ? AND deleted = 0 ORDER BY name, id",
            COLS = Self::cols()
        )
    }

    fn insert_sql() -> &'static str {
        "INSERT INTO rine (name, created, deleted, create_revision, prev_revision, lease, version, value) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    }

    fn mark_deleted_sql() -> &'static str {
        "UPDATE rine SET deleted = ? WHERE name = ? AND deleted = 0"
    }

    fn set_deleted_by_id_sql() -> &'static str {
        "UPDATE rine SET deleted = ? WHERE id = ? AND deleted = 0"
    }

    fn set_create_rev_sql() -> &'static str {
        "UPDATE rine SET create_revision = ? WHERE id = ?"
    }

    fn compact_delete_sql() -> &'static str {
        "DELETE FROM rine WHERE id < ? AND name <> 'compact_rev_key' \
         AND id NOT IN (SELECT MAX(id) FROM rine WHERE id <= ? GROUP BY name)"
    }

    fn set_compact_rev_sql() -> &'static str {
        "UPDATE rine SET prev_revision = ? WHERE name = 'compact_rev_key'"
    }

    fn set_compact_rev_insert_sql() -> &'static str {
        "INSERT INTO rine (name, created, deleted, create_revision, prev_revision, lease) \
         VALUES ('compact_rev_key', 0, 0, 0, ?, 0)"
    }

    fn db_size_sql() -> &'static str {
        "SELECT COALESCE(SUM(LENGTH(name) + LENGTH(value)), 0) FROM kine"
    }

    fn is_missing_table(msg: &str) -> bool {
        msg.contains("no such table")
    }

    fn is_already_exists(msg: &str) -> bool {
        msg.contains("already exists")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_are_positional_qmark() {
        let owned = [
            SqliteDialect::list_range_sql(),
            SqliteDialect::live_by_name_sql(),
            SqliteDialect::history_by_name_sql(),
            SqliteDialect::events_sql(),
        ];
        for sql in owned.iter().map(String::as_str).chain([
            SqliteDialect::insert_sql(),
            SqliteDialect::mark_deleted_sql(),
            SqliteDialect::set_compact_rev_sql(),
            SqliteDialect::set_compact_rev_insert_sql(),
            SqliteDialect::compact_delete_sql(),
            SqliteDialect::set_deleted_by_id_sql(),
            SqliteDialect::set_create_rev_sql(),
            SqliteDialect::resolve_id_sql(),
        ]) {
            assert!(sql.contains('?'), "{sql}");
            assert!(!sql.contains(":1"), "{sql}");
        }
    }
}
