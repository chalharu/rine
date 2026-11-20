//! SQLite SQL dialect for `rine` table.
//!
//! Positional `?` binds; `AUTOINCREMENT` ids; `COALESCE` instead of `NVL`;
//! `LIMIT 1` instead of `ROWNUM = 1`. The compact-revision bookkeeping is an
//! update-then-insert pair (SQLite has no `MERGE`).

use rine_etcd::backend::ListOptions;
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
            "CREATE INDEX rine_name_index ON rine(name)",
            "CREATE INDEX rine_name_id_index ON rine(name, id)",
            "CREATE INDEX rine_id_deleted_index ON rine(id, deleted)",
            "CREATE INDEX rine_prev_revision_index ON rine(prev_revision)",
            "CREATE UNIQUE INDEX rine_name_prev_revision_uindex ON rine(name, prev_revision)",
        ]
    }

    fn ensure_indexes_sql() -> &'static [&'static str] {
        &[
            "CREATE INDEX IF NOT EXISTS rine_live_name_index ON rine(name) \
           WHERE deleted = 0 AND name <> 'compact_rev_key'",
        ]
    }

    fn probe_sql() -> &'static str {
        "SELECT 1 FROM rine LIMIT 1"
    }

    fn bind(_: usize) -> String {
        "?".into()
    }

    fn first_row() -> &'static str {
        "LIMIT 1"
    }

    fn compacted_rev_sql() -> &'static str {
        "SELECT prev_revision FROM rine WHERE name = 'compact_rev_key'"
    }

    fn events_sql() -> String {
        format!(
            "SELECT {COLS} FROM rine WHERE created >= ? AND created <= ? AND name <> 'compact_rev_key' ORDER BY id",
            COLS = Self::cols(),
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

    fn live_range_queries(
        end: Option<&[u8]>,
        opts: &ListOptions,
    ) -> Option<(Option<String>, Option<String>)> {
        let predicate = match end {
            None | Some([]) => "name = ?",
            Some([0]) => "name >= ?",
            Some(_) => "name >= ? AND name < ?",
        };
        let filter = format!("{predicate} AND deleted = 0 AND name <> 'compact_rev_key'");
        let rows = (!opts.count_only).then(|| {
            let cols = if opts.keys_only {
                "id, name, created, deleted, create_revision, prev_revision, lease, version, NULL"
            } else {
                Self::cols()
            };
            let limit = if opts.limit > 0 { " LIMIT ?" } else { "" };
            format!("SELECT {cols} FROM rine WHERE {filter} ORDER BY name, id{limit}")
        });
        let count = (opts.count_only || (opts.limit > 0 && !matches!(end, None | Some([]))))
            .then(|| format!("SELECT COUNT(*) FROM rine WHERE {filter}"));
        Some((rows, count))
    }

    fn set_deleted_by_id_sql() -> &'static str {
        "UPDATE rine SET deleted = ? WHERE id = ? AND deleted = 0"
    }

    fn compact_delete_sql() -> &'static str {
        "DELETE FROM rine WHERE created < ? AND name <> 'compact_rev_key' \
         AND id NOT IN (SELECT MAX(id) FROM rine WHERE created <= ? GROUP BY name)"
    }

    fn set_compact_rev_sql() -> &'static str {
        "UPDATE rine SET prev_revision = ? WHERE name = 'compact_rev_key'"
    }

    fn set_compact_rev_insert_sql() -> &'static str {
        "INSERT INTO rine (id, name, created, deleted, create_revision, prev_revision, lease) \
         VALUES (0, 'compact_rev_key', 0, 0, 0, ?, 0)"
    }

    fn db_size_sql() -> &'static str {
        "SELECT COALESCE(SUM(LENGTH(name) + LENGTH(value)), 0) FROM rine"
    }

    fn required_indexes() -> &'static [(&'static str, &'static str)] {
        &[(
            "rine_name_prev_revision_uindex",
            "CREATE UNIQUE INDEX IF NOT EXISTS rine_name_prev_revision_uindex ON rine(name, prev_revision)",
        )]
    }

    fn index_probe_sql() -> String {
        "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_are_positional_qmark() {
        let owned = [
            SqliteDialect::list_range_sql(),
            SqliteDialect::live_by_names_sql(2),
            SqliteDialect::events_sql(),
        ];
        for sql in owned.iter().map(String::as_str).chain([
            SqliteDialect::set_compact_rev_sql(),
            SqliteDialect::set_compact_rev_insert_sql(),
            SqliteDialect::compact_delete_sql(),
            SqliteDialect::set_deleted_by_id_sql(),
        ]) {
            assert!(sql.contains('?'), "{sql}");
            assert!(!sql.contains(":1"), "{sql}");
        }
    }
}
