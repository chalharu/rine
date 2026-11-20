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
            "CREATE UNIQUE INDEX IF NOT EXISTS rine_name_prev_revision_uindex ON rine(name, prev_revision)",
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

    /// Latest read plus revision/count metadata in one statement, mirroring
    /// the Oracle fast path: leading revision and total columns, then the kv
    /// columns (`row_to_kv_at` offset 2). The `(SELECT 1)` row guarantees the
    /// metadata survives empty ranges with a NULL id.
    fn live_range_sql(end: Option<&[u8]>, opts: &ListOptions) -> String {
        let predicate = match end {
            None | Some([]) => "name = ?",
            Some([0]) => "name >= ?",
            Some(_) => "name >= ? AND name < ?",
        };
        let filter = format!("{predicate} AND deleted = 0 AND name <> 'compact_rev_key'");
        // Uncorrelated scalar: evaluated once. Never NULL (MAX floor of 1).
        let revision = "MAX(COALESCE((SELECT created FROM rine ORDER BY id DESC LIMIT 1), 1), 1)";
        if opts.count_only {
            return format!(
                "SELECT {revision}, (SELECT COUNT(*) FROM rine WHERE {filter}), \
                 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL FROM (SELECT 1)"
            );
        }
        let cols = if opts.keys_only {
            "id, name, created, deleted, create_revision, prev_revision, lease, version, NULL AS value"
        } else {
            Self::cols()
        };
        let (limit, total) = if opts.limit > 0 {
            let total = if matches!(end, None | Some([])) {
                ""
            } else {
                ", COUNT(*) OVER () AS total"
            };
            (" ORDER BY name, id LIMIT ?", total)
        } else {
            ("", "")
        };
        let count = if total.is_empty() {
            "NULL"
        } else {
            "COALESCE(r.total, 0)"
        };
        let projection = Self::cols()
            .split(", ")
            .map(|c| format!("r.{c}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT {revision}, {count}, {projection} FROM (SELECT 1) \
             LEFT JOIN (SELECT {cols}{total} FROM rine WHERE {filter}{limit}) AS r ON 1=1 \
             ORDER BY r.name, r.id"
        )
    }

    /// Commit lookup in one statement, mirroring the Oracle fast path:
    /// current revision, highest physical id, then one row per matched key
    /// (latest physical id plus its live revision, NULL when deleted/absent).
    fn commit_state_sql(names: usize) -> String {
        let meta = "(SELECT created, id FROM rine ORDER BY id DESC LIMIT 1)";
        let header = "MAX(COALESCE(m.created, 1), 1), MAX(COALESCE(m.id, 1), 1)";
        if names == 0 {
            return format!("SELECT {header}, NULL FROM (SELECT 1) LEFT JOIN {meta} AS m ON 1=1");
        }
        let binds = (0..names).map(|_| "?").collect::<Vec<_>>().join(", ");
        format!(
            "SELECT {header}, k.name, k.key_id, k.live_revision FROM (SELECT 1) \
             LEFT JOIN {meta} AS m ON 1=1 \
             LEFT JOIN (SELECT name, COALESCE(MAX(CASE WHEN deleted = 0 THEN id END), MAX(id)) AS key_id, \
                  MAX(CASE WHEN deleted = 0 THEN created END) AS live_revision \
                  FROM rine WHERE name IN ({binds}) GROUP BY name) AS k ON 1=1"
        )
    }

    fn set_deleted_by_id_sql() -> &'static str {
        "UPDATE rine SET deleted = ? WHERE id = ? AND deleted = 0"
    }

    fn compact_delete_sql() -> &'static str {
        "DELETE FROM rine WHERE id IN (SELECT id FROM rine WHERE created < ? AND name <> 'compact_rev_key' \
         AND id NOT IN (SELECT MAX(id) FROM rine WHERE created <= ? GROUP BY name) LIMIT ?)"
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_are_positional_qmark() {
        let owned = [
            SqliteDialect::list_range_sql(),
            SqliteDialect::live_range_sql(None, &ListOptions::default()),
            SqliteDialect::commit_state_sql(2),
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
