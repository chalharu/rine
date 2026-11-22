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
            // Partial live index: pre-existing tables converge here since a
            // concurrent creator's `AlreadyExists` is tolerated.
            "CREATE INDEX rine_live_name_index ON rine(name) \
            WHERE deleted = 0 AND name <> 'compact_rev_key'",
        ]
    }

    fn probe_sql() -> &'static str {
        "SELECT 1 FROM rine LIMIT 1"
    }

    fn current_revision_sql() -> String {
        "SELECT created FROM rine ORDER BY id DESC LIMIT 1".into()
    }

    fn bind(_: usize) -> String {
        "?".into()
    }

    fn events_sql() -> String {
        format!(
            "SELECT {COLS} FROM rine WHERE created >= ? AND created <= ? AND name <> 'compact_rev_key' ORDER BY id",
            COLS = Self::cols(),
        )
    }

    /// Latest read split into rows + count so each uses the covering index:
    /// rows in index order with LIMIT, count without touching values. No
    /// `COUNT(*) OVER ()` sort over the full range. Both statements run in
    /// one `query_batch` (one read transaction).
    fn live_range_queries(
        end: Option<&[u8]>,
        opts: &ListOptions,
    ) -> (Option<String>, Option<String>) {
        let (predicate, _) = Self::range_predicate(end);
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
        // Point lookups and unlimited ranges derive the count from the rows.
        let count = Self::needs_count(end, opts)
            .then(|| format!("SELECT COUNT(*) FROM rine WHERE {filter}"));
        (rows, count)
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

    /// Database file size in bytes: page accounting is O(1) no matter how
    /// large the table grows, unlike a SUM over every row. Includes
    /// freelist space, exactly like etcd's own backend-size reporting.
    fn db_size_sql() -> &'static str {
        "SELECT page_count * page_size FROM pragma_page_count(), pragma_page_size()"
    }

    fn create_leases_table_sql() -> &'static str {
        "CREATE TABLE rine_leases (\
            id INTEGER PRIMARY KEY, \
            granted_ttl INTEGER DEFAULT 0, \
            expires_at_ms INTEGER DEFAULT 0, \
            keys BLOB\
        )"
    }

    fn leases_probe_sql() -> &'static str {
        "SELECT 1 FROM rine_leases LIMIT 1"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_are_positional_qmark() {
        let (rows, count) = SqliteDialect::live_range_queries(
            Some(b"x"),
            &ListOptions {
                limit: 10,
                ..Default::default()
            },
        );
        let owned = [
            SqliteDialect::list_history_sql(Some(b"x")),
            SqliteDialect::commit_state_sql(2),
            SqliteDialect::events_sql(),
            rows.unwrap(),
            count.unwrap(),
            SqliteDialect::lease_update_sql(),
            SqliteDialect::lease_insert_sql(),
            SqliteDialect::lease_select_sql(),
            SqliteDialect::lease_delete_sql(),
            SqliteDialect::lease_expired_select_sql(),
            SqliteDialect::lease_expired_delete_sql(),
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
