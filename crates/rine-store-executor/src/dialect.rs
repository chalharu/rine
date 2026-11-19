//! SQL dialects for `rine` backends.
//!
//! [`Dialect`] abstracts the per-database SQL (`:1` vs `?` placeholders,
//! DDL, a few functions); the MVCC logic in `SqlBackend` is shared. The
//! strings are covered by unit tests (via `MockExec`) so SQL regressions are
//! caught without a database; the live XE test in CI covers Oracle against a
//! real database.

use rine_etcd::backend::ListOptions;

/// SQL dialect for one database.
pub trait Dialect: Send + Sync + 'static {
    fn create_table_sql() -> &'static str;
    fn create_indexes_sql() -> &'static [&'static str];

    /// Idempotent index DDL to apply to new and existing tables.
    fn ensure_indexes_sql() -> &'static [&'static str] {
        &[]
    }

    /// Schema probe for `ensure_schema`.
    fn probe_sql() -> &'static str;

    fn current_revision_sql() -> String {
        format!(
            "SELECT {} FROM rine ORDER BY id DESC {}",
            Self::logical_revision(),
            Self::first_row()
        )
    }
    fn last_row_id_sql() -> &'static str {
        "SELECT MAX(id) FROM rine"
    }
    fn bind(index: usize) -> String;
    fn first_row() -> &'static str;

    // Older rows used the physical ID as their revision and left `created` zero.
    fn logical_revision() -> &'static str {
        "CASE WHEN created > 0 THEN created ELSE id END"
    }

    fn insert_event_sql() -> String {
        let binds = (1..=9).map(Self::bind).collect::<Vec<_>>().join(", ");
        format!("INSERT INTO rine ({}) VALUES ({binds})", Self::cols())
    }

    fn predecessor_sql() -> String {
        format!(
            "SELECT {} FROM rine WHERE id = {}",
            Self::cols(),
            Self::bind(1)
        )
    }

    fn latest_by_name_sql() -> String {
        format!(
            "SELECT id FROM rine WHERE name = {} ORDER BY id DESC {}",
            Self::bind(1),
            Self::first_row()
        )
    }

    fn snapshot_sql(end: Option<&[u8]>) -> String {
        let key = Self::bind(1);
        let (predicate, revision) = match end {
            None | Some([]) => (format!("name = {key}"), Self::bind(2)),
            Some([0]) => (format!("name >= {key}"), Self::bind(2)),
            Some(_) => (
                format!("name >= {key} AND name < {}", Self::bind(2)),
                Self::bind(3),
            ),
        };
        let cols = Self::cols();
        let logical = Self::logical_revision();
        format!(
            "SELECT {cols} FROM (SELECT {cols}, ROW_NUMBER() OVER (PARTITION BY name ORDER BY id DESC) AS snapshot_rank \
             FROM rine WHERE {predicate} AND {logical} <= {revision} AND name <> 'compact_rev_key') \
             WHERE snapshot_rank = 1 AND deleted <> 1 ORDER BY name"
        )
    }
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

    /// Optional latest-range pushdown. When both queries are supplied the
    /// count must describe the full range, not just the limited result.
    fn live_range_queries(
        _end: Option<&[u8]>,
        _opts: &ListOptions,
    ) -> Option<(Option<String>, Option<String>)> {
        None
    }

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
    /// True when `msg` reports a transient lock conflict with a concurrent
    /// creator or writer (worth a bounded retry, e.g. DDL racing DML).
    fn is_lock_contention(_msg: &str) -> bool {
        false
    }
    /// True when `msg` reports a unique-constraint violation: another writer
    /// committed a conflicting row first. Callers treat it as an optimistic
    /// concurrency conflict, never as corruption.
    fn is_unique_violation(msg: &str) -> bool;

    /// Revision bookkeeping mirrors rine's `compact_rev_key` row.
    fn compacted_rev_key() -> &'static str {
        "compact_rev_key"
    }

    /// Shared column list: id, name, created, deleted, create_revision,
    /// prev_revision, lease, version, value.
    fn cols() -> &'static str {
        "id, name, created, deleted, create_revision, prev_revision, lease, version, value"
    }
}
