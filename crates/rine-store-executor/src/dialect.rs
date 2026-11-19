//! SQL dialects for kine-style `rine` backends.
//!
//! [`Dialect`] abstracts the per-database SQL (`:1` vs `?` placeholders,
//! DDL, a few functions); the MVCC logic in `SqlBackend` is shared. The
//! strings are covered by unit tests (via `MockExec`) so SQL regressions are
//! caught without a database; the live XE test in CI covers Oracle against a
//! real database.

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
