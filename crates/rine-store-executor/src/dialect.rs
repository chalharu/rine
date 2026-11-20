//! SQL dialects for `rine` backends.
//!
//! [`Dialect`] abstracts the per-database SQL (`:1` vs `?` placeholders,
//! DDL, a few functions); the MVCC logic in `SqlBackend` is shared. The
//! strings are covered by unit tests asserting placeholder style, plus the
//! live XE test in CI covers Oracle against a real database.

use rine_etcd::backend::ListOptions;

/// SQL dialect for one database.
pub trait Dialect: Send + Sync + 'static {
    fn create_table_sql() -> &'static str;
    fn create_indexes_sql() -> &'static [&'static str];

    /// Schema probe for `ensure_schema`.
    fn probe_sql() -> &'static str;

    fn current_revision_sql() -> String;
    fn bind(index: usize) -> String;

    fn insert_event_sql() -> String {
        let binds = (1..=9).map(Self::bind).collect::<Vec<_>>().join(", ");
        format!("INSERT INTO rine ({}) VALUES ({binds})", Self::cols())
    }

    /// Predecessor rows for several ids at once.
    fn predecessor_set_sql(n: usize) -> String {
        let binds = (1..=n).map(Self::bind).collect::<Vec<_>>().join(", ");
        format!("SELECT {} FROM rine WHERE id IN ({binds})", Self::cols())
    }

    /// `WHERE` predicate for the end-of-range convention (`None`/`[]` = single
    /// key, `[0]` = `>= key`, else `[key, end)`), with bind placeholders.
    /// Returns the predicate and the number of key binds (2 for `[key, end)`).
    fn range_predicate(end: Option<&[u8]>) -> (String, usize) {
        let (first, second) = (Self::bind(1), Self::bind(2));
        match end {
            None | Some([]) => (format!("name = {first}"), 1),
            Some([0]) => (format!("name >= {first}"), 1),
            Some(_) => (format!("name >= {first} AND name < {second}"), 2),
        }
    }

    /// Whether a range read needs a separate full-count query: point lookups
    /// and unlimited ranges derive the count from the returned rows.
    fn needs_count(end: Option<&[u8]>, opts: &ListOptions) -> bool {
        opts.count_only || (opts.limit > 0 && !matches!(end, None | Some([])))
    }

    fn snapshot_sql(end: Option<&[u8]>) -> String {
        let (predicate, binds) = Self::range_predicate(end);
        let revision = Self::bind(binds + 1);
        let cols = Self::cols();
        format!(
            "SELECT {cols} FROM (SELECT {cols}, ROW_NUMBER() OVER (PARTITION BY name ORDER BY id DESC) AS snapshot_rank \
             FROM rine WHERE {predicate} AND created <= {revision} AND name <> 'compact_rev_key') \
             WHERE snapshot_rank = 1 AND deleted <> 1 ORDER BY name"
        )
    }
    fn compacted_rev_sql() -> &'static str;

    /// Event log slice for watch replay.
    fn events_sql() -> String;

    /// Historical range read: live rows at `revision == cur` semantics are
    /// handled by the caller; this covers point lookups, `>= key`, and
    /// `[key, end)` with the end-of-range convention above.
    fn list_history_sql(end: Option<&[u8]>) -> String {
        let (predicate, _) = Self::range_predicate(end);
        format!(
            "SELECT {COLS} FROM rine WHERE {predicate} AND deleted = 0 ORDER BY name, id",
            COLS = Self::cols(),
        )
    }

    /// Latest-range pushdown: split value and count retrieval so a limited
    /// range can serve rows in index order with LIMIT while the count uses
    /// a covering index (a single-statement `COUNT(*) OVER ()` forces an
    /// ORDER BY sort over the full range into a temp B-tree). The first
    /// element is the rows query (`None` for `count_only`), the second the
    /// full-range count (`None` when the count is derived from the rows:
    /// point lookups and unlimited ranges). Both run in one `query_batch`,
    /// i.e. one read transaction.
    fn live_range_queries(
        end: Option<&[u8]>,
        opts: &ListOptions,
    ) -> (Option<String>, Option<String>);

    /// Commit lookup: current revision, highest physical id, key,
    /// latest physical id for that key, and its live revision (or NULL).
    fn commit_state_sql(names: usize) -> String;

    /// Delete marks the live row deleted and appends a tombstone revision.
    fn set_deleted_by_id_sql() -> &'static str;

    /// Drop history below `rev`, keeping the newest row at or below `rev`
    /// per key (that row is the historical read base).
    fn compact_delete_sql() -> &'static str;
    /// Upsert of the compaction watermark, expressed as update ...
    fn set_compact_rev_sql() -> &'static str;
    /// ... followed by insert-if-missing (for dialects without `MERGE`).
    fn set_compact_rev_insert_sql() -> &'static str;

    fn db_size_sql() -> &'static str;

    /// Shared column list: id, name, created, deleted, create_revision,
    /// prev_revision, lease, version, value.
    fn cols() -> &'static str {
        "id, name, created, deleted, create_revision, prev_revision, lease, version, value"
    }
}
