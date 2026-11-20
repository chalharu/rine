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

    /// Index DDL applied to pre-existing tables (idempotent: re-running
    /// tolerates an already-existing index). Fresh tables get these from
    /// `create_indexes_sql`; tables created before an index existed converge
    /// here instead of failing or running degraded.
    fn ensure_indexes_sql() -> &'static [&'static str] {
        &[]
    }

    /// Schema probe for `ensure_schema`.
    fn probe_sql() -> &'static str;

    fn current_revision_sql() -> String {
        format!(
            "SELECT created FROM rine ORDER BY id DESC {}",
            Self::first_row()
        )
    }
    fn bind(index: usize) -> String;
    fn first_row() -> &'static str;

    fn insert_event_sql() -> String {
        let binds = (1..=9).map(Self::bind).collect::<Vec<_>>().join(", ");
        format!("INSERT INTO rine ({}) VALUES ({binds})", Self::cols())
    }

    /// Predecessor rows for several ids at once.
    fn predecessor_set_sql(n: usize) -> String {
        let binds = (1..=n).map(Self::bind).collect::<Vec<_>>().join(", ");
        format!("SELECT {} FROM rine WHERE id IN ({binds})", Self::cols())
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
        format!(
            "SELECT {cols} FROM (SELECT {cols}, ROW_NUMBER() OVER (PARTITION BY name ORDER BY id DESC) AS snapshot_rank \
             FROM rine WHERE {predicate} AND created <= {revision} AND name <> 'compact_rev_key') \
             WHERE snapshot_rank = 1 AND deleted <> 1 ORDER BY name"
        )
    }
    fn compacted_rev_sql() -> &'static str;

    /// Event log slice for watch replay.
    fn events_sql() -> String;

    /// Range reads. Caller picks the predicate by end-of-range convention:
    /// empty = single key, `x00` = `>= key`, else `[key, end)`.
    fn list_exact_sql() -> String;
    fn list_ge_sql() -> String;
    fn list_range_sql() -> String;

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
