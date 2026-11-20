//! In-memory `SqlExecutor` for tests and examples.
//!
//! Interprets exactly the SQL strings `OracleBackend` generates (see
//! `dialect`), so backend logic is exercised without a database.

use std::collections::HashMap;

use async_trait::async_trait;
use rine_etcd::error::{Error, Result};
use rine_store_executor::{Dialect as _, Row, SqlExecutor, SqlValue};
use tokio::sync::Mutex;

use crate::dialect::{CREATE_INDEXES, CREATE_TABLE, OracleDialect};

#[derive(Debug, Clone)]
struct MockRow {
    id: i64,
    name: String,
    created: i64,
    deleted: i64,
    create_revision: i64,
    prev_revision: i64,
    lease: i64,
    version: i64,
    value: Option<Vec<u8>>,
}

impl MockRow {
    fn revision(&self) -> i64 {
        self.created
    }

    fn to_row(&self) -> Row {
        Row {
            values: vec![
                SqlValue::Int(self.id),
                SqlValue::Str(self.name.clone()),
                SqlValue::Int(self.created),
                SqlValue::Int(self.deleted),
                SqlValue::Int(self.create_revision),
                SqlValue::Int(self.prev_revision),
                SqlValue::Int(self.lease),
                SqlValue::Int(self.version),
                self.value
                    .clone()
                    .map(SqlValue::Bytes)
                    .unwrap_or(SqlValue::Null),
            ],
        }
    }
}

/// Predicate shape of an Oracle live-range pushdown query.
#[derive(Debug, Clone, Copy, PartialEq)]
enum LivePredicate {
    Exact,
    Prefix,
    Range,
}

/// In-memory executor matching `OracleBackend`'s SQL. Not for production.
pub struct MockExec {
    rows: Mutex<Vec<MockRow>>,
}

impl MockExec {
    pub fn new() -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
        }
    }

    fn str_param(p: &[SqlValue], i: usize) -> String {
        match &p[i] {
            SqlValue::Str(s) => s.clone(),
            _ => panic!("param {i} not a string"),
        }
    }

    fn int_param(p: &[SqlValue], i: usize) -> i64 {
        match &p[i] {
            SqlValue::Int(v) => *v,
            _ => panic!("param {i} not an int"),
        }
    }
    fn apply(rows: &mut Vec<MockRow>, sql: &str, params: Vec<SqlValue>) -> Result<u64> {
        if sql == OracleDialect::insert_event_sql() {
            let id = Self::int_param(&params, 0);
            let name = Self::str_param(&params, 1);
            let prev_revision = Self::int_param(&params, 5);
            if rows
                .iter()
                .any(|r| r.id == id || (r.name == name && r.prev_revision == prev_revision))
            {
                // Same as a real unique-constraint violation: another writer
                // committed a conflicting row first.
                return Err(Error::Conflict);
            }
            rows.push(MockRow {
                id,
                name,
                created: Self::int_param(&params, 2),
                deleted: Self::int_param(&params, 3),
                create_revision: Self::int_param(&params, 4),
                prev_revision,
                lease: Self::int_param(&params, 6),
                version: Self::int_param(&params, 7),
                value: match &params[8] {
                    SqlValue::Bytes(b) => Some(b.clone()),
                    SqlValue::Null => None,
                    _ => panic!("value param"),
                },
            });
            return Ok(1);
        }
        if sql == OracleDialect::set_deleted_by_id_sql() {
            let (new_del, id) = (Self::int_param(&params, 0), Self::int_param(&params, 1));
            for r in rows.iter_mut() {
                if r.id == id && r.deleted == 0 {
                    r.deleted = new_del;
                    return Ok(1);
                }
            }
            return Ok(0);
        }
        if sql == OracleDialect::set_compact_rev_sql() {
            let rev = Self::int_param(&params, 0);
            let mut n = 0;
            for r in rows.iter_mut() {
                if r.name == OracleDialect::compacted_rev_key() {
                    r.prev_revision = rev;
                    n += 1;
                }
            }
            return Ok(n);
        }
        if sql == OracleDialect::set_compact_rev_insert_sql() {
            let rev = Self::int_param(&params, 0);
            rows.push(MockRow {
                id: 0,
                name: OracleDialect::compacted_rev_key().into(),
                created: 0,
                deleted: 0,
                create_revision: 0,
                prev_revision: rev,
                lease: 0,
                version: 1,
                value: None,
            });
            return Ok(1);
        }
        if sql == OracleDialect::compact_delete_sql() {
            let rev = Self::int_param(&params, 0);
            // newest row at or below rev per name survives
            let mut keep: HashMap<String, i64> = HashMap::new();
            for r in rows.iter() {
                if r.revision() <= rev {
                    keep.entry(r.name.clone())
                        .and_modify(|m| *m = (*m).max(r.id))
                        .or_insert(r.id);
                }
            }
            let before = rows.len();
            rows.retain(|r| {
                !(r.revision() < rev
                    && r.name != OracleDialect::compacted_rev_key()
                    && keep.get(&r.name) != Some(&r.id))
            });
            return Ok((before - rows.len()) as u64);
        }
        if sql == CREATE_TABLE || CREATE_INDEXES.contains(&sql) {
            return Ok(0);
        }
        panic!("mock execute: unknown sql {sql}");
    }

    /// Oracle live-range pushdown shape (see `OracleDialect::live_range_queries`):
    /// rows queries carry the `compact_rev_key` exclusion plus an optional
    /// `FETCH FIRST` cap; count queries are `SELECT COUNT(*)`. Windowed
    /// snapshot queries share the exclusion literal and are excluded here.
    fn live_range_predicate(sql: &str) -> Option<LivePredicate> {
        if !sql.contains("name <> 'compact_rev_key'") || sql.contains("snapshot_rank") {
            return None;
        }
        if sql.contains("name < :2") {
            Some(LivePredicate::Range)
        } else if sql.contains("name = :1") {
            Some(LivePredicate::Exact)
        } else if sql.contains("name >= :1") {
            Some(LivePredicate::Prefix)
        } else {
            None
        }
    }

    /// Latest-visible rows for one live-range query, in `(name, id)` order.
    fn filter_live(
        rows: &[MockRow],
        pred: LivePredicate,
        params: &[SqlValue],
        keys_only: bool,
    ) -> Vec<Row> {
        let prefix = Self::str_param(params, 0);
        let mut out: Vec<&MockRow> = rows
            .iter()
            .filter(|r| {
                r.name != OracleDialect::compacted_rev_key()
                    && r.deleted == 0
                    && match pred {
                        LivePredicate::Exact => r.name == prefix,
                        LivePredicate::Prefix => r.name.as_str() >= prefix.as_str(),
                        LivePredicate::Range => {
                            let end = Self::str_param(params, 1);
                            r.name.as_str() >= prefix.as_str() && r.name.as_str() < end.as_str()
                        }
                    }
            })
            .collect();
        out.sort_by(|a, b| (&a.name, a.id).cmp(&(&b.name, b.id)));
        out.into_iter()
            .map(|r| {
                let mut row = MockRow::to_row(r);
                if keys_only {
                    row.values[8] = SqlValue::Null;
                }
                row
            })
            .collect()
    }
}

impl Default for MockExec {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SqlExecutor for MockExec {
    async fn execute(&self, sql: &str, params: Vec<SqlValue>) -> Result<u64> {
        let mut rows = self.rows.lock().await;
        Self::apply(&mut rows, sql, params)
    }

    async fn execute_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<()> {
        let mut rows = self.rows.lock().await;
        let mut pending = rows.clone();
        for (sql, params) in statements {
            Self::apply(&mut pending, &sql, params)?;
        }
        *rows = pending;
        Ok(())
    }

    async fn query_row(&self, sql: &str, _params: Vec<SqlValue>) -> Result<Option<Row>> {
        let rows = self.rows.lock().await;
        if sql == OracleDialect::probe_sql() {
            // Mock table always exists.
            return Ok(Some(Row {
                values: vec![SqlValue::Int(1)],
            }));
        }
        if sql == OracleDialect::index_probe_sql() {
            // Mock schema always carries the required indexes.
            return Ok(Some(Row {
                values: vec![SqlValue::Int(1)],
            }));
        }
        if sql == OracleDialect::current_revision_sql() {
            let max = rows.iter().max_by_key(|r| r.id).map(MockRow::revision);
            return Ok(max.map(|m| Row {
                values: vec![SqlValue::Int(m)],
            }));
        }
        if sql == OracleDialect::last_row_id_sql() {
            return Ok(rows.iter().map(|r| r.id).max().map(|id| Row {
                values: vec![SqlValue::Int(id)],
            }));
        }
        if sql == OracleDialect::compacted_rev_sql() {
            return Ok(rows
                .iter()
                .find(|r| r.name == OracleDialect::compacted_rev_key())
                .map(|r| Row {
                    values: vec![SqlValue::Int(r.prev_revision)],
                }));
        }
        if sql == OracleDialect::db_size_sql() {
            let sum: i64 = rows
                .iter()
                .map(|r| {
                    r.name.len() as i64 + r.value.as_ref().map(|v| v.len() as i64).unwrap_or(0)
                })
                .sum();
            return Ok(Some(Row {
                values: vec![SqlValue::Int(sum)],
            }));
        }
        panic!("mock query_row: unknown sql {sql}");
    }

    async fn query_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<Vec<Vec<Row>>> {
        // Single-row revision lookups route through `query_row`; everything
        // else (including multi-row IN-list batches) through `query_all`.
        let mut out = Vec::with_capacity(statements.len());
        for (sql, params) in statements {
            if sql == OracleDialect::current_revision_sql()
                || sql == OracleDialect::last_row_id_sql()
            {
                out.push(self.query_row(&sql, params).await?.into_iter().collect());
            } else {
                out.push(self.query_all(&sql, params).await?);
            }
        }
        Ok(out)
    }

    async fn query_all(&self, sql: &str, params: Vec<SqlValue>) -> Result<Vec<Row>> {
        let rows = self.rows.lock().await;
        let cols_prefix = format!("SELECT {} FROM rine WHERE ", OracleDialect::cols());
        if sql.starts_with(&format!("{cols_prefix}name IN (")) && sql.ends_with(" AND deleted = 0")
        {
            let mut out: Vec<&MockRow> = rows
                .iter()
                .filter(|r| {
                    r.deleted == 0
                        && params
                            .iter()
                            .any(|p| matches!(p, SqlValue::Str(s) if *s == r.name))
                })
                .collect();
            out.sort_by(|a, b| (&a.name, a.id).cmp(&(&b.name, b.id)));
            return Ok(out.into_iter().map(MockRow::to_row).collect());
        }
        if sql.starts_with("SELECT name, MAX(id) FROM rine WHERE name IN (") {
            let mut out = Vec::new();
            for param in &params {
                if let SqlValue::Str(name) = param
                    && let Some(max) = rows.iter().filter(|r| &r.name == name).map(|r| r.id).max()
                {
                    out.push(Row {
                        values: vec![SqlValue::Str(name.clone()), SqlValue::Int(max)],
                    });
                }
            }
            return Ok(out);
        }
        if sql.starts_with(&format!("{cols_prefix}id IN (")) {
            let mut out = Vec::new();
            for param in &params {
                if let SqlValue::Int(id) = param
                    && let Some(r) = rows.iter().find(|r| r.id == *id)
                {
                    out.push(MockRow::to_row(r));
                }
            }
            return Ok(out);
        }
        if sql == OracleDialect::events_sql() {
            let (from, to) = (Self::int_param(&params, 0), Self::int_param(&params, 1));
            let mut out: Vec<&MockRow> = rows
                .iter()
                .filter(|r| {
                    r.name != OracleDialect::compacted_rev_key()
                        && r.revision() >= from
                        && r.revision() <= to
                })
                .collect();
            out.sort_by_key(|r| r.id);
            return Ok(out.into_iter().map(MockRow::to_row).collect());
        }
        if sql.starts_with("SELECT COUNT(*) FROM rine WHERE")
            && let Some(pred) = Self::live_range_predicate(sql)
        {
            let count = Self::filter_live(&rows, pred, &params, false).len() as i64;
            return Ok(vec![Row {
                values: vec![SqlValue::Int(count)],
            }]);
        }
        if let Some(pred) = Self::live_range_predicate(sql) {
            let keys_only = sql.contains(", NULL");
            let mut out = Self::filter_live(&rows, pred, &params, keys_only);
            if sql.contains("FETCH FIRST") {
                let limit = Self::int_param(&params, params.len() - 1).max(0) as usize;
                out.truncate(limit);
            }
            return Ok(out);
        }
        let historical = sql == OracleDialect::snapshot_sql(None)
            || sql == OracleDialect::snapshot_sql(Some(&[0]))
            || sql == OracleDialect::snapshot_sql(Some(b"end"));
        let is_exact =
            sql == OracleDialect::list_exact_sql() || sql == OracleDialect::snapshot_sql(None);
        let is_ge =
            sql == OracleDialect::list_ge_sql() || sql == OracleDialect::snapshot_sql(Some(&[0]));
        let is_range = sql == OracleDialect::list_range_sql()
            || sql == OracleDialect::snapshot_sql(Some(b"end"));
        assert!(
            is_exact || is_ge || is_range,
            "mock query_all: unknown sql {sql}"
        );
        let prefix = Self::str_param(&params, 0);
        let mut out: Vec<&MockRow> = rows
            .iter()
            .filter(|r| {
                (if historical {
                    r.name != OracleDialect::compacted_rev_key()
                        && r.revision() <= Self::int_param(&params, if is_range { 2 } else { 1 })
                } else {
                    r.deleted == 0
                }) && if is_exact {
                    r.name == prefix
                } else if is_ge {
                    r.name.as_str() >= prefix.as_str()
                } else {
                    let end = Self::str_param(&params, 1);
                    r.name.as_str() >= prefix.as_str() && r.name.as_str() < end.as_str()
                }
            })
            .collect();
        out.sort_by(|a, b| (&a.name, a.id).cmp(&(&b.name, b.id)));
        if historical {
            out.reverse();
            out.dedup_by(|a, b| a.name == b.name);
            out.retain(|r| r.deleted != 1);
            out.reverse();
        }
        Ok(out.into_iter().map(MockRow::to_row).collect())
    }
}
