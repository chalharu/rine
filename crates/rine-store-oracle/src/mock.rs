//! In-memory `SqlExecutor` for tests and examples.
//!
//! Interprets exactly the SQL strings `OracleBackend` generates (see
//! `dialect`), so backend logic is exercised without a database.

use std::collections::HashMap;

use async_trait::async_trait;
use rine_etcd::error::{Error, Result};
use rine_store_executor::{Dialect as _, Row, SqlExecutor, SqlValue};
use tokio::sync::Mutex;

use crate::dialect::OracleDialect;

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
        if self.created > 0 {
            self.created
        } else {
            self.id
        }
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

/// In-memory executor matching `OracleBackend`'s SQL. Not for production.
pub struct MockExec {
    rows: Mutex<Vec<MockRow>>,
    next_id: Mutex<i64>,
}

impl MockExec {
    pub fn new() -> Self {
        Self {
            rows: Mutex::new(Vec::new()),
            next_id: Mutex::new(1),
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
    fn apply(
        rows: &mut Vec<MockRow>,
        next: &mut i64,
        sql: &str,
        params: Vec<SqlValue>,
    ) -> Result<u64> {
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
        if sql == OracleDialect::insert_sql() {
            rows.push(MockRow {
                id: *next,
                name: Self::str_param(&params, 0),
                created: Self::int_param(&params, 1),
                deleted: Self::int_param(&params, 2),
                create_revision: Self::int_param(&params, 3),
                prev_revision: Self::int_param(&params, 4),
                lease: Self::int_param(&params, 5),
                version: Self::int_param(&params, 6),
                value: match &params[7] {
                    SqlValue::Bytes(b) => Some(b.clone()),
                    SqlValue::Null => None,
                    _ => panic!("value param"),
                },
            });
            *next += 1;
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
        if sql == OracleDialect::set_create_rev_sql() {
            let (rev, id) = (Self::int_param(&params, 0), Self::int_param(&params, 1));
            for r in rows.iter_mut() {
                if r.id == id {
                    r.create_revision = rev;
                    return Ok(1);
                }
            }
            return Ok(0);
        }
        if sql == OracleDialect::mark_deleted_sql() {
            let (new_del, name) = (Self::int_param(&params, 0), Self::str_param(&params, 1));
            let mut n = 0;
            for r in rows.iter_mut() {
                if r.name == name && r.deleted == 0 {
                    r.deleted = new_del;
                    n += 1;
                }
            }
            return Ok(n);
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
        if sql == crate::ddl::CREATE_TABLE || crate::ddl::CREATE_INDEXES.contains(&sql) {
            return Ok(0);
        }
        panic!("mock execute: unknown sql {sql}");
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
        let mut next = self.next_id.lock().await;
        Self::apply(&mut rows, &mut next, sql, params)
    }

    async fn execute_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<()> {
        let mut rows = self.rows.lock().await;
        let mut next = self.next_id.lock().await;
        let mut pending = rows.clone();
        let mut pending_next = *next;
        for (sql, params) in statements {
            Self::apply(&mut pending, &mut pending_next, &sql, params)?;
        }
        *rows = pending;
        *next = pending_next;
        Ok(())
    }

    async fn query_row(&self, sql: &str, params: Vec<SqlValue>) -> Result<Option<Row>> {
        let rows = self.rows.lock().await;
        if sql == OracleDialect::probe_sql() {
            // Mock table always exists.
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
        if sql == OracleDialect::latest_by_name_sql() {
            let name = Self::str_param(&params, 0);
            return Ok(rows
                .iter()
                .filter(|r| r.name == name)
                .map(|r| r.id)
                .max()
                .map(|id| Row {
                    values: vec![SqlValue::Int(id)],
                }));
        }
        if sql == OracleDialect::predecessor_sql() {
            let id = Self::int_param(&params, 0);
            return Ok(rows.iter().find(|r| r.id == id).map(MockRow::to_row));
        }
        if sql == OracleDialect::compact_revision_sql() {
            let max = rows
                .iter()
                .filter(|r| r.name == OracleDialect::compacted_rev_key())
                .map(|r| r.prev_revision)
                .max();
            return Ok(max.map(|m| Row {
                values: vec![SqlValue::Int(m)],
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
        if sql == OracleDialect::live_by_name_sql() {
            let name = Self::str_param(&params, 0);
            return Ok(rows
                .iter()
                .find(|r| r.name == name && r.deleted == 0)
                .map(MockRow::to_row));
        }
        if sql == OracleDialect::resolve_id_sql() {
            let (name, prev) = (Self::str_param(&params, 0), Self::int_param(&params, 1));
            return Ok(rows
                .iter()
                .find(|r| r.name == name && r.prev_revision == prev)
                .map(|r| Row {
                    values: vec![SqlValue::Int(r.id)],
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
        // Guard reads are single-row lookups; route them through `query_row`.
        let mut out = Vec::with_capacity(statements.len());
        for (sql, params) in statements {
            if sql == OracleDialect::live_by_name_sql()
                || sql == OracleDialect::current_revision_sql()
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
        if sql == OracleDialect::history_by_name_sql() {
            let (name, max_id) = (Self::str_param(&params, 0), Self::int_param(&params, 1));
            let mut out: Vec<&MockRow> = rows
                .iter()
                .filter(|r| r.name == name && r.id <= max_id)
                .collect();
            out.sort_by_key(|r| -r.id);
            return Ok(out.into_iter().map(MockRow::to_row).collect());
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
