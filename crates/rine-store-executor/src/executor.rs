//! SQL execution abstraction for one database session.

use async_trait::async_trait;
use rine_etcd::{
    backend::{Event, Revision},
    error::{Error, Result},
};

/// Bound parameter values. Oracle NUMBER maps to `Int`, VARCHAR2 to `Str`,
/// BLOB/RAW to `Bytes`.
#[derive(Debug, Clone)]
pub enum SqlValue {
    Int(i64),
    Str(String),
    Bytes(Vec<u8>),
    Null,
}

/// A result row: positional column values in SELECT order.
#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<SqlValue>,
}

impl Row {
    pub fn get_int(&self, i: usize) -> Result<i64> {
        match self.values.get(i) {
            Some(SqlValue::Int(v)) => Ok(*v),
            Some(v) => Err(Error::Backend(
                format!("column {i} is not a number: {v:?}").into(),
            )),
            None => Err(Error::Backend(format!("missing column {i}").into())),
        }
    }

    pub fn get_str(&self, i: usize) -> Result<String> {
        match self.values.get(i) {
            Some(SqlValue::Str(s)) => Ok(s.clone()),
            Some(v) => Err(Error::Backend(
                format!("column {i} is not a string: {v:?}").into(),
            )),
            None => Err(Error::Backend(format!("missing column {i}").into())),
        }
    }

    pub fn get_bytes(&self, i: usize) -> Result<Vec<u8>> {
        match self.values.get(i) {
            Some(SqlValue::Bytes(b)) => Ok(b.clone()),
            Some(SqlValue::Null) => Ok(Vec::new()),
            Some(v) => Err(Error::Backend(
                format!("column {i} is not bytes: {v:?}").into(),
            )),
            None => Err(Error::Backend(format!("missing column {i}").into())),
        }
    }

    pub fn is_null(&self, i: usize) -> bool {
        matches!(self.values.get(i), Some(SqlValue::Null))
    }
}

#[async_trait]
pub trait SqlExecutor: Send + Sync {
    /// Optional durable single-Put path. Defaults to `None` (unsupported):
    /// an implementation must validate the predecessor under the same
    /// cross-node lock used by normal commits.
    async fn try_commit_put(&self, _event: &Event) -> Result<Option<Revision>> {
        Ok(None)
    }

    /// Run INSERT/UPDATE/DELETE/MERGE. Returns affected rows.
    async fn execute(&self, sql: &str, params: Vec<SqlValue>) -> Result<u64>;
    /// Execute all statements in one database transaction.
    async fn execute_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<()>;
    /// Run SELECT expecting zero or one row.
    async fn query_row(&self, sql: &str, params: Vec<SqlValue>) -> Result<Option<Row>>;
    /// Run SELECT returning all rows.
    async fn query_all(&self, sql: &str, params: Vec<SqlValue>) -> Result<Vec<Row>>;
    /// Run related reads together. Executors can use one connection and snapshot.
    async fn query_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<Vec<Vec<Row>>>;

    /// Open a commit unit: serialize writers across nodes until
    /// [`end_unit`](Self::end_unit). Validation reads and batched writes issued
    /// between the two run atomically.
    async fn begin_unit(&self) -> Result<()>;

    /// Close the commit unit opened by [`begin_unit`](Self::begin_unit).
    /// `commit == true` makes the unit's writes durable, `false` rolls back
    /// everything (including nothing, when validation rejected the batch).
    async fn end_unit(&self, _commit: bool) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_getters() {
        let r = Row {
            values: vec![
                SqlValue::Int(7),
                SqlValue::Str("k".into()),
                SqlValue::Bytes(vec![1, 2]),
                SqlValue::Null,
            ],
        };
        assert_eq!(r.get_int(0).unwrap(), 7);
        assert_eq!(r.get_str(1).unwrap(), "k");
        assert_eq!(r.get_bytes(2).unwrap(), vec![1, 2]);
        assert!(r.is_null(3));
        assert!(!r.is_null(9));
        assert!(r.get_int(1).is_err());
        assert!(r.get_int(9).is_err());
        assert!(r.get_str(0).is_err());
        assert!(r.get_str(9).is_err());
        assert!(r.get_bytes(0).is_err());
        assert!(r.get_bytes(9).is_err());
    }
}
