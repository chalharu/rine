//! SQL execution abstraction over an Oracle session.
//!
//! `OracleBackend` programs against this trait; the step-1 driver implements
//! it for `Connection` once query execution lands. Tests use `MockExecutor`.

use async_trait::async_trait;
use rine_etcd::error::{Error, Result};

/// Bound parameter values. Oracle NUMBER maps to `Int`, VARCHAR2 to `Str`,
/// BLOB/RAW to `Bytes`.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Int(i64),
    Str(String),
    Bytes(Vec<u8>),
    Null,
}

/// A result row: positional column values in SELECT order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    pub values: Vec<SqlValue>,
}

impl Row {
    pub fn get_int(&self, i: usize) -> Result<i64> {
        match self.values.get(i) {
            Some(SqlValue::Int(v)) => Ok(*v),
            Some(v) => Err(Error::Backend(format!("column {i} is not a number: {v:?}"))),
            None => Err(Error::Backend(format!("missing column {i}"))),
        }
    }

    pub fn get_str(&self, i: usize) -> Result<String> {
        match self.values.get(i) {
            Some(SqlValue::Str(s)) => Ok(s.clone()),
            Some(v) => Err(Error::Backend(format!("column {i} is not a string: {v:?}"))),
            None => Err(Error::Backend(format!("missing column {i}"))),
        }
    }

    pub fn get_bytes(&self, i: usize) -> Result<Vec<u8>> {
        match self.values.get(i) {
            Some(SqlValue::Bytes(b)) => Ok(b.clone()),
            Some(SqlValue::Null) => Ok(Vec::new()),
            Some(v) => Err(Error::Backend(format!("column {i} is not bytes: {v:?}"))),
            None => Err(Error::Backend(format!("missing column {i}"))),
        }
    }

    pub fn is_null(&self, i: usize) -> bool {
        matches!(self.values.get(i), Some(SqlValue::Null) | None)
    }
}

#[async_trait]
pub trait SqlExecutor: Send + Sync {
    /// Run INSERT/UPDATE/DELETE/MERGE. Returns affected rows.
    async fn execute(&self, sql: &str, params: Vec<SqlValue>) -> Result<u64>;
    /// Run SELECT expecting zero or one row.
    async fn query_row(&self, sql: &str, params: Vec<SqlValue>) -> Result<Option<Row>>;
    /// Run SELECT returning all rows.
    async fn query_all(&self, sql: &str, params: Vec<SqlValue>) -> Result<Vec<Row>>;
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
        assert!(r.get_int(1).is_err());
        assert!(r.get_int(9).is_err());
    }
}
