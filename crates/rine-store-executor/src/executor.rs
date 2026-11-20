//! SQL execution abstraction for one database session.

use async_trait::async_trait;
use rine_etcd::{
    backend::{Event, Revision},
    error::{Error, Result},
};

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
        matches!(self.values.get(i), Some(SqlValue::Null) | None)
    }
}

#[async_trait]
pub trait SqlExecutor: Send + Sync {
    /// Optional durable single-Put path. An unsupported event returns `None`
    /// without side effects. An implementation must validate the predecessor
    /// under the same cross-node lock used by normal commits.
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
    async fn query_batch(&self, statements: Vec<(String, Vec<SqlValue>)>) -> Result<Vec<Vec<Row>>> {
        let mut results = Vec::with_capacity(statements.len());
        for (sql, params) in statements {
            results.push(self.query_all(&sql, params).await?);
        }
        Ok(results)
    }

    /// Open a commit unit: serialize writers across nodes until
    /// [`end_unit`](Self::end_unit). Validation reads and batched writes issued
    /// between the two run atomically. The default is a no-op (callers already
    /// hold the process-local write mutex; single-node backends need nothing
    /// more).
    async fn begin_unit(&self) -> Result<()> {
        Ok(())
    }

    /// Close the commit unit opened by [`begin_unit`](Self::begin_unit).
    /// `commit == true` makes the unit's writes durable, `false` rolls back
    /// everything (including nothing, when validation rejected the batch).
    async fn end_unit(&self, _commit: bool) -> Result<()> {
        Ok(())
    }
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

#[cfg(test)]
mod default_tests {
    use super::*;

    /// Minimal executor: only the required methods, so the default
    /// `try_commit_put` / `query_batch` / `begin_unit` / `end_unit` run here.
    struct Passthrough;

    #[async_trait]
    impl SqlExecutor for Passthrough {
        async fn execute(&self, _sql: &str, _params: Vec<SqlValue>) -> Result<u64> {
            Ok(1)
        }
        async fn execute_batch(&self, _statements: Vec<(String, Vec<SqlValue>)>) -> Result<()> {
            Ok(())
        }
        async fn query_row(&self, _sql: &str, _params: Vec<SqlValue>) -> Result<Option<Row>> {
            Ok(None)
        }
        async fn query_all(&self, _sql: &str, _params: Vec<SqlValue>) -> Result<Vec<Row>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn executor_defaults_are_noops() {
        use rine_etcd::backend::Event;
        let e = Passthrough;
        let event = Event {
            revision: 1,
            key: Default::default(),
            deleted: false,
            kv: None,
            prev_kv: None,
        };
        assert!(e.try_commit_put(&event).await.unwrap().is_none());
        e.execute_batch(vec![]).await.unwrap();
        let rows = e
            .query_batch(vec![("SELECT 1".to_string(), vec![SqlValue::Int(1)])])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        e.begin_unit().await.unwrap();
        e.end_unit(true).await.unwrap();
        e.end_unit(false).await.unwrap();
    }

    #[test]
    fn row_getter_type_errors() {
        use super::Row;
        let r = Row {
            values: vec![SqlValue::Int(1)],
        };
        assert!(r.get_str(0).is_err());
        assert!(r.get_str(9).is_err());
        assert!(r.get_bytes(0).is_err());
        assert!(r.get_bytes(9).is_err());
    }
}
