//! gRPC over the Oracle-backed store (mock executor): proves the full
//! stack serves the etcd API without a database.

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use rine_etcd::{
    error::Result,
    etcdserverpb::{
        Compare, PutRequest, RangeRequest, RequestOp, TxnRequest,
        compare::{CompareResult, CompareTarget, TargetUnion},
        kv_client::KvClient,
        request_op,
    },
    server::EtcdServer,
};
use rine_store_executor::{Dialect as _, Row, SqlExecutor, SqlValue};
use rine_store_oracle::{MockExec, OracleBackend};
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
        if sql == OracleDialect::insert_sql() {
            let mut next = self.next_id.lock().await;
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
            let mut next = self.next_id.lock().await;
            rows.push(MockRow {
                id: *next,
                name: OracleDialect::compacted_rev_key().into(),
                created: 0,
                deleted: 0,
                create_revision: 0,
                prev_revision: rev,
                lease: 0,
                version: 1,
                value: None,
            });
            *next += 1;
            return Ok(1);
        }
        if sql == OracleDialect::compact_delete_sql() {
            let rev = Self::int_param(&params, 0);
            // newest row at or below rev per name survives
            let mut keep: HashMap<String, i64> = HashMap::new();
            for r in rows.iter() {
                if r.id <= rev {
                    keep.entry(r.name.clone())
                        .and_modify(|m| *m = (*m).max(r.id))
                        .or_insert(r.id);
                }
            }
            let before = rows.len();
            rows.retain(|r| {
                !(r.id < rev
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

    async fn query_row(&self, sql: &str, params: Vec<SqlValue>) -> Result<Option<Row>> {
        let rows = self.rows.lock().await;
        if sql == OracleDialect::probe_sql() {
            // Mock table always exists.
            return Ok(Some(Row {
                values: vec![SqlValue::Int(1)],
            }));
        }
        if sql == OracleDialect::current_revision_sql() {
            let max = rows.iter().map(|r| r.id).max();
            return Ok(max.map(|m| Row {
                values: vec![SqlValue::Int(m)],
            }));
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
            let mut out: Vec<&MockRow> =
                rows.iter().filter(|r| r.id >= from && r.id <= to).collect();
            out.sort_by_key(|r| r.id);
            return Ok(out.into_iter().map(MockRow::to_row).collect());
        }
        let is_exact = sql == OracleDialect::list_exact_sql();
        let is_ge = sql == OracleDialect::list_ge_sql();
        let is_range = sql == OracleDialect::list_range_sql();
        assert!(
            is_exact || is_ge || is_range,
            "mock query_all: unknown sql {sql}"
        );
        let prefix = Self::str_param(&params, 0);
        let mut out: Vec<&MockRow> = rows
            .iter()
            .filter(|r| {
                r.deleted == 0
                    && if is_exact {
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
        Ok(out.into_iter().map(MockRow::to_row).collect())
    }
}

async fn spawn() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let endpoint = format!("http://{addr}");
    let backend = Arc::new(OracleBackend::new(Arc::new(MockExec::new())));
    tokio::spawn(async move {
        EtcdServer::new(backend).serve(addr).await.unwrap();
    });
    for _ in 0..50 {
        if let Ok(mut c) = KvClient::connect(endpoint.clone()).await {
            if c.range(RangeRequest {
                key: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .is_ok()
            {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    endpoint
}

#[tokio::test]
async fn oracle_backend_serves_kv_and_txn() {
    let ep = spawn().await;
    let mut kv = KvClient::connect(ep).await.unwrap();

    // create via txn (MOD == 0)
    let r = kv
        .txn(TxnRequest {
            compare: vec![Compare {
                result: CompareResult::Equal as i32,
                target: CompareTarget::Mod as i32,
                key: b"ok".to_vec(),
                target_union: Some(TargetUnion::ModRevision(0)),
                ..Default::default()
            }],
            success: vec![RequestOp {
                request: Some(request_op::Request::RequestPut(PutRequest {
                    key: b"ok".to_vec(),
                    value: b"v1".to_vec(),
                    ..Default::default()
                })),
            }],
            failure: vec![],
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r.succeeded);

    let got = kv
        .range(RangeRequest {
            key: b"ok".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.kvs.len(), 1);
    assert_eq!(got.kvs[0].value, b"v1".to_vec());
    assert_eq!(got.kvs[0].version, 1);

    // direct put bumps version, preserves create_revision
    kv.put(PutRequest {
        key: b"ok".to_vec(),
        value: b"v2".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let got = kv
        .range(RangeRequest {
            key: b"ok".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.kvs[0].value, b"v2".to_vec());
    assert_eq!(got.kvs[0].version, 2);
    assert_eq!(got.kvs[0].create_revision, 1);
    assert_eq!(got.kvs[0].mod_revision, 2);
}
