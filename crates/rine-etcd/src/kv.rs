//! KV service: Range/Put/DeleteRange/Txn/Compact over any `Backend`.
//!
//! Txn implements full compare/branch/apply semantics (nested txn included),
//! so the k8s apiserver patterns (create/cas/delete/compact-on-key) work
//! alongside generic clients.

use crate::backend::{Backend, KeyValue, ListOptions};
use crate::error::Error;
use crate::etcdserverpb::kv_server::Kv;
use crate::etcdserverpb::range_request::{SortOrder, SortTarget};
use crate::etcdserverpb::{
    CompactionRequest, CompactionResponse, Compare, DeleteRangeRequest, DeleteRangeResponse,
    PutRequest, PutResponse, RangeRequest, RangeResponse, RequestOp, ResponseHeader, ResponseOp,
    TxnRequest, TxnResponse,
};
use crate::lease::LeaseManager;
use crate::mvccpb;
use bytes::Bytes;
use std::sync::Arc;

const MAX_TXN_DEPTH: usize = 8;

pub(crate) fn status_err(e: Error) -> tonic::Status {
    match e {
        Error::Compacted { .. } => {
            tonic::Status::out_of_range("etcdserver: mvcc: required revision has been compacted")
        }
        Error::FutureRev { .. } => {
            tonic::Status::out_of_range("etcdserver: mvcc: required revision is a future revision")
        }
        Error::NotSupported(s) => tonic::Status::unimplemented(s),
        Error::KeyExists(_) => tonic::Status::already_exists("key already exists"),
        Error::Backend(s) => tonic::Status::internal(s),
    }
}

pub struct KvService<B: Backend> {
    backend: Arc<B>,
    leases: LeaseManager<B>,
    pub cluster_id: u64,
    pub member_id: u64,
}

impl<B: Backend> KvService<B> {
    pub fn new(backend: Arc<B>, leases: LeaseManager<B>) -> Self {
        Self {
            backend,
            leases,
            cluster_id: 0x7269_6e65_0000_0001,
            member_id: 0x7269_6e65_0000_0002,
        }
    }

    fn header(&self, revision: i64) -> ResponseHeader {
        ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision,
            raft_term: 0,
        }
    }

    // ---- core handlers (shared by unary RPCs and Txn branches) ----

    async fn do_range(&self, req: &RangeRequest) -> Result<RangeResponse, tonic::Status> {
        if req.key.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "etcdserver: key is not provided",
            ));
        }
        if SortOrder::try_from(req.sort_order).is_err() {
            return Err(tonic::Status::invalid_argument(
                "etcdserver: invalid sort order",
            ));
        }
        if SortTarget::try_from(req.sort_target).is_err() {
            return Err(tonic::Status::invalid_argument(
                "etcdserver: invalid sort target",
            ));
        }
        let range_end = if req.range_end.is_empty() {
            None
        } else {
            Some(req.range_end.as_slice())
        };
        let (rev, mut kvs, count) = self
            .backend
            .list(
                &req.key,
                range_end,
                ListOptions {
                    revision: req.revision,
                    limit: 0,
                    keys_only: req.keys_only,
                    count_only: req.count_only,
                },
            )
            .await
            .map_err(status_err)?;

        // min/max revision filters (0 = unset).
        kvs.retain(|kv| {
            (req.min_mod_revision == 0 || kv.mod_revision >= req.min_mod_revision)
                && (req.max_mod_revision == 0 || kv.mod_revision <= req.max_mod_revision)
                && (req.min_create_revision == 0 || kv.create_revision >= req.min_create_revision)
                && (req.max_create_revision == 0 || kv.create_revision <= req.max_create_revision)
        });
        let filtered = kvs.len() as i64;
        let count = if req.min_mod_revision != 0
            || req.max_mod_revision != 0
            || req.min_create_revision != 0
            || req.max_create_revision != 0
        {
            filtered
        } else {
            count
        };

        sort_kvs(&mut kvs, req.sort_order, req.sort_target);

        let (out, more) = if req.limit > 0 && (kvs.len() as i64) > req.limit {
            kvs.truncate(req.limit as usize);
            (kvs, true)
        } else {
            (kvs, false)
        };

        Ok(RangeResponse {
            header: Some(self.header(rev)),
            kvs: if req.count_only {
                Vec::new()
            } else {
                out.into_iter().map(to_proto_kv).collect()
            },
            more,
            count,
        })
    }

    async fn do_put(&self, req: &PutRequest) -> Result<PutResponse, tonic::Status> {
        if req.key.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "etcdserver: key is not provided",
            ));
        }
        let (_, prev) = self.backend.get(&req.key, 0).await.map_err(status_err)?;

        let (value, lease) = match (req.ignore_value, req.ignore_lease) {
            (true, _) | (_, true) => match &prev {
                None => {
                    return Err(tonic::Status::invalid_argument(
                        "etcdserver: ignore_value/ignore_lease on missing key",
                    ))
                }
                Some(p) => (
                    if req.ignore_value {
                        p.value.clone()
                    } else {
                        Bytes::copy_from_slice(&req.value)
                    },
                    if req.ignore_lease { p.lease } else { req.lease },
                ),
            },
            _ => (Bytes::copy_from_slice(&req.value), req.lease),
        };
        if lease != 0 && !self.leases.exists(lease).await {
            return Err(tonic::Status::failed_precondition(
                "etcdserver: requested lease not found",
            ));
        }

        let rev = match &prev {
            Some(_) => self
                .backend
                .update(Bytes::copy_from_slice(&req.key), value, 0, lease)
                .await
                .map_err(status_err)?,
            None => self
                .backend
                .create(Bytes::copy_from_slice(&req.key), value, lease)
                .await
                .map_err(status_err)?,
        };
        if lease != 0 {
            self.leases.attach(lease, req.key.clone()).await;
        }
        Ok(PutResponse {
            header: Some(self.header(rev)),
            prev_kv: if req.prev_kv {
                prev.map(to_proto_kv)
            } else {
                None
            },
        })
    }

    async fn do_delete(
        &self,
        req: &DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, tonic::Status> {
        if req.key.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "etcdserver: key is not provided",
            ));
        }
        let range_end = if req.range_end.is_empty() {
            None
        } else {
            Some(req.range_end.as_slice())
        };
        let (_, kvs, _) = self
            .backend
            .list(
                &req.key,
                range_end,
                ListOptions {
                    revision: 0,
                    ..Default::default()
                },
            )
            .await
            .map_err(status_err)?;
        let mut prev_kvs = Vec::new();
        let mut rev = self.backend.current_revision().await;
        for kv in &kvs {
            let (r, prev) = self.backend.delete(&kv.key).await.map_err(status_err)?;
            rev = r;
            if req.prev_kv {
                if let Some(p) = prev {
                    prev_kvs.push(to_proto_kv(p));
                }
            }
        }
        // NOTE: revision is not bumped when nothing matched; etcd bumps it.
        Ok(DeleteRangeResponse {
            header: Some(self.header(rev)),
            deleted: kvs.len() as i64,
            prev_kvs,
        })
    }

    async fn eval_compare(&self, c: &Compare) -> Result<bool, tonic::Status> {
        use crate::etcdserverpb::compare::{CompareResult, CompareTarget, TargetUnion};
        let result = CompareResult::try_from(c.result)
            .map_err(|_| tonic::Status::invalid_argument("etcdserver: bad compare result"))?;
        let target = CompareTarget::try_from(c.target)
            .map_err(|_| tonic::Status::invalid_argument("etcdserver: bad compare target"))?;
        let union = c.target_union.as_ref().ok_or_else(|| {
            tonic::Status::invalid_argument("etcdserver: compare target_union missing")
        })?;

        // Gather (version, create, mod, value, lease) for each subject key.
        let mut subjects: Vec<(i64, i64, i64, Vec<u8>, i64)> = Vec::new();
        if c.range_end.is_empty() {
            let (_, kv) = self.backend.get(&c.key, 0).await.map_err(status_err)?;
            subjects.push(unpack(kv));
        } else {
            let (_, kvs, _) = self
                .backend
                .list(&c.key, Some(c.range_end.as_slice()), ListOptions::default())
                .await
                .map_err(status_err)?;
            for kv in kvs {
                subjects.push(unpack(Some(kv)));
            }
        }

        // All subjects must satisfy (vacuously true for empty ranges).
        for (version, create, mod_rev, value, lease) in subjects {
            let ord = match (&target, union) {
                (CompareTarget::Version, TargetUnion::Version(v)) => version.cmp(v),
                (CompareTarget::Create, TargetUnion::CreateRevision(v)) => create.cmp(v),
                (CompareTarget::Mod, TargetUnion::ModRevision(v)) => mod_rev.cmp(v),
                (CompareTarget::Value, TargetUnion::Value(v)) => value.as_slice().cmp(v.as_slice()),
                (CompareTarget::Lease, TargetUnion::Lease(v)) => lease.cmp(v),
                _ => {
                    return Err(tonic::Status::invalid_argument(
                        "etcdserver: compare target/type mismatch",
                    ))
                }
            };
            let ok = match result {
                CompareResult::Equal => ord.is_eq(),
                CompareResult::Greater => ord.is_gt(),
                CompareResult::Less => ord.is_lt(),
                CompareResult::NotEqual => !ord.is_eq(),
            };
            if !ok {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn apply_op<'a>(
        &'a self,
        op: &'a RequestOp,
        depth: usize,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ResponseOp, tonic::Status>> + Send + 'a>,
    > {
        Box::pin(async move {
            use crate::etcdserverpb::{request_op::Request, response_op::Response};
            match op.request.as_ref() {
                Some(Request::RequestRange(r)) => Ok(ResponseOp {
                    response: Some(Response::ResponseRange(self.do_range(r).await?)),
                }),
                Some(Request::RequestPut(p)) => Ok(ResponseOp {
                    response: Some(Response::ResponsePut(self.do_put(p).await?)),
                }),
                Some(Request::RequestDeleteRange(d)) => Ok(ResponseOp {
                    response: Some(Response::ResponseDeleteRange(self.do_delete(d).await?)),
                }),
                Some(Request::RequestTxn(t)) => Ok(ResponseOp {
                    response: Some(Response::ResponseTxn(self.do_txn(t, depth + 1).await?)),
                }),
                None => Err(tonic::Status::invalid_argument(
                    "etcdserver: empty request op",
                )),
            }
        })
    }

    fn do_txn<'a>(
        &'a self,
        req: &'a TxnRequest,
        depth: usize,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<TxnResponse, tonic::Status>> + Send + 'a>,
    > {
        Box::pin(async move {
            if depth > MAX_TXN_DEPTH {
                return Err(tonic::Status::invalid_argument(
                    "etcdserver: txn nested too deep",
                ));
            }
            let mut ok = true;
            for c in &req.compare {
                if !self.eval_compare(c).await? {
                    ok = false;
                    break;
                }
            }
            let ops = if ok { &req.success } else { &req.failure };
            let mut responses = Vec::with_capacity(ops.len());
            for op in ops {
                responses.push(self.apply_op(op, depth).await?);
            }
            let rev = self.backend.current_revision().await;
            Ok(TxnResponse {
                header: Some(self.header(rev)),
                succeeded: ok,
                responses,
            })
        })
    }
}

fn unpack(kv: Option<KeyValue>) -> (i64, i64, i64, Vec<u8>, i64) {
    match kv {
        None => (0, 0, 0, Vec::new(), 0),
        Some(k) => (
            k.version,
            k.create_revision,
            k.mod_revision,
            k.value.to_vec(),
            k.lease,
        ),
    }
}

pub(crate) fn to_proto_kv(kv: KeyValue) -> mvccpb::KeyValue {
    mvccpb::KeyValue {
        key: kv.key.to_vec(),
        create_revision: kv.create_revision,
        mod_revision: kv.mod_revision,
        version: kv.version,
        value: kv.value.to_vec(),
        lease: kv.lease,
    }
}

fn sort_kvs(kvs: &mut [KeyValue], order: i32, target: i32) {
    if order == SortOrder::None as i32 {
        return;
    }
    let asc = order == SortOrder::Ascend as i32;
    kvs.sort_by(|a, b| {
        let o = match target {
            x if x == SortTarget::Key as i32 => a.key.cmp(&b.key),
            x if x == SortTarget::Version as i32 => a.version.cmp(&b.version),
            x if x == SortTarget::Create as i32 => a.create_revision.cmp(&b.create_revision),
            x if x == SortTarget::Mod as i32 => a.mod_revision.cmp(&b.mod_revision),
            x if x == SortTarget::Value as i32 => a.value.cmp(&b.value),
            _ => a.key.cmp(&b.key),
        };
        if asc {
            o
        } else {
            o.reverse()
        }
    });
}

#[tonic::async_trait]
impl<B: Backend> Kv for KvService<B> {
    async fn range(
        &self,
        request: tonic::Request<RangeRequest>,
    ) -> Result<tonic::Response<RangeResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.do_range(&request.into_inner()).await?,
        ))
    }

    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.do_put(&request.into_inner()).await?,
        ))
    }

    async fn delete_range(
        &self,
        request: tonic::Request<DeleteRangeRequest>,
    ) -> Result<tonic::Response<DeleteRangeResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.do_delete(&request.into_inner()).await?,
        ))
    }

    async fn txn(
        &self,
        request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.do_txn(&request.into_inner(), 0).await?,
        ))
    }

    async fn compact(
        &self,
        request: tonic::Request<CompactionRequest>,
    ) -> Result<tonic::Response<CompactionResponse>, tonic::Status> {
        let req = request.into_inner();
        let cur = self.backend.current_revision().await;
        if req.revision <= 0 || req.revision > cur {
            return Err(tonic::Status::out_of_range(
                "etcdserver: mvcc: required revision is a future revision",
            ));
        }
        let rev = self
            .backend
            .compact(req.revision)
            .await
            .map_err(status_err)?;
        Ok(tonic::Response::new(CompactionResponse {
            header: Some(self.header(rev)),
        }))
    }
}
