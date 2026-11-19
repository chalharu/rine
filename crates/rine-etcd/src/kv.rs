//! KV service: Range/Put/DeleteRange/Txn/Compact over any `Backend`.
//!
//! Txn implements full compare/branch/apply semantics (nested txn included),
//! so the k8s apiserver patterns (create/cas/delete/compact-on-key) work
//! alongside generic clients.

use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use tokio::sync::OwnedMutexGuard;

use crate::{
    backend::{Backend, Event, KeyValue, ListOptions, in_range},
    error::Error,
    etcdserverpb::{
        CompactionRequest, CompactionResponse, Compare, DeleteRangeRequest, DeleteRangeResponse,
        PutRequest, PutResponse, RangeRequest, RangeResponse, RequestOp, ResponseHeader,
        ResponseOp, TxnRequest, TxnResponse,
        kv_server::Kv,
        range_request::{SortOrder, SortTarget},
    },
    lease::LeaseManager,
    mvccpb,
};

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
        Error::Conflict => tonic::Status::aborted("storage revision changed during transaction"),
        Error::KeyExists(_) => tonic::Status::already_exists("key already exists"),
        Error::Backend(s) => tonic::Status::internal(s),
    }
}

struct WriteBatch {
    base: i64,
    events: Vec<Event>,
}

impl WriteBatch {
    fn revision(&self) -> i64 {
        self.base + i64::from(!self.events.is_empty())
    }

    fn push(&mut self, event: Event) -> Result<(), tonic::Status> {
        if self.events.iter().any(|e| e.key == event.key) {
            return Err(tonic::Status::invalid_argument(
                "etcdserver: duplicate key given in txn request",
            ));
        }
        self.events.push(event);
        Ok(())
    }
}

pub struct KvService {
    backend: Arc<dyn Backend>,
    leases: LeaseManager,
    pub cluster_id: u64,
    pub member_id: u64,
}

impl KvService {
    pub fn new(backend: Arc<dyn Backend>, leases: LeaseManager) -> Self {
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

    async fn batch(&self) -> WriteBatch {
        WriteBatch {
            base: self.backend.current_revision().await,
            events: Vec::new(),
        }
    }

    async fn commit(
        &self,
        batch: WriteBatch,
        guard: OwnedMutexGuard<()>,
    ) -> Result<(), tonic::Status> {
        if batch.events.is_empty() {
            return Ok(());
        }
        let changes: Vec<_> = batch
            .events
            .iter()
            .map(|event| {
                (
                    event.key.clone(),
                    event.prev_kv.as_ref().map_or(0, |kv| kv.lease),
                    event.kv.as_ref().map_or(0, |kv| kv.lease),
                )
            })
            .collect();
        // Once the database commit starts, a dropped RPC must not interrupt
        // watch publication or lease bookkeeping.
        let backend = self.backend.clone();
        let leases = self.leases.clone();
        tokio::spawn(async move {
            let _guard = guard;
            backend
                .commit(batch.base, batch.events)
                .await
                .map_err(status_err)?;
            for (key, old, new) in changes {
                if old != new {
                    if old != 0 {
                        leases.detach(old, &key).await;
                    }
                    if new != 0 {
                        leases.attach(new, key.to_vec()).await;
                    }
                }
            }
            Ok(())
        })
        .await
        .map_err(|error| tonic::Status::internal(format!("commit task failed: {error}")))?
    }

    async fn list(
        &self,
        key: &[u8],
        end: &[u8],
        revision: i64,
        batch: Option<&WriteBatch>,
    ) -> Result<(i64, Vec<KeyValue>), tonic::Status> {
        let end = (!end.is_empty()).then_some(end);
        let (rev, kvs, _) = self
            .backend
            .list(
                key,
                end,
                ListOptions {
                    revision,
                    ..Default::default()
                },
            )
            .await
            .map_err(status_err)?;
        if let Some(batch) = batch.filter(|batch| revision <= 0 && !batch.events.is_empty()) {
            let mut kvs: BTreeMap<_, _> = kvs.into_iter().map(|kv| (kv.key.clone(), kv)).collect();
            for event in batch.events.iter().filter(|e| in_range(&e.key, key, end)) {
                if let Some(kv) = &event.kv {
                    kvs.insert(event.key.clone(), kv.clone());
                } else {
                    kvs.remove(&event.key);
                }
            }
            Ok((batch.revision(), kvs.into_values().collect()))
        } else {
            Ok((rev, kvs))
        }
    }

    async fn do_range(
        &self,
        req: &RangeRequest,
        batch: Option<&WriteBatch>,
    ) -> Result<RangeResponse, tonic::Status> {
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
        let (rev, mut kvs) = self
            .list(&req.key, &req.range_end, req.revision, batch)
            .await?;

        // min/max revision filters (0 = unset).
        kvs.retain(|kv| {
            (req.min_mod_revision == 0 || kv.mod_revision >= req.min_mod_revision)
                && (req.max_mod_revision == 0 || kv.mod_revision <= req.max_mod_revision)
                && (req.min_create_revision == 0 || kv.create_revision >= req.min_create_revision)
                && (req.max_create_revision == 0 || kv.create_revision <= req.max_create_revision)
        });
        let count = kvs.len() as i64;

        sort_kvs(&mut kvs, req.sort_order, req.sort_target);
        if req.keys_only {
            for kv in &mut kvs {
                kv.value = Bytes::new();
            }
        }

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

    async fn do_put(
        &self,
        req: &PutRequest,
        batch: &mut WriteBatch,
    ) -> Result<PutResponse, tonic::Status> {
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
                    ));
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

        let rev = batch.base + 1;
        let key = Bytes::copy_from_slice(&req.key);
        batch.push(Event {
            revision: rev,
            key: key.clone(),
            deleted: false,
            kv: Some(KeyValue {
                key,
                value,
                lease,
                mod_revision: rev,
                create_revision: prev.as_ref().map_or(rev, |kv| kv.create_revision),
                version: prev.as_ref().map_or(1, |kv| kv.version + 1),
            }),
            prev_kv: prev.clone(),
        })?;
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
        batch: &mut WriteBatch,
    ) -> Result<DeleteRangeResponse, tonic::Status> {
        if req.key.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "etcdserver: key is not provided",
            ));
        }
        let (_, kvs) = self.list(&req.key, &req.range_end, 0, None).await?;
        let mut prev_kvs = Vec::new();
        for kv in &kvs {
            batch.push(Event {
                revision: batch.base + 1,
                key: kv.key.clone(),
                deleted: true,
                kv: None,
                prev_kv: Some(kv.clone()),
            })?;
            if req.prev_kv {
                prev_kvs.push(to_proto_kv(kv.clone()));
            }
        }
        Ok(DeleteRangeResponse {
            header: Some(self.header(batch.revision())),
            deleted: kvs.len() as i64,
            prev_kvs,
        })
    }

    async fn eval_compare(&self, c: &Compare, batch: &WriteBatch) -> Result<bool, tonic::Status> {
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
        let (_, kvs) = self.list(&c.key, &c.range_end, 0, Some(batch)).await?;
        if kvs.is_empty() {
            if target == CompareTarget::Value {
                return Ok(false);
            }
            subjects.push(unpack(None));
        }
        subjects.extend(kvs.into_iter().map(|kv| unpack(Some(kv))));

        // All subjects must satisfy the comparison.
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
                    ));
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
        batch: &'a mut WriteBatch,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<ResponseOp, tonic::Status>> + Send + 'a>,
    > {
        Box::pin(async move {
            use crate::etcdserverpb::{request_op::Request, response_op::Response};
            match op.request.as_ref() {
                Some(Request::RequestRange(r)) => Ok(ResponseOp {
                    response: Some(Response::ResponseRange(
                        self.do_range(r, Some(batch)).await?,
                    )),
                }),
                Some(Request::RequestPut(p)) => Ok(ResponseOp {
                    response: Some(Response::ResponsePut(self.do_put(p, batch).await?)),
                }),
                Some(Request::RequestDeleteRange(d)) => Ok(ResponseOp {
                    response: Some(Response::ResponseDeleteRange(
                        self.do_delete(d, batch).await?,
                    )),
                }),
                Some(Request::RequestTxn(t)) => Ok(ResponseOp {
                    response: Some(Response::ResponseTxn(
                        self.do_txn(t, depth + 1, batch).await?,
                    )),
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
        batch: &'a mut WriteBatch,
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
                if !self.eval_compare(c, batch).await? {
                    ok = false;
                    break;
                }
            }
            let ops = if ok { &req.success } else { &req.failure };
            let mut responses = Vec::with_capacity(ops.len());
            for op in ops {
                responses.push(self.apply_op(op, depth, batch).await?);
            }
            Ok(TxnResponse {
                header: Some(self.header(batch.revision())),
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
        if asc { o } else { o.reverse() }
    });
}

#[tonic::async_trait]
impl Kv for KvService {
    async fn range(
        &self,
        request: tonic::Request<RangeRequest>,
    ) -> Result<tonic::Response<RangeResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            self.do_range(&request.into_inner(), None).await?,
        ))
    }

    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<tonic::Response<PutResponse>, tonic::Status> {
        let guard = self.leases.writes.clone().lock_owned().await;
        let mut batch = self.batch().await;
        let response = self.do_put(&request.into_inner(), &mut batch).await?;
        self.commit(batch, guard).await?;
        Ok(tonic::Response::new(response))
    }

    async fn delete_range(
        &self,
        request: tonic::Request<DeleteRangeRequest>,
    ) -> Result<tonic::Response<DeleteRangeResponse>, tonic::Status> {
        let guard = self.leases.writes.clone().lock_owned().await;
        let mut batch = self.batch().await;
        let response = self.do_delete(&request.into_inner(), &mut batch).await?;
        self.commit(batch, guard).await?;
        Ok(tonic::Response::new(response))
    }

    async fn txn(
        &self,
        request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        let guard = self.leases.writes.clone().lock_owned().await;
        let mut batch = self.batch().await;
        let response = self.do_txn(&request.into_inner(), 0, &mut batch).await?;
        self.commit(batch, guard).await?;
        Ok(tonic::Response::new(response))
    }

    async fn compact(
        &self,
        request: tonic::Request<CompactionRequest>,
    ) -> Result<tonic::Response<CompactionResponse>, tonic::Status> {
        let req = request.into_inner();
        let _guard = self.leases.writes.lock().await;
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
