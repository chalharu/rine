//! KV service: Range/Put/DeleteRange/Txn/Compact over any `Backend`.
//!
//! Txn implements full compare/branch/apply semantics (nested txn included),
//! so the k8s apiserver patterns (create/cas/delete/compact-on-key) work
//! alongside generic clients.

use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use tokio::sync::OwnedMutexGuard;

use crate::{
    backend::{
        Backend, CommitLock, Event, KeyValue, ListOptions, ObservedRange, ReadSet, header, in_range,
    },
    error::{Error, status_err},
    etcdserverpb::{
        CompactionRequest, CompactionResponse, Compare, DeleteRangeRequest, DeleteRangeResponse,
        PutRequest, PutResponse, RangeRequest, RangeResponse, RequestOp, ResponseOp, TxnRequest,
        TxnResponse,
        kv_server::Kv,
        range_request::{SortOrder, SortTarget},
        response_op::Response,
    },
    lease::LeaseManager,
    mvccpb,
};

const MAX_TXN_DEPTH: usize = 8;

struct WriteBatch {
    base: i64,
    reads: ReadSet,
    events: Vec<Event>,
}

impl WriteBatch {
    fn revision(&self) -> i64 {
        if self.events.is_empty() {
            self.base
        } else {
            self.base + 1
        }
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

    /// Record a single-key read at the base state. Only the first observation
    /// of a key counts: later reads see this transaction's own staged writes.
    fn record_point(&mut self, key: &[u8], observed: Option<i64>) {
        if !self.reads.points.iter().any(|(k, _)| k.as_ref() == key) {
            self.reads
                .points
                .push((Bytes::copy_from_slice(key), observed));
        }
    }

    /// Record a range read at the base state (pre-overlay snapshot).
    fn record_range(
        &mut self,
        key: &[u8],
        end: Option<&[u8]>,
        opts: &ListOptions,
        kvs: &[KeyValue],
        count: i64,
    ) {
        self.reads.ranges.push(ObservedRange {
            prefix: Bytes::copy_from_slice(key),
            range_end: end.map(Bytes::copy_from_slice),
            opts: opts.clone(),
            observed: kvs
                .iter()
                .map(|kv| (kv.key.clone(), kv.mod_revision))
                .collect(),
            count,
        });
    }
}

pub struct KvService {
    backend: Arc<dyn Backend>,
    leases: LeaseManager,
    writes: CommitLock,
}

impl KvService {
    pub fn new(backend: Arc<dyn Backend>, leases: LeaseManager, writes: CommitLock) -> Self {
        Self {
            backend,
            leases,
            writes,
        }
    }

    // ---- core handlers (shared by unary RPCs and Txn branches) ----

    async fn batch(&self) -> WriteBatch {
        WriteBatch {
            base: self.backend.current_revision().await,
            reads: ReadSet::default(),
            events: Vec::new(),
        }
    }

    /// Commit the batch and return the freshly assigned revision. Unrelated
    /// concurrent commits never fail us: only our own read set is revalidated.
    /// `Backend::commit` is cancel-safe, so a direct call suffices; lease
    /// bookkeeping runs after the commit resolves.
    async fn commit(&self, batch: WriteBatch, _guard: OwnedMutexGuard<()>) -> Result<i64, Error> {
        if batch.events.is_empty() {
            return Ok(self.backend.current_revision().await);
        }
        let changes: Vec<_> = batch
            .events
            .iter()
            .filter_map(|event| {
                let old = event.prev_kv.as_ref().map_or(0, |kv| kv.lease);
                let new = event.kv.as_ref().map_or(0, |kv| kv.lease);
                (old != new).then(|| (event.key.clone(), old, new))
            })
            .collect();
        let fresh = self.backend.commit(batch.reads, batch.events).await?;
        for (key, old, new) in changes {
            if old != 0 {
                self.leases.detach(old, &key).await;
            }
            if new != 0 {
                self.leases.attach(new, key.to_vec()).await;
            }
        }
        Ok(fresh)
    }

    /// Rewrite staged (`base + 1`) header revisions with the assigned one.
    fn retarget(op: &mut ResponseOp, revision: i64) {
        let response = match op.response.as_mut() {
            Some(r) => r,
            None => return,
        };
        match response {
            Response::ResponseRange(r) => {
                if let Some(h) = r.header.as_mut() {
                    h.revision = revision;
                }
            }
            Response::ResponsePut(r) => {
                if let Some(h) = r.header.as_mut() {
                    h.revision = revision;
                }
            }
            Response::ResponseDeleteRange(r) => {
                if let Some(h) = r.header.as_mut() {
                    h.revision = revision;
                }
            }
            Response::ResponseTxn(t) => {
                if let Some(h) = t.header.as_mut() {
                    h.revision = revision;
                }
                for inner in &mut t.responses {
                    Self::retarget(inner, revision);
                }
            }
        }
    }

    /// Backend-level read plus read-set recording for the active transaction.
    /// Returns `(revision, rows, full count)`; callers overlay staged writes
    /// or project/filter afterwards.
    async fn fetch(
        &self,
        key: &[u8],
        end: Option<&[u8]>,
        opts: ListOptions,
        batch: &mut Option<&mut WriteBatch>,
    ) -> Result<(i64, Vec<KeyValue>, i64), tonic::Status> {
        let (rev, kvs, count) = self
            .backend
            .list(key, end, opts.clone())
            .await
            .map_err(status_err)?;
        if opts.revision <= 0
            && let Some(batch) = batch.as_mut()
        {
            match end {
                None => batch.record_point(key, kvs.first().map(|kv| kv.mod_revision)),
                Some(end) => batch.record_range(key, Some(end), &opts, &kvs, count),
            }
        }
        Ok((rev, kvs, count))
    }

    async fn list(
        &self,
        key: &[u8],
        end: &[u8],
        revision: i64,
        mut batch: Option<&mut WriteBatch>,
    ) -> Result<(i64, Vec<KeyValue>), tonic::Status> {
        let end = (!end.is_empty()).then_some(end);
        let opts = ListOptions {
            revision,
            ..Default::default()
        };
        let (rev, kvs, _) = self.fetch(key, end, opts, &mut batch).await?;
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
        mut batch: Option<&mut WriteBatch>,
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
        let overlay = batch
            .as_ref()
            .is_some_and(|b| req.revision <= 0 && !b.events.is_empty());
        let filtered = req.min_mod_revision != 0
            || req.max_mod_revision != 0
            || req.min_create_revision != 0
            || req.max_create_revision != 0;
        let key_order = req.sort_order == SortOrder::None as i32
            || (req.sort_order == SortOrder::Ascend as i32
                && req.sort_target == SortTarget::Key as i32);
        if !overlay && !filtered && (key_order || req.count_only) {
            let end = (!req.range_end.is_empty()).then_some(req.range_end.as_slice());
            let opts = ListOptions {
                revision: req.revision,
                limit: req.limit,
                keys_only: req.keys_only,
                count_only: req.count_only,
            };
            let (rev, kvs, count) = self.fetch(&req.key, end, opts, &mut batch).await?;
            return Ok(RangeResponse {
                header: Some(header(rev)),
                kvs: kvs.into_iter().map(to_proto_kv).collect(),
                more: req.limit > 0 && count > req.limit,
                count,
            });
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
            header: Some(header(rev)),
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
        unary: bool,
    ) -> Result<PutResponse, tonic::Status> {
        if req.key.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "etcdserver: key is not provided",
            ));
        }
        // Predecessor fetch, deliberately *not* recorded in the read set:
        // the commit revalidates written keys against its batched snapshot.
        let (revision, rows, _) = self
            .fetch(&req.key, None, ListOptions::default(), &mut None)
            .await?;
        if unary {
            batch.base = revision;
        }
        let prev = rows.into_iter().next();

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
            header: Some(header(rev)),
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
            header: Some(header(batch.revision())),
            deleted: kvs.len() as i64,
            prev_kvs,
        })
    }

    async fn eval_compare(
        &self,
        c: &Compare,
        batch: &mut WriteBatch,
    ) -> Result<bool, tonic::Status> {
        use crate::etcdserverpb::compare::{CompareResult, CompareTarget, TargetUnion};
        let result = CompareResult::try_from(c.result)
            .map_err(|_| tonic::Status::invalid_argument("etcdserver: bad compare result"))?;
        let target = CompareTarget::try_from(c.target)
            .map_err(|_| tonic::Status::invalid_argument("etcdserver: bad compare target"))?;
        let union = c.target_union.as_ref().ok_or_else(|| {
            tonic::Status::invalid_argument("etcdserver: compare target_union missing")
        })?;

        // Every covered key must satisfy the comparison. A missing key only
        // counts as zeros for non-value targets (etcd: no value to compare).
        let (_, kvs) = self.list(&c.key, &c.range_end, 0, Some(batch)).await?;
        let check = |kv: Option<&KeyValue>| -> Result<bool, tonic::Status> {
            if kv.is_none() && target == CompareTarget::Value {
                return Ok(false);
            }
            let ord = match (&target, union) {
                (CompareTarget::Version, TargetUnion::Version(v)) => {
                    kv.map_or(0, |kv| kv.version).cmp(v)
                }
                (CompareTarget::Create, TargetUnion::CreateRevision(v)) => {
                    kv.map_or(0, |kv| kv.create_revision).cmp(v)
                }
                (CompareTarget::Mod, TargetUnion::ModRevision(v)) => {
                    kv.map_or(0, |kv| kv.mod_revision).cmp(v)
                }
                (CompareTarget::Value, TargetUnion::Value(v)) => {
                    kv.map_or(&[][..], |kv| kv.value.as_ref()).cmp(v.as_slice())
                }
                (CompareTarget::Lease, TargetUnion::Lease(v)) => kv.map_or(0, |kv| kv.lease).cmp(v),
                _ => {
                    return Err(tonic::Status::invalid_argument(
                        "etcdserver: compare target/type mismatch",
                    ));
                }
            };
            Ok(match result {
                CompareResult::Equal => ord.is_eq(),
                CompareResult::Greater => ord.is_gt(),
                CompareResult::Less => ord.is_lt(),
                CompareResult::NotEqual => !ord.is_eq(),
            })
        };
        if kvs.is_empty() {
            return check(None);
        }
        for kv in &kvs {
            if !check(Some(kv))? {
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
                    response: Some(Response::ResponsePut(self.do_put(p, batch, false).await?)),
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
                header: Some(header(batch.revision())),
                succeeded: ok,
                responses,
            })
        })
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
        let guard = self.writes.clone().lock_owned().await;
        // No `batch()` revision fetch: `do_put` sets `base` from its own
        // predecessor fetch for unary puts, so the initial value is dead.
        let mut batch = WriteBatch {
            base: 0,
            reads: ReadSet::default(),
            events: Vec::new(),
        };
        let mut response = self.do_put(&request.into_inner(), &mut batch, true).await?;
        let fresh = self.commit(batch, guard).await.map_err(status_err)?;
        if let Some(h) = response.header.as_mut() {
            h.revision = fresh;
        }
        Ok(tonic::Response::new(response))
    }

    async fn delete_range(
        &self,
        request: tonic::Request<DeleteRangeRequest>,
    ) -> Result<tonic::Response<DeleteRangeResponse>, tonic::Status> {
        let guard = self.writes.clone().lock_owned().await;
        let mut batch = self.batch().await;
        let mut response = self.do_delete(&request.into_inner(), &mut batch).await?;
        let fresh = self.commit(batch, guard).await.map_err(status_err)?;
        if let Some(h) = response.header.as_mut() {
            h.revision = fresh;
        }
        Ok(tonic::Response::new(response))
    }

    async fn txn(
        &self,
        request: tonic::Request<TxnRequest>,
    ) -> Result<tonic::Response<TxnResponse>, tonic::Status> {
        let guard = self.writes.clone().lock_owned().await;
        let req = request.into_inner();
        let mut batch = self.batch().await;
        let mut response = self.do_txn(&req, 0, &mut batch).await?;
        match self.commit(batch, guard).await {
            Ok(fresh) => {
                if let Some(h) = response.header.as_mut() {
                    h.revision = fresh;
                }
                for op in &mut response.responses {
                    Self::retarget(op, fresh);
                }
                Ok(tonic::Response::new(response))
            }
            Err(Error::Conflict) => {
                // A guarded key moved under us. etcd reports this as a
                // transaction conflict (succeeded=false), not an error: the
                // staged success-branch writes were rolled back, so run the
                // failure branch against the new state instead.
                let guard = self.writes.clone().lock_owned().await;
                let mut retry = self.batch().await;
                let mut responses = Vec::with_capacity(req.failure.len());
                for op in &req.failure {
                    responses.push(self.apply_op(op, 0, &mut retry).await?);
                }
                let fresh = self.commit(retry, guard).await.map_err(status_err)?;
                for op in &mut responses {
                    Self::retarget(op, fresh);
                }
                Ok(tonic::Response::new(TxnResponse {
                    header: Some(header(fresh)),
                    succeeded: false,
                    responses,
                }))
            }
            Err(other) => Err(status_err(other)),
        }
    }

    async fn compact(
        &self,
        request: tonic::Request<CompactionRequest>,
    ) -> Result<tonic::Response<CompactionResponse>, tonic::Status> {
        // No write lock: compaction only drops non-live history below the
        // watermark (live rows are never touched), and the backend serializes
        // the watermark plus each delete chunk on its own lock, so writers
        // keep committing between chunks instead of stalling.
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
            header: Some(header(rev)),
        }))
    }
}
