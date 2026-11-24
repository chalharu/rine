//! Plain-HTTP endpoints served on the same client listeners as gRPC.
//!
//! etcd exposes `/health`, `/version` and `/metrics` on its client URLs;
//! without them every HTTP-based monitor (Prometheus scrape, k8s probes,
//! load-balancer checks) reports the server down even though gRPC works.
//! Only GET on these three paths is served; anything else is 404.
//!
//! `/metrics` carries an honest minimal set: `etcd_server_has_leader`,
//! `etcd_server_is_leader` (both 1: single member by design) and
//! `etcd_mvcc_db_total_size_in_bytes` (+ `_in_use_`) from the backend's O(1)
//! accounting, plus `process_*` from the runtime collector on Linux only
//! (`procfs` does not exist elsewhere). raft/wal internals we don't have
//! are omitted rather than faked.

use std::{convert::Infallible, future::Future, sync::Arc, time::Instant};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Method, Request, Response, StatusCode, body::Incoming};
#[cfg(target_os = "linux")]
use prometheus::process_collector::ProcessCollector;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, Opts, Registry,
    TextEncoder,
};

use crate::backend::{Backend, VERSION};

/// Shared state for one listener's HTTP routes.
#[derive(Clone)]
pub(crate) struct HttpState {
    backend: Arc<dyn Backend>,
    metrics: Arc<Metrics>,
}

impl HttpState {
    /// Share one recorder across listeners and RPC services: per-listener
    /// state would multiply the process collector and gauge sets by
    /// listener count.
    pub fn with_metrics(backend: Arc<dyn Backend>, metrics: Arc<Metrics>) -> Self {
        Self { backend, metrics }
    }

    /// Shared recorder handed to every RPC service; gauges, counters and
    /// histograms are cheap to clone and safe to share across listeners.
    pub(crate) fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    async fn route(&self, req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
        let ok = |body: &str, content_type: &str| {
            Response::builder()
                .status(StatusCode::OK)
                .header(hyper::header::CONTENT_TYPE, content_type)
                .body(Full::new(Bytes::copy_from_slice(body.as_bytes())))
                .unwrap()
        };
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/health") => Ok(ok(r#"{"health":"true"}"#, "application/json")),
            (&Method::GET, "/version") => Ok(ok(
                &format!(r#"{{"etcdserver":"{VERSION}","etcdcluster":"{VERSION}"}}"#),
                "application/json",
            )),
            (&Method::GET, "/metrics") => Ok(ok(
                &self.metrics.render(&self.backend).await,
                "text/plain; version=0.0.4",
            )),
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from_static(b"not found")))
                .unwrap()),
        }
    }

    /// Serve one accepted connection (plaintext or already TLS-terminated)
    /// as HTTP/1.1. Prometheus, curl and k8s probes all speak HTTP/1.1.
    /// `TokioIo` adapts our [`Conn`] to hyper's IO traits.
    pub async fn serve_conn<I>(self, io: I)
    where
        I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let _ = hyper::server::conn::http1::Builder::new()
            .serve_connection(
                hyper_util::rt::TokioIo::new(io),
                hyper::service::service_fn(|req| self.route(req)),
            )
            .await;
    }
}

/// All series rine reports, recorded by the RPC services and rendered by
/// `/metrics`. Names match upstream etcd wherever the semantics genuinely
/// match (`etcd_disk_backend_commit_duration_seconds`,
/// `grpc_server_handled_total`, `grpc_server_handling_seconds`,
/// `etcd_mvcc_{range,put,delete,txn}_total`, `etcd_mvcc_watchers_total`),
/// so stock etcd alerts and dashboards keep working. Series for machinery
/// rine does not have (raft, WAL, peers) are omitted rather than faked.
pub(crate) struct Metrics {
    registry: Registry,
    has_leader: Gauge,
    is_leader: Gauge,
    db_size: Gauge,
    db_size_in_use: Gauge,
    commit_duration: Histogram,
    grpc_handled: CounterVec,
    grpc_handling: HistogramVec,
    pub(crate) mvcc_range: Counter,
    pub(crate) mvcc_put: Counter,
    pub(crate) mvcc_delete: Counter,
    pub(crate) mvcc_txn: Counter,
    pub(crate) watchers: Gauge,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        let registry = Registry::new();
        // `ProcessCollector` only exists on Linux (procfs-backed).
        #[cfg(target_os = "linux")]
        registry
            .register(Box::new(ProcessCollector::for_self()))
            .expect("fresh registry");
        let gauge = |name: &str, help: &str| {
            let g = Gauge::new(name, help).expect("valid gauge");
            registry
                .register(Box::new(g.clone()))
                .expect("fresh registry");
            g
        };
        let counter = |name: &str, help: &str| {
            let c = Counter::new(name, help).expect("valid counter");
            registry
                .register(Box::new(c.clone()))
                .expect("fresh registry");
            c
        };
        // Same doubling shape as upstream's backend-commit histogram.
        let commit_duration = Histogram::with_opts(
            HistogramOpts::new(
                "etcd_disk_backend_commit_duration_seconds",
                "Backend commit duration in seconds.",
            )
            .buckets(vec![
                0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.024, 2.048,
                4.096, 8.192,
            ]),
        )
        .expect("valid histogram");
        registry
            .register(Box::new(commit_duration.clone()))
            .expect("fresh registry");
        let grpc_handled = CounterVec::new(
            Opts::new(
                "grpc_server_handled_total",
                "Total number of RPCs completed, by method and code.",
            ),
            &["grpc_service", "grpc_method", "grpc_code"],
        )
        .expect("valid counter");
        registry
            .register(Box::new(grpc_handled.clone()))
            .expect("fresh registry");
        let grpc_handling = HistogramVec::new(
            HistogramOpts::new(
                "grpc_server_handling_seconds",
                "RPC handling latency in seconds.",
            )
            .buckets(vec![
                0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
                10.0,
            ]),
            &["grpc_service", "grpc_method"],
        )
        .expect("valid histogram");
        registry
            .register(Box::new(grpc_handling.clone()))
            .expect("fresh registry");
        Self {
            has_leader: gauge(
                "etcd_server_has_leader",
                "whether this member believes it has a leader",
            ),
            is_leader: gauge("etcd_server_is_leader", "whether this member is the leader"),
            db_size: gauge(
                "etcd_mvcc_db_total_size_in_bytes",
                "backend database size physically allocated in bytes",
            ),
            db_size_in_use: gauge(
                "etcd_mvcc_db_total_size_in_use_in_bytes",
                "backend database size logically in use in bytes",
            ),
            mvcc_range: counter("etcd_mvcc_range_total", "Total number of Range requests."),
            mvcc_put: counter("etcd_mvcc_put_total", "Total number of Put requests."),
            mvcc_delete: counter(
                "etcd_mvcc_delete_total",
                "Total number of DeleteRange requests.",
            ),
            mvcc_txn: counter("etcd_mvcc_txn_total", "Total number of Txn requests."),
            watchers: gauge(
                "etcd_mvcc_watchers_total",
                "Total number of currently active watchers.",
            ),
            commit_duration,
            grpc_handled,
            grpc_handling,
            registry,
        }
    }

    /// Time one real backend commit (user writes and lease-expiry deletes).
    pub(crate) fn observe_commit(&self, elapsed: std::time::Duration) {
        self.commit_duration.observe(elapsed.as_secs_f64());
    }

    /// Wrap one RPC handler: count it by (service, method, code) and time
    /// it. Streaming RPCs (watch, keep-alive, snapshot) are accounted once
    /// at establishment, not per message.
    pub(crate) async fn observe<R, F>(
        &self,
        service: &'static str,
        method: &'static str,
        fut: F,
    ) -> Result<tonic::Response<R>, tonic::Status>
    where
        F: Future<Output = Result<tonic::Response<R>, tonic::Status>>,
    {
        let start = Instant::now();
        let out = fut.await;
        let code = match &out {
            Ok(_) => tonic::Code::Ok,
            Err(status) => status.code(),
        };
        self.grpc_handled
            .with_label_values(&[service, method, grpc_code_name(code)])
            .inc();
        self.grpc_handling
            .with_label_values(&[service, method])
            .observe(start.elapsed().as_secs_f64());
        out
    }

    /// Refresh the truthful gauges (leader flags are constants of the
    /// single-member design; size is the backend's O(1) accounting) and
    /// render the Prometheus text exposition format.
    async fn render(&self, backend: &Arc<dyn Backend>) -> String {
        self.has_leader.set(1.0);
        self.is_leader.set(1.0);
        let size = backend.db_size().await as f64;
        self.db_size.set(size);
        self.db_size_in_use.set(size);
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .expect("encoding to a Vec cannot fail");
        String::from_utf8(buf).expect("exposition format is ASCII")
    }
}

/// Prometheus label form of a gRPC status code, matching upstream naming.
fn grpc_code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "Canceled",
        tonic::Code::Unknown => "Unknown",
        tonic::Code::InvalidArgument => "InvalidArgument",
        tonic::Code::DeadlineExceeded => "DeadlineExceeded",
        tonic::Code::NotFound => "NotFound",
        tonic::Code::AlreadyExists => "AlreadyExists",
        tonic::Code::PermissionDenied => "PermissionDenied",
        tonic::Code::ResourceExhausted => "ResourceExhausted",
        tonic::Code::FailedPrecondition => "FailedPrecondition",
        tonic::Code::Aborted => "Aborted",
        tonic::Code::OutOfRange => "OutOfRange",
        tonic::Code::Unimplemented => "Unimplemented",
        tonic::Code::Internal => "Internal",
        tonic::Code::Unavailable => "Unavailable",
        tonic::Code::DataLoss => "DataLoss",
        tonic::Code::Unauthenticated => "Unauthenticated",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryBackend;

    /// Route-level paths are covered by the `http_endpoints` integration
    /// test over a real socket; here only the gauge rendering is pinned.
    #[tokio::test]
    async fn metrics_carry_leader_and_size_gauges() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let body = Metrics::new().render(&backend).await;
        for line in [
            "etcd_server_has_leader 1",
            "etcd_server_is_leader 1",
            "etcd_mvcc_db_total_size_in_bytes 0",
            "etcd_mvcc_watchers_total 0",
        ] {
            assert!(body.lines().any(|l| l == line), "{line}\n{body}");
        }
    }

    #[tokio::test]
    async fn recording_renders_new_series() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let m = Metrics::new();
        m.mvcc_put.inc();
        m.observe_commit(std::time::Duration::from_millis(2));
        m.observe("svc", "Method", async {
            Ok::<_, tonic::Status>(tonic::Response::new(()))
        })
        .await
        .unwrap();
        m.observe("svc", "Method", async {
            Err::<tonic::Response<()>, _>(tonic::Status::not_found("gone"))
        })
        .await
        .unwrap_err();
        let body = m.render(&backend).await;
        for line in [
            "etcd_mvcc_put_total 1",
            "etcd_disk_backend_commit_duration_seconds_count 1",
            "etcd_disk_backend_commit_duration_seconds_sum 0.002",
            "grpc_server_handled_total{grpc_code=\"NotFound\",grpc_method=\"Method\",grpc_service=\"svc\"} 1",
            "grpc_server_handled_total{grpc_code=\"OK\",grpc_method=\"Method\",grpc_service=\"svc\"} 1",
            "grpc_server_handling_seconds_count{grpc_method=\"Method\",grpc_service=\"svc\"} 2",
        ] {
            assert!(body.lines().any(|l| l == line), "{line}\n{body}");
        }
    }
}
