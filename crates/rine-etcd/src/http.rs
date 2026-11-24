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

use std::{convert::Infallible, sync::Arc};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Method, Request, Response, StatusCode, body::Incoming};
#[cfg(target_os = "linux")]
use prometheus::process_collector::ProcessCollector;
use prometheus::{Encoder, Gauge, Registry, TextEncoder};

use crate::backend::{Backend, VERSION};

/// Shared state for one listener's HTTP routes.
#[derive(Clone)]
pub(crate) struct HttpState {
    backend: Arc<dyn Backend>,
    metrics: Arc<Metrics>,
}

impl HttpState {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            metrics: Arc::new(Metrics::new()),
        }
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

struct Metrics {
    registry: Registry,
    has_leader: Gauge,
    is_leader: Gauge,
    db_size: Gauge,
    db_size_in_use: Gauge,
}

impl Metrics {
    fn new() -> Self {
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
            registry,
        }
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
        ] {
            assert!(body.lines().any(|l| l == line), "{line}\n{body}");
        }
    }
}
