//! gRPC server wiring.
//!
//! Transport *mechanism* lives here: applying a [`TlsIdentity`] to rustls,
//! serving plaintext and TLS listeners on one shared backend, and routing
//! each accepted connection: HTTP/2 preface (or TLS ALPN `h2`) goes to the
//! gRPC router, anything else (GET /health, /version, /metrics) to the
//! plain-HTTP handler. Transport *configuration* (flag parsing, file reads,
//! URL interpretation) lives with the binary (`rine/src/listen.rs`); this
//! module only consumes an address plus already-loaded identity bytes.

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Duration};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};

use crate::{
    backend::Backend,
    cluster::{ClusterService, MaintenanceService},
    etcdserverpb::{
        cluster_server::ClusterServer, kv_server::KvServer, lease_server::LeaseServer,
        maintenance_server::MaintenanceServer, watch_server::WatchServer,
    },
    http::HttpState,
    kv::KvService,
    lease::{LeaseManager, LeaseService},
    watch::WatchService,
};

pub struct EtcdServer {
    backend: Arc<dyn Backend>,
}

/// Loaded TLS identity for one `https://` listener: PEM-encoded server
/// certificate chain, private key, and the optional client CA root (mTLS).
/// Bytes only — no file paths: loading them is the binary's job.
#[derive(Debug, Clone)]
pub struct TlsIdentity {
    pub cert: Vec<u8>,
    pub key: Vec<u8>,
    pub client_ca: Option<Vec<u8>>,
}

/// One client listener: address plus optional TLS identity. `https://`
/// listeners require an identity; `http://` listeners stay plaintext.
pub struct ListenAddr {
    pub addr: SocketAddr,
    pub tls: Option<TlsIdentity>,
}

impl EtcdServer {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    /// Serve several listeners (mixed `http://` plaintext and `https://` TLS)
    /// on one shared backend and one shared lease manager.
    pub async fn serve_many(self, listeners: Vec<ListenAddr>) -> anyhow::Result<()> {
        if listeners.is_empty() {
            anyhow::bail!("no listen addresses");
        }
        // rustls 0.23 leaves process-wide provider selection to the
        // application; without this every TLS handshake panics.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let writes: crate::backend::CommitLock = Arc::new(tokio::sync::Mutex::new(()));
        let leases = LeaseManager::new(self.backend.clone(), writes.clone());
        leases.spawn_expiry_loop();
        let mut set = tokio::task::JoinSet::new();
        for l in listeners {
            let (backend, leases, writes) = (self.backend.clone(), leases.clone(), writes.clone());
            let addr = l.addr;
            set.spawn(async move {
                Self::serve_on(l.addr, l.tls, backend, leases, writes)
                    .await
                    .map_err(|e| anyhow::anyhow!("listener {addr}: {e:#}"))
            });
        }
        while let Some(res) = set.join_next().await {
            res??;
        }
        Ok(())
    }

    async fn serve_on(
        addr: SocketAddr,
        tls: Option<TlsIdentity>,
        backend: Arc<dyn Backend>,
        leases: LeaseManager,
        writes: crate::backend::CommitLock,
    ) -> anyhow::Result<()> {
        let scheme = if tls.is_some() { "https" } else { "http" };
        let client_url = format!("{scheme}://{addr}");
        tracing::info!(%addr, tls = tls.is_some(), "serving etcd-compatible gRPC");
        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter.set_serving::<KvServer<KvService>>().await;
        health_reporter
            .set_serving::<WatchServer<WatchService>>()
            .await;
        health_reporter
            .set_serving::<LeaseServer<LeaseService>>()
            .await;
        health_reporter
            .set_serving::<ClusterServer<ClusterService>>()
            .await;
        health_reporter
            .set_serving::<MaintenanceServer<MaintenanceService>>()
            .await;
        let router = tonic::transport::Server::builder()
            .add_service(KvServer::new(KvService::new(backend.clone(), writes)))
            .add_service(WatchServer::new(WatchService::new(backend.clone())))
            .add_service(LeaseServer::new(LeaseService::new(backend.clone(), leases)))
            .add_service(ClusterServer::new(ClusterService::new(
                backend.clone(),
                client_url,
            )))
            .add_service(MaintenanceServer::new(MaintenanceService::new(
                backend.clone(),
            )))
            .add_service(health_service);
        let (grpc_tx, grpc_rx) =
            tokio::sync::mpsc::unbounded_channel::<Result<Conn, std::io::Error>>();
        let http = HttpState::new(backend);
        let listener = TcpListener::bind(addr).await?;
        tokio::spawn(accept_loop(listener, tls, grpc_tx, http));
        router
            .serve_with_incoming(tokio_stream::wrappers::UnboundedReceiverStream::new(
                grpc_rx,
            ))
            .await?;
        Ok(())
    }
}

/// Serve `backend` on an OS-assigned plaintext port and wait until a real
/// Range succeeds. Returns the `http://` endpoint for gRPC clients.
/// Test and embedding helper so the bind/serve/readiness probe lives
/// once instead of once per integration test.
pub async fn serve_ephemeral(backend: Arc<dyn Backend>) -> String {
    use crate::etcdserverpb::{RangeRequest, kv_client::KvClient};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let endpoint = format!("http://{addr}");
    tokio::spawn(async move {
        EtcdServer::new(backend)
            .serve_many(vec![ListenAddr { addr, tls: None }])
            .await
            .unwrap();
    });
    // Wait until servable.
    for _ in 0..50 {
        if let Ok(mut c) = KvClient::connect(endpoint.clone()).await
            && c.range(RangeRequest {
                key: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    endpoint
}

/// One accepted connection, plaintext or already TLS-terminated, feeding
/// either the gRPC router or the plain-HTTP handler.
enum Conn {
    Plain(TcpStream),
    // Boxed: the TLS stream is ~1KB and every pooled connection would
    // otherwise pay that size (`large_enum_variant`).
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Conn::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Conn::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_flush(cx),
            Conn::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Conn::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

impl tonic::transport::server::Connected for Conn {
    type ConnectInfo = tonic::transport::server::TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        let (local_addr, remote_addr) = match self {
            Conn::Plain(s) => (s.local_addr().ok(), s.peer_addr().ok()),
            Conn::Tls(s) => {
                let inner = &s.get_ref().0;
                (inner.local_addr().ok(), inner.peer_addr().ok())
            }
        };
        tonic::transport::server::TcpConnectInfo {
            local_addr,
            remote_addr,
        }
    }
}

/// True when the socket opens with the HTTP/2 connection preface, i.e. a
/// gRPC client. Anything else (GET /health, Prometheus scrape) is HTTP/1.1.
/// Peeking never consumes, so the socket passes on untouched.
async fn is_grpc(sock: &TcpStream) -> bool {
    let mut head = [0u8; 4];
    let mut n = 0;
    let peek = async {
        while n < head.len() {
            let m = sock.peek(&mut head[n..]).await?;
            if m == 0 {
                break;
            }
            n += m;
        }
        Ok::<_, std::io::Error>(())
    };
    tokio::time::timeout(Duration::from_secs(5), peek)
        .await
        .is_ok()
        && head == *b"PRI "
}

fn tls_acceptor(id: &TlsIdentity) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
    use rustls::{
        RootCertStore,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _},
        server::WebPkiClientVerifier,
    };
    let certs = CertificateDer::pem_slice_iter(&id.cert).collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_slice(&id.key)?;
    let builder = rustls::ServerConfig::builder();
    let builder = match &id.client_ca {
        Some(pem) => {
            let cas = CertificateDer::pem_slice_iter(pem).collect::<Result<Vec<_>, _>>()?;
            let mut roots = RootCertStore::empty();
            roots.add_parsable_certificates(cas);
            builder
                .with_client_cert_verifier(WebPkiClientVerifier::builder(Arc::new(roots)).build()?)
        }
        None => builder.with_no_client_auth(),
    };
    let mut config = builder.with_single_cert(certs, key)?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Accept forever on one listener, routing gRPC to the tonic router and
/// plain HTTP to [`HttpState`](crate::http::HttpState). Runs as a task;
/// accept errors are logged and skipped, never fatal.
async fn accept_loop(
    listener: TcpListener,
    tls: Option<TlsIdentity>,
    grpc_tx: tokio::sync::mpsc::UnboundedSender<Result<Conn, std::io::Error>>,
    http: HttpState,
) {
    let acceptor = tls.as_ref().map(tls_acceptor);
    let acceptor = match acceptor {
        Some(Ok(a)) => Some(a),
        Some(Err(e)) => {
            tracing::error!("invalid TLS identity: {e:#}");
            return;
        }
        None => None,
    };
    loop {
        let (sock, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!("accept failed: {e}");
                continue;
            }
        };
        let (grpc_tx, http) = (grpc_tx.clone(), http.clone());
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            if let Some(acceptor) = acceptor {
                match acceptor.accept(sock).await {
                    Ok(tls) => {
                        let h2 = tls.get_ref().1.alpn_protocol() == Some(b"h2".as_slice());
                        if h2 {
                            let _ = grpc_tx.send(Ok(Conn::Tls(Box::new(tls))));
                        } else {
                            http.serve_conn(Conn::Tls(Box::new(tls))).await;
                        }
                    }
                    Err(e) => tracing::warn!("TLS handshake failed: {e}"),
                }
                return;
            }
            if is_grpc(&sock).await {
                let _ = grpc_tx.send(Ok(Conn::Plain(sock)));
            } else {
                http.serve_conn(Conn::Plain(sock)).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryBackend;

    #[tokio::test]
    async fn empty_listeners_are_rejected() {
        let err = EtcdServer::new(Arc::new(MemoryBackend::new()))
            .serve_many(Vec::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no listen addresses"));
    }
}
