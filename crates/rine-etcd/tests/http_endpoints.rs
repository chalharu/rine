//! Same-port HTTP endpoints (/health, /version, /metrics) plus grpc health:
//! raw HTTP/1.1 over TCP alongside a working gRPC client on one listener.

use std::io::{Read, Write};

use rine_etcd::{Backend as _, MemoryBackend, server::serve_ephemeral};
fn http_get(endpoint: &str, path: &str) -> (String, String) {
    let addr = endpoint.trim_start_matches("http://");
    let mut sock = std::net::TcpStream::connect(addr).unwrap();
    sock.set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    write!(
        sock,
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match sock.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => raw.extend_from_slice(&buf[..n]),
            Err(e) => panic!(
                "{path}: read failed after {} bytes: {e} partial={:?}",
                raw.len(),
                String::from_utf8_lossy(&raw)
            ),
        }
    }
    let raw = String::from_utf8(raw).unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    (head.to_string(), body.to_string())
}

// Blocking std sockets starve a current-thread executor (the server tasks
// never run), so this test needs worker threads.
#[tokio::test(flavor = "multi_thread")]
async fn same_port_serves_http_and_grpc() {
    let backend = std::sync::Arc::new(MemoryBackend::new());
    backend.initialize().await.unwrap();
    let endpoint = serve_ephemeral(backend).await;

    // Plain HTTP routes.
    let (head, body) = http_get(&endpoint, "/health");
    assert!(head.contains("200"), "{head}");
    assert_eq!(body, r#"{"health":"true"}"#);
    let (head, body) = http_get(&endpoint, "/version");
    assert!(head.contains("200"), "{head}");
    assert!(body.contains("3.5.0"), "{body}");
    let (head, body) = http_get(&endpoint, "/metrics");
    assert!(head.contains("200"), "{head}");
    assert!(head.contains("text/plain"), "{head}");
    for line in [
        "etcd_server_has_leader 1",
        "etcd_server_is_leader 1",
        "etcd_mvcc_db_total_size_in_bytes",
    ] {
        assert!(body.lines().any(|l| l.starts_with(line)), "{line}\n{body}");
    }
    let (head, _) = http_get(&endpoint, "/nope");
    assert!(head.contains("404"), "{head}");

    // gRPC on the same port is unaffected by the peek routing.
    let mut kv = rine_etcd::etcdserverpb::kv_client::KvClient::connect(endpoint.clone())
        .await
        .unwrap();
    let put = kv
        .put(rine_etcd::etcdserverpb::PutRequest {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(put.header.unwrap().revision >= 2);
    let got = kv
        .range(rine_etcd::etcdserverpb::RangeRequest {
            key: b"k".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.kvs.len(), 1);

    // Standard grpc health protocol reports SERVING.
    let channel = tonic::transport::Channel::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut hc = tonic_health::pb::health_client::HealthClient::new(channel);
    let status = hc
        .check(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap()
        .into_inner()
        .status;
    assert_eq!(
        status,
        tonic_health::pb::health_check_response::ServingStatus::Serving as i32
    );
}
