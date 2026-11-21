//! mTLS: the server presents a certificate and, when configured with a
//! client CA root, requires clients to present a certificate signed by it.
//!
//! Certificates are generated in-process with rcgen; no fixture files.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use rine_etcd::{
    etcdserverpb::{PutRequest, RangeRequest, kv_client::KvClient},
    server::{EtcdServer, ListenAddr, TlsIdentity},
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

struct Pki {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn leaf_params(
    sans: Vec<String>,
    ip: std::net::IpAddr,
    eku: rcgen::ExtendedKeyUsagePurpose,
) -> rcgen::CertificateParams {
    let mut params = rcgen::CertificateParams::new(sans).unwrap();
    params.subject_alt_names.push(rcgen::SanType::IpAddress(ip));
    params.extended_key_usages = vec![eku];
    params.is_ca = rcgen::IsCa::NoCa;
    params
}

fn test_pki() -> Pki {
    let mut ca_params = rcgen::CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_pem = ca_cert.pem();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);

    let ip: std::net::IpAddr = "127.0.0.1".parse().unwrap();
    let srv_key = rcgen::KeyPair::generate().unwrap();
    let srv_cert = leaf_params(
        vec!["localhost".to_string()],
        ip,
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
    )
    .signed_by(&srv_key, &issuer)
    .unwrap();

    let cli_key = rcgen::KeyPair::generate().unwrap();
    let cli_cert = leaf_params(
        vec!["test-client".to_string()],
        ip,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    )
    .signed_by(&cli_key, &issuer)
    .unwrap();

    Pki {
        ca_pem,
        server_cert_pem: srv_cert.pem(),
        server_key_pem: srv_key.serialize_pem(),
        client_cert_pem: cli_cert.pem(),
        client_key_pem: cli_key.serialize_pem(),
    }
}

fn server_identity(pki: &Pki, client_auth: bool) -> TlsIdentity {
    TlsIdentity {
        cert: pki.server_cert_pem.as_bytes().to_vec(),
        key: pki.server_key_pem.as_bytes().to_vec(),
        client_ca: client_auth.then(|| pki.ca_pem.as_bytes().to_vec()),
    }
}

async fn spawn(tls: Option<TlsIdentity>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let backend = Arc::new(rine_etcd::MemoryBackend::new());
    tokio::spawn(async move {
        EtcdServer::new(backend)
            .serve_many(vec![ListenAddr { addr, tls }])
            .await
            .unwrap();
    });
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return addr;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("mTLS server did not start on {addr}");
}

fn client_tls(pki: &Pki, with_identity: bool) -> ClientTlsConfig {
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&pki.ca_pem))
        .domain_name("localhost");
    if with_identity {
        tls = tls.identity(Identity::from_pem(
            &pki.client_cert_pem,
            &pki.client_key_pem,
        ));
    }
    tls
}

async fn put_get(addr: SocketAddr, tls: ClientTlsConfig) -> anyhow::Result<Vec<u8>> {
    let channel = Channel::from_shared(format!("https://{addr}"))?
        .tls_config(tls)?
        .connect()
        .await?;
    let mut kv = KvClient::new(channel);
    kv.put(PutRequest {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
        ..Default::default()
    })
    .await?;
    let got = kv
        .range(RangeRequest {
            key: b"k".to_vec(),
            ..Default::default()
        })
        .await?
        .into_inner();
    Ok(got.kvs.first().map(|k| k.value.clone()).unwrap_or_default())
}

#[tokio::test]
async fn mtls_client_with_certificate_succeeds() {
    let pki = test_pki();
    let addr = spawn(Some(server_identity(&pki, true))).await;
    let value = put_get(addr, client_tls(&pki, true)).await.unwrap();
    assert_eq!(value, b"v".to_vec());
}

#[tokio::test]
async fn mtls_client_without_certificate_fails() {
    let pki = test_pki();
    let addr = spawn(Some(server_identity(&pki, true))).await;
    assert!(put_get(addr, client_tls(&pki, false)).await.is_err());
}

#[tokio::test]
async fn tls_without_client_auth_accepts_ca_only_client() {
    let pki = test_pki();
    let addr = spawn(Some(server_identity(&pki, false))).await;
    let value = put_get(addr, client_tls(&pki, false)).await.unwrap();
    assert_eq!(value, b"v".to_vec());
}

/// Reserve a likely-free port. The bind is dropped before the server
/// rebinds, so a rival process can steal it in between (TOCTOU): callers
/// retry the whole bind on `AddrInUse` instead of failing the test.
async fn free_port() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

#[tokio::test]
async fn mixed_plaintext_and_mtls_share_one_backend() {
    use rine_etcd::server::ListenAddr;

    let pki = test_pki();
    let backend = Arc::new(rine_etcd::MemoryBackend::new());
    // Retry the bind: a port reserved by `free_port` can be stolen before
    // the server rebinds it.
    for _ in 0..10 {
        let plain = free_port().await;
        let tls_addr = free_port().await;
        let tls_cfg = server_identity(&pki, true);
        let backend = backend.clone();
        let bound = tokio::spawn(async move {
            EtcdServer::new(backend)
                .serve_many(vec![
                    ListenAddr {
                        addr: plain,
                        tls: None,
                    },
                    ListenAddr {
                        addr: tls_addr,
                        tls: Some(tls_cfg),
                    },
                ])
                .await
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        if bound.is_finished() {
            continue;
        }
        if run_mixed_case(plain, tls_addr, &pki).await {
            return;
        }
        bound.abort();
    }
    panic!("mixed listeners never bound: ports kept colliding");
}

async fn run_mixed_case(plain: SocketAddr, tls_addr: SocketAddr, pki: &Pki) -> bool {
    // Readiness: a stolen port never accepts, so bail out fast and retry
    // the bind instead of failing the assertions below.
    for _ in 0..40 {
        let plain_ok = tokio::net::TcpStream::connect(plain).await.is_ok();
        let tls_ok = tokio::net::TcpStream::connect(tls_addr).await.is_ok();
        if plain_ok && tls_ok {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if tokio::net::TcpStream::connect(plain).await.is_err()
        || tokio::net::TcpStream::connect(tls_addr).await.is_err()
    {
        return false;
    }

    // Write over plaintext, read back over mTLS: one shared backend.
    let channel = Channel::from_shared(format!("http://{plain}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut kv = KvClient::new(channel);
    kv.put(PutRequest {
        key: b"k".to_vec(),
        value: b"v".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let value = put_get(tls_addr, client_tls(pki, true)).await.unwrap();
    assert_eq!(value, b"v".to_vec());
    // And the reverse direction: write over mTLS, read over plaintext.
    let channel = Channel::from_shared(format!("https://{tls_addr}"))
        .unwrap()
        .tls_config(client_tls(pki, true))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut kv = KvClient::new(channel);
    kv.put(PutRequest {
        key: b"k2".to_vec(),
        value: b"v2".to_vec(),
        ..Default::default()
    })
    .await
    .unwrap();
    let channel = Channel::from_shared(format!("http://{plain}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut kv = KvClient::new(channel);
    let got = kv
        .range(RangeRequest {
            key: b"k2".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        got.kvs.first().map(|k| k.value.clone()).unwrap_or_default(),
        b"v2".to_vec()
    );
    true
}
