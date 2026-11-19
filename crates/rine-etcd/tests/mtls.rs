//! mTLS: the server presents a certificate and, when configured with a
//! client CA root, requires clients to present a certificate signed by it.
//!
//! Certificates are generated in-process with rcgen; no fixture files.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use rine_etcd::{
    etcdserverpb::{PutRequest, RangeRequest, kv_client::KvClient},
    server::EtcdServer,
};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity, ServerTlsConfig};

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

fn server_tls(pki: &Pki, client_auth: bool) -> ServerTlsConfig {
    let mut tls = ServerTlsConfig::new().identity(Identity::from_pem(
        &pki.server_cert_pem,
        &pki.server_key_pem,
    ));
    if client_auth {
        tls = tls.client_ca_root(Certificate::from_pem(&pki.ca_pem));
    }
    tls
}

async fn spawn(tls: ServerTlsConfig) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let backend = Arc::new(rine_etcd::MemoryBackend::new());
    tokio::spawn(async move {
        EtcdServer::new(backend).serve_tls(addr, tls).await.unwrap();
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
    let addr = spawn(server_tls(&pki, true)).await;
    let value = put_get(addr, client_tls(&pki, true)).await.unwrap();
    assert_eq!(value, b"v".to_vec());
}

#[tokio::test]
async fn mtls_client_without_certificate_fails() {
    let pki = test_pki();
    let addr = spawn(server_tls(&pki, true)).await;
    assert!(put_get(addr, client_tls(&pki, false)).await.is_err());
}

#[tokio::test]
async fn tls_without_client_auth_accepts_ca_only_client() {
    let pki = test_pki();
    let addr = spawn(server_tls(&pki, false)).await;
    let value = put_get(addr, client_tls(&pki, false)).await.unwrap();
    assert_eq!(value, b"v".to_vec());
}
