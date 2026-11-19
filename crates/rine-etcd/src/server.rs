//! gRPC server wiring.

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use tonic::transport::ServerTlsConfig;

use crate::{
    backend::Backend,
    cluster::{ClusterService, MaintenanceService},
    etcdserverpb::{
        cluster_server::ClusterServer, kv_server::KvServer, lease_server::LeaseServer,
        maintenance_server::MaintenanceServer, watch_server::WatchServer,
    },
    kv::KvService,
    lease::{LeaseManager, LeaseService},
    watch::WatchService,
};

pub struct EtcdServer {
    pub backend: Arc<dyn Backend>,
}

/// One client listener: address plus optional TLS. `https://` listeners
/// require a TLS config; `http://` listeners stay plaintext.
pub struct ListenAddr {
    pub addr: SocketAddr,
    pub tls: Option<ServerTlsConfig>,
}

/// How the server should listen: etcd-style client URLs plus the TLS
/// identity. The binary fills this from CLI flags; all parsing and
/// validation (URL schemes, cert/key pairing, mTLS requirements) lives
/// here with the transport code, not in `main`.
pub struct ListenConfig {
    /// `--listen` fallback when no client URLs are given.
    pub listen: SocketAddr,
    /// `--listen-client-urls` value (comma-separated), if any.
    pub listen_client_urls: Option<String>,
    /// `--cert-file`: PEM server certificate chain.
    pub cert_file: Option<PathBuf>,
    /// `--key-file`: PEM server private key.
    pub key_file: Option<PathBuf>,
    /// `--client-cert-auth`: require client certificates.
    pub client_cert_auth: bool,
    /// `--trusted-ca-file`: PEM CA used to verify client certificates.
    pub trusted_ca_file: Option<PathBuf>,
}

impl ListenConfig {
    /// Resolve to concrete listeners. The identity is loaded once and
    /// applied to every `https://` listener; `http://` entries stay
    /// plaintext (etcd `--listen-client-urls` semantics).
    pub async fn into_listeners(self) -> anyhow::Result<Vec<ListenAddr>> {
        // Identity is loaded once and applied to every https:// listener.
        let identity: Option<(Vec<u8>, Vec<u8>)> = match (&self.cert_file, &self.key_file) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some((std::fs::read(cert)?, std::fs::read(key)?)),
            _ => anyhow::bail!("--cert-file and --key-file must be given together"),
        };
        if identity.is_none() && self.client_cert_auth {
            anyhow::bail!("--client-cert-auth needs TLS (--cert-file/--key-file)");
        }
        let client_ca: Option<Vec<u8>> = if self.client_cert_auth {
            let ca = self.trusted_ca_file.clone().ok_or_else(|| {
                anyhow::anyhow!(
                    "--client-cert-auth needs --trusted-ca-file (or RINE_TRUSTED_CA_FILE)"
                )
            })?;
            Some(std::fs::read(ca)?)
        } else {
            None
        };

        let urls = match &self.listen_client_urls {
            Some(urls) => {
                let mut parsed = Vec::new();
                for s in urls.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                    parsed.push(parse_listen_url(s).await?);
                }
                parsed
            }
            None => vec![(self.listen, false)],
        };
        if urls.is_empty() {
            anyhow::bail!("no listen URLs");
        }
        let mut listeners = Vec::with_capacity(urls.len());
        for (addr, https) in urls {
            let tls = if https {
                let (cert, key) = identity.clone().ok_or_else(|| {
                    anyhow::anyhow!("https:// listener {addr} needs --cert-file/--key-file")
                })?;
                let mut config = tonic::transport::ServerTlsConfig::new()
                    .identity(tonic::transport::Identity::from_pem(cert, key));
                if let Some(ca) = &client_ca {
                    config =
                        config.client_ca_root(tonic::transport::Certificate::from_pem(ca.clone()));
                }
                Some(config)
            } else {
                None
            };
            listeners.push(ListenAddr { addr, tls });
        }
        if identity.is_some() && listeners.iter().all(|l| l.tls.is_none()) {
            tracing::warn!("--cert-file/--key-file given but no https:// listener; TLS unused");
        }
        Ok(listeners)
    }
}

/// Split `http(s)://host:port` (bare `host:port` means http) into an address
/// plus whether it serves TLS.
async fn parse_listen_url(s: &str) -> anyhow::Result<(SocketAddr, bool)> {
    let s = s.trim_end_matches('/');
    let (scheme, rest) = match s.split_once("://") {
        Some((scheme, rest)) => (scheme, rest),
        None => ("http", s),
    };
    let https = match scheme {
        "http" => false,
        "https" => true,
        other => anyhow::bail!(
            "unsupported scheme in listen URL: {other} (expected http:// or https://)"
        ),
    };
    if let Ok(addr) = rest.parse::<SocketAddr>() {
        return Ok((addr, https));
    }
    let mut addrs = tokio::net::lookup_host(rest).await?;
    addrs
        .next()
        .map(|addr| (addr, https))
        .ok_or_else(|| anyhow::anyhow!("could not resolve listen address: {rest}"))
}

impl EtcdServer {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    pub async fn serve(self, addr: SocketAddr) -> anyhow::Result<()> {
        self.serve_inner(addr, None).await
    }

    /// Serve with TLS. When the config carries a client CA root, clients must
    /// present a certificate signed by that CA (mTLS).
    pub async fn serve_tls(self, addr: SocketAddr, tls: ServerTlsConfig) -> anyhow::Result<()> {
        self.serve_inner(addr, Some(tls)).await
    }

    async fn serve_inner(
        self,
        addr: SocketAddr,
        tls: Option<ServerTlsConfig>,
    ) -> anyhow::Result<()> {
        let leases = LeaseManager::new(self.backend.clone());
        Self::serve_on(addr, tls, self.backend, leases).await
    }

    /// Serve several listeners (mixed `http://` plaintext and `https://` TLS)
    /// on one shared backend and one shared lease manager.
    pub async fn serve_many(self, listeners: Vec<ListenAddr>) -> anyhow::Result<()> {
        if listeners.is_empty() {
            anyhow::bail!("no listen addresses");
        }
        let leases = LeaseManager::new(self.backend.clone());
        let mut set = tokio::task::JoinSet::new();
        for l in listeners {
            set.spawn(Self::serve_on(
                l.addr,
                l.tls,
                self.backend.clone(),
                leases.clone(),
            ));
        }
        while let Some(res) = set.join_next().await {
            res??;
        }
        Ok(())
    }

    async fn serve_on(
        addr: SocketAddr,
        tls: Option<ServerTlsConfig>,
        backend: Arc<dyn Backend>,
        leases: LeaseManager,
    ) -> anyhow::Result<()> {
        // rustls 0.23 leaves process-wide provider selection to the
        // application; without this every TLS handshake panics.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let scheme = if tls.is_some() { "https" } else { "http" };
        let client_url = format!("{scheme}://{addr}");
        tracing::info!(%addr, tls = tls.is_some(), "serving etcd-compatible gRPC");
        let mut builder = tonic::transport::Server::builder();
        if let Some(tls) = tls {
            builder = builder.tls_config(tls)?;
        }
        builder
            .add_service(KvServer::new(KvService::new(
                backend.clone(),
                leases.clone(),
            )))
            .add_service(WatchServer::new(WatchService::new(backend.clone())))
            .add_service(LeaseServer::new(LeaseService::new(backend.clone(), leases)))
            .add_service(ClusterServer::new(ClusterService::new(
                backend.clone(),
                client_url,
            )))
            .add_service(MaintenanceServer::new(MaintenanceService::new(backend)))
            .serve(addr)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain(urls: Option<&str>) -> ListenConfig {
        ListenConfig {
            listen: "127.0.0.1:2379".parse().unwrap(),
            listen_client_urls: urls.map(str::to_string),
            cert_file: None,
            key_file: None,
            client_cert_auth: false,
            trusted_ca_file: None,
        }
    }

    #[tokio::test]
    async fn bare_and_http_urls_stay_plaintext() {
        let listeners = plain(Some("http://127.0.0.1:2379, 127.0.0.1:2381"))
            .into_listeners()
            .await
            .unwrap();
        assert_eq!(listeners.len(), 2);
        assert!(listeners.iter().all(|l| l.tls.is_none()));
        assert_eq!(listeners[1].addr.port(), 2381);
    }

    #[tokio::test]
    async fn falls_back_to_listen_flag() {
        let listeners = plain(None).into_listeners().await.unwrap();
        assert_eq!(listeners.len(), 1);
        assert_eq!(listeners[0].addr.port(), 2379);
    }

    #[tokio::test]
    async fn validation_errors() {
        async fn err_of(cfg: ListenConfig) -> String {
            match cfg.into_listeners().await {
                Ok(_) => panic!("expected an error"),
                Err(e) => e.to_string(),
            }
        }

        // Unknown scheme.
        let err = err_of(plain(Some("ftp://127.0.0.1:21"))).await;
        assert!(err.contains("unsupported scheme"), "{err:?}");

        // Empty URL list.
        let err = err_of(plain(Some(" , "))).await;
        assert!(err.contains("no listen URLs"), "{err:?}");

        // Cert without key.
        let mut cfg = plain(None);
        cfg.cert_file = Some(PathBuf::from("/nonexistent-cert"));
        let err = err_of(cfg).await;
        assert!(err.contains("must be given together"), "{err:?}");

        // mTLS without TLS.
        let mut cfg = plain(None);
        cfg.client_cert_auth = true;
        let err = err_of(cfg).await;
        assert!(err.contains("needs TLS"), "{err:?}");

        // https without identity.
        let err = err_of(plain(Some("https://127.0.0.1:2380"))).await;
        assert!(err.contains("needs --cert-file"), "{err:?}");
    }
}
