//! Listener configuration: etcd-style client URLs plus the TLS identity.
//!
//! This is the binary's side of the transport boundary. CLI-shaped concerns
//! live here: comma-separated URL parsing, file paths, PEM loading, and flag
//! pairing validation. The result is an address plus already-loaded identity
//! bytes ([`TlsIdentity`](rine_etcd::server::TlsIdentity)); applying that
//! identity to tonic/rustls (the TLS *mechanism*) stays in `rine-etcd`.

use std::{net::SocketAddr, path::PathBuf};

use rine_etcd::server::{ListenAddr, TlsIdentity};

/// How the server should listen: etcd-style client URLs plus the TLS
/// identity. The binary fills this from CLI flags; [`into_listeners`](Self::into_listeners)
/// resolves it to concrete listeners.
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
            (Some(cert), Some(key)) => {
                Some((tokio::fs::read(cert).await?, tokio::fs::read(key).await?))
            }
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
            Some(tokio::fs::read(ca).await?)
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
        let identity: Option<TlsIdentity> = identity.map(|(cert, key)| TlsIdentity {
            cert,
            key,
            client_ca,
        });
        let mut listeners = Vec::with_capacity(urls.len());
        for (addr, https) in urls {
            let tls = if https {
                Some(identity.clone().ok_or_else(|| {
                    anyhow::anyhow!("https:// listener {addr} needs --cert-file/--key-file")
                })?)
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
    let mut addrs = tokio::net::lookup_host(rest)
        .await
        .map_err(|e| anyhow::anyhow!("could not resolve listen address: {rest} ({e:#})"))?;
    addrs
        .next()
        .map(|addr| (addr, https))
        .ok_or_else(|| anyhow::anyhow!("could not resolve listen address: {rest}"))
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

    async fn err_of(cfg: ListenConfig) -> Result<String, anyhow::Error> {
        let r = cfg.into_listeners().await;
        assert!(r.is_err());
        r.map(|_| String::new())
    }

    #[tokio::test]
    async fn validation_errors() {
        // Unknown scheme.
        let err = err_of(plain(Some("ftp://127.0.0.1:21")))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("unsupported scheme"), "{err:?}");

        // Empty URL list.
        let err = err_of(plain(Some(" , "))).await.unwrap_err().to_string();
        assert!(err.contains("no listen URLs"), "{err:?}");

        // Cert without key.
        let mut cfg = plain(None);
        cfg.cert_file = Some(PathBuf::from("/nonexistent-cert"));
        let err = err_of(cfg).await.unwrap_err().to_string();
        assert!(err.contains("must be given together"), "{err:?}");

        // mTLS without TLS.
        let mut cfg = plain(None);
        cfg.client_cert_auth = true;
        let err = err_of(cfg).await.unwrap_err().to_string();
        assert!(err.contains("needs TLS"), "{err:?}");

        // https without identity.
        let err = err_of(plain(Some("https://127.0.0.1:2380")))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("needs --cert-file"), "{err:?}");
    }

    fn write_temp(name: &str, content: &[u8]) -> PathBuf {
        let path = std::env::temp_dir().join(format!("rine-test-{}-{name}", std::process::id()));
        std::fs::write(&path, content).unwrap();
        path
    }

    #[tokio::test]
    async fn https_identity_and_dns_names() {
        let cert = write_temp("cert", b"cert-bytes");
        let key = write_temp("key", b"key-bytes");
        let ca = write_temp("ca", b"ca-bytes");
        let mut cfg = plain(Some("https://localhost:2380"));
        cfg.cert_file = Some(cert.clone());
        cfg.key_file = Some(key.clone());
        let listeners = cfg.into_listeners().await.unwrap();
        assert_eq!(listeners.len(), 1);
        assert!(listeners[0].tls.is_some());
        // Hostnames resolve via DNS instead of direct parsing.
        let listeners = plain(Some("http://localhost:2381"))
            .into_listeners()
            .await
            .unwrap();
        assert_eq!(listeners[0].addr.port(), 2381);
        // mTLS without a CA file is rejected; with one it loads.
        let mut cfg = plain(None);
        cfg.cert_file = Some(cert.clone());
        cfg.key_file = Some(key.clone());
        cfg.client_cert_auth = true;
        let err = err_of(cfg).await.unwrap_err().to_string();
        assert!(err.contains("needs --trusted-ca-file"), "{err}");
        // Cert files with a plaintext-only listener warn instead of failing.
        let mut cfg = plain(None);
        cfg.cert_file = Some(cert.clone());
        cfg.key_file = Some(key.clone());
        let listeners = cfg.into_listeners().await.unwrap();
        assert!(listeners.iter().all(|l| l.tls.is_none()));
        // mTLS identity loads the CA bytes.
        let mut cfg = plain(Some("https://127.0.0.1:2382"));
        cfg.cert_file = Some(cert.clone());
        cfg.key_file = Some(key.clone());
        cfg.client_cert_auth = true;
        cfg.trusted_ca_file = Some(ca.clone());
        let listeners = cfg.into_listeners().await.unwrap();
        assert!(listeners[0].tls.as_ref().unwrap().client_ca.is_some());
        for p in [cert, key, ca] {
            let _ = std::fs::remove_file(p);
        }
    }

    #[tokio::test]
    async fn unresolvable_host_is_rejected() {
        let err = err_of(plain(Some("http://nonexistent.invalid:1")))
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("could not resolve"), "{err}");
    }
}
