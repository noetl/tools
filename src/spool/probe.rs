//! Active downstream health probes (RFC §8.1).
//!
//! The circuit breaker ([`super::circuit`]) is pure logic; this module is
//! the I/O it's fed from. The worker subscription runtime calls
//! [`probe_downstream`] on each declared downstream on a cadence
//! ([`super::circuit::CircuitConfig::probe_interval_ms`]) and on the
//! half-open probe, then feeds the result into the breaker.
//!
//! A probe answers one question: *is this downstream reachable right now?*
//! It is deliberately cheap and side-effect-free (HEAD/GET, TCP connect) so
//! it can run frequently without load on the dependency. The result is a
//! boolean; the breaker owns the trip/recover policy.

use std::time::Duration;

use super::circuit::{DownstreamSpec, ProbeKind};

/// Default probe timeout — short, so a hung downstream reads as "down"
/// quickly rather than holding the probe.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Probe one downstream's health.
///
/// Returns:
/// - `Some(true)` — reachable (up).
/// - `Some(false)` — unreachable (connection refused / timeout / 5xx).
/// - `None` — [`ProbeKind::Passive`] (no active probe; the breaker is fed
///   only by dispatch outcomes) or a missing target.
pub async fn probe_downstream(spec: &DownstreamSpec) -> Option<bool> {
    let target = spec.target.as_deref()?;
    match spec.probe {
        ProbeKind::Passive => None,
        ProbeKind::Http => Some(probe_http(target).await),
        ProbeKind::Tcp => Some(probe_tcp(target).await),
        ProbeKind::Nats => Some(probe_tcp(&strip_nats_scheme(target)).await),
    }
}

/// HTTP probe: any answered status (even 4xx) means the server is up; a
/// transport error or a 5xx means down. Mirrors the dispatch-failure
/// classification — a 5xx is the downstream saying it can't serve.
async fn probe_http(url: &str) -> bool {
    let client = match reqwest::Client::builder().timeout(PROBE_TIMEOUT).build() {
        Ok(c) => c,
        Err(_) => return false,
    };
    match client.get(url).send().await {
        Ok(resp) => !resp.status().is_server_error(),
        Err(_) => false,
    }
}

/// TCP probe: a successful connect means the port is accepting.
async fn probe_tcp(host_port: &str) -> bool {
    let addr = host_port.trim();
    matches!(
        tokio::time::timeout(PROBE_TIMEOUT, tokio::net::TcpStream::connect(addr)).await,
        Ok(Ok(_stream))
    )
}

/// Strip a `nats://` scheme so a NATS url can be TCP-probed as `host:port`.
fn strip_nats_scheme(url: &str) -> String {
    url.strip_prefix("nats://")
        .or_else(|| url.strip_prefix("tls://"))
        .unwrap_or(url)
        .trim_end_matches('/')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn passive_probe_returns_none() {
        let spec = DownstreamSpec {
            name: "x".into(),
            probe: ProbeKind::Passive,
            target: None,
        };
        assert_eq!(probe_downstream(&spec).await, None);
    }

    #[tokio::test]
    async fn missing_target_returns_none() {
        let spec = DownstreamSpec {
            name: "x".into(),
            probe: ProbeKind::Http,
            target: None,
        };
        assert_eq!(probe_downstream(&spec).await, None);
    }

    #[tokio::test]
    async fn tcp_probe_dead_port_is_down() {
        // Port 1 on localhost is reliably closed.
        let spec = DownstreamSpec {
            name: "x".into(),
            probe: ProbeKind::Tcp,
            target: Some("127.0.0.1:1".into()),
        };
        assert_eq!(probe_downstream(&spec).await, Some(false));
    }

    #[tokio::test]
    async fn http_probe_unreachable_is_down() {
        // Unroutable / closed port → transport error → down.
        let spec = DownstreamSpec {
            name: "x".into(),
            probe: ProbeKind::Http,
            target: Some("http://127.0.0.1:1/health".into()),
        };
        assert_eq!(probe_downstream(&spec).await, Some(false));
    }

    #[test]
    fn strip_scheme_variants() {
        assert_eq!(strip_nats_scheme("nats://nats:4222"), "nats:4222");
        assert_eq!(strip_nats_scheme("tls://nats:4222/"), "nats:4222");
        assert_eq!(strip_nats_scheme("nats:4222"), "nats:4222");
    }
}
