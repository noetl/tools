//! Google Pub/Sub pull source backend for the `subscription` tool.
//!
//! Feature-gated (`pubsub`).  Implements a bounded pull drain over the
//! Pub/Sub REST API (`subscriptions:pull` + `subscriptions:acknowledge`),
//! reusing the existing `reqwest` client and [`crate::auth::gcp::GcpAuth`]
//! ADC token provider — no gRPC dependency.
//!
//! Bounded semantics: a synchronous pull with `maxMessages = batch` and a
//! client-side deadline of `timeout_ms`.  When the deadline elapses with no
//! messages the drain returns an empty batch (not an error) — exactly the
//! bounded "fetch up to N / until empty / until timeout" contract.
//!
//! ### Emulator
//!
//! Set `endpoint` (or the `PUBSUB_EMULATOR_HOST` env var) to a plaintext
//! emulator base URL to run without GCP credentials — the backend skips the
//! `Authorization` header when pointed at an emulator endpoint.  This is the
//! path the unit/integration tests drive.

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use std::time::Duration;

use crate::auth::GcpAuth;
use crate::error::ToolError;

use super::{
    decode_payload, normalize_headers, PollOptions, PollOutcome, PolledMessage, SourceClient,
};

/// Pub/Sub scope (covered by the broader cloud-platform scope ADC returns).
const PUBSUB_SCOPE: &str = "https://www.googleapis.com/auth/pubsub";

/// How the backend authenticates to the Pub/Sub endpoint.
enum PubSubAuth {
    /// Application Default Credentials (real Pub/Sub).
    Adc(GcpAuth),
    /// A literal bearer token resolved from a credential alias.
    Token(String),
    /// Emulator — no `Authorization` header.
    Emulator,
}

/// A bounded Google Pub/Sub pull drain.
pub struct PubSubSource {
    /// Full subscription resource: `projects/<p>/subscriptions/<s>`.
    subscription: String,
    /// API base URL (real or emulator), no trailing slash.
    endpoint: String,
    auth: PubSubAuth,
    client: reqwest::Client,
}

impl PubSubSource {
    /// Build a Pub/Sub source.
    ///
    /// `subscription` is the full resource path.  `endpoint` overrides the
    /// API base (emulator); `bearer_token`, when present, is used verbatim
    /// instead of ADC.  When the endpoint is a plaintext emulator and no
    /// token is supplied, auth is skipped.
    pub(crate) fn new(
        subscription: String,
        endpoint: Option<String>,
        bearer_token: Option<String>,
    ) -> Result<Self, ToolError> {
        // Resolve the emulator host from explicit config or the standard env
        // var the Google SDKs honor.
        let emulator_host = endpoint
            .clone()
            .or_else(|| std::env::var("PUBSUB_EMULATOR_HOST").ok());

        let (endpoint, is_emulator) = match emulator_host {
            Some(host) if !host.is_empty() => {
                let base = if host.starts_with("http://") || host.starts_with("https://") {
                    host
                } else {
                    format!("http://{}", host)
                };
                let is_emu = base.starts_with("http://");
                (base.trim_end_matches('/').to_string(), is_emu)
            }
            _ => ("https://pubsub.googleapis.com".to_string(), false),
        };

        let auth = if let Some(token) = bearer_token {
            PubSubAuth::Token(token)
        } else if is_emulator {
            PubSubAuth::Emulator
        } else {
            PubSubAuth::Adc(GcpAuth::new())
        };

        Ok(Self {
            subscription,
            endpoint,
            auth,
            client: reqwest::Client::new(),
        })
    }

    /// Resolve the `Authorization` header value, if any.
    async fn bearer(&self) -> Result<Option<String>, ToolError> {
        match &self.auth {
            PubSubAuth::Emulator => Ok(None),
            PubSubAuth::Token(t) => Ok(Some(format!("Bearer {}", t))),
            PubSubAuth::Adc(gcp) => {
                let token = gcp.get_token(&[PUBSUB_SCOPE]).await?;
                Ok(Some(format!("Bearer {}", token)))
            }
        }
    }
}

#[async_trait]
impl SourceClient for PubSubSource {
    fn source_name(&self) -> &'static str {
        "pubsub"
    }

    async fn poll(&self, opts: &PollOptions) -> Result<PollOutcome, ToolError> {
        let bearer = self.bearer().await?;
        let pull_url = format!("{}/v1/{}:pull", self.endpoint, self.subscription);

        // Synchronous pull; the client deadline bounds the wait so the call
        // never blocks past the worker-slot cap.  A small buffer over
        // `timeout_ms` lets the server's own short wait return first.
        let mut req = self
            .client
            .post(&pull_url)
            .timeout(Duration::from_millis(opts.timeout_ms + 500))
            .json(&serde_json::json!({ "maxMessages": opts.batch }));
        if let Some(ref auth) = bearer {
            req = req.header("Authorization", auth);
        }

        let resp = match req.send().await {
            Ok(r) => r,
            // A client-side timeout with no messages is a normal empty drain,
            // not a failure.
            Err(e) if e.is_timeout() => {
                return Ok(PollOutcome {
                    messages: Vec::new(),
                    acked: false,
                    ack_ids: Vec::new(),
                });
            }
            Err(e) => {
                return Err(ToolError::ExecutionFailed(format!(
                    "subscription[pubsub] pull failed: {}",
                    e
                )));
            }
        };

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ToolError::ExecutionFailed(format!(
                "subscription[pubsub] pull HTTP {}: {}",
                status, body
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(|e| {
            ToolError::ExecutionFailed(format!("subscription[pubsub] decode: {}", e))
        })?;

        let received = body
            .get("receivedMessages")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut messages: Vec<PolledMessage> = Vec::with_capacity(received.len());
        let mut ack_ids: Vec<String> = Vec::with_capacity(received.len());

        for rm in &received {
            let ack_id = rm.get("ackId").and_then(|v| v.as_str()).unwrap_or("");
            let msg = rm
                .get("message")
                .cloned()
                .unwrap_or(serde_json::Value::Null);

            let message_id = msg
                .get("messageId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            // `data` is base64; decode then JSON-or-string normalize.
            let data = match msg.get("data").and_then(|v| v.as_str()) {
                Some(b64) => match BASE64.decode(b64) {
                    Ok(bytes) => decode_payload(&bytes),
                    Err(_) => serde_json::Value::Null,
                },
                None => serde_json::Value::Null,
            };

            // Attributes are a flat string→string map.
            let attributes = msg
                .get("attributes")
                .cloned()
                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
            let header_entries: Vec<(String, Vec<String>)> = attributes
                .as_object()
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), vec![v.as_str().unwrap_or("").to_string()]))
                        .collect()
                })
                .unwrap_or_default();

            if !ack_id.is_empty() {
                ack_ids.push(ack_id.to_string());
            }

            messages.push(PolledMessage {
                id: message_id,
                data,
                headers: normalize_headers(header_entries),
                attributes,
                metadata: serde_json::json!({
                    "messageId": msg.get("messageId").cloned().unwrap_or(serde_json::Value::Null),
                    "publishTime": msg.get("publishTime").cloned().unwrap_or(serde_json::Value::Null),
                    "deliveryAttempt": rm.get("deliveryAttempt").cloned().unwrap_or(serde_json::Value::Null),
                    "ackId": ack_id,
                }),
                ack_id: if ack_id.is_empty() {
                    None
                } else {
                    Some(ack_id.to_string())
                },
            });
        }

        // Ack within the drain when requested and there's something to ack.
        let acked = if opts.ack.should_ack() && !ack_ids.is_empty() {
            let ack_url = format!("{}/v1/{}:acknowledge", self.endpoint, self.subscription);
            let mut areq = self
                .client
                .post(&ack_url)
                .timeout(Duration::from_millis(opts.timeout_ms + 500))
                .json(&serde_json::json!({ "ackIds": ack_ids }));
            if let Some(ref auth) = bearer {
                areq = areq.header("Authorization", auth);
            }
            let aresp = areq.send().await.map_err(|e| {
                ToolError::ExecutionFailed(format!(
                    "subscription[pubsub] acknowledge failed: {}",
                    e
                ))
            })?;
            if !aresp.status().is_success() {
                let status = aresp.status();
                let body = aresp.text().await.unwrap_or_default();
                return Err(ToolError::ExecutionFailed(format!(
                    "subscription[pubsub] acknowledge HTTP {}: {}",
                    status, body
                )));
            }
            true
        } else {
            false
        };

        Ok(PollOutcome {
            messages,
            acked,
            // Surface ack ids only when we did NOT ack (manual mode).
            ack_ids: if acked { Vec::new() } else { ack_ids },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emulator_endpoint_from_host_without_scheme() {
        let src = PubSubSource::new(
            "projects/p/subscriptions/s".into(),
            Some("localhost:8085".into()),
            None,
        )
        .unwrap();
        assert_eq!(src.endpoint, "http://localhost:8085");
        assert!(matches!(src.auth, PubSubAuth::Emulator));
    }

    #[test]
    fn explicit_https_endpoint_is_not_emulator() {
        let src = PubSubSource::new(
            "projects/p/subscriptions/s".into(),
            Some("https://pubsub.example.com".into()),
            None,
        )
        .unwrap();
        assert_eq!(src.endpoint, "https://pubsub.example.com");
        assert!(matches!(src.auth, PubSubAuth::Adc(_)));
    }

    #[test]
    fn default_endpoint_uses_adc() {
        let src = PubSubSource::new("projects/p/subscriptions/s".into(), None, None).unwrap();
        // With no emulator env var the default is the real API + ADC.
        if std::env::var("PUBSUB_EMULATOR_HOST").is_err() {
            assert_eq!(src.endpoint, "https://pubsub.googleapis.com");
            assert!(matches!(src.auth, PubSubAuth::Adc(_)));
        }
    }

    #[test]
    fn explicit_token_overrides_adc() {
        let src = PubSubSource::new(
            "projects/p/subscriptions/s".into(),
            Some("https://pubsub.googleapis.com".into()),
            Some("tok123".into()),
        )
        .unwrap();
        assert!(matches!(src.auth, PubSubAuth::Token(_)));
    }
}
