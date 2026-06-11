//! `subscription` tool — bounded-drain message subscription poll.
//!
//! Phase 1 of the subscription/listener RFC
//! ([noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90)).
//!
//! This is the atomic registry tool that lets a playbook *fetch* a bounded
//! batch from a message source — NATS JetStream, Google Pub/Sub (pull), or
//! Kafka — ack it, and return the batch as the tool result.  It is
//! `js_consume` generalised across backends behind the
//! [`SourceClient`](super::source::SourceClient) abstraction.
//!
//! It is **not** a long-lived listener: every operation is a bounded drain
//! that returns within `timeout_ms` (hard-capped), so it fits the worker
//! slot contract exactly like every other tool.  The continuous runtime,
//! gateway push ingress, spool, and header directives are later RFC phases.
//!
//! ### Playbook config shape
//!
//! ```yaml
//! - step: drain_telemetry
//!   tool:
//!     kind: subscription
//!     source: nats                  # nats | pubsub | kafka
//!     operation: poll               # bounded fetch — returns immediately
//!     auth: "nats_main"             # credential alias (no default connection)
//!     stream: "ORDERS"              # NATS
//!     consumer: "orders-drain"      # NATS durable consumer (must exist)
//!     batch: 100
//!     timeout_ms: 4000
//!     ack: on_success               # on_success | auto | manual | none
//! ```
//!
//! Pub/Sub uses `subscription: "projects/<p>/subscriptions/<s>"` (+ optional
//! `endpoint` for the emulator); Kafka uses `brokers`, `topic`, `group`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

use super::source::{PollOptions, PollOutcome, SourceClient};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Playbook-facing configuration for the `subscription` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    /// Source backend: `nats` | `pubsub` | `kafka`.
    pub source: String,

    /// Operation. Phase 1 supports only `poll` (bounded drain).
    #[serde(default = "default_operation")]
    pub operation: String,

    /// Credential alias for secret resolution (no default connection —
    /// `agents/rules/no-default-connection.md`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<String>,

    // --- bounded-drain knobs (shared) ---
    /// Max messages to fetch in one drain. Default 100; capped at 1000.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch: Option<u32>,

    /// Max wait for the drain. Default 1000ms; capped at 5000ms.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,

    /// Ack policy: `on_success` (default) | `auto` | `manual` | `none`, or a
    /// bool (`true` → ack, `false` → manual).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ack: Option<serde_json::Value>,

    // --- NATS ---
    /// NATS server URL (when not using a credential alias).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// NATS user (explicit auth).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// NATS password (explicit auth or secret alias).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// NATS token (explicit auth or secret alias).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// JetStream stream name (NATS).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    /// JetStream durable pull-consumer name (NATS). Must already exist.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer: Option<String>,

    // --- Pub/Sub ---
    /// Full subscription resource: `projects/<p>/subscriptions/<s>`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription: Option<String>,
    /// API base override (Pub/Sub emulator host).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    // --- Kafka ---
    /// Broker list — a comma-separated string or an array of `host:port`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub brokers: Option<serde_json::Value>,
    /// Kafka topic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Kafka consumer group.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

fn default_operation() -> String {
    "poll".to_string()
}

// ---------------------------------------------------------------------------
// Tool
// ---------------------------------------------------------------------------

/// The `subscription` tool.
pub struct SubscriptionTool {
    template_engine: TemplateEngine,
}

impl SubscriptionTool {
    /// Create a new subscription tool.
    pub fn new() -> Self {
        Self {
            template_engine: TemplateEngine::new(),
        }
    }

    fn parse_config(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<SubscriptionConfig, ToolError> {
        let template_ctx = ctx.to_template_context();
        let rendered = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;
        serde_json::from_value(rendered)
            .map_err(|e| ToolError::Configuration(format!("Invalid subscription config: {}", e)))
    }

    /// Build the source-client for the configured backend (instance method —
    /// delegates to the public [`build_source`] factory).
    fn build_source(
        &self,
        cfg: &SubscriptionConfig,
        ctx: &ExecutionContext,
    ) -> Result<Box<dyn SourceClient>, ToolError> {
        build_source(cfg, ctx)
    }
}

/// Build a [`SourceClient`] for the configured backend.
///
/// Public so the **continuous subscription runtime** (RFC Mode B, Phase 2 —
/// `noetl/worker` run-mode) can construct the same source the bounded-drain
/// tool uses and call [`SourceClient::poll`] in a loop.  Connection fields
/// (`url` / `user` / `password` / `brokers` / `endpoint`) may be supplied
/// inline on `cfg` (the runtime resolves the credential up front and merges
/// the connection fields in, exactly as the worker's `apply_credential` does)
/// or via a credential alias resolved from `ctx`.
pub fn build_source(
    cfg: &SubscriptionConfig,
    ctx: &ExecutionContext,
) -> Result<Box<dyn SourceClient>, ToolError> {
    match cfg.source.as_str() {
        "nats" => {
            let conn = crate::tools::nats::resolve_nats_conn(
                cfg.auth.as_deref(),
                cfg.url.as_deref(),
                cfg.user.as_deref(),
                cfg.password.as_deref(),
                cfg.token.as_deref(),
                ctx,
            )?;
            let stream = cfg.stream.clone().ok_or_else(|| {
                ToolError::Configuration("subscription[nats] requires 'stream'".into())
            })?;
            let consumer = cfg.consumer.clone().ok_or_else(|| {
                ToolError::Configuration("subscription[nats] requires 'consumer'".into())
            })?;
            Ok(Box::new(super::source::nats::NatsSource::new(
                conn, stream, consumer,
            )))
        }

        #[cfg(feature = "pubsub")]
        "pubsub" => {
            let subscription = cfg.subscription.clone().ok_or_else(|| {
                ToolError::Configuration(
                    "subscription[pubsub] requires 'subscription' \
                     (projects/<p>/subscriptions/<s>)"
                        .into(),
                )
            })?;
            // A bearer token may be carried in the credential alias.
            let bearer = cfg.auth.as_deref().and_then(|alias| {
                ctx.get_secret(alias).and_then(|raw| {
                    serde_json::from_str::<serde_json::Value>(raw)
                        .ok()
                        .and_then(|v| {
                            v.get("token")
                                .or_else(|| v.get("access_token"))
                                .and_then(|t| t.as_str())
                                .map(str::to_string)
                        })
                })
            });
            Ok(Box::new(super::source::pubsub::PubSubSource::new(
                subscription,
                cfg.endpoint.clone(),
                bearer,
            )?))
        }

        #[cfg(feature = "kafka")]
        "kafka" => {
            let brokers = parse_brokers(cfg)?;
            let topic = cfg.topic.clone().ok_or_else(|| {
                ToolError::Configuration("subscription[kafka] requires 'topic'".into())
            })?;
            let group = cfg.group.clone().ok_or_else(|| {
                ToolError::Configuration("subscription[kafka] requires 'group'".into())
            })?;
            Ok(Box::new(super::source::kafka::KafkaSource::new(
                brokers, topic, group,
            )))
        }

        other => Err(ToolError::Configuration(format!(
            "Unknown or unavailable subscription source '{}'. \
             Available: {}",
            other,
            available_sources()
        ))),
    }
}

/// List the source backends compiled into this build (feature-gated).
fn available_sources() -> &'static str {
    match (cfg!(feature = "pubsub"), cfg!(feature = "kafka")) {
        (true, true) => "nats, pubsub, kafka",
        (true, false) => "nats, pubsub",
        (false, true) => "nats, kafka",
        (false, false) => "nats",
    }
}

/// Parse the Kafka broker list (comma-separated string or array).
#[cfg(feature = "kafka")]
fn parse_brokers(cfg: &SubscriptionConfig) -> Result<Vec<String>, ToolError> {
    match cfg.brokers.as_ref() {
        Some(serde_json::Value::String(s)) => Ok(s
            .split(',')
            .map(|b| b.trim().to_string())
            .filter(|b| !b.is_empty())
            .collect()),
        Some(serde_json::Value::Array(arr)) => Ok(arr
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect()),
        _ => Err(ToolError::Configuration(
            "subscription[kafka] requires 'brokers' (string or array of host:port)".into(),
        )),
    }
}

impl Default for SubscriptionTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for SubscriptionTool {
    fn name(&self) -> &'static str {
        "subscription"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let cfg = self.parse_config(config, ctx)?;
        let execution_id = ctx.execution_id;

        // Phase 1 is poll-only.
        if cfg.operation != "poll" {
            return Err(ToolError::Configuration(format!(
                "subscription: unsupported operation '{}'. Phase 1 supports: poll",
                cfg.operation
            )));
        }

        let ack = super::source::AckMode::parse(cfg.ack.as_ref())?;
        let opts = PollOptions::new(cfg.batch, cfg.timeout_ms, ack);
        let source = self.build_source(&cfg, ctx)?;
        let source_name = source.source_name();

        // Observability triad (agents/rules/observability.md):
        // span `tool.dispatch.subscription`, the
        // `noetl_subscription_messages_fetched_total{source}` count in the
        // result, `execution_id` on every line.
        let span = tracing::info_span!(
            "tool.dispatch.subscription",
            source = source_name,
            operation = %cfg.operation,
            execution_id,
        );
        let _guard = span.enter();

        let start = std::time::Instant::now();
        let outcome: PollOutcome = source.poll(&opts).await?;
        let duration_ms = start.elapsed().as_millis() as u64;

        tracing::debug!(
            source = source_name,
            execution_id,
            count = outcome.count(),
            acked = outcome.acked,
            duration_ms,
            "subscription poll complete"
        );

        let data = serde_json::json!({
            "status": "success",
            "source": source_name,
            "operation": cfg.operation,
            "count": outcome.count(),
            "messages": outcome.messages,
            "acked": outcome.acked,
            "ack_mode": ack.as_str(),
            "ack_ids": outcome.ack_ids,
            // Per-source fetched-count, named for the observability metric the
            // worker exports (noetl_subscription_messages_fetched_total{source}).
            "metrics": {
                "noetl_subscription_messages_fetched_total": outcome.count(),
                "source": source_name,
            },
        });

        Ok(ToolResult::success(data).with_duration(duration_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg_from(value: serde_json::Value) -> SubscriptionConfig {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn tool_name() {
        assert_eq!(SubscriptionTool::new().name(), "subscription");
    }

    #[test]
    fn config_defaults_operation_to_poll() {
        let cfg = cfg_from(serde_json::json!({
            "source": "nats",
            "stream": "ORDERS",
            "consumer": "orders-drain",
        }));
        assert_eq!(cfg.operation, "poll");
        assert_eq!(cfg.source, "nats");
        assert!(cfg.batch.is_none());
    }

    #[test]
    fn config_parses_all_drain_knobs() {
        let cfg = cfg_from(serde_json::json!({
            "source": "nats",
            "operation": "poll",
            "auth": "nats_main",
            "stream": "ORDERS",
            "consumer": "orders-drain",
            "batch": 250,
            "timeout_ms": 3000,
            "ack": "manual",
        }));
        assert_eq!(cfg.batch, Some(250));
        assert_eq!(cfg.timeout_ms, Some(3000));
        assert_eq!(cfg.ack, Some(serde_json::json!("manual")));
    }

    #[test]
    fn nats_source_requires_stream_and_consumer() {
        let tool = SubscriptionTool::new();
        let ctx = ExecutionContext::default();

        let cfg = cfg_from(serde_json::json!({
            "source": "nats",
            "url": "nats://localhost:4222",
            "consumer": "c",
        }));
        let err = tool.build_source(&cfg, &ctx).err().unwrap();
        assert!(matches!(err, ToolError::Configuration(_)));
        assert!(format!("{err}").contains("stream"));
    }

    #[test]
    fn nats_source_builds_with_explicit_url() {
        let tool = SubscriptionTool::new();
        let ctx = ExecutionContext::default();
        let cfg = cfg_from(serde_json::json!({
            "source": "nats",
            "url": "nats://localhost:4222",
            "stream": "ORDERS",
            "consumer": "orders-drain",
        }));
        let source = tool.build_source(&cfg, &ctx).unwrap();
        assert_eq!(source.source_name(), "nats");
    }

    #[test]
    fn unknown_source_errors() {
        let tool = SubscriptionTool::new();
        let ctx = ExecutionContext::default();
        let cfg = cfg_from(serde_json::json!({ "source": "rabbitmq" }));
        let err = tool.build_source(&cfg, &ctx).err().unwrap();
        assert!(format!("{err}").contains("Unknown or unavailable subscription source"));
    }

    #[tokio::test]
    async fn unsupported_operation_rejected() {
        let tool = SubscriptionTool::new();
        let ctx = ExecutionContext::default();
        let config = ToolConfig {
            kind: "subscription".to_string(),
            config: serde_json::json!({
                "source": "nats",
                "operation": "subscribe",
                "stream": "ORDERS",
                "consumer": "orders-drain",
                "url": "nats://localhost:4222",
            }),
            timeout: None,
            retry: None,
            auth: None,
        };
        let err = tool.execute(&config, &ctx).await.unwrap_err();
        assert!(format!("{err}").contains("unsupported operation"));
    }

    #[cfg(feature = "pubsub")]
    #[test]
    fn pubsub_source_requires_subscription() {
        let tool = SubscriptionTool::new();
        let ctx = ExecutionContext::default();
        let cfg = cfg_from(serde_json::json!({ "source": "pubsub" }));
        let err = tool.build_source(&cfg, &ctx).err().unwrap();
        assert!(format!("{err}").contains("subscription"));
    }

    #[cfg(feature = "pubsub")]
    #[test]
    fn pubsub_source_builds_with_emulator_endpoint() {
        let tool = SubscriptionTool::new();
        let ctx = ExecutionContext::default();
        let cfg = cfg_from(serde_json::json!({
            "source": "pubsub",
            "subscription": "projects/p/subscriptions/s",
            "endpoint": "localhost:8085",
        }));
        let source = tool.build_source(&cfg, &ctx).unwrap();
        assert_eq!(source.source_name(), "pubsub");
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_brokers_parse_string_and_array() {
        let s = cfg_from(serde_json::json!({
            "source": "kafka",
            "brokers": "a:9092, b:9092",
            "topic": "t",
            "group": "g",
        }));
        assert_eq!(parse_brokers(&s).unwrap(), vec!["a:9092", "b:9092"]);

        let a = cfg_from(serde_json::json!({
            "source": "kafka",
            "brokers": ["a:9092", "b:9092"],
            "topic": "t",
            "group": "g",
        }));
        assert_eq!(parse_brokers(&a).unwrap(), vec!["a:9092", "b:9092"]);
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_source_requires_topic_and_group() {
        let tool = SubscriptionTool::new();
        let ctx = ExecutionContext::default();
        let cfg = cfg_from(serde_json::json!({
            "source": "kafka",
            "brokers": "localhost:9092",
            "group": "g",
        }));
        let err = tool.build_source(&cfg, &ctx).err().unwrap();
        assert!(format!("{err}").contains("topic"));
    }

    // --- Integration tests (gated behind env vars) ---

    /// Live NATS JetStream bounded-drain proof.  Set
    /// `NOETL_TEST_NATS_URL=nats://localhost:4222` to run; set
    /// `NOETL_TEST_NATS_USER` + `NOETL_TEST_NATS_PASS` when the server
    /// requires account auth (the in-cluster NoETL NATS does).  Creates a
    /// stream + durable pull consumer, publishes a few messages, then drives
    /// the `subscription` tool's NATS backend end to end: drain → ack →
    /// normalized batch in the result.
    #[tokio::test]
    async fn nats_integration_bounded_drain() {
        let nats_url = match std::env::var("NOETL_TEST_NATS_URL") {
            Ok(u) => u,
            Err(_) => return, // skip when no live NATS available
        };
        let user = std::env::var("NOETL_TEST_NATS_USER").ok();
        let pass = std::env::var("NOETL_TEST_NATS_PASS").ok();

        use async_nats::jetstream::{self, consumer, stream};

        // `async_nats::connect` does not apply URL-embedded userinfo, so set
        // user/password explicitly when provided.
        let mut connect_opts = async_nats::ConnectOptions::new();
        if let (Some(u), Some(p)) = (&user, &pass) {
            connect_opts = connect_opts.user_and_password(u.clone(), p.clone());
        }
        let nc = connect_opts.connect(&nats_url).await.expect("connect");
        let js = jetstream::new(nc);

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let stream_name = format!("NOETL_SUB_TEST_{}", suffix);
        let subject = format!("noetl.subtest.{}", suffix);
        let consumer_name = format!("sub_drain_{}", suffix);

        js.create_stream(stream::Config {
            name: stream_name.clone(),
            subjects: vec![subject.clone()],
            ..Default::default()
        })
        .await
        .expect("create stream");

        // Publish 3 messages.
        for i in 0..3 {
            js.publish(subject.clone(), format!(r#"{{"n":{}}}"#, i).into())
                .await
                .expect("publish")
                .await
                .expect("publish ack");
        }

        // Durable pull consumer (the subscription tool does not create it).
        let stream_handle = js.get_stream(&stream_name).await.expect("get stream");
        stream_handle
            .create_consumer(consumer::pull::Config {
                durable_name: Some(consumer_name.clone()),
                ..Default::default()
            })
            .await
            .expect("create consumer");

        let tool = SubscriptionTool::new();
        let mut ctx = ExecutionContext::default();
        // The tool resolves user/password from the credential JSON.
        let cred = match (&user, &pass) {
            (Some(u), Some(p)) => {
                serde_json::json!({ "url": nats_url, "user": u, "password": p })
            }
            _ => serde_json::json!({ "url": nats_url }),
        };
        ctx.set_secret("nats_test", cred.to_string());

        let config = ToolConfig {
            kind: "subscription".to_string(),
            config: serde_json::json!({
                "source": "nats",
                "operation": "poll",
                "auth": "nats_test",
                "stream": stream_name,
                "consumer": consumer_name,
                "batch": 10,
                "timeout_ms": 3000,
                "ack": "on_success",
            }),
            timeout: None,
            retry: None,
            auth: None,
        };

        let result = tool
            .execute(&config, &ctx)
            .await
            .expect("subscription poll");
        assert!(result.is_success());
        let data = result.data.unwrap();
        assert_eq!(data["source"], "nats");
        assert_eq!(data["count"], 3);
        assert_eq!(data["acked"], true);
        let messages = data["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 3);
        assert!(messages.iter().any(|m| m["data"]["n"] == 0));

        // A second drain returns nothing (the first acked everything).
        let again = tool.execute(&config, &ctx).await.expect("second poll");
        assert_eq!(again.data.unwrap()["count"], 0);

        let _ = js.delete_stream(&stream_name).await;
    }

    /// Live Pub/Sub emulator bounded-drain proof.  Set
    /// `PUBSUB_EMULATOR_HOST=localhost:8085` (the emulator's host:port) to
    /// run.  Creates a topic + subscription via the emulator REST surface,
    /// publishes a message, then drives the `subscription` tool's Pub/Sub
    /// backend: pull → ack → normalized batch.
    #[cfg(feature = "pubsub")]
    #[tokio::test]
    async fn pubsub_emulator_bounded_drain() {
        let host = match std::env::var("PUBSUB_EMULATOR_HOST") {
            Ok(h) if !h.is_empty() => h,
            _ => return, // skip when no emulator available
        };
        let base = if host.starts_with("http") {
            host.clone()
        } else {
            format!("http://{}", host)
        };
        let client = reqwest::Client::new();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let project = "noetl-test";
        let topic = format!("projects/{}/topics/sub_{}", project, suffix);
        let subscription = format!("projects/{}/subscriptions/sub_{}", project, suffix);

        // Create topic + subscription (emulator needs no auth).
        client
            .put(format!("{}/v1/{}", base, topic))
            .json(&serde_json::json!({}))
            .send()
            .await
            .expect("create topic");
        client
            .put(format!("{}/v1/{}", base, subscription))
            .json(&serde_json::json!({ "topic": topic, "ackDeadlineSeconds": 30 }))
            .send()
            .await
            .expect("create subscription");

        // Publish one message: data is base64 of {"hello":"world"}.
        use base64::Engine;
        let payload = base64::engine::general_purpose::STANDARD.encode(br#"{"hello":"world"}"#);
        client
            .post(format!("{}/v1/{}:publish", base, topic))
            .json(&serde_json::json!({
                "messages": [{ "data": payload, "attributes": { "x-kind": "greeting" } }]
            }))
            .send()
            .await
            .expect("publish");

        let tool = SubscriptionTool::new();
        let ctx = ExecutionContext::default();
        let config = ToolConfig {
            kind: "subscription".to_string(),
            config: serde_json::json!({
                "source": "pubsub",
                "operation": "poll",
                "subscription": subscription,
                "endpoint": base,
                "batch": 10,
                "timeout_ms": 3000,
                "ack": "on_success",
            }),
            timeout: None,
            retry: None,
            auth: None,
        };

        // Pub/Sub delivery can lag a beat after publish; retry the drain.
        let mut data = serde_json::Value::Null;
        for _ in 0..5 {
            let result = tool.execute(&config, &ctx).await.expect("pubsub poll");
            data = result.data.unwrap();
            if data["count"].as_u64().unwrap_or(0) >= 1 {
                break;
            }
        }
        assert_eq!(data["source"], "pubsub");
        assert_eq!(data["count"], 1);
        assert_eq!(data["acked"], true);
        let msg = &data["messages"][0];
        assert_eq!(msg["data"]["hello"], "world");
        assert_eq!(msg["headers"]["x-kind"], "greeting");
    }
}
