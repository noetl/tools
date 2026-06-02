//! NATS JetStream / K/V Store / Object Store tool.
//!
//! Provides playbook-facing operations for:
//! - Key-Value store: `kv_get`, `kv_put` (with optional TTL), `kv_delete`, `kv_keys`, `kv_purge`
//! - Object Store:   `object_get`, `object_put`, `object_delete`, `object_list`, `object_info`
//! - JetStream:      `js_publish`, `js_get_msg`, `js_stream_info`
//!
//! **No subscriptions / pull operations** — those would hold a worker slot while
//! waiting for an external event, which violates the NoETL execution model
//! (`agents/rules/execution-model.md`).
//!
//! ## Playbook config shape
//!
//! ```yaml
//! tool:
//!   kind: nats
//!   url: "nats://localhost:4222"      # or resolved from auth credential
//!   auth: my_nats_credential          # credential alias → { url, user?, password?, token? }
//!   operation: kv_get
//!   bucket: my_bucket
//!   key: my_key
//! ```
//!
//! Credential shape resolved via `ctx.get_secret`:
//! ```json
//! { "url": "nats://host:4222", "user": "...", "password": "...", "token": "..." }
//! ```

use async_nats::jetstream::{self, kv, object_store};
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// NATS tool configuration (playbook-facing surface).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    // --- connection ---

    /// NATS server URL (e.g. `nats://localhost:4222`).
    /// Omit when using a credential alias via `auth`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// Credential alias for secret resolution.  The resolved credential must
    /// contain at minimum a `url` field; optionally `user`, `password`, `token`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<String>,

    /// Username for user/password auth.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,

    /// Password for user/password auth (or credential alias).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,

    /// Token for token auth (or credential alias).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,

    // --- operation ---

    /// Operation to perform.  See module-level docs for the full list.
    pub operation: String,

    // --- KV / Object Store common ---

    /// Bucket name (KV or Object Store).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,

    // --- KV fields ---

    /// Key within the bucket (KV operations).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// Value to store (KV `put`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,

    /// TTL in seconds for KV `put` (informational; NATS enforces TTL at the bucket level
    /// via `KeyValueConfig::max_age`, not per-key).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<u64>,

    /// Glob pattern filter for `kv_keys` (optional).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,

    // --- Object Store fields ---

    /// Object name within the bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Object data (string or base64-encoded bytes; see `encoding`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,

    /// Encoding for Object Store get/put: `"utf-8"` (default) or `"base64"`.
    #[serde(default = "default_encoding")]
    pub encoding: String,

    /// Description for Object Store `put`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    // --- JetStream fields ---

    /// JetStream stream name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,

    /// JetStream subject for `js_publish`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,

    /// Headers for `js_publish`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<std::collections::HashMap<String, String>>,

    /// Sequence number for `js_get_msg`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,

    /// Fetch the last message in the stream (`js_get_msg`).
    #[serde(default)]
    pub last: bool,
}

fn default_encoding() -> String {
    "utf-8".to_string()
}

// ---------------------------------------------------------------------------
// Tool struct
// ---------------------------------------------------------------------------

/// NATS tool implementation.
pub struct NatsTool {
    template_engine: TemplateEngine,
}

impl NatsTool {
    /// Create a new NATS tool.
    pub fn new() -> Self {
        Self {
            template_engine: TemplateEngine::new(),
        }
    }

    /// Parse and template-render config from a [`ToolConfig`].
    fn parse_config(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<NatsConfig, ToolError> {
        let template_ctx = ctx.to_template_context();
        let rendered = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;
        serde_json::from_value(rendered)
            .map_err(|e| ToolError::Configuration(format!("Invalid nats config: {}", e)))
    }

    /// Resolve the NATS URL + optional auth from config + context secrets.
    ///
    /// Resolution order:
    /// 1. `auth` field → credential alias looked up in `ctx.secrets`.
    ///    The credential JSON must contain a `url` field; optionally `user`,
    ///    `password`, `token`.
    /// 2. Explicit `url` + `user` / `password` / `token` fields in config.
    fn resolve_connection(
        &self,
        cfg: &NatsConfig,
        ctx: &ExecutionContext,
    ) -> Result<NatsConnParams, ToolError> {
        // --- Step 1: try credential alias ---
        if let Some(ref alias) = cfg.auth {
            if let Some(raw) = ctx.get_secret(alias) {
                let cred: serde_json::Value = serde_json::from_str(raw).map_err(|e| {
                    ToolError::Auth(format!("Credential '{}' is not valid JSON: {}", alias, e))
                })?;

                let url = cred["url"]
                    .as_str()
                    .or_else(|| cred["nats_url"].as_str())
                    .ok_or_else(|| {
                        ToolError::Auth(format!(
                            "Credential '{}' missing required 'url' field",
                            alias
                        ))
                    })?
                    .to_string();

                return Ok(NatsConnParams {
                    url,
                    user: cred["user"]
                        .as_str()
                        .or_else(|| cred["username"].as_str())
                        .map(str::to_string),
                    password: cred["password"].as_str().map(str::to_string),
                    token: cred["token"].as_str().map(str::to_string),
                });
            }
        }

        // --- Step 2: explicit config fields ---
        let url = cfg.url.clone().ok_or_else(|| {
            ToolError::Configuration(
                "NATS tool requires 'url' or an 'auth' credential alias with a 'url' field"
                    .to_string(),
            )
        })?;

        // Password may itself be a secret alias.
        let password = cfg.password.as_deref().map(|pw| {
            ctx.get_secret(pw)
                .map(str::to_string)
                .unwrap_or_else(|| pw.to_string())
        });

        // Token may itself be a secret alias.
        let token = cfg.token.as_deref().map(|tok| {
            ctx.get_secret(tok)
                .map(str::to_string)
                .unwrap_or_else(|| tok.to_string())
        });

        Ok(NatsConnParams {
            url,
            user: cfg.user.clone(),
            password,
            token,
        })
    }
}

impl Default for NatsTool {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Connection params
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct NatsConnParams {
    url: String,
    user: Option<String>,
    password: Option<String>,
    token: Option<String>,
}

impl NatsConnParams {
    async fn connect(&self) -> Result<async_nats::Client, ToolError> {
        let opts = self.build_connect_options();
        opts.connect(&self.url).await.map_err(|e| {
            ToolError::ExecutionFailed(format!("NATS connect to '{}' failed: {}", self.url, e))
        })
    }

    fn build_connect_options(&self) -> async_nats::ConnectOptions {
        let mut opts = async_nats::ConnectOptions::new();

        if let Some(ref token) = self.token {
            opts = opts.token(token.clone());
        } else if let (Some(ref user), Some(ref password)) = (&self.user, &self.password) {
            opts = opts.user_and_password(user.clone(), password.clone());
        }

        opts
    }
}

// ---------------------------------------------------------------------------
// Tool trait impl
// ---------------------------------------------------------------------------

#[async_trait]
impl Tool for NatsTool {
    fn name(&self) -> &'static str {
        "nats"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let nats_cfg = self.parse_config(config, ctx)?;
        let conn_params = self.resolve_connection(&nats_cfg, ctx)?;

        let op = nats_cfg.operation.as_str();
        let execution_id = ctx.execution_id;

        tracing::debug!(
            operation = op,
            execution_id,
            url = %conn_params.url,
            "NATS tool dispatch"
        );

        let start = std::time::Instant::now();

        // Connect once per execution; each operation function receives the
        // JetStream context.  The connection is dropped at end of scope.
        let nc = conn_params.connect().await?;
        let js = async_nats::jetstream::new(nc);

        let result = {
            let span = tracing::info_span!(
                "nats.op",
                operation = op,
                execution_id,
            );
            let _guard = span.enter();

            match op {
                "kv_get" => kv_get(&js, &nats_cfg).await,
                "kv_put" => kv_put(&js, &nats_cfg).await,
                "kv_delete" => kv_delete(&js, &nats_cfg).await,
                "kv_keys" => kv_keys(&js, &nats_cfg).await,
                "kv_purge" => kv_purge(&js, &nats_cfg).await,
                "object_get" => object_get(&js, &nats_cfg).await,
                "object_put" => object_put(&js, &nats_cfg).await,
                "object_delete" => object_delete(&js, &nats_cfg).await,
                "object_list" => object_list(&js, &nats_cfg).await,
                "object_info" => object_info(&js, &nats_cfg).await,
                "js_publish" => js_publish(&js, &nats_cfg).await,
                "js_get_msg" => js_get_msg(&js, &nats_cfg).await,
                "js_stream_info" => js_stream_info(&js, &nats_cfg).await,
                unknown => Err(ToolError::Configuration(format!(
                    "Unknown NATS operation '{}'. Valid: kv_get, kv_put, kv_delete, kv_keys, \
                     kv_purge, object_get, object_put, object_delete, object_list, object_info, \
                     js_publish, js_get_msg, js_stream_info",
                    unknown
                ))),
            }
        };

        let duration_ms = start.elapsed().as_millis() as u64;
        tracing::debug!(
            operation = op,
            duration_ms,
            ok = result.is_ok(),
            "NATS operation complete"
        );

        result.map(|data| ToolResult::success(data).with_duration(duration_ms))
    }
}

// ---------------------------------------------------------------------------
// KV operations
// ---------------------------------------------------------------------------

fn require_bucket(cfg: &NatsConfig) -> Result<&str, ToolError> {
    cfg.bucket
        .as_deref()
        .ok_or_else(|| ToolError::Configuration("NATS KV/Object operation requires 'bucket'".into()))
}

fn require_key(cfg: &NatsConfig) -> Result<&str, ToolError> {
    cfg.key
        .as_deref()
        .ok_or_else(|| ToolError::Configuration("NATS KV operation requires 'key'".into()))
}

async fn open_kv(js: &jetstream::Context, bucket: &str) -> Result<kv::Store, ToolError> {
    js.get_key_value(bucket).await.map_err(|e| {
        ToolError::ExecutionFailed(format!("Cannot open KV bucket '{}': {}", bucket, e))
    })
}

async fn kv_get(js: &jetstream::Context, cfg: &NatsConfig) -> Result<serde_json::Value, ToolError> {
    let bucket = require_bucket(cfg)?;
    let key = require_key(cfg)?;
    let store = open_kv(js, bucket).await?;

    match store.get(key).await {
        Ok(Some(bytes)) => {
            let raw = std::str::from_utf8(&bytes).unwrap_or("");
            let value: serde_json::Value = serde_json::from_str(raw)
                .unwrap_or_else(|_| serde_json::Value::String(raw.to_string()));
            Ok(serde_json::json!({
                "status": "success",
                "bucket": bucket,
                "key": key,
                "value": value,
            }))
        }
        Ok(None) => Ok(serde_json::json!({
            "status": "not_found",
            "bucket": bucket,
            "key": key,
            "value": null,
        })),
        Err(e) => Err(ToolError::ExecutionFailed(format!("kv_get failed: {}", e))),
    }
}

async fn kv_put(js: &jetstream::Context, cfg: &NatsConfig) -> Result<serde_json::Value, ToolError> {
    let bucket = require_bucket(cfg)?;
    let key = require_key(cfg)?;
    let store = open_kv(js, bucket).await?;

    let payload = serialize_value(cfg.value.as_ref())?;
    let revision = store
        .put(key, bytes::Bytes::from(payload))
        .await
        .map_err(|e| ToolError::ExecutionFailed(format!("kv_put failed: {}", e)))?;

    Ok(serde_json::json!({
        "status": "success",
        "bucket": bucket,
        "key": key,
        "revision": revision,
    }))
}

async fn kv_delete(
    js: &jetstream::Context,
    cfg: &NatsConfig,
) -> Result<serde_json::Value, ToolError> {
    let bucket = require_bucket(cfg)?;
    let key = require_key(cfg)?;
    let store = open_kv(js, bucket).await?;

    store
        .delete(key)
        .await
        .map_err(|e| ToolError::ExecutionFailed(format!("kv_delete failed: {}", e)))?;

    Ok(serde_json::json!({
        "status": "success",
        "bucket": bucket,
        "key": key,
    }))
}

async fn kv_keys(js: &jetstream::Context, cfg: &NatsConfig) -> Result<serde_json::Value, ToolError> {
    let bucket = require_bucket(cfg)?;
    let store = open_kv(js, bucket).await?;
    let pattern = cfg.pattern.as_deref();

    let keys_stream = store
        .keys()
        .await
        .map_err(|e| ToolError::ExecutionFailed(format!("kv_keys failed: {}", e)))?;

    let all_keys: Vec<String> = keys_stream
        .filter_map(|r| async move { r.ok() })
        .collect()
        .await;

    let filtered: Vec<&String> = if let Some(pat) = pattern {
        all_keys.iter().filter(|k| glob_match(pat, k)).collect()
    } else {
        all_keys.iter().collect()
    };

    Ok(serde_json::json!({
        "status": "success",
        "bucket": bucket,
        "keys": filtered,
        "count": filtered.len(),
    }))
}

async fn kv_purge(
    js: &jetstream::Context,
    cfg: &NatsConfig,
) -> Result<serde_json::Value, ToolError> {
    let bucket = require_bucket(cfg)?;
    let key = require_key(cfg)?;
    let store = open_kv(js, bucket).await?;

    store
        .purge(key)
        .await
        .map_err(|e| ToolError::ExecutionFailed(format!("kv_purge failed: {}", e)))?;

    Ok(serde_json::json!({
        "status": "success",
        "bucket": bucket,
        "key": key,
    }))
}

// ---------------------------------------------------------------------------
// Object Store operations
// ---------------------------------------------------------------------------

fn require_object_name(cfg: &NatsConfig) -> Result<&str, ToolError> {
    cfg.name
        .as_deref()
        .ok_or_else(|| ToolError::Configuration("NATS Object Store operation requires 'name'".into()))
}

async fn open_object_store(
    js: &jetstream::Context,
    bucket: &str,
) -> Result<object_store::ObjectStore, ToolError> {
    js.get_object_store(bucket).await.map_err(|e| {
        ToolError::ExecutionFailed(format!("Cannot open Object Store bucket '{}': {}", bucket, e))
    })
}

async fn object_get(
    js: &jetstream::Context,
    cfg: &NatsConfig,
) -> Result<serde_json::Value, ToolError> {
    use tokio::io::AsyncReadExt;

    let bucket = require_bucket(cfg)?;
    let name = require_object_name(cfg)?;
    let store = open_object_store(js, bucket).await?;

    let mut object = store.get(name).await.map_err(|e| {
        ToolError::ExecutionFailed(format!("object_get '{}' failed: {}", name, e))
    })?;

    let mut buf = Vec::new();
    object.read_to_end(&mut buf).await.map_err(|e| {
        ToolError::ExecutionFailed(format!("object_get read failed: {}", e))
    })?;

    let size = buf.len();
    let data: serde_json::Value = if cfg.encoding == "base64" {
        serde_json::Value::String(BASE64.encode(&buf))
    } else {
        serde_json::Value::String(String::from_utf8_lossy(&buf).into_owned())
    };

    Ok(serde_json::json!({
        "status": "success",
        "bucket": bucket,
        "name": name,
        "data": data,
        "size": size,
    }))
}

async fn object_put(
    js: &jetstream::Context,
    cfg: &NatsConfig,
) -> Result<serde_json::Value, ToolError> {
    let bucket = require_bucket(cfg)?;
    let name = require_object_name(cfg)?;
    let store = open_object_store(js, bucket).await?;

    let binary = encode_object_data(cfg)?;
    let size = binary.len();

    let meta = object_store::ObjectMetadata {
        name: name.to_string(),
        description: cfg.description.clone(),
        chunk_size: None,
    };

    // `put` takes `impl AsyncRead + Unpin`; use a cursor over our Vec<u8>.
    let mut reader = std::io::Cursor::new(binary);
    store.put(meta, &mut reader).await.map_err(|e| {
        ToolError::ExecutionFailed(format!("object_put '{}' failed: {}", name, e))
    })?;

    Ok(serde_json::json!({
        "status": "success",
        "bucket": bucket,
        "name": name,
        "size": size,
    }))
}

async fn object_delete(
    js: &jetstream::Context,
    cfg: &NatsConfig,
) -> Result<serde_json::Value, ToolError> {
    let bucket = require_bucket(cfg)?;
    let name = require_object_name(cfg)?;
    let store = open_object_store(js, bucket).await?;

    store.delete(name).await.map_err(|e| {
        ToolError::ExecutionFailed(format!("object_delete '{}' failed: {}", name, e))
    })?;

    Ok(serde_json::json!({
        "status": "success",
        "bucket": bucket,
        "name": name,
    }))
}

async fn object_list(
    js: &jetstream::Context,
    cfg: &NatsConfig,
) -> Result<serde_json::Value, ToolError> {
    let bucket = require_bucket(cfg)?;
    let store = open_object_store(js, bucket).await?;

    let mut list_stream = store.list().await.map_err(|e| {
        ToolError::ExecutionFailed(format!("object_list failed: {}", e))
    })?;

    let mut objects = Vec::new();
    while let Some(item) = list_stream.next().await {
        match item {
            Ok(info) => {
                objects.push(serde_json::json!({
                    "name": info.name,
                    "size": info.size,
                    "description": info.description,
                    "chunks": info.chunks,
                }));
            }
            Err(e) => {
                tracing::warn!("object_list entry error: {}", e);
            }
        }
    }

    let count = objects.len();
    Ok(serde_json::json!({
        "status": "success",
        "bucket": bucket,
        "objects": objects,
        "count": count,
    }))
}

async fn object_info(
    js: &jetstream::Context,
    cfg: &NatsConfig,
) -> Result<serde_json::Value, ToolError> {
    let bucket = require_bucket(cfg)?;
    let name = require_object_name(cfg)?;
    let store = open_object_store(js, bucket).await?;

    let info = store.info(name).await.map_err(|e| {
        ToolError::ExecutionFailed(format!("object_info '{}' failed: {}", name, e))
    })?;

    Ok(serde_json::json!({
        "status": "success",
        "bucket": bucket,
        "name": info.name,
        "size": info.size,
        "description": info.description,
        "chunks": info.chunks,
    }))
}

// ---------------------------------------------------------------------------
// JetStream operations
// ---------------------------------------------------------------------------

async fn js_publish(
    js: &jetstream::Context,
    cfg: &NatsConfig,
) -> Result<serde_json::Value, ToolError> {
    // Own the subject string so it satisfies ToSubject (String impl).
    let subject: String = cfg
        .subject
        .clone()
        .ok_or_else(|| ToolError::Configuration("js_publish requires 'subject'".into()))?;

    let payload = bytes::Bytes::from(serialize_value(cfg.data.as_ref())?);

    let ack = if let Some(ref hdrs) = cfg.headers {
        let mut header_map = async_nats::HeaderMap::new();
        for (k, v) in hdrs {
            header_map.insert(k.as_str(), v.as_str());
        }
        js.publish_with_headers(subject, header_map, payload)
            .await
            .map_err(|e| ToolError::ExecutionFailed(format!("js_publish failed: {}", e)))?
            .await
            .map_err(|e| ToolError::ExecutionFailed(format!("js_publish ack failed: {}", e)))?
    } else {
        js.publish(subject, payload)
            .await
            .map_err(|e| ToolError::ExecutionFailed(format!("js_publish failed: {}", e)))?
            .await
            .map_err(|e| ToolError::ExecutionFailed(format!("js_publish ack failed: {}", e)))?
    };

    Ok(serde_json::json!({
        "status": "success",
        "stream": ack.stream,
        "seq": ack.sequence,
        "duplicate": ack.duplicate,
    }))
}

async fn js_get_msg(
    js: &jetstream::Context,
    cfg: &NatsConfig,
) -> Result<serde_json::Value, ToolError> {
    let stream_name = cfg
        .stream
        .as_deref()
        .ok_or_else(|| ToolError::Configuration("js_get_msg requires 'stream'".into()))?;

    let stream = js.get_stream(stream_name).await.map_err(|e| {
        ToolError::ExecutionFailed(format!(
            "js_get_msg: stream '{}' not found: {}",
            stream_name, e
        ))
    })?;

    let msg = if let Some(seq) = cfg.seq {
        stream.get_raw_message(seq).await.map_err(|e| {
            ToolError::ExecutionFailed(format!("js_get_msg seq={} failed: {}", seq, e))
        })?
    } else if cfg.last || cfg.subject.is_some() {
        let subj = cfg.subject.as_deref().unwrap_or(">");
        stream
            .get_last_raw_message_by_subject(subj)
            .await
            .map_err(|e| {
                ToolError::ExecutionFailed(format!(
                    "js_get_msg last/subject='{}' failed: {}",
                    subj, e
                ))
            })?
    } else {
        return Err(ToolError::Configuration(
            "js_get_msg requires one of: 'seq', 'last: true', or 'subject'".into(),
        ));
    };

    let payload_str = std::str::from_utf8(&msg.payload).unwrap_or("");
    let data: serde_json::Value = serde_json::from_str(payload_str)
        .unwrap_or_else(|_| serde_json::Value::String(payload_str.to_string()));

    Ok(serde_json::json!({
        "status": "success",
        "stream": stream_name,
        "subject": msg.subject,
        "seq": msg.sequence,
        "data": data,
    }))
}

async fn js_stream_info(
    js: &jetstream::Context,
    cfg: &NatsConfig,
) -> Result<serde_json::Value, ToolError> {
    let stream_name = cfg
        .stream
        .as_deref()
        .ok_or_else(|| ToolError::Configuration("js_stream_info requires 'stream'".into()))?;

    let mut stream = js.get_stream(stream_name).await.map_err(|e| {
        ToolError::ExecutionFailed(format!("js_stream_info: '{}': {}", stream_name, e))
    })?;

    let info = stream.info().await.map_err(|e| {
        ToolError::ExecutionFailed(format!("js_stream_info fetch failed: {}", e))
    })?;

    Ok(serde_json::json!({
        "status": "success",
        "stream": stream_name,
        "config": {
            "name": info.config.name,
            "subjects": info.config.subjects,
            "max_msgs": info.config.max_messages,
            "max_bytes": info.config.max_bytes,
        },
        "state": {
            "messages": info.state.messages,
            "bytes": info.state.bytes,
            "first_seq": info.state.first_sequence,
            "last_seq": info.state.last_sequence,
            "consumer_count": info.state.consumer_count,
        },
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Serialize a JSON value to bytes for NATS payloads.
fn serialize_value(value: Option<&serde_json::Value>) -> Result<Vec<u8>, ToolError> {
    match value {
        None => Ok(Vec::new()),
        Some(serde_json::Value::String(s)) => Ok(s.as_bytes().to_vec()),
        Some(v) => serde_json::to_vec(v)
            .map_err(|e| ToolError::Json(format!("Failed to serialize value: {}", e))),
    }
}

/// Encode object data to bytes, respecting `cfg.encoding`.
fn encode_object_data(cfg: &NatsConfig) -> Result<Vec<u8>, ToolError> {
    match cfg.data.as_ref() {
        None => Ok(Vec::new()),
        Some(serde_json::Value::String(s)) => {
            if cfg.encoding == "base64" {
                BASE64.decode(s).map_err(|e| {
                    ToolError::Configuration(format!("base64 decode failed: {}", e))
                })
            } else {
                Ok(s.as_bytes().to_vec())
            }
        }
        Some(v) => serde_json::to_vec(v)
            .map_err(|e| ToolError::Json(format!("Failed to serialize object data: {}", e))),
    }
}

/// Minimal glob matcher (supports `*` as any-character wildcard).
fn glob_match(pattern: &str, s: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == s;
    }
    let mut remaining = s;
    for (i, part) in parts.iter().enumerate() {
        if i == 0 {
            if !remaining.starts_with(part) {
                return false;
            }
            remaining = &remaining[part.len()..];
        } else if i == parts.len() - 1 {
            return remaining.ends_with(part);
        } else {
            match remaining.find(part) {
                Some(pos) => remaining = &remaining[pos + part.len()..],
                None => return false,
            }
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Config parsing ---

    #[test]
    fn test_nats_config_kv_get() {
        let json = serde_json::json!({
            "url": "nats://localhost:4222",
            "operation": "kv_get",
            "bucket": "my_bucket",
            "key": "my_key",
        });
        let cfg: NatsConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.operation, "kv_get");
        assert_eq!(cfg.bucket.as_deref(), Some("my_bucket"));
        assert_eq!(cfg.key.as_deref(), Some("my_key"));
        assert_eq!(cfg.encoding, "utf-8"); // default
    }

    #[test]
    fn test_nats_config_kv_put_with_ttl() {
        let json = serde_json::json!({
            "url": "nats://localhost:4222",
            "operation": "kv_put",
            "bucket": "cache",
            "key": "token",
            "value": {"access": "abc"},
            "ttl": 3600,
        });
        let cfg: NatsConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.operation, "kv_put");
        assert_eq!(cfg.ttl, Some(3600));
        assert!(cfg.value.is_some());
    }

    #[test]
    fn test_nats_config_js_publish() {
        let json = serde_json::json!({
            "url": "nats://localhost:4222",
            "operation": "js_publish",
            "subject": "events.orders",
            "data": {"order_id": 42},
            "headers": {"X-Source": "test"},
        });
        let cfg: NatsConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.operation, "js_publish");
        assert_eq!(cfg.subject.as_deref(), Some("events.orders"));
        assert!(cfg.headers.is_some());
    }

    #[test]
    fn test_nats_config_object_get_base64() {
        let json = serde_json::json!({
            "url": "nats://localhost:4222",
            "operation": "object_get",
            "bucket": "blobs",
            "name": "report.pdf",
            "encoding": "base64",
        });
        let cfg: NatsConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.encoding, "base64");
    }

    #[test]
    fn test_nats_config_js_get_msg_by_seq() {
        let json = serde_json::json!({
            "url": "nats://localhost:4222",
            "operation": "js_get_msg",
            "stream": "ORDERS",
            "seq": 100,
        });
        let cfg: NatsConfig = serde_json::from_value(json).unwrap();
        assert_eq!(cfg.seq, Some(100));
        assert!(!cfg.last);
    }

    #[test]
    fn test_nats_config_js_get_msg_last() {
        let json = serde_json::json!({
            "url": "nats://localhost:4222",
            "operation": "js_get_msg",
            "stream": "ORDERS",
            "last": true,
        });
        let cfg: NatsConfig = serde_json::from_value(json).unwrap();
        assert!(cfg.last);
    }

    // --- Auth resolution ---

    #[test]
    fn test_resolve_connection_explicit_url() {
        let tool = NatsTool::new();
        let ctx = ExecutionContext::default();
        let cfg = NatsConfig {
            url: Some("nats://localhost:4222".to_string()),
            operation: "kv_get".to_string(),
            auth: None,
            user: None,
            password: None,
            token: None,
            bucket: None,
            key: None,
            value: None,
            ttl: None,
            pattern: None,
            name: None,
            data: None,
            encoding: "utf-8".to_string(),
            description: None,
            stream: None,
            subject: None,
            headers: None,
            seq: None,
            last: false,
        };
        let params = tool.resolve_connection(&cfg, &ctx).unwrap();
        assert_eq!(params.url, "nats://localhost:4222");
        assert!(params.user.is_none());
        assert!(params.token.is_none());
    }

    #[test]
    fn test_resolve_connection_missing_url_error() {
        let tool = NatsTool::new();
        let ctx = ExecutionContext::default();
        let cfg = NatsConfig {
            url: None,
            operation: "kv_get".to_string(),
            auth: None,
            user: None,
            password: None,
            token: None,
            bucket: None,
            key: None,
            value: None,
            ttl: None,
            pattern: None,
            name: None,
            data: None,
            encoding: "utf-8".to_string(),
            description: None,
            stream: None,
            subject: None,
            headers: None,
            seq: None,
            last: false,
        };
        let result = tool.resolve_connection(&cfg, &ctx);
        assert!(matches!(result, Err(ToolError::Configuration(_))));
    }

    #[test]
    fn test_resolve_connection_from_credential_alias() {
        let tool = NatsTool::new();
        let mut ctx = ExecutionContext::default();
        ctx.set_secret(
            "my_nats_cred",
            r#"{"url":"nats://secure:4222","token":"s3cr3t"}"#,
        );
        let cfg = NatsConfig {
            url: None,
            operation: "kv_get".to_string(),
            auth: Some("my_nats_cred".to_string()),
            user: None,
            password: None,
            token: None,
            bucket: None,
            key: None,
            value: None,
            ttl: None,
            pattern: None,
            name: None,
            data: None,
            encoding: "utf-8".to_string(),
            description: None,
            stream: None,
            subject: None,
            headers: None,
            seq: None,
            last: false,
        };
        let params = tool.resolve_connection(&cfg, &ctx).unwrap();
        assert_eq!(params.url, "nats://secure:4222");
        assert_eq!(params.token.as_deref(), Some("s3cr3t"));
    }

    #[test]
    fn test_resolve_connection_credential_missing_url() {
        let tool = NatsTool::new();
        let mut ctx = ExecutionContext::default();
        ctx.set_secret("bad_cred", r#"{"token":"only-token"}"#);
        let cfg = NatsConfig {
            url: None,
            operation: "kv_get".to_string(),
            auth: Some("bad_cred".to_string()),
            user: None,
            password: None,
            token: None,
            bucket: None,
            key: None,
            value: None,
            ttl: None,
            pattern: None,
            name: None,
            data: None,
            encoding: "utf-8".to_string(),
            description: None,
            stream: None,
            subject: None,
            headers: None,
            seq: None,
            last: false,
        };
        let result = tool.resolve_connection(&cfg, &ctx);
        assert!(matches!(result, Err(ToolError::Auth(_))));
    }

    // --- Helpers ---

    #[test]
    fn test_serialize_value_string() {
        let v = serde_json::json!("hello");
        let bytes = serialize_value(Some(&v)).unwrap();
        assert_eq!(bytes, b"hello");
    }

    #[test]
    fn test_serialize_value_json() {
        let v = serde_json::json!({"a": 1});
        let bytes = serialize_value(Some(&v)).unwrap();
        let back: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back["a"], 1);
    }

    #[test]
    fn test_serialize_value_none() {
        let bytes = serialize_value(None).unwrap();
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("foo.*", "foo.bar"));
        assert!(glob_match("*.bar", "foo.bar"));
        assert!(glob_match("*", "anything"));
        assert!(!glob_match("foo.*", "bar.baz"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "notexact"));
        assert!(glob_match("a*b*c", "axbxc"));
        assert!(glob_match("a*b*c", "axbc")); // "a" + "x" + "b" + "" + "c"
        assert!(!glob_match("a*b*c", "axbx")); // doesn't end with "c"
    }

    #[test]
    fn test_encode_object_data_utf8() {
        let cfg = NatsConfig {
            data: Some(serde_json::json!("hello world")),
            encoding: "utf-8".to_string(),
            url: None,
            auth: None,
            user: None,
            password: None,
            token: None,
            operation: "object_put".to_string(),
            bucket: None,
            key: None,
            value: None,
            ttl: None,
            pattern: None,
            name: None,
            description: None,
            stream: None,
            subject: None,
            headers: None,
            seq: None,
            last: false,
        };
        let bytes = encode_object_data(&cfg).unwrap();
        assert_eq!(bytes, b"hello world");
    }

    #[test]
    fn test_encode_object_data_base64() {
        let raw = b"binary data";
        let encoded = BASE64.encode(raw);
        let cfg = NatsConfig {
            data: Some(serde_json::json!(encoded)),
            encoding: "base64".to_string(),
            url: None,
            auth: None,
            user: None,
            password: None,
            token: None,
            operation: "object_put".to_string(),
            bucket: None,
            key: None,
            value: None,
            ttl: None,
            pattern: None,
            name: None,
            description: None,
            stream: None,
            subject: None,
            headers: None,
            seq: None,
            last: false,
        };
        let bytes = encode_object_data(&cfg).unwrap();
        assert_eq!(bytes, raw);
    }

    // --- Tool interface ---

    #[tokio::test]
    async fn test_nats_tool_name() {
        let tool = NatsTool::new();
        assert_eq!(tool.name(), "nats");
    }

    // --- Integration tests (gated behind env var) ---

    /// Set `NOETL_TEST_NATS_URL=nats://localhost:4222` to run live-server tests.
    #[tokio::test]
    async fn test_nats_integration_kv_roundtrip() {
        let nats_url = match std::env::var("NOETL_TEST_NATS_URL") {
            Ok(u) => u,
            Err(_) => return, // skip when no live NATS available
        };

        let nc = async_nats::connect(&nats_url).await.expect("connect");
        let js = async_nats::jetstream::new(nc);

        // Create KV bucket for test
        let bucket_name = format!("noetl_test_{}", uuid::Uuid::new_v4().simple());
        js.create_key_value(kv::Config {
            bucket: bucket_name.clone(),
            ..Default::default()
        })
        .await
        .expect("create bucket");

        let tool = NatsTool::new();
        let mut ctx = ExecutionContext::default();
        ctx.set_secret("test_cred", format!(r#"{{"url":"{}"}}"#, nats_url));

        // Put
        let put_cfg = ToolConfig {
            kind: "nats".to_string(),
            config: serde_json::json!({
                "auth": "test_cred",
                "operation": "kv_put",
                "bucket": bucket_name,
                "key": "hello",
                "value": "world",
            }),
            timeout: None,
            retry: None,
            auth: None,
        };
        let put_result = tool.execute(&put_cfg, &ctx).await.expect("kv_put");
        assert!(put_result.is_success());

        // Get
        let get_cfg = ToolConfig {
            kind: "nats".to_string(),
            config: serde_json::json!({
                "auth": "test_cred",
                "operation": "kv_get",
                "bucket": bucket_name,
                "key": "hello",
            }),
            timeout: None,
            retry: None,
            auth: None,
        };
        let get_result = tool.execute(&get_cfg, &ctx).await.expect("kv_get");
        assert!(get_result.is_success());
        let data = get_result.data.unwrap();
        assert_eq!(data["status"], "success");
        assert_eq!(data["value"], "world");

        // Cleanup: delete the bucket (best-effort)
        let _ = js.delete_key_value(&bucket_name).await;
    }
}
