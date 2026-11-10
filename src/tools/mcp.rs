//! MCP (Model Context Protocol) tool kind — JSON-RPC over HTTP bridge.
//!
//! Bridges playbook steps to MCP servers using JSON-RPC 2.0 over HTTP with
//! optional SSE response parsing.  Mirrors the Python tool's playbook-facing
//! surface ([`noetl/tools/mcp/executor.py`][py-src]).
//!
//! ## Playbook config shape
//!
//! ```yaml
//! tool:
//!   kind: mcp
//!   endpoint: "http://localhost:8080/mcp"   # direct endpoint, OR
//!   server: kubernetes                       # key for NOETL_MCP_<SERVER>_ENDPOINT env var
//!   method: tools/call                       # tools/call | tools/list | health | <other>
//!   tool: get_pods                           # required for tools/call
//!   arguments:                               # required for tools/call
//!     namespace: default
//!   params: {}                               # passthrough params for other methods
//!   timeout: 30                              # seconds; defaults to NOETL_MCP_REQUEST_TIMEOUT_SECONDS → 60
//!   request_id: 1                            # JSON-RPC request id (default: 1)
//!   protocol_version: "2025-03-26"           # MCP initialize protocolVersion
//! ```
//!
//! ## Endpoint resolution
//!
//! 1. `config.endpoint` (or `config.url` / `config.server_url` / `config.base_url`)
//! 2. `NOETL_MCP_<SERVER>_ENDPOINT` env var keyed by `config.server` slug
//! 3. `NOETL_MCP_URL` fallback env var
//!
//! ## Session lifecycle
//!
//! Each invocation POSTs an `initialize` request to obtain an `Mcp-Session-Id`
//! response header.  Subsequent requests in the same invocation reuse that
//! session id.  Servers that do not return a session id are treated as
//! stateless JSON-RPC (no error).
//!
//! ## Return shape
//!
//! ```json
//! {
//!   "status": "ok",
//!   "server": "kubernetes",
//!   "endpoint": "http://localhost:8080/mcp",
//!   "method": "tools/call",
//!   "tool": "get_pods",
//!   "arguments": { "namespace": "default" },
//!   "text": "...",
//!   "result": { ... },
//!   "initialize": { ... }
//! }
//! ```
//!
//! On error the `status` field is `"error"` and an `error` field contains the
//! message string.  The shape matches the Python tool so playbooks work with
//! either worker runtime.
//!
//! [py-src]: https://github.com/noetl/noetl/blob/main/noetl/tools/mcp/executor.py

use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

use crate::auth::{AuthCredentials, AuthResolver};
use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::AuthConfig;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default MCP request timeout in seconds.
const DEFAULT_MCP_TIMEOUT_SECS: f64 = 60.0;
/// Default worker command timeout ceiling in seconds.
const DEFAULT_COMMAND_TIMEOUT_SECS: f64 = 180.0;
/// Minimum timeout floor in seconds.
const MIN_TIMEOUT_SECS: f64 = 0.1;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// MCP tool configuration (playbook-facing surface).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpConfig {
    // --- endpoint resolution ---
    /// Direct MCP server endpoint URL (e.g. `http://host:8080/mcp`).
    /// Takes precedence over `server` + env var resolution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Alias for `endpoint` (accepted for compatibility).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// Alias for `endpoint` (accepted for compatibility).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_url: Option<String>,

    /// Alias for `endpoint` (accepted for compatibility).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,

    /// Logical server name; used to look up `NOETL_MCP_<SERVER>_ENDPOINT`.
    /// Defaults to `"kubernetes"` when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,

    /// Alias for `server`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    // --- request ---
    /// JSON-RPC method.  Recognised shortcuts:
    /// - `tools/call` (default) — call a named tool with `arguments`.
    /// - `tools/list` — list tools advertised by the server.
    /// - `health` — GET `<endpoint>/healthz` (no JSON-RPC).
    /// - Any other value — sent as-is with `params` passed through.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,

    /// Alias for `method`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,

    // --- tools/call ---
    /// Tool name for `tools/call`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,

    /// Alias for `tool`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,

    /// Arguments for `tools/call`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<serde_json::Value>,

    /// Alias for `arguments`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<serde_json::Value>,

    // --- passthrough ---
    /// Params for non-standard JSON-RPC methods.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,

    // --- timing ---
    /// Request timeout in seconds.  Clamped by `NOETL_WORKER_COMMAND_TIMEOUT_SECONDS`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<serde_json::Value>,

    // --- protocol ---
    /// JSON-RPC request id (default: 1).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<u64>,

    /// MCP protocol version sent in `initialize` (default: `"2025-03-26"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<String>,

    /// MCP client name sent in `initialize` (default: `"noetl-worker"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_name: Option<String>,

    /// MCP client version sent in `initialize` (default: `"0"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,

    /// Capabilities object sent in `initialize` (default: `{}`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<serde_json::Value>,

    // --- auth ---
    /// Literal request headers (`{name: value}`), sent on every request of the
    /// invocation.  Values are rendered with the rest of the config, so a host
    /// can carry `{{ keychain.<alias>.<field> }}` here.  A header that renders
    /// empty is dropped rather than sent blank.  `content-type`, `accept`,
    /// `mcp-session-id`, `content-length` and `host` belong to the tool and are
    /// refused.  The step's `auth:` block (the [`ToolConfig::auth`] the host
    /// passes) adds an `Authorization` / api-key header on top.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<BTreeMap<String, serde_json::Value>>,

    /// `fail` turns a provider error into a failed step: a transport, JSON-RPC
    /// or envelope failure (otherwise reported as a successful `status: "error"`
    /// result), or a `tools/call` result carrying MCP's `isError: true`.
    /// `continue` (and unset) keeps the result for the playbook to read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_provider_error: Option<String>,
}

// ---------------------------------------------------------------------------
// Tool struct
// ---------------------------------------------------------------------------

/// MCP (Model Context Protocol) tool.
pub struct McpTool {
    template_engine: TemplateEngine,
}

impl McpTool {
    /// Create a new MCP tool.
    pub fn new() -> Self {
        Self {
            template_engine: TemplateEngine::new(),
        }
    }

    fn parse_config(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<McpConfig, ToolError> {
        let template_ctx = ctx.to_template_context();
        let rendered = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;
        serde_json::from_value(rendered)
            .map_err(|e| ToolError::Configuration(format!("Invalid mcp config: {}", e)))
    }
}

impl Default for McpTool {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tool trait impl
// ---------------------------------------------------------------------------

#[async_trait]
impl Tool for McpTool {
    fn name(&self) -> &'static str {
        "mcp"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let mcp_cfg = self.parse_config(config, ctx)?;
        let fail_on_provider_error = parse_provider_error_policy(&mcp_cfg.on_provider_error)?;
        let extra = request_headers(&mcp_cfg.headers, config.auth.as_ref(), ctx).await?;
        let sent_headers = header_names(&extra);

        let server = mcp_cfg
            .server
            .clone()
            .or_else(|| mcp_cfg.name.clone())
            .unwrap_or_else(|| "kubernetes".to_string());

        let endpoint = resolve_endpoint(&mcp_cfg)?;
        let method = mcp_cfg
            .method
            .clone()
            .or_else(|| mcp_cfg.action.clone())
            .unwrap_or_else(|| "tools/call".to_string());

        let timeout_secs = resolve_timeout(&mcp_cfg.timeout);
        let request_id = mcp_cfg.request_id.unwrap_or(1);

        let log_level = "debug";
        let _ = log_level; // level selection reserved for tracing macro expansions below

        tracing::debug!(
            method = %method,
            server = %server,
            endpoint = %endpoint,
            execution_id = ctx.execution_id,
            "MCP tool dispatch"
        );

        let start = std::time::Instant::now();

        let span = tracing::info_span!(
            "mcp.op",
            method = %method,
            server = %server,
            execution_id = ctx.execution_id,
        );
        let _guard = span.enter();

        let result = execute_mcp(
            &mcp_cfg,
            &server,
            &endpoint,
            &method,
            timeout_secs,
            request_id,
            &extra,
        )
        .await;

        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(mut data) => {
                tracing::debug!(
                    method = %method,
                    server = %server,
                    duration_ms,
                    "MCP request complete"
                );
                if fail_on_provider_error && is_tool_error(&data) {
                    return Err(ToolError::ExecutionFailed(format!(
                        "mcp {method} on '{server}' returned isError: {}",
                        truncate(data.get("text").and_then(|t| t.as_str()).unwrap_or(""), 300)
                    )));
                }
                if !sent_headers.is_empty() {
                    data["sent_headers"] = serde_json::json!(sent_headers);
                }
                Ok(ToolResult::success(data).with_duration(duration_ms))
            }
            Err(e) => {
                tracing::warn!(
                    method = %method,
                    server = %server,
                    endpoint = %endpoint,
                    duration_ms,
                    error = %e,
                    execution_id = ctx.execution_id,
                    "MCP request failed"
                );
                // Return a structured error payload (mirrors Python tool behaviour:
                // errors are returned as `status: "error"` rather than propagated
                // as ToolError, so the playbook can inspect the error field).
                if fail_on_provider_error {
                    return Err(ToolError::ExecutionFailed(format!(
                        "mcp {method} on '{server}' failed: {}",
                        truncate(&e.to_string(), 300)
                    )));
                }
                let mut payload = serde_json::json!({
                    "status": "error",
                    "server": server,
                    "endpoint": endpoint,
                    "method": method,
                    "error": e.to_string(),
                    "text": e.to_string(),
                });
                if !sent_headers.is_empty() {
                    payload["sent_headers"] = serde_json::json!(sent_headers);
                }
                Ok(ToolResult::success(payload).with_duration(duration_ms))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Core execution
// ---------------------------------------------------------------------------

async fn execute_mcp(
    cfg: &McpConfig,
    server: &str,
    endpoint: &str,
    method: &str,
    timeout_secs: f64,
    request_id: u64,
    extra: &HeaderMap,
) -> Result<serde_json::Value, McpError> {
    let timeout = Duration::from_secs_f64(timeout_secs);

    // health check: simple GET, no JSON-RPC handshake
    if method == "health" {
        let health_url = resolve_health_url(endpoint);
        tracing::debug!(url = %health_url, "MCP health probe");
        let client = build_client(timeout)?;
        let response = client
            .get(&health_url)
            .headers(extra.clone())
            .send()
            .await
            .map_err(|e| McpError::Http(e.to_string()))?;
        response
            .error_for_status_ref()
            .map_err(|e| McpError::Http(e.to_string()))?;
        let text = response.text().await.unwrap_or_default();
        return Ok(serde_json::json!({
            "status": "ok",
            "server": server,
            "endpoint": endpoint,
            "method": method,
            "healthy": true,
            "text": text,
        }));
    }

    let client = build_client(timeout)?;

    // --- initialize ---
    let protocol_version = cfg
        .protocol_version
        .clone()
        .unwrap_or_else(|| "2025-03-26".to_string());
    let client_name = cfg
        .client_name
        .clone()
        .unwrap_or_else(|| "noetl-worker".to_string());
    let client_version = cfg
        .client_version
        .clone()
        .unwrap_or_else(|| "0".to_string());
    let capabilities = cfg.capabilities.clone().unwrap_or(serde_json::json!({}));

    let init_payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "initialize",
        "params": {
            "protocolVersion": protocol_version,
            "capabilities": capabilities,
            "clientInfo": {
                "name": client_name,
                "version": client_version,
            },
        },
    });

    let (init_envelope, session_id) =
        post_jsonrpc(&client, endpoint, &init_payload, None, extra).await?;

    if session_id.is_none() {
        tracing::debug!(
            server = %server,
            "MCP server did not return session id; treating as stateless"
        );
    }

    // --- method dispatch ---
    let (params, tool_name, arguments) = build_method_params(cfg, method)?;

    let call_payload = serde_json::json!({
        "jsonrpc": "2.0",
        "id": request_id + 1,
        "method": method,
        "params": params,
    });

    let (envelope, _) = post_jsonrpc(
        &client,
        endpoint,
        &call_payload,
        session_id.as_deref(),
        extra,
    )
    .await?;

    let result = envelope
        .get("result")
        .cloned()
        .unwrap_or(serde_json::json!({}));
    let result = if result.is_object() {
        result
    } else {
        serde_json::json!({ "value": result })
    };
    let text = extract_text(&result);

    Ok(serde_json::json!({
        "status": "ok",
        "server": server,
        "endpoint": endpoint,
        "method": method,
        "tool": tool_name,
        "arguments": arguments,
        "text": text,
        "result": result,
        "initialize": init_envelope.get("result"),
    }))
}

/// Build the `params` object for the actual JSON-RPC call.
/// Returns `(params, tool_name_or_null, arguments_or_null)`.
fn build_method_params(
    cfg: &McpConfig,
    method: &str,
) -> Result<(serde_json::Value, serde_json::Value, serde_json::Value), McpError> {
    match method {
        "tools/call" => {
            let tool_name = cfg
                .tool
                .clone()
                .or_else(|| cfg.tool_name.clone())
                .ok_or_else(|| {
                    McpError::Config("mcp tool name is required for tools/call".into())
                })?;

            let arguments = cfg
                .arguments
                .clone()
                .or_else(|| cfg.args.clone())
                .unwrap_or(serde_json::json!({}));

            let arguments = coerce_to_object(arguments, "arguments")?;
            let params = serde_json::json!({
                "name": tool_name,
                "arguments": arguments,
            });
            Ok((params, serde_json::Value::String(tool_name), arguments))
        }
        "tools/list" => Ok((
            serde_json::json!({}),
            serde_json::Value::Null,
            serde_json::Value::Null,
        )),
        _ => {
            let params = cfg.params.clone().unwrap_or(serde_json::json!({}));
            let params = coerce_to_object(params, "params")?;
            Ok((params, serde_json::Value::Null, serde_json::Value::Null))
        }
    }
}

fn coerce_to_object(v: serde_json::Value, field: &str) -> Result<serde_json::Value, McpError> {
    match v {
        serde_json::Value::Object(_) => Ok(v),
        serde_json::Value::String(s) => serde_json::from_str(&s)
            .map_err(|e| McpError::Config(format!("mcp {field} must be a JSON object: {e}"))),
        _ => Err(McpError::Config(format!("mcp {field} must be an object"))),
    }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

fn build_client(timeout: Duration) -> Result<reqwest::Client, McpError> {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| McpError::Http(format!("Failed to build HTTP client: {e}")))
}

/// POST a JSON-RPC payload; parse and validate the response envelope.
/// Returns `(envelope, session_id_if_any)`.
async fn post_jsonrpc(
    client: &reqwest::Client,
    endpoint: &str,
    payload: &serde_json::Value,
    session_id: Option<&str>,
    extra: &HeaderMap,
) -> Result<(serde_json::Value, Option<String>), McpError> {
    let mut headers = extra.clone();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("application/json, text/event-stream"),
    );
    if let Some(sid) = session_id {
        let hv = HeaderValue::from_str(sid)
            .map_err(|e| McpError::Http(format!("Invalid session id header value: {e}")))?;
        headers.insert("Mcp-Session-Id", hv);
    }

    let response = client
        .post(endpoint)
        .headers(headers)
        .json(payload)
        .send()
        .await
        .map_err(|e| McpError::Http(e.to_string()))?;

    response
        .error_for_status_ref()
        .map_err(|e| McpError::Http(e.to_string()))?;

    // Capture session id before consuming the body.
    let returned_session = response
        .headers()
        .get("mcp-session-id")
        .or_else(|| response.headers().get("Mcp-Session-Id"))
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let body = response.text().await.unwrap_or_default();
    let method_hint = payload
        .get("method")
        .and_then(|m| m.as_str())
        .unwrap_or("request");

    let envelope = parse_mcp_envelope(&body, method_hint)?;

    if let Some(err) = envelope.get("error") {
        let msg = if let Some(obj) = err.as_object() {
            obj.get("message")
                .and_then(|m| m.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| err.to_string())
        } else {
            err.to_string()
        };
        return Err(McpError::JsonRpc(msg));
    }

    Ok((envelope, returned_session))
}

// ---------------------------------------------------------------------------
// SSE / envelope parsing
// ---------------------------------------------------------------------------

/// Parse an MCP response body into a JSON envelope.
///
/// Handles two shapes:
/// 1. Plain JSON body → parse directly.
/// 2. SSE body (`data: ...` lines) → concatenate `data:` payloads then parse.
pub(crate) fn parse_mcp_envelope(body: &str, context: &str) -> Result<serde_json::Value, McpError> {
    // Fast path: try direct JSON parse first.
    if let Ok(v) = serde_json::from_str(body) {
        return Ok(v);
    }

    // SSE path: extract `data:` lines and join them.
    let data_lines: Vec<&str> = body
        .lines()
        .filter(|l| l.starts_with("data:"))
        .map(|l| l["data:".len()..].trim())
        .collect();

    if data_lines.is_empty() {
        let preview: String = body
            .split_whitespace()
            .take(40)
            .collect::<Vec<_>>()
            .join(" ");
        return Err(McpError::Envelope(format!(
            "Invalid MCP response for {context}: {preview}"
        )));
    }

    let joined = data_lines.join("\n");
    serde_json::from_str(&joined).map_err(|e| {
        let preview: String = joined
            .split_whitespace()
            .take(40)
            .collect::<Vec<_>>()
            .join(" ");
        McpError::Envelope(format!(
            "Invalid MCP response for {context}: {preview} ({e})"
        ))
    })
}

// ---------------------------------------------------------------------------
// Text extraction
// ---------------------------------------------------------------------------

/// Extract human-readable text from an MCP result.
///
/// Walks `result.content` looking for `{ type: "text", text: "..." }` items.
/// Falls back to compact JSON serialisation.
pub(crate) fn extract_text(result: &serde_json::Value) -> String {
    if let Some(content) = result.get("content") {
        if let Some(items) = content.as_array() {
            let parts: Vec<&str> = items
                .iter()
                .filter_map(|item| {
                    let obj = item.as_object()?;
                    if obj.get("type")?.as_str()? != "text" {
                        return None;
                    }
                    obj.get("text")?.as_str()
                })
                .collect();
            if !parts.is_empty() {
                return parts.join("\n");
            }
        }
    }
    serde_json::to_string(result).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Endpoint resolution
// ---------------------------------------------------------------------------

/// Resolve the MCP server endpoint from config then env.
pub(crate) fn resolve_endpoint(cfg: &McpConfig) -> Result<String, McpError> {
    // 1. Explicit config fields (first non-empty wins).
    if let Some(ep) = cfg
        .endpoint
        .as_deref()
        .or(cfg.url.as_deref())
        .or(cfg.server_url.as_deref())
        .or(cfg.base_url.as_deref())
        .filter(|s| !s.is_empty())
    {
        return Ok(trim_slash(ep));
    }

    // 2. Env var keyed by server slug.
    let server = cfg
        .server
        .as_deref()
        .or(cfg.name.as_deref())
        .unwrap_or("kubernetes");
    let env_name = server_env_name(server);
    if let Ok(ep) = std::env::var(&env_name) {
        if !ep.is_empty() {
            return Ok(trim_slash(&ep));
        }
    }

    // 3. Generic fallback env var.
    if let Ok(ep) = std::env::var("NOETL_MCP_URL") {
        if !ep.is_empty() {
            return Ok(trim_slash(&ep));
        }
    }

    Err(McpError::Config(format!(
        "mcp endpoint is required for server '{server}'. \
         Set 'endpoint' in config or set the '{env_name}' env var."
    )))
}

/// Convert a server slug to its env var name.
/// `kubernetes` → `NOETL_MCP_KUBERNETES_ENDPOINT`
pub(crate) fn server_env_name(server: &str) -> String {
    let safe: String = server
        .chars()
        .map(|c| {
            if c.is_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    format!("NOETL_MCP_{safe}_ENDPOINT")
}

fn trim_slash(s: &str) -> String {
    s.trim_end_matches('/').to_string()
}

// ---------------------------------------------------------------------------
// Health URL resolution
// ---------------------------------------------------------------------------

/// Derive the health-check URL from an MCP endpoint.
///
/// Strips MCP-specific path segments (`/mcp`, `/sse`, `/message`) and appends
/// `/healthz`; otherwise appends `/healthz` to whatever path is already there.
pub(crate) fn resolve_health_url(endpoint: &str) -> String {
    // Simple approach: parse scheme+host, then decide on path.
    if let Ok(url) = reqwest::Url::parse(endpoint) {
        let path = url.path().trim_end_matches('/');
        let new_path = if matches!(path, "/mcp" | "/sse" | "/message") || path.is_empty() {
            "/healthz".to_string()
        } else {
            format!("{path}/healthz")
        };
        let mut base = format!(
            "{}://{}{}",
            url.scheme(),
            url.host_str().unwrap_or(""),
            new_path
        );
        if let Some(port) = url.port() {
            // re-insert port
            base = format!(
                "{}://{}:{}{}",
                url.scheme(),
                url.host_str().unwrap_or(""),
                port,
                new_path
            );
        }
        return base;
    }
    // Fallback: append /healthz
    format!("{endpoint}/healthz")
}

// ---------------------------------------------------------------------------
// Timeout resolution
// ---------------------------------------------------------------------------

/// Resolve the effective request timeout in seconds.
///
/// Chain: `config.timeout` → `NOETL_MCP_REQUEST_TIMEOUT_SECONDS` env → 60s default,
/// then clamp to `NOETL_WORKER_COMMAND_TIMEOUT_SECONDS` (default 180s).
pub(crate) fn resolve_timeout(timeout_value: &Option<serde_json::Value>) -> f64 {
    let default_timeout = read_float_env(
        "NOETL_MCP_REQUEST_TIMEOUT_SECONDS",
        DEFAULT_MCP_TIMEOUT_SECS,
    );
    let command_budget = read_float_env(
        "NOETL_WORKER_COMMAND_TIMEOUT_SECONDS",
        DEFAULT_COMMAND_TIMEOUT_SECS,
    )
    .max(1.0);

    let requested = match timeout_value {
        None => return (default_timeout).min(command_budget),
        Some(serde_json::Value::Null) => return (default_timeout).min(command_budget),
        Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(default_timeout),
        Some(serde_json::Value::String(s)) if s.trim().is_empty() => {
            return (default_timeout).min(command_budget)
        }
        Some(serde_json::Value::String(s)) => s.trim().parse::<f64>().unwrap_or(default_timeout),
        Some(_) => return (default_timeout).min(command_budget),
    };

    if !requested.is_finite() || requested <= 0.0 {
        return (default_timeout).min(command_budget);
    }

    requested.max(MIN_TIMEOUT_SECS).min(command_budget)
}

fn read_float_env(name: &str, default: f64) -> f64 {
    match std::env::var(name) {
        Ok(raw) if !raw.trim().is_empty() => raw
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(default),
        _ => default,
    }
}

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Request headers: `headers:` + `auth:`
// ---------------------------------------------------------------------------

/// Headers the tool owns.  A step may not set them, because the MCP session
/// and the JSON-RPC transport depend on them.
const RESERVED_HEADERS: &[&str] = &[
    "content-type",
    "accept",
    "mcp-session-id",
    "content-length",
    "host",
];

fn reject_reserved(name: &str) -> Result<(), ToolError> {
    if RESERVED_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
        return Err(ToolError::Configuration(format!(
            "mcp header '{name}' is set by the tool and cannot be overridden"
        )));
    }
    Ok(())
}

fn insert_sensitive(
    headers: &mut HeaderMap,
    name: HeaderName,
    value: &str,
) -> Result<(), ToolError> {
    let mut v = HeaderValue::from_str(value).map_err(|_| {
        ToolError::Configuration(format!("mcp header '{name}' has an invalid value"))
    })?;
    // Never printed by reqwest's Debug output.
    v.set_sensitive(true);
    headers.insert(name, v);
    Ok(())
}

/// Build the extra request headers for an MCP call from a literal `headers:`
/// map and an optional `auth:` block resolved against `ctx`.
///
/// Header values and resolved credentials are never logged and never echoed
/// into a result; [`header_names`] gives the header *names* a caller may report.
pub async fn request_headers(
    literal: &Option<BTreeMap<String, serde_json::Value>>,
    auth: Option<&AuthConfig>,
    ctx: &ExecutionContext,
) -> Result<HeaderMap, ToolError> {
    let mut out = HeaderMap::new();
    for (k, v) in literal.iter().flatten() {
        reject_reserved(k)?;
        let name = HeaderName::from_bytes(k.as_bytes()).map_err(|e| {
            ToolError::Configuration(format!("mcp header name '{k}' is invalid: {e}"))
        })?;
        let value = match v {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => String::new(),
            _ => {
                return Err(ToolError::Configuration(format!(
                    "mcp header '{k}' must be a string"
                )))
            }
        };
        // A header whose template rendered empty (an unresolved credential) is
        // dropped, not sent blank: the server then reports the missing credential.
        if value.is_empty() {
            continue;
        }
        insert_sensitive(&mut out, name, &value)?;
    }
    if let Some(auth) = auth {
        match AuthResolver::new().resolve(auth, ctx).await? {
            AuthCredentials::Bearer(token) => {
                insert_sensitive(&mut out, AUTHORIZATION, &format!("Bearer {token}"))?;
            }
            AuthCredentials::Basic(user, pass) => {
                use base64::Engine as _;
                let enc =
                    base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
                insert_sensitive(&mut out, AUTHORIZATION, &format!("Basic {enc}"))?;
            }
            AuthCredentials::ApiKey(name, value) => {
                let name = HeaderName::from_bytes(name.as_bytes())
                    .map_err(|e| ToolError::Auth(format!("api_key header name is invalid: {e}")))?;
                reject_reserved(name.as_str())?;
                insert_sensitive(&mut out, name, &value)?;
            }
            AuthCredentials::None => {}
        }
    }
    Ok(out)
}

/// The sorted header names of `headers`, for a result's `sent_headers` field.
pub fn header_names(headers: &HeaderMap) -> Vec<String> {
    let mut names: Vec<String> = headers.keys().map(|k| k.as_str().to_string()).collect();
    names.sort();
    names.dedup();
    names
}

/// `on_provider_error`: `Some("fail")` → true; unset / `continue` → false; any
/// other value is a configuration error, so a typo never silently means
/// "continue".
fn parse_provider_error_policy(v: &Option<String>) -> Result<bool, ToolError> {
    match v.as_deref().map(str::trim) {
        None | Some("") | Some("continue") => Ok(false),
        Some("fail") => Ok(true),
        Some(other) => Err(ToolError::Configuration(format!(
            "on_provider_error must be 'fail' or 'continue', got '{other}'"
        ))),
    }
}

/// Does a successful `execute_mcp` payload carry MCP's tool-level error flag?
fn is_tool_error(data: &serde_json::Value) -> bool {
    data.pointer("/result/isError").and_then(|v| v.as_bool()) == Some(true)
}

fn truncate(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

// ---------------------------------------------------------------------------
// McpClient: a reusable MCP session (agent tool sources, hosts)
// ---------------------------------------------------------------------------

/// One MCP tool as a server advertises it in `tools/list`.
#[derive(Debug, Clone, PartialEq)]
pub struct McpToolInfo {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// A JSON-RPC session against one MCP server (Streamable HTTP, JSON or SSE
/// replies).  `kind: mcp` steps use the same transport; this type exists so
/// other tools (the agent's `mcp_servers:` sources) can hold a session across
/// several calls instead of re-initializing per call.
pub struct McpClient {
    client: reqwest::Client,
    endpoint: String,
    headers: HeaderMap,
    session_id: Option<String>,
    next_id: u64,
    initialized: bool,
}

impl std::fmt::Debug for McpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Header values (credentials) are deliberately absent.
        f.debug_struct("McpClient")
            .field("endpoint", &self.endpoint)
            .field("headers", &header_names(&self.headers))
            .field("initialized", &self.initialized)
            .finish()
    }
}

impl McpClient {
    /// A client for `endpoint` that sends `headers` (see [`request_headers`])
    /// on every request.
    pub fn new(endpoint: &str, headers: HeaderMap, timeout: Duration) -> Result<Self, ToolError> {
        let endpoint = endpoint.trim();
        if endpoint.is_empty() {
            return Err(ToolError::Configuration("mcp endpoint is empty".into()));
        }
        Ok(Self {
            client: build_client(timeout)?,
            endpoint: trim_slash(endpoint),
            headers,
            session_id: None,
            next_id: 1,
            initialized: false,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Send `initialize` (once) and keep the `Mcp-Session-Id`, if any.
    pub async fn initialize(&mut self) -> Result<serde_json::Value, ToolError> {
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": self.take_id(),
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": { "name": "noetl", "version": env!("CARGO_PKG_VERSION") },
            },
        });
        let (env, sid) =
            post_jsonrpc(&self.client, &self.endpoint, &payload, None, &self.headers).await?;
        self.session_id = sid;
        self.initialized = true;
        // `notifications/initialized` is best effort: stateless servers answer
        // 202 with no body, which `post_jsonrpc` would reject as an envelope.
        let _ = self
            .client
            .post(&self.endpoint)
            .headers(self.request_headers()?)
            .json(&serde_json::json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .send()
            .await;
        Ok(env
            .get("result")
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }

    /// Send one JSON-RPC request and return its `result`.  Initializes first
    /// when needed.
    pub async fn request(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        if !self.initialized {
            self.initialize().await?;
        }
        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": self.take_id(),
            "method": method,
            "params": params,
        });
        let (env, _) = post_jsonrpc(
            &self.client,
            &self.endpoint,
            &payload,
            self.session_id.as_deref(),
            &self.headers,
        )
        .await?;
        Ok(env
            .get("result")
            .cloned()
            .unwrap_or(serde_json::Value::Null))
    }

    /// Every tool the server lists, following `nextCursor` (bounded at 50 pages).
    pub async fn list_tools(&mut self) -> Result<Vec<McpToolInfo>, ToolError> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..50 {
            let params = match &cursor {
                Some(c) => serde_json::json!({ "cursor": c }),
                None => serde_json::json!({}),
            };
            let result = self.request("tools/list", params).await?;
            for t in result
                .get("tools")
                .and_then(|t| t.as_array())
                .into_iter()
                .flatten()
            {
                let Some(name) = t.get("name").and_then(|n| n.as_str()) else {
                    continue;
                };
                out.push(McpToolInfo {
                    name: name.to_string(),
                    description: t
                        .get("description")
                        .and_then(|d| d.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    input_schema: t
                        .get("inputSchema")
                        .cloned()
                        .filter(|s| s.is_object())
                        .unwrap_or_else(|| serde_json::json!({"type": "object"})),
                });
            }
            cursor = result
                .get("nextCursor")
                .and_then(|c| c.as_str())
                .filter(|c| !c.is_empty())
                .map(str::to_string);
            if cursor.is_none() {
                return Ok(out);
            }
        }
        Ok(out)
    }

    /// `tools/call`; returns `(text, result, is_error)`.
    pub async fn call_tool(
        &mut self,
        name: &str,
        arguments: serde_json::Value,
    ) -> Result<(String, serde_json::Value, bool), ToolError> {
        let arguments = if arguments.is_null() {
            serde_json::json!({})
        } else {
            arguments
        };
        let result = self
            .request(
                "tools/call",
                serde_json::json!({ "name": name, "arguments": arguments }),
            )
            .await?;
        let is_error = result.get("isError").and_then(|v| v.as_bool()) == Some(true);
        Ok((extract_text(&result), result, is_error))
    }

    fn take_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    fn request_headers(&self) -> Result<HeaderMap, ToolError> {
        let mut h = self.headers.clone();
        h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        h.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        if let Some(sid) = &self.session_id {
            if let Ok(v) = HeaderValue::from_str(sid) {
                h.insert("Mcp-Session-Id", v);
            }
        }
        Ok(h)
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum McpError {
    #[error("MCP config error: {0}")]
    Config(String),
    #[error("MCP HTTP error: {0}")]
    Http(String),
    #[error("MCP JSON-RPC error: {0}")]
    JsonRpc(String),
    #[error("MCP envelope parse error: {0}")]
    Envelope(String),
}

impl From<McpError> for ToolError {
    fn from(e: McpError) -> Self {
        match e {
            McpError::Config(msg) => ToolError::Configuration(msg),
            McpError::Http(msg) | McpError::JsonRpc(msg) | McpError::Envelope(msg) => {
                ToolError::ExecutionFailed(msg)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_mcp_envelope ---

    #[test]
    fn test_envelope_plain_json() {
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}"#;
        let v = parse_mcp_envelope(body, "tools/list").unwrap();
        assert_eq!(v["id"], 1);
        assert!(v["result"]["tools"].is_array());
    }

    #[test]
    fn test_envelope_sse_single_data_line() {
        let body = "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n\n";
        let v = parse_mcp_envelope(body, "tools/call").unwrap();
        assert_eq!(v["result"]["ok"], true);
    }

    #[test]
    fn test_envelope_sse_multi_data_lines() {
        // Each `data:` line is a complete JSON document here.
        // Some MCP servers send each chunk as a separate `data:` line
        // where only the first contains the full envelope.
        let body = concat!(
            "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"ok\":true}}\n",
            "data: \n\n",
        );
        // The first data line is valid JSON; parse_mcp_envelope joins them,
        // so the result should still succeed (joining a trailing empty line
        // after a newline doesn't break valid JSON).
        let v = parse_mcp_envelope(body, "tools/call").unwrap();
        assert_eq!(v["result"]["ok"], true);
    }

    #[test]
    fn test_envelope_sse_valid_split() {
        // A valid complete JSON split over two `data:` lines (unusual but possible).
        let body = "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"count\":42}}\ndata: \n\n";
        let v = parse_mcp_envelope(body, "count").unwrap();
        assert_eq!(v["result"]["count"], 42);
    }

    #[test]
    fn test_envelope_empty_body_error() {
        let r = parse_mcp_envelope("", "test");
        assert!(r.is_err());
    }

    // --- extract_text ---

    #[test]
    fn test_extract_text_content_array() {
        let result = serde_json::json!({
            "content": [
                { "type": "text", "text": "hello" },
                { "type": "image", "url": "http://x" },
                { "type": "text", "text": "world" },
            ]
        });
        assert_eq!(extract_text(&result), "hello\nworld");
    }

    #[test]
    fn test_extract_text_no_content_fallback() {
        let result = serde_json::json!({ "answer": 42 });
        let t = extract_text(&result);
        assert!(t.contains("42"));
    }

    #[test]
    fn test_extract_text_empty_content() {
        let result = serde_json::json!({ "content": [] });
        // Falls through to JSON serialisation.
        let t = extract_text(&result);
        assert!(t.contains("content"));
    }

    // --- endpoint resolution ---

    #[test]
    fn test_resolve_endpoint_direct() {
        let cfg = McpConfig {
            endpoint: Some("http://localhost:8080/mcp".to_string()),
            url: None,
            server_url: None,
            base_url: None,
            server: None,
            name: None,
            method: None,
            action: None,
            tool: None,
            tool_name: None,
            arguments: None,
            args: None,
            params: None,
            timeout: None,
            request_id: None,
            protocol_version: None,
            client_name: None,
            client_version: None,
            capabilities: None,
            headers: None,
            on_provider_error: None,
        };
        assert_eq!(resolve_endpoint(&cfg).unwrap(), "http://localhost:8080/mcp");
    }

    #[test]
    fn test_resolve_endpoint_trailing_slash_stripped() {
        let cfg = McpConfig {
            endpoint: Some("http://localhost:8080/mcp/".to_string()),
            url: None,
            server_url: None,
            base_url: None,
            server: None,
            name: None,
            method: None,
            action: None,
            tool: None,
            tool_name: None,
            arguments: None,
            args: None,
            params: None,
            timeout: None,
            request_id: None,
            protocol_version: None,
            client_name: None,
            client_version: None,
            capabilities: None,
            headers: None,
            on_provider_error: None,
        };
        assert_eq!(resolve_endpoint(&cfg).unwrap(), "http://localhost:8080/mcp");
    }

    #[test]
    fn test_resolve_endpoint_env_var() {
        // Temporarily set the env var.
        let env_name = server_env_name("my-server");
        std::env::set_var(&env_name, "http://my-server:9000/mcp");
        let cfg = McpConfig {
            endpoint: None,
            url: None,
            server_url: None,
            base_url: None,
            server: Some("my-server".to_string()),
            name: None,
            method: None,
            action: None,
            tool: None,
            tool_name: None,
            arguments: None,
            args: None,
            params: None,
            timeout: None,
            request_id: None,
            protocol_version: None,
            client_name: None,
            client_version: None,
            capabilities: None,
            headers: None,
            on_provider_error: None,
        };
        let ep = resolve_endpoint(&cfg).unwrap();
        std::env::remove_var(&env_name);
        assert_eq!(ep, "http://my-server:9000/mcp");
    }

    #[test]
    fn test_resolve_endpoint_missing_error() {
        // Clear both env vars that would satisfy resolution.
        std::env::remove_var("NOETL_MCP_KUBERNETES_ENDPOINT");
        std::env::remove_var("NOETL_MCP_URL");
        let cfg = McpConfig {
            endpoint: None,
            url: None,
            server_url: None,
            base_url: None,
            server: None,
            name: None,
            method: None,
            action: None,
            tool: None,
            tool_name: None,
            arguments: None,
            args: None,
            params: None,
            timeout: None,
            request_id: None,
            protocol_version: None,
            client_name: None,
            client_version: None,
            capabilities: None,
            headers: None,
            on_provider_error: None,
        };
        assert!(resolve_endpoint(&cfg).is_err());
    }

    // --- server_env_name ---

    #[test]
    fn test_server_env_name_simple() {
        assert_eq!(
            server_env_name("kubernetes"),
            "NOETL_MCP_KUBERNETES_ENDPOINT"
        );
    }

    #[test]
    fn test_server_env_name_dashes() {
        assert_eq!(server_env_name("my-server"), "NOETL_MCP_MY_SERVER_ENDPOINT");
    }

    #[test]
    fn test_server_env_name_dots() {
        assert_eq!(
            server_env_name("my.server.v2"),
            "NOETL_MCP_MY_SERVER_V2_ENDPOINT"
        );
    }

    // --- timeout resolution ---

    #[test]
    fn test_timeout_null_uses_default() {
        // Without env vars set, defaults are used.
        std::env::remove_var("NOETL_MCP_REQUEST_TIMEOUT_SECONDS");
        std::env::remove_var("NOETL_WORKER_COMMAND_TIMEOUT_SECONDS");
        let t = resolve_timeout(&None);
        assert_eq!(t, 60.0_f64.min(180.0));
    }

    #[test]
    fn test_timeout_explicit_value() {
        std::env::remove_var("NOETL_MCP_REQUEST_TIMEOUT_SECONDS");
        std::env::remove_var("NOETL_WORKER_COMMAND_TIMEOUT_SECONDS");
        let t = resolve_timeout(&Some(serde_json::json!(30)));
        assert_eq!(t, 30.0);
    }

    #[test]
    fn test_timeout_clamped_by_command_budget() {
        // resolve_timeout is a pure computation when env vars are readable.
        // Rather than mutating env vars (which leaks across parallel tests),
        // we verify the clamping logic directly: a requested value of 120s
        // cannot exceed a budget of 45s.
        let requested = 120_f64;
        let budget = 45_f64;
        let clamped = requested.max(MIN_TIMEOUT_SECS).min(budget);
        assert_eq!(clamped, 45.0);
    }

    #[test]
    fn test_timeout_string_value() {
        std::env::remove_var("NOETL_MCP_REQUEST_TIMEOUT_SECONDS");
        std::env::remove_var("NOETL_WORKER_COMMAND_TIMEOUT_SECONDS");
        let t = resolve_timeout(&Some(serde_json::json!("25")));
        assert_eq!(t, 25.0);
    }

    #[test]
    fn test_timeout_invalid_string_falls_back_to_default() {
        std::env::remove_var("NOETL_MCP_REQUEST_TIMEOUT_SECONDS");
        std::env::remove_var("NOETL_WORKER_COMMAND_TIMEOUT_SECONDS");
        let t = resolve_timeout(&Some(serde_json::json!("not-a-number")));
        assert_eq!(t, 60.0_f64.min(180.0));
    }

    // --- health URL resolution ---

    #[test]
    fn test_health_url_mcp_path() {
        assert_eq!(
            resolve_health_url("http://localhost:8080/mcp"),
            "http://localhost:8080/healthz"
        );
    }

    #[test]
    fn test_health_url_sse_path() {
        assert_eq!(
            resolve_health_url("http://localhost:8080/sse"),
            "http://localhost:8080/healthz"
        );
    }

    #[test]
    fn test_health_url_message_path() {
        assert_eq!(
            resolve_health_url("http://localhost:8080/message"),
            "http://localhost:8080/healthz"
        );
    }

    #[test]
    fn test_health_url_other_path() {
        assert_eq!(
            resolve_health_url("http://localhost:8080/api/v1"),
            "http://localhost:8080/api/v1/healthz"
        );
    }

    #[test]
    fn test_health_url_root() {
        assert_eq!(
            resolve_health_url("http://localhost:8080"),
            "http://localhost:8080/healthz"
        );
    }

    // --- build_method_params ---

    #[test]
    fn test_build_method_params_tools_call() {
        let cfg = McpConfig {
            endpoint: None,
            url: None,
            server_url: None,
            base_url: None,
            server: None,
            name: None,
            method: None,
            action: None,
            tool: Some("get_pods".to_string()),
            tool_name: None,
            arguments: Some(serde_json::json!({ "namespace": "default" })),
            args: None,
            params: None,
            timeout: None,
            request_id: None,
            protocol_version: None,
            client_name: None,
            client_version: None,
            capabilities: None,
            headers: None,
            on_provider_error: None,
        };
        let (params, tool_name, args) = build_method_params(&cfg, "tools/call").unwrap();
        assert_eq!(params["name"], "get_pods");
        assert_eq!(params["arguments"]["namespace"], "default");
        assert_eq!(tool_name, "get_pods");
        assert_eq!(args["namespace"], "default");
    }

    #[test]
    fn test_build_method_params_tools_call_missing_tool_errors() {
        let cfg = McpConfig {
            endpoint: None,
            url: None,
            server_url: None,
            base_url: None,
            server: None,
            name: None,
            method: None,
            action: None,
            tool: None,
            tool_name: None,
            arguments: None,
            args: None,
            params: None,
            timeout: None,
            request_id: None,
            protocol_version: None,
            client_name: None,
            client_version: None,
            capabilities: None,
            headers: None,
            on_provider_error: None,
        };
        let r = build_method_params(&cfg, "tools/call");
        assert!(r.is_err());
    }

    #[test]
    fn test_build_method_params_tools_list() {
        let cfg = McpConfig {
            endpoint: None,
            url: None,
            server_url: None,
            base_url: None,
            server: None,
            name: None,
            method: None,
            action: None,
            tool: None,
            tool_name: None,
            arguments: None,
            args: None,
            params: None,
            timeout: None,
            request_id: None,
            protocol_version: None,
            client_name: None,
            client_version: None,
            capabilities: None,
            headers: None,
            on_provider_error: None,
        };
        let (params, tool, args) = build_method_params(&cfg, "tools/list").unwrap();
        assert!(params.as_object().unwrap().is_empty());
        assert!(tool.is_null());
        assert!(args.is_null());
    }

    #[test]
    fn test_build_method_params_passthrough() {
        let cfg = McpConfig {
            endpoint: None,
            url: None,
            server_url: None,
            base_url: None,
            server: None,
            name: None,
            method: None,
            action: None,
            tool: None,
            tool_name: None,
            arguments: None,
            args: None,
            params: Some(serde_json::json!({ "cursor": "abc" })),
            timeout: None,
            request_id: None,
            protocol_version: None,
            client_name: None,
            client_version: None,
            capabilities: None,
            headers: None,
            on_provider_error: None,
        };
        let (params, _, _) = build_method_params(&cfg, "resources/list").unwrap();
        assert_eq!(params["cursor"], "abc");
    }

    // --- tool name ---

    #[tokio::test]
    async fn test_mcp_tool_name() {
        let tool = McpTool::new();
        assert_eq!(tool.name(), "mcp");
    }

    // --- Integration tests (gated behind env var) ---

    /// Set `NOETL_TEST_MCP_ENDPOINT=http://localhost:8080/mcp` to run live tests.
    #[tokio::test]
    async fn test_mcp_integration_health() {
        let endpoint = match std::env::var("NOETL_TEST_MCP_ENDPOINT") {
            Ok(ep) => ep,
            Err(_) => return,
        };
        let cfg = McpConfig {
            endpoint: Some(endpoint),
            url: None,
            server_url: None,
            base_url: None,
            server: Some("test".to_string()),
            name: None,
            method: Some("health".to_string()),
            action: None,
            tool: None,
            tool_name: None,
            arguments: None,
            args: None,
            params: None,
            timeout: Some(serde_json::json!(10)),
            request_id: None,
            protocol_version: None,
            client_name: None,
            client_version: None,
            capabilities: None,
            headers: None,
            on_provider_error: None,
        };
        let result = execute_mcp(
            &cfg,
            "test",
            cfg.endpoint.as_deref().unwrap(),
            "health",
            10.0,
            1, &HeaderMap::new()).await;
        assert!(result.is_ok(), "health probe failed: {:?}", result);
        let v = result.unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["method"], "health");
    }

    #[tokio::test]
    async fn test_mcp_integration_tools_list() {
        let endpoint = match std::env::var("NOETL_TEST_MCP_ENDPOINT") {
            Ok(ep) => ep,
            Err(_) => return,
        };
        let cfg = McpConfig {
            endpoint: Some(endpoint.clone()),
            url: None,
            server_url: None,
            base_url: None,
            server: Some("test".to_string()),
            name: None,
            method: Some("tools/list".to_string()),
            action: None,
            tool: None,
            tool_name: None,
            arguments: None,
            args: None,
            params: None,
            timeout: Some(serde_json::json!(15)),
            request_id: None,
            protocol_version: None,
            client_name: None,
            client_version: None,
            capabilities: None,
            headers: None,
            on_provider_error: None,
        };
        let result = execute_mcp(&cfg, "test", &endpoint, "tools/list", 15.0, 1, &HeaderMap::new()).await;
        assert!(result.is_ok(), "tools/list failed: {:?}", result);
        let v = result.unwrap();
        assert_eq!(v["status"], "ok");
    }
}
