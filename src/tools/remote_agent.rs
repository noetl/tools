//! Calling an external agent: a third-party (or another NoETL instance's)
//! agent reached over HTTP, used by `kind: agent` in two ways:
//!
//! - `framework: remote` — the whole step is one call to a remote agent; no
//!   model runs here.
//! - `agents:` tool sources on `framework: claude` — each remote agent becomes a
//!   tool the model may call.
//!
//! Two wire protocols:
//!
//! | `protocol` | Send | Result |
//! |---|---|---|
//! | `noetl` (default) | `POST {endpoint}/api/agents/send` `{agent, input}` → `202 {task_id}` | poll `GET {endpoint}/api/agents/tasks/{task_id}` until `completed` / `failed` / `canceled` |
//! | `a2a` | JSON-RPC `message/send` to `endpoint` with one text part | a `Message`, or a `Task` polled with `tasks/get` until terminal |
//!
//! `noetl` is what noetl-server (`NOETL_AGENT_ENDPOINT=true`) and `noetl serve`
//! answer, so a playbook on any runtime can call a playbook-as-agent on any
//! other.  `a2a` is a subset of the A2A JSON-RPC binding: text in, text and
//! data parts out, no streaming, no push notifications.
//!
//! The call blocks until the remote task ends or `timeout` passes.  Callers on
//! a worker should keep remote agents short; a long-running remote task belongs
//! behind a callback (`POST /api/agents/send` with `callback_url`), which is
//! the server's job, not this client's.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::AuthConfig;
use crate::tools::mcp::request_headers;

const DEFAULT_TIMEOUT_SECS: u64 = 120;
const MAX_TIMEOUT_SECS: u64 = 3600;

/// Wire protocol of a remote agent.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RemoteProtocol {
    /// NoETL agent endpoint (`/api/agents/send` + task polling).
    #[default]
    Noetl,
    /// A2A JSON-RPC (`message/send`, `tasks/get`).
    A2a,
}

/// One remote agent: where it is and how to talk to it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteAgentSpec {
    /// Tool name when used as an `agents:` source (`[A-Za-z0-9_-]{1,64}`).
    #[serde(default)]
    pub name: Option<String>,
    /// What the agent does (the model reads it).  For `protocol: noetl` an
    /// empty description is filled from the agent card.
    #[serde(default)]
    pub description: Option<String>,
    /// Base URL for `noetl` (e.g. `https://host`), JSON-RPC URL for `a2a`.
    pub endpoint: String,
    #[serde(default)]
    pub protocol: RemoteProtocol,
    /// `noetl` only: the agent's playbook path (the card's `agent`).
    #[serde(default)]
    pub agent: Option<String>,
    /// Literal headers, rendered like the rest of the config.
    #[serde(default)]
    pub headers: Option<BTreeMap<String, Value>>,
    /// Credential for the remote agent; resolved like any tool `auth:`.
    #[serde(default)]
    pub auth: Option<AuthConfig>,
    /// Seconds to wait for the remote task (default 120, max 3600).
    #[serde(default)]
    pub timeout: Option<u64>,
}

/// A resolved remote agent, ready to call.
#[derive(Clone)]
pub struct RemoteAgent {
    spec: RemoteAgentSpec,
    headers: HeaderMap,
    client: reqwest::Client,
    timeout: Duration,
    poll: Duration,
}

impl std::fmt::Debug for RemoteAgent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteAgent")
            .field("endpoint", &self.spec.endpoint)
            .field("protocol", &self.spec.protocol)
            .field("agent", &self.spec.agent)
            .finish()
    }
}

/// What a remote call produced.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteOutcome {
    /// `completed`, `failed`, `canceled`, `rejected` or `timeout`.
    pub status: String,
    /// The agent's result (`noetl`) or its parts/artifacts (`a2a`).
    pub result: Value,
    /// Text for a model to read.
    pub text: String,
    /// The remote task id, when there is one.
    pub task_id: Option<String>,
}

impl RemoteOutcome {
    pub fn ok(&self) -> bool {
        self.status == "completed"
    }

    /// The step-result shape (`framework: remote`).
    pub fn to_value(&self, spec: &RemoteAgentSpec) -> Value {
        json!({
            "status": self.status,
            "protocol": spec.protocol,
            "endpoint": spec.endpoint,
            "agent": spec.agent,
            "task_id": self.task_id,
            "result": self.result,
            "text": self.text,
        })
    }
}

impl RemoteAgent {
    /// Validate the spec and resolve its headers against `ctx`.
    pub async fn resolve(spec: RemoteAgentSpec, ctx: &ExecutionContext) -> Result<Self, ToolError> {
        let endpoint = spec.endpoint.trim().trim_end_matches('/').to_string();
        if endpoint.is_empty() {
            return Err(ToolError::Configuration(
                "remote agent `endpoint` is required".into(),
            ));
        }
        let parsed = reqwest::Url::parse(&endpoint).map_err(|e| {
            ToolError::Configuration(format!(
                "remote agent endpoint '{endpoint}' is not a URL: {e}"
            ))
        })?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(ToolError::Configuration(format!(
                "remote agent endpoint must be http(s), got '{}'",
                parsed.scheme()
            )));
        }
        if spec.protocol == RemoteProtocol::Noetl
            && spec
                .agent
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err(ToolError::Configuration(
                "remote agent with protocol noetl needs `agent` (the agent's playbook path)".into(),
            ));
        }
        let headers = request_headers(&spec.headers, spec.auth.as_ref(), ctx).await?;
        let timeout = Duration::from_secs(
            spec.timeout
                .unwrap_or(DEFAULT_TIMEOUT_SECS)
                .clamp(1, MAX_TIMEOUT_SECS),
        );
        let client = reqwest::Client::builder()
            // Redirects are not followed: a remote agent cannot bounce the
            // credential header to another host.
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30).min(timeout))
            .build()
            .map_err(|e| ToolError::Http(format!("failed to build HTTP client: {e}")))?;
        Ok(Self {
            spec: RemoteAgentSpec { endpoint, ..spec },
            headers,
            client,
            timeout,
            poll: Duration::from_millis(500),
        })
    }

    /// Test hook: poll faster.
    #[doc(hidden)]
    pub fn with_poll_interval(mut self, poll: Duration) -> Self {
        self.poll = poll;
        self
    }

    pub fn spec(&self) -> &RemoteAgentSpec {
        &self.spec
    }

    /// For `protocol: noetl`, fetch the agent card (description, input schema).
    /// `None` when the card cannot be read; the caller falls back to the spec.
    pub async fn card(&self) -> Option<Value> {
        if self.spec.protocol != RemoteProtocol::Noetl {
            return None;
        }
        let url = format!(
            "{}/api/agents/cards/{}",
            self.spec.endpoint,
            self.spec.agent.as_deref()?.trim_matches('/')
        );
        let resp = self
            .client
            .get(url)
            .headers(self.headers.clone())
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        resp.json::<Value>().await.ok()
    }

    /// Send `input` and wait for the remote task to end.
    ///
    /// `noetl`: `input` must be an object (the agent's input schema decides its
    /// keys).  `a2a`: a string is sent as the text part; anything else is sent
    /// as JSON text.
    pub async fn call(&self, input: Value) -> Result<RemoteOutcome, ToolError> {
        match self.spec.protocol {
            RemoteProtocol::Noetl => self.call_noetl(input).await,
            RemoteProtocol::A2a => self.call_a2a(input).await,
        }
    }

    async fn call_noetl(&self, input: Value) -> Result<RemoteOutcome, ToolError> {
        let input = match input {
            Value::Object(_) => input,
            Value::Null => json!({}),
            other => {
                return Err(ToolError::Configuration(format!(
                    "remote noetl agent input must be an object, got {}",
                    type_name(&other)
                )))
            }
        };
        let agent = self
            .spec
            .agent
            .as_deref()
            .unwrap_or_default()
            .trim_matches('/');
        let resp = self
            .client
            .post(format!("{}/api/agents/send", self.spec.endpoint))
            .headers(self.headers.clone())
            .json(&json!({ "agent": agent, "input": input }))
            .send()
            .await
            .map_err(|e| ToolError::Http(format!("remote agent send failed: {e}")))?;
        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            let msg = body
                .get("error")
                .and_then(Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| body.to_string());
            return Ok(RemoteOutcome {
                status: "rejected".into(),
                text: format!("remote agent refused the task ({status}): {msg}"),
                result: body,
                task_id: None,
            });
        }
        let task_id = body
            .get("task_id")
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| v.to_string())
            })
            .ok_or_else(|| {
                ToolError::ExecutionFailed("remote agent reply has no task_id".into())
            })?;

        let deadline = Instant::now() + self.timeout;
        let mut wait = self.poll;
        loop {
            let view: Value = self
                .client
                .get(format!(
                    "{}/api/agents/tasks/{}",
                    self.spec.endpoint, task_id
                ))
                .headers(self.headers.clone())
                .send()
                .await
                .map_err(|e| ToolError::Http(format!("remote agent poll failed: {e}")))?
                .json()
                .await
                .unwrap_or(Value::Null);
            let state = view.get("status").and_then(Value::as_str).unwrap_or("");
            if matches!(state, "completed" | "failed" | "canceled") {
                let result = if state == "completed" {
                    view.get("result").cloned().unwrap_or(Value::Null)
                } else {
                    view.get("error").cloned().unwrap_or(Value::Null)
                };
                return Ok(RemoteOutcome {
                    status: state.to_string(),
                    text: as_text(&result),
                    result,
                    task_id: Some(task_id),
                });
            }
            if Instant::now() + wait > deadline {
                return Ok(RemoteOutcome {
                    status: "timeout".into(),
                    text: format!(
                        "remote task {task_id} did not finish within {}s",
                        self.timeout.as_secs()
                    ),
                    result: view,
                    task_id: Some(task_id),
                });
            }
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(Duration::from_secs(5));
        }
    }

    async fn call_a2a(&self, input: Value) -> Result<RemoteOutcome, ToolError> {
        let text = match &input {
            Value::String(s) => s.clone(),
            Value::Object(m) if m.len() == 1 && m.get("message").is_some_and(Value::is_string) => {
                m["message"].as_str().unwrap_or_default().to_string()
            }
            other => other.to_string(),
        };
        let message_id = format!("noetl-{}", nonce());
        let mut result = self
            .rpc(
                "message/send",
                json!({
                    "message": {
                        "role": "user",
                        "kind": "message",
                        "messageId": message_id,
                        "parts": [{ "kind": "text", "text": text }],
                    }
                }),
            )
            .await?;
        let deadline = Instant::now() + self.timeout;
        let mut wait = self.poll;
        loop {
            // A bare Message is a final answer.
            if result.get("kind").and_then(Value::as_str) == Some("message")
                || (result.get("parts").is_some() && result.get("status").is_none())
            {
                return Ok(RemoteOutcome {
                    status: "completed".into(),
                    text: a2a_text(&result),
                    result,
                    task_id: None,
                });
            }
            let task_id = result.get("id").and_then(Value::as_str).map(str::to_string);
            let state = result
                .pointer("/status/state")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let terminal = match state.as_str() {
                "completed" => Some("completed"),
                "failed" => Some("failed"),
                "canceled" => Some("canceled"),
                "rejected" => Some("rejected"),
                // The remote agent wants more from us; a non-interactive
                // caller cannot answer, so report it rather than hang.
                "input-required" | "auth-required" => Some("failed"),
                _ => None,
            };
            if let Some(status) = terminal {
                return Ok(RemoteOutcome {
                    status: status.into(),
                    text: a2a_text(&result),
                    result,
                    task_id,
                });
            }
            let Some(id) = task_id else {
                return Err(ToolError::ExecutionFailed(
                    "a2a reply is neither a message nor a task with an id".into(),
                ));
            };
            if Instant::now() + wait > deadline {
                return Ok(RemoteOutcome {
                    status: "timeout".into(),
                    text: format!(
                        "a2a task {id} did not finish within {}s",
                        self.timeout.as_secs()
                    ),
                    result,
                    task_id: Some(id),
                });
            }
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(Duration::from_secs(5));
            result = self.rpc("tasks/get", json!({ "id": id })).await?;
        }
    }

    async fn rpc(&self, method: &str, params: Value) -> Result<Value, ToolError> {
        let resp = self
            .client
            .post(&self.spec.endpoint)
            .headers(self.headers.clone())
            .json(&json!({ "jsonrpc": "2.0", "id": nonce(), "method": method, "params": params }))
            .send()
            .await
            .map_err(|e| ToolError::Http(format!("a2a {method} failed: {e}")))?;
        let status = resp.status();
        let body: Value = resp.json().await.map_err(|e| {
            ToolError::ExecutionFailed(format!("a2a {method}: reply is not JSON ({status}): {e}"))
        })?;
        if let Some(err) = body.get("error") {
            let msg = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("error");
            return Err(ToolError::ExecutionFailed(format!("a2a {method}: {msg}")));
        }
        body.get("result").cloned().ok_or_else(|| {
            ToolError::ExecutionFailed(format!("a2a {method}: reply has no result ({status})"))
        })
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

fn nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{t:x}-{}", N.fetch_add(1, Ordering::Relaxed))
}

fn as_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Text from an A2A Message or Task: text parts (and data parts as JSON) of
/// the artifacts, then of the status message, then of the message itself.
fn a2a_text(v: &Value) -> String {
    fn take(parts: Option<&Value>, out: &mut Vec<String>) {
        for p in parts.and_then(Value::as_array).into_iter().flatten() {
            if let Some(t) = p.get("text").and_then(Value::as_str) {
                out.push(t.to_string());
            } else if let Some(d) = p.get("data") {
                out.push(d.to_string());
            }
        }
    }
    let mut out: Vec<String> = Vec::new();
    for a in v
        .get("artifacts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        take(a.get("parts"), &mut out);
    }
    if out.is_empty() {
        take(v.pointer("/status/message/parts"), &mut out);
    }
    if out.is_empty() {
        take(v.get("parts"), &mut out);
    }
    out.join("\n")
}
