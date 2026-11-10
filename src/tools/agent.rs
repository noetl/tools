//! Agent tool (`kind: agent`) — a native Rust tool-use loop against the
//! Anthropic Messages API.
//!
//! The loop is exposed turn-by-turn through [`AgentSession`] rather than as one
//! opaque call, because the agent's own tools are *hosted by the caller*: in the
//! CLI local runtime each tool call runs a sub-playbook, and that runner is
//! synchronous and owns its own tokio entry points.  Driving the loop from the
//! host — `next_turn` → run the requested tools → `submit_tool_results` →
//! `next_turn` … — keeps sub-playbook execution outside any `block_on`, and keeps
//! every model turn a discrete unit (the shape a distributed, callback-driven
//! runner would need, since the session's message history is plain JSON).
//!
//! [`AgentTool`] (the registry entry) runs the same loop for agents that declare
//! no tools; a model that asks for a tool there fails loudly instead of being
//! silently answered.
//!
//! ## Config
//!
//! ```yaml
//! tool:
//!   kind: agent
//!   framework: claude            # the only framework this tool implements
//!   model: claude-sonnet-5-5
//!   system: "You triage email."  # optional
//!   prompt: "Summarise {{ workload.account }}'s unread mail."
//!   max_turns: 8                 # model calls before the step fails (default 8)
//!   max_tokens: 4096             # per model call (default 4096)
//!   auth:                        # required — no ambient API key
//!     type: api_key
//!     credential: anthropic      # keychain alias
//!   tools:                       # optional; bindings are host-specific
//!     - name: list_unread
//!       description: List unread messages for one account.
//!       input_schema: { type: object, properties: { account: { type: string } } }
//!       playbook: gmail/list_unread.yaml
//! ```
//!
//! ## `framework: claude-code`
//!
//! Runs the official Claude Code CLI headless (`claude -p --output-format
//! json`), so usage is billed to whatever account that CLI is signed in to — a
//! Claude subscription included — instead of an API key.  Claude Code runs its
//! own tool loop, so `tools:` and `auth:` are refused; grant tools with
//! `allowed_tools` (Claude Code tool names / `mcp__<server>__<tool>`) and MCP
//! servers with `mcp_config`.  `ANTHROPIC_API_KEY` / `ANTHROPIC_AUTH_TOKEN` are
//! removed from the child's environment so the run cannot silently switch to
//! API billing.  The run is bounded by the step `timeout` (default 300 s).
//!
//! ```yaml
//! tool:
//!   kind: agent
//!   framework: claude-code
//!   model: claude-sonnet-5-5
//!   prompt: "Triage: {{ list.result }}"
//!   claude_bin: /home/me/.local/bin/claude   # optional; default `claude` on PATH
//!   allowed_tools: [mcp__gmail__search]      # optional; default: no tools
//!   mcp_config: [/home/me/.config/noetl/gmail-mcp.json]  # optional
//! ```

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::Instrument;

use crate::auth::{AuthCredentials, AuthResolver};
use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;
use crate::tools::mcp::{request_headers, McpClient};
use crate::tools::remote_agent::{RemoteAgent, RemoteAgentSpec, RemoteProtocol};

/// Anthropic Messages API base URL used when `endpoint` is not set.
pub const DEFAULT_ANTHROPIC_ENDPOINT: &str = "https://api.anthropic.com";
/// `anthropic-version` header value the request/response shapes here follow.
pub const ANTHROPIC_VERSION: &str = "2023-06-01";

fn default_max_turns() -> u32 {
    8
}

fn default_max_tokens() -> u32 {
    4096
}

fn default_prompt_cache() -> bool {
    true
}

fn default_input_schema() -> serde_json::Value {
    serde_json::json!({ "type": "object", "properties": {} })
}

/// Agent framework.  The field is required so a playbook written for another
/// framework is rejected instead of silently run here.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AgentFramework {
    /// Native tool-use loop on the Anthropic Messages API (API key billing).
    #[serde(rename = "claude")]
    Claude,
    /// The Claude Code CLI run headless; billed to the CLI's signed-in account.
    #[serde(rename = "claude-code")]
    ClaudeCode,
    /// No model here: the step is one call to an external agent (`remote:`),
    /// over the NoETL agent protocol or A2A.  See [`super::remote_agent`].
    #[serde(rename = "remote")]
    Remote,
}

/// An MCP server whose tools the model may call (`mcp_servers:`).  The agent
/// session lists the server's tools at start and runs each call itself, so an
/// agent with only MCP / remote-agent tools runs on any runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerSpec {
    /// Prefix of the tool names the model sees (`<name>__<tool>`).
    pub name: String,
    /// MCP Streamable HTTP endpoint.
    pub endpoint: String,
    /// Literal headers (rendered like the rest of the config).
    #[serde(default)]
    pub headers: Option<std::collections::BTreeMap<String, serde_json::Value>>,
    /// Credential for the server; resolved like any tool `auth:`.
    #[serde(default)]
    pub auth: Option<crate::registry::AuthConfig>,
    /// Allowlist of the server's tool names.  Unset exposes every tool the
    /// server lists; an empty list exposes none.
    #[serde(default)]
    pub tools: Option<Vec<String>>,
    /// Seconds per MCP request (default 60).
    #[serde(default)]
    pub timeout: Option<u64>,
}

/// One tool the model may call.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentToolSpec {
    /// Tool name the model uses in `tool_use` blocks.
    pub name: String,
    /// What the tool does — the model reads this to decide when to call it.
    #[serde(default)]
    pub description: String,
    /// JSON Schema for the tool's input object.
    #[serde(default = "default_input_schema")]
    pub input_schema: serde_json::Value,
    /// Host-specific binding (e.g. `playbook: path.yaml` in the CLI).  Never
    /// sent to the model.
    #[serde(flatten)]
    pub binding: serde_json::Map<String, serde_json::Value>,
}

/// Parsed `kind: agent` configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub framework: AgentFramework,
    /// Required for `claude` / `claude-code`; unused by `remote`.
    #[serde(default)]
    pub model: String,
    /// Required for `claude` / `claude-code`.  For `remote` it is the message
    /// sent when `input` is unset.
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub system: Option<String>,
    #[serde(default = "default_max_turns")]
    pub max_turns: u32,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    #[serde(default)]
    pub temperature: Option<f64>,
    /// API base URL override — tests and API-compatible gateways only.
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub tools: Vec<AgentToolSpec>,
    /// `claude-code` only: CLI binary path (default `claude` on `PATH`).
    #[serde(default)]
    pub claude_bin: Option<String>,
    /// `claude-code` only: tools Claude Code may use (`--allowed-tools`).
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    /// `claude-code` only: MCP config files (`--mcp-config`, strict).
    #[serde(default)]
    pub mcp_config: Vec<String>,
    /// `claude-code` only: working directory for the CLI.
    #[serde(default)]
    pub workdir: Option<String>,
    /// `claude` only: mark the conversation for Anthropic prompt caching so each
    /// turn re-reads the previous turns at the cached rate (default on).
    #[serde(default = "default_prompt_cache")]
    pub prompt_cache: bool,
    /// `claude` only: `disabled` or `adaptive`, sent as `thinking: {type}`.  Unset
    /// leaves the model default; some models think adaptively on hard turns,
    /// which spends `max_tokens` on reasoning.
    #[serde(default)]
    pub thinking: Option<String>,
    /// `claude` only: `low` / `medium` / `high`, sent as `output_config.effort`.
    #[serde(default)]
    pub effort: Option<String>,
    /// `claude` only: MCP servers whose tools the model may call.
    #[serde(default)]
    pub mcp_servers: Vec<McpServerSpec>,
    /// `claude` only: external agents the model may call, one tool each.
    #[serde(default)]
    pub agents: Vec<RemoteAgentSpec>,
    /// `remote` only: the agent the step calls.
    #[serde(default)]
    pub remote: Option<RemoteAgentSpec>,
    /// `remote` only: the input sent (`noetl`: an object; `a2a`: text or JSON).
    /// Unset sends `{prompt}` (`noetl`) or `prompt` (`a2a`).
    ///
    /// `args` is accepted too: noetl-server's step model decodes a tool's
    /// `input:` into `args` (the alias the python / shell tools rely on), so on
    /// the cluster worker this field arrives under that name.
    #[serde(default, alias = "args")]
    pub input: Option<serde_json::Value>,
}

/// Binding keys the session owns.  A `tools:` entry may not use them, so a
/// playbook cannot forge a binding the host would mistake for its own.
const BINDING_MCP_SERVER: &str = "mcp_server";
const BINDING_MCP_TOOL: &str = "mcp_tool";
const BINDING_REMOTE_AGENT: &str = "remote_agent";

/// `<prefix>__<name>` restricted to the model's tool-name alphabet
/// (`[A-Za-z0-9_-]{1,64}`).
fn source_tool_name(prefix: &str, name: &str) -> String {
    let clean = |s: &str| -> String {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    };
    let joined = if prefix.is_empty() {
        clean(name)
    } else {
        format!("{}__{}", clean(prefix), clean(name))
    };
    joined.chars().take(64).collect()
}

/// A tool call the model requested in its latest turn.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentToolCall {
    pub id: String,
    pub name: String,
    pub input: serde_json::Value,
}

/// The host's answer to one [`AgentToolCall`].
#[derive(Debug, Clone, PartialEq)]
pub struct AgentToolOutput {
    pub tool_use_id: String,
    pub content: String,
    pub is_error: bool,
}

/// Outcome of one model turn.
#[derive(Debug, Clone, PartialEq)]
pub enum AgentTurn {
    /// The model wants these tools run before it continues.
    ToolCalls(Vec<AgentToolCall>),
    /// The model finished; the value is the step result.
    Final(serde_json::Value),
}

/// A running agent conversation.  Holds the message history between turns.
pub struct AgentSession {
    config: AgentConfig,
    /// `None` for `claude-code`, which authenticates through the CLI.
    api_key: Option<String>,
    client: reqwest::Client,
    timeout: Duration,
    messages: Vec<serde_json::Value>,
    turns: u32,
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: u64,
    cache_write_tokens: u64,
    /// `mcp_servers:` sessions, by server name.
    mcp: std::collections::HashMap<String, McpClient>,
    /// `agents:` sources, by tool name.
    remotes: std::collections::HashMap<String, RemoteAgent>,
    /// `framework: remote`'s agent.
    remote: Option<RemoteAgent>,
    /// Tool calls the session answered itself (MCP / remote agents).
    source_calls: u32,
}

impl std::fmt::Debug for AgentSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The API key is deliberately absent.
        f.debug_struct("AgentSession")
            .field("model", &self.config.model)
            .field("turns", &self.turns)
            .field("messages", &self.messages.len())
            .finish()
    }
}

impl AgentSession {
    /// Parse `config.config` (already template-rendered by the caller), resolve
    /// the API key from `config.auth`, and seed the conversation with `prompt`.
    ///
    /// `auth` is required: there is no fallback to an environment variable or
    /// any other ambient key.
    pub async fn start(config: &ToolConfig, ctx: &ExecutionContext) -> Result<Self, ToolError> {
        let parsed: AgentConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| ToolError::Configuration(format!("Invalid agent config: {}", e)))?;
        if parsed.max_turns == 0 {
            return Err(ToolError::Configuration(
                "agent max_turns must be at least 1".to_string(),
            ));
        }
        for spec in &parsed.tools {
            if spec.name.trim().is_empty() {
                return Err(ToolError::Configuration(
                    "every agent tool needs a non-empty name".to_string(),
                ));
            }
        }

        for spec in &parsed.tools {
            for reserved in [BINDING_MCP_SERVER, BINDING_MCP_TOOL, BINDING_REMOTE_AGENT] {
                if spec.binding.contains_key(reserved) {
                    return Err(ToolError::Configuration(format!(
                        "agent tool `{}` uses the reserved binding key `{}`; declare MCP servers under `mcp_servers:` and external agents under `agents:`",
                        spec.name, reserved
                    )));
                }
            }
        }
        let has_sources = !parsed.mcp_servers.is_empty() || !parsed.agents.is_empty();
        if parsed.framework != AgentFramework::Remote
            && (parsed.model.trim().is_empty() || parsed.prompt.trim().is_empty())
        {
            return Err(ToolError::Configuration(
                "agent `model` and `prompt` are required".to_string(),
            ));
        }
        if parsed.framework != AgentFramework::Remote
            && (parsed.remote.is_some() || parsed.input.is_some())
        {
            return Err(ToolError::Configuration(
                "`remote:` / `input:` belong to `framework: remote`".to_string(),
            ));
        }

        let api_key = match parsed.framework {
            AgentFramework::Remote => {
                if config.auth.is_some() {
                    return Err(ToolError::Configuration(
                        "framework remote runs no model; put the remote agent's credential under `remote.auth`"
                            .to_string(),
                    ));
                }
                if !parsed.tools.is_empty() || has_sources {
                    return Err(ToolError::Configuration(
                        "framework remote calls one agent; `tools:`, `mcp_servers:` and `agents:` need a model (`framework: claude`)"
                            .to_string(),
                    ));
                }
                if parsed.remote.is_none() {
                    return Err(ToolError::Configuration(
                        "framework remote needs `remote: {endpoint, protocol, agent}`".to_string(),
                    ));
                }
                None
            }
            AgentFramework::ClaudeCode => {
                if has_sources {
                    return Err(ToolError::Configuration(
                        "framework claude-code connects MCP servers itself; use `mcp_config` instead of `mcp_servers:` / `agents:`"
                            .to_string(),
                    ));
                }
                if config.auth.is_some() {
                    return Err(ToolError::Configuration(
                        "framework claude-code uses the claude CLI's own sign-in; remove `auth:`"
                            .to_string(),
                    ));
                }
                if !parsed.tools.is_empty() {
                    return Err(ToolError::Configuration(
                        "framework claude-code runs its own tools; use `allowed_tools` / \
                         `mcp_config` instead of `tools:`"
                            .to_string(),
                    ));
                }
                None
            }
            AgentFramework::Claude => {
                let auth = config.auth.as_ref().ok_or_else(|| {
                    ToolError::Auth(
                        "agent requires an `auth:` block (e.g. `type: api_key`, `credential: <alias>`)"
                            .to_string(),
                    )
                })?;
                match AuthResolver::new().resolve(auth, ctx).await? {
                    AuthCredentials::ApiKey(_, key) | AuthCredentials::Bearer(key) => Some(key),
                    other => {
                        return Err(ToolError::Auth(format!(
                            "agent auth must resolve to an API key, got {}",
                            match other {
                                AuthCredentials::Basic(..) => "basic credentials",
                                _ => "no credentials",
                            }
                        )))
                    }
                }
            }
        };

        let timeout = Duration::from_secs(config.timeout.unwrap_or(300).max(1));
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| ToolError::Http(format!("failed to build HTTP client: {}", e)))?;

        let messages = vec![serde_json::json!({
            "role": "user",
            "content": parsed.prompt,
        })];

        let mut parsed = parsed;
        let remote = match parsed.remote.clone() {
            Some(spec) => Some(RemoteAgent::resolve(spec, ctx).await?),
            None => None,
        };

        // Tool sources: list each MCP server's tools and describe each remote
        // agent, then add them to the model's tool list.
        let mut mcp = std::collections::HashMap::new();
        for server in &parsed.mcp_servers {
            if server.name.trim().is_empty() {
                return Err(ToolError::Configuration(
                    "every `mcp_servers` entry needs a `name`".into(),
                ));
            }
            if mcp.contains_key(&server.name) {
                return Err(ToolError::Configuration(format!(
                    "mcp_servers name `{}` is used twice",
                    server.name
                )));
            }
            let headers = request_headers(&server.headers, server.auth.as_ref(), ctx).await?;
            let mut client = McpClient::new(
                &server.endpoint,
                headers,
                Duration::from_secs(server.timeout.unwrap_or(60).clamp(1, 600)),
            )?;
            let listed = client.list_tools().await.map_err(|e| {
                ToolError::ExecutionFailed(format!(
                    "agent mcp_servers `{}`: tools/list failed: {}",
                    server.name, e
                ))
            })?;
            for t in listed {
                if let Some(allow) = &server.tools {
                    if !allow.iter().any(|a| a == &t.name) {
                        continue;
                    }
                }
                let mut binding = serde_json::Map::new();
                binding.insert(BINDING_MCP_SERVER.into(), serde_json::json!(server.name));
                binding.insert(BINDING_MCP_TOOL.into(), serde_json::json!(t.name));
                parsed.tools.push(AgentToolSpec {
                    name: source_tool_name(&server.name, &t.name),
                    description: t.description,
                    input_schema: t.input_schema,
                    binding,
                });
            }
            mcp.insert(server.name.clone(), client);
        }
        let mut remotes = std::collections::HashMap::new();
        for spec in &parsed.agents {
            let name = spec
                .name
                .clone()
                .or_else(|| spec.agent.clone())
                .unwrap_or_default();
            let tool_name = source_tool_name("", &name);
            if tool_name.is_empty() {
                return Err(ToolError::Configuration(
                    "every `agents` entry needs a `name`".into(),
                ));
            }
            let agent = RemoteAgent::resolve(spec.clone(), ctx).await?;
            let card = agent.card().await;
            let description = spec
                .description
                .clone()
                .filter(|d| !d.trim().is_empty())
                .or_else(|| {
                    card.as_ref()
                        .and_then(|c| c.get("description"))
                        .and_then(|d| d.as_str())
                        .map(str::to_string)
                })
                .unwrap_or_else(|| format!("External agent {name}."));
            let input_schema = match spec.protocol {
                RemoteProtocol::Noetl => card
                    .as_ref()
                    .and_then(|c| c.get("inputSchema"))
                    .filter(|s| s.is_object())
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({ "type": "object" })),
                RemoteProtocol::A2a => serde_json::json!({
                    "type": "object",
                    "properties": { "message": { "type": "string", "description": "What to ask the agent." } },
                    "required": ["message"],
                }),
            };
            let mut binding = serde_json::Map::new();
            binding.insert(BINDING_REMOTE_AGENT.into(), serde_json::json!(tool_name));
            parsed.tools.push(AgentToolSpec {
                name: tool_name.clone(),
                description,
                input_schema,
                binding,
            });
            remotes.insert(tool_name, agent);
        }
        let mut seen = std::collections::HashSet::new();
        for t in &parsed.tools {
            if !seen.insert(t.name.as_str()) {
                return Err(ToolError::Configuration(format!(
                    "agent tool name `{}` is declared twice (check `tools:`, `mcp_servers:` and `agents:`)",
                    t.name
                )));
            }
        }

        Ok(Self {
            config: parsed,
            api_key,
            client,
            timeout,
            messages,
            turns: 0,
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            mcp,
            remotes,
            remote,
            source_calls: 0,
        })
    }

    /// Is this tool answered by the session itself (an `mcp_servers:` or
    /// `agents:` source) rather than by the host?
    pub fn is_source_tool(&self, name: &str) -> bool {
        self.tool_spec(name).is_some_and(|spec| {
            spec.binding
                .get(BINDING_MCP_SERVER)
                .and_then(|v| v.as_str())
                .is_some_and(|s| self.mcp.contains_key(s))
                || spec
                    .binding
                    .get(BINDING_REMOTE_AGENT)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| self.remotes.contains_key(s))
        })
    }

    /// Answer a tool call bound to an MCP server or a remote agent.  `None`
    /// means the call belongs to the host (a `tools:` entry).
    ///
    /// Failures become `is_error` outputs for the model to react to; they do
    /// not fail the step.
    pub async fn run_source_tool(&mut self, call: &AgentToolCall) -> Option<AgentToolOutput> {
        if !self.is_source_tool(&call.name) {
            return None;
        }
        let spec = self.tool_spec(&call.name)?.clone();
        self.source_calls += 1;
        let output = |content: String, is_error: bool| AgentToolOutput {
            tool_use_id: call.id.clone(),
            content,
            is_error,
        };
        if let Some(server) = spec
            .binding
            .get(BINDING_MCP_SERVER)
            .and_then(|v| v.as_str())
        {
            let tool = spec
                .binding
                .get(BINDING_MCP_TOOL)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let span =
                tracing::info_span!("tool.dispatch.agent.mcp", server = %server, tool = %tool);
            let client = self.mcp.get_mut(server)?;
            return Some(
                match client
                    .call_tool(&tool, call.input.clone())
                    .instrument(span)
                    .await
                {
                    Ok((text, result, is_error)) => {
                        let content = if text.trim().is_empty() {
                            result.to_string()
                        } else {
                            text
                        };
                        output(content, is_error)
                    }
                    Err(e) => output(format!("mcp {server}/{tool} failed: {e}"), true),
                },
            );
        }
        let name = spec
            .binding
            .get(BINDING_REMOTE_AGENT)
            .and_then(|v| v.as_str())?;
        let agent = self.remotes.get(name)?.clone();
        let span = tracing::info_span!("tool.dispatch.agent.remote", agent = %name);
        Some(
            match agent.call(call.input.clone()).instrument(span).await {
                Ok(outcome) if outcome.ok() => output(outcome.text, false),
                Ok(outcome) => output(
                    format!(
                        "remote agent {name} ended {}: {}",
                        outcome.status, outcome.text
                    ),
                    true,
                ),
                Err(e) => output(format!("remote agent {name} failed: {e}"), true),
            },
        )
    }

    /// `framework: remote`: one call to the configured agent.
    async fn run_remote(&mut self) -> Result<AgentTurn, ToolError> {
        let agent = self
            .remote
            .as_ref()
            .ok_or_else(|| ToolError::Configuration("framework remote needs `remote:`".into()))?;
        let input = match (&self.config.input, agent.spec().protocol) {
            (Some(v), _) => v.clone(),
            (None, RemoteProtocol::Noetl) => serde_json::json!({ "prompt": self.config.prompt }),
            (None, RemoteProtocol::A2a) => serde_json::json!(self.config.prompt),
        };
        let span = tracing::info_span!("tool.dispatch.agent", framework = "remote", endpoint = %agent.spec().endpoint);
        let outcome = agent.call(input).instrument(span).await?;
        if !outcome.ok() {
            return Err(ToolError::ExecutionFailed(format!(
                "remote agent ended {}: {}",
                outcome.status,
                outcome.text.chars().take(300).collect::<String>()
            )));
        }
        let mut value = outcome.to_value(agent.spec());
        value["framework"] = serde_json::json!("remote");
        Ok(AgentTurn::Final(value))
    }

    /// The parsed configuration.
    pub fn config(&self) -> &AgentConfig {
        &self.config
    }

    /// Look up a declared tool by the name the model used.
    pub fn tool_spec(&self, name: &str) -> Option<&AgentToolSpec> {
        self.config.tools.iter().find(|t| t.name == name)
    }

    /// Model calls made so far.
    pub fn turns(&self) -> u32 {
        self.turns
    }

    /// Ask the model for its next turn.
    ///
    /// Fails once `max_turns` model calls have been made without a final
    /// answer, so a looping agent cannot run unbounded.
    pub async fn next_turn(&mut self) -> Result<AgentTurn, ToolError> {
        if self.turns >= self.config.max_turns {
            return Err(ToolError::ExecutionFailed(format!(
                "agent did not finish within max_turns={}",
                self.config.max_turns
            )));
        }
        self.turns += 1;

        if self.config.framework == AgentFramework::ClaudeCode {
            return self.run_claude_code().await;
        }
        if self.config.framework == AgentFramework::Remote {
            return self.run_remote().await;
        }
        let api_key = self.api_key.as_deref().unwrap_or_default();

        let span = tracing::info_span!(
            "tool.dispatch.agent",
            model = %self.config.model,
            turn = self.turns
        );
        let base = self
            .config
            .endpoint
            .as_deref()
            .unwrap_or(DEFAULT_ANTHROPIC_ENDPOINT)
            .trim_end_matches('/');
        let url = format!("{}/v1/messages", base);

        let (status, text) = async {
            let response = self
                .client
                .post(&url)
                .header("x-api-key", api_key)
                .header("anthropic-version", ANTHROPIC_VERSION)
                .json(&self.request_body())
                .send()
                .await
                .map_err(|e| ToolError::Http(format!("agent request failed: {}", e)))?;
            let status = response.status();
            let text = response
                .text()
                .await
                .map_err(|e| ToolError::Http(format!("agent response unreadable: {}", e)))?;
            Ok::<_, ToolError>((status, text))
        }
        .instrument(span)
        .await?;

        let body: serde_json::Value = serde_json::from_str(&text).map_err(|_| {
            ToolError::Http(format!(
                "Anthropic API returned {} with a non-JSON body ({} bytes)",
                status.as_u16(),
                text.len()
            ))
        })?;
        if !status.is_success() {
            let message = body
                .pointer("/error/message")
                .and_then(|m| m.as_str())
                .unwrap_or("no error message");
            return Err(ToolError::Http(format!(
                "Anthropic API returned {}: {}",
                status.as_u16(),
                message
            )));
        }

        self.input_tokens += body
            .pointer("/usage/input_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        self.output_tokens += body
            .pointer("/usage/output_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        self.cache_read_tokens += body
            .pointer("/usage/cache_read_input_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        self.cache_write_tokens += body
            .pointer("/usage/cache_creation_input_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let content = body
            .get("content")
            .and_then(|c| c.as_array())
            .cloned()
            .ok_or_else(|| {
                ToolError::Http("Anthropic response has no `content` array".to_string())
            })?;
        let stop_reason = body
            .get("stop_reason")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();

        self.messages.push(serde_json::json!({
            "role": "assistant",
            "content": content,
        }));

        let calls: Vec<AgentToolCall> = content
            .iter()
            .filter(|block| block.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
            .map(|block| AgentToolCall {
                id: block["id"].as_str().unwrap_or_default().to_string(),
                name: block["name"].as_str().unwrap_or_default().to_string(),
                input: block.get("input").cloned().unwrap_or_default(),
            })
            .collect();

        let text: Vec<&str> = content
            .iter()
            .filter(|block| block.get("type").and_then(|t| t.as_str()) == Some("text"))
            .filter_map(|block| block.get("text").and_then(|t| t.as_str()))
            .collect();
        let text = text.join("\n");

        // A turn cut off by max_tokens may carry incomplete tool calls, or no
        // answer at all; neither may pass as a finished run.
        let truncated = stop_reason == "max_tokens";
        if truncated && (!calls.is_empty() || text.trim().is_empty()) {
            let types: Vec<&str> = content
                .iter()
                .filter_map(|b| b.get("type").and_then(|t| t.as_str()))
                .collect();
            return Err(ToolError::ExecutionFailed(format!(
                "agent output hit max_tokens={} on turn {} before finishing (content blocks: [{}]); raise max_tokens or ask for a shorter answer",
                self.config.max_tokens,
                self.turns,
                types.join(", ")
            )));
        }

        if !calls.is_empty() {
            tracing::debug!(calls = calls.len(), "agent requested tool calls");
            return Ok(AgentTurn::ToolCalls(calls));
        }

        Ok(AgentTurn::Final(serde_json::json!({
            "text": text,
            "stop_reason": stop_reason,
            "truncated": truncated,
            "model": self.config.model,
            "turns": self.turns,
            "source_tool_calls": self.source_calls,
            "usage": {
                "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens,
                "cache_read_input_tokens": self.cache_read_tokens,
                "cache_creation_input_tokens": self.cache_write_tokens,
            },
        })))
    }

    /// Feed the results of the latest turn's tool calls back to the model.
    /// Every call from that turn must be answered, in any order.
    pub fn submit_tool_results(&mut self, outputs: Vec<AgentToolOutput>) {
        let blocks: Vec<serde_json::Value> = outputs
            .into_iter()
            .map(|o| {
                serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": o.tool_use_id,
                    "content": o.content,
                    "is_error": o.is_error,
                })
            })
            .collect();
        self.messages.push(serde_json::json!({
            "role": "user",
            "content": blocks,
        }));
    }

    /// Arguments for `claude -p`.  The prompt is NOT among them: it goes on
    /// stdin, because `--allowed-tools` / `--mcp-config` take variadic values
    /// that would swallow a trailing positional prompt.
    fn claude_code_args(&self) -> Vec<String> {
        let c = &self.config;
        let mut args: Vec<String> = vec![
            "-p".into(),
            "--output-format".into(),
            "json".into(),
            "--no-session-persistence".into(),
            "--model".into(),
            c.model.clone(),
        ];
        if let Some(system) = &c.system {
            // Append, not replace: Claude Code's own system prompt carries its
            // tool-use instructions.
            args.push("--append-system-prompt".into());
            args.push(system.clone());
        }
        if !c.mcp_config.is_empty() {
            args.push("--strict-mcp-config".into());
            args.push("--mcp-config".into());
            args.extend(c.mcp_config.iter().cloned());
        }
        if !c.allowed_tools.is_empty() {
            args.push("--allowed-tools".into());
            args.extend(c.allowed_tools.iter().cloned());
        }
        args
    }

    /// One headless Claude Code run: prompt on stdin, JSON result on stdout.
    async fn run_claude_code(&mut self) -> Result<AgentTurn, ToolError> {
        use tokio::io::AsyncWriteExt;

        let bin = self.config.claude_bin.as_deref().unwrap_or("claude");
        let mut cmd = tokio::process::Command::new(bin);
        cmd.args(self.claude_code_args())
            .env_remove("ANTHROPIC_API_KEY")
            .env_remove("ANTHROPIC_AUTH_TOKEN")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        if let Some(dir) = &self.config.workdir {
            cmd.current_dir(dir);
        }

        let span = tracing::info_span!(
            "tool.dispatch.agent",
            model = %self.config.model,
            framework = "claude-code"
        );
        let prompt = self.config.prompt.clone();
        let run = async move {
            let mut child = cmd.spawn().map_err(|e| {
                ToolError::Process(format!(
                    "cannot start `{}` ({}); install the Claude Code CLI or set `claude_bin`",
                    bin, e
                ))
            })?;
            if let Some(mut stdin) = child.stdin.take() {
                stdin
                    .write_all(prompt.as_bytes())
                    .await
                    .map_err(|e| ToolError::Process(format!("writing prompt to claude: {}", e)))?;
            }
            child
                .wait_with_output()
                .await
                .map_err(|e| ToolError::Process(format!("waiting for claude: {}", e)))
        }
        .instrument(span);
        let output = tokio::time::timeout(self.timeout, run)
            .await
            .map_err(|_| ToolError::Timeout(self.timeout.as_secs()))??;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let body: serde_json::Value = serde_json::from_str(stdout.trim()).map_err(|_| {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let tail: String = stderr
                .trim()
                .chars()
                .rev()
                .take(300)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            ToolError::Process(format!(
                "claude exited with {} and no JSON result: {}",
                output.status, tail
            ))
        })?;

        let text = body
            .get("result")
            .and_then(|r| r.as_str())
            .unwrap_or_default()
            .to_string();
        if body
            .get("is_error")
            .and_then(|e| e.as_bool())
            .unwrap_or(!output.status.success())
        {
            return Err(ToolError::ExecutionFailed(format!(
                "claude-code run failed: {}",
                if text.is_empty() { "no message" } else { &text }
            )));
        }

        Ok(AgentTurn::Final(serde_json::json!({
            "text": text,
            "stop_reason": body.get("terminal_reason").or_else(|| body.get("subtype")).cloned()
                .unwrap_or(serde_json::Value::Null),
            "model": self.config.model,
            "framework": "claude-code",
            "turns": body.get("num_turns").cloned().unwrap_or(serde_json::json!(1)),
            "usage": {
                "input_tokens": body.pointer("/usage/input_tokens").cloned().unwrap_or(serde_json::json!(0)),
                "output_tokens": body.pointer("/usage/output_tokens").cloned().unwrap_or(serde_json::json!(0)),
            },
            "permission_denials": body.get("permission_denials").cloned().unwrap_or(serde_json::json!([])),
        })))
    }

    fn request_body(&self) -> serde_json::Value {
        let mut messages = self.messages.clone();
        if self.config.prompt_cache {
            mark_cache_breakpoint(&mut messages);
        }
        let mut body = serde_json::json!({
            "model": self.config.model,
            "max_tokens": self.config.max_tokens,
            "messages": messages,
        });
        if let Some(system) = &self.config.system {
            body["system"] = serde_json::json!(system);
        }
        if let Some(t) = self.config.temperature {
            body["temperature"] = serde_json::json!(t);
        }
        if let Some(mode) = &self.config.thinking {
            body["thinking"] = serde_json::json!({ "type": mode });
        }
        if let Some(effort) = &self.config.effort {
            body["output_config"] = serde_json::json!({ "effort": effort });
        }
        if !self.config.tools.is_empty() {
            body["tools"] = serde_json::Value::Array(
                self.config
                    .tools
                    .iter()
                    .map(|t| {
                        serde_json::json!({
                            "name": t.name,
                            "description": t.description,
                            "input_schema": t.input_schema,
                        })
                    })
                    .collect(),
            );
        }
        body
    }
}

/// Put a prompt-cache breakpoint on the last content block of the last
/// message.  The cached prefix then covers tools, system and every earlier
/// turn, so the next turn reads it at the cached rate instead of paying full
/// price again.  Prefixes below the model's minimum cacheable length are
/// simply not cached by the API.
fn mark_cache_breakpoint(messages: &mut [serde_json::Value]) {
    let Some(last) = messages.last_mut() else {
        return;
    };
    let content = &mut last["content"];
    if let Some(text) = content.as_str().map(str::to_string) {
        *content = serde_json::json!([{ "type": "text", "text": text }]);
    }
    if let Some(block) = content.as_array_mut().and_then(|blocks| blocks.last_mut()) {
        block["cache_control"] = serde_json::json!({ "type": "ephemeral" });
    }
}

/// Registry entry for `kind: agent`, the path a host that is not the CLI
/// local runtime (the cluster worker) dispatches.
///
/// It runs the loop itself and answers every tool call bound to an
/// `mcp_servers:` or `agents:` source, so an agent whose tools are MCP servers
/// or external agents runs here exactly as in the CLI.  A `tools:` entry
/// (a playbook binding) needs a host that runs sub-playbooks between turns —
/// the CLI local runtime does; on the cluster that is RFC Phase 6 Stage B — so
/// it is refused before the first model call rather than half-run.
pub struct AgentTool {
    template_engine: TemplateEngine,
    allow_claude_code: bool,
}

impl AgentTool {
    pub fn new() -> Self {
        Self {
            template_engine: TemplateEngine::new(),
            allow_claude_code: true,
        }
    }

    /// For shared, multi-tenant pools: refuse `framework: claude-code`, which
    /// would run a local CLI under whatever account that host is signed in to.
    pub fn for_shared_pool() -> Self {
        Self {
            allow_claude_code: false,
            ..Self::new()
        }
    }
}

impl Default for AgentTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for AgentTool {
    fn name(&self) -> &'static str {
        "agent"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let started = std::time::Instant::now();
        if !self.allow_claude_code
            && config.config.get("framework").and_then(|f| f.as_str()) == Some("claude-code")
        {
            return Err(ToolError::Configuration(
                "framework claude-code runs a local CLI under its signed-in account and is not available on this runtime; use framework claude or remote"
                    .to_string(),
            ));
        }
        let rendered = ToolConfig {
            config: self
                .template_engine
                .render_value(&config.config, &ctx.to_template_context())?,
            ..config.clone()
        };
        let mut session = AgentSession::start(&rendered, ctx).await?;
        if let Some(host_tool) = session
            .config()
            .tools
            .iter()
            .find(|t| !session.is_source_tool(&t.name))
        {
            return Err(ToolError::Configuration(format!(
                "agent tool `{}` is bound to a playbook, which needs a host that runs sub-playbooks (the CLI local runtime); on this runtime declare tools as `mcp_servers:` or `agents:`",
                host_tool.name
            )));
        }
        loop {
            match session.next_turn().await? {
                AgentTurn::Final(value) => {
                    return Ok(ToolResult::success(value)
                        .with_duration(started.elapsed().as_millis() as u64))
                }
                AgentTurn::ToolCalls(calls) => {
                    let mut outputs = Vec::with_capacity(calls.len());
                    for call in &calls {
                        match session.run_source_tool(call).await {
                            Some(out) => outputs.push(out),
                            None => {
                                return Err(ToolError::ExecutionFailed(format!(
                                    "model requested undeclared tool `{}`",
                                    call.name
                                )))
                            }
                        }
                    }
                    session.submit_tool_results(outputs);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{AuthConfig, AuthType};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    fn tool_config(body: serde_json::Value, with_auth: bool) -> ToolConfig {
        ToolConfig {
            kind: "agent".into(),
            config: body,
            timeout: Some(10),
            retry: None,
            auth: with_auth.then(|| AuthConfig {
                auth_type: AuthType::ApiKey,
                credential: Some("anthropic".into()),
                token: None,
                username: None,
                password: None,
                header: None,
                scopes: None,
            }),
        }
    }

    fn ctx_with_key() -> ExecutionContext {
        let mut ctx = ExecutionContext::default();
        ctx.set_secret("anthropic", "test-key");
        ctx
    }

    fn text_reply(text: &str) -> serde_json::Value {
        serde_json::json!({
            "content": [{ "type": "text", "text": text }],
            "stop_reason": "end_turn",
            "usage": { "input_tokens": 10, "output_tokens": 5 },
        })
    }

    #[tokio::test]
    async fn missing_auth_is_refused() {
        let cfg = tool_config(
            serde_json::json!({ "framework": "claude", "model": "m", "prompt": "hi" }),
            false,
        );
        let err = AgentSession::start(&cfg, &ctx_with_key())
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Auth(_)), "{err}");
    }

    #[tokio::test]
    async fn unknown_framework_is_refused() {
        let cfg = tool_config(
            serde_json::json!({ "framework": "langchain", "model": "m", "prompt": "hi" }),
            true,
        );
        let err = AgentSession::start(&cfg, &ctx_with_key())
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Configuration(_)), "{err}");
    }

    #[tokio::test]
    async fn unresolved_credential_is_refused() {
        let cfg = tool_config(
            serde_json::json!({ "framework": "claude", "model": "m", "prompt": "hi" }),
            true,
        );
        let err = AgentSession::start(&cfg, &ExecutionContext::default())
            .await
            .unwrap_err();
        assert!(matches!(err, ToolError::Auth(_)), "{err}");
    }

    #[tokio::test]
    async fn tool_free_agent_returns_final_text() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .and(header("x-api-key", "test-key"))
            .and(header("anthropic-version", ANTHROPIC_VERSION))
            .respond_with(ResponseTemplate::new(200).set_body_json(text_reply("done")))
            .expect(1)
            .mount(&server)
            .await;

        let cfg = tool_config(
            serde_json::json!({
                "framework": "claude",
                "model": "claude-test",
                "prompt": "say done",
                "endpoint": server.uri(),
            }),
            true,
        );
        let result = AgentTool::new()
            .execute(&cfg, &ctx_with_key())
            .await
            .unwrap();
        let data = result.data.unwrap();
        assert_eq!(data["text"], "done");
        assert_eq!(data["turns"], 1);
        assert_eq!(data["usage"]["output_tokens"], 5);
    }

    #[tokio::test]
    async fn tool_loop_round_trips_results() {
        let server = MockServer::start().await;
        // Second call: the request must carry the tool_result we submitted.
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .and(|req: &Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                body["messages"].as_array().map(|m| m.len()) == Some(3)
            })
            .respond_with(ResponseTemplate::new(200).set_body_json(text_reply("3 unread")))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [
                    { "type": "text", "text": "checking" },
                    { "type": "tool_use", "id": "tu_1", "name": "list_unread",
                      "input": { "account": "a" } }
                ],
                "stop_reason": "tool_use",
                "usage": { "input_tokens": 7, "output_tokens": 3 },
            })))
            .up_to_n_times(1)
            .mount(&server)
            .await;

        let cfg = tool_config(
            serde_json::json!({
                "framework": "claude",
                "model": "claude-test",
                "prompt": "how many unread?",
                "endpoint": server.uri(),
                "tools": [{
                    "name": "list_unread",
                    "description": "list",
                    "playbook": "gmail/list_unread.yaml",
                }],
            }),
            true,
        );
        let mut session = AgentSession::start(&cfg, &ctx_with_key()).await.unwrap();
        assert_eq!(
            session.tool_spec("list_unread").unwrap().binding["playbook"],
            "gmail/list_unread.yaml"
        );
        // The host binding never reaches the model.
        assert!(session.request_body()["tools"][0].get("playbook").is_none());

        let calls = match session.next_turn().await.unwrap() {
            AgentTurn::ToolCalls(c) => c,
            other => panic!("expected tool calls, got {other:?}"),
        };
        assert_eq!(calls[0].name, "list_unread");
        assert_eq!(calls[0].input["account"], "a");

        session.submit_tool_results(vec![AgentToolOutput {
            tool_use_id: "tu_1".into(),
            content: "3".into(),
            is_error: false,
        }]);
        match session.next_turn().await.unwrap() {
            AgentTurn::Final(v) => {
                assert_eq!(v["text"], "3 unread");
                assert_eq!(v["turns"], 2);
                assert_eq!(v["usage"]["input_tokens"], 17);
            }
            other => panic!("expected final, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn max_turns_bounds_the_loop() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [{ "type": "tool_use", "id": "t", "name": "x", "input": {} }],
                "stop_reason": "tool_use",
            })))
            .mount(&server)
            .await;
        let cfg = tool_config(
            serde_json::json!({
                "framework": "claude", "model": "m", "prompt": "loop",
                "endpoint": server.uri(), "max_turns": 1,
                "tools": [{ "name": "x" }],
            }),
            true,
        );
        let mut session = AgentSession::start(&cfg, &ctx_with_key()).await.unwrap();
        assert!(matches!(
            session.next_turn().await.unwrap(),
            AgentTurn::ToolCalls(_)
        ));
        let err = session.next_turn().await.unwrap_err();
        assert!(err.to_string().contains("max_turns=1"), "{err}");
    }

    /// Write an executable fake `claude` that echoes its stdin, argv and the
    /// API-key env var back inside a Claude Code JSON result.
    fn fake_claude(name: &str, is_error: bool) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let dir =
            std::env::temp_dir().join(format!("noetl-fake-claude-{}-{}", std::process::id(), name));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("claude");
        std::fs::write(
            &path,
            format!(
                "#!/bin/sh\nprompt=$(cat)\nprintf '{{\"type\":\"result\",\"subtype\":\"success\",\
                 \"is_error\":{},\"num_turns\":2,\"result\":\"prompt=%s args=%s key=%s\",\
                 \"usage\":{{\"input_tokens\":3,\"output_tokens\":4}},\"permission_denials\":[]}}' \
                 \"$prompt\" \"$*\" \"${{ANTHROPIC_API_KEY:-none}}\"\n",
                is_error
            ),
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn claude_code_config(bin: &str, extra: serde_json::Value) -> ToolConfig {
        let mut body = serde_json::json!({
            "framework": "claude-code",
            "model": "claude-test",
            "prompt": "hello",
            "claude_bin": bin,
        });
        for (k, v) in extra.as_object().unwrap() {
            body[k] = v.clone();
        }
        tool_config(body, false)
    }

    #[tokio::test]
    async fn claude_code_runs_cli_with_prompt_on_stdin_and_no_api_key() {
        let bin = fake_claude("ok", false);
        // The child must not see an API key even when the parent has one.
        std::env::set_var("ANTHROPIC_API_KEY", "must-not-leak");
        let cfg = claude_code_config(
            bin.to_str().unwrap(),
            serde_json::json!({
                "system": "be brief",
                "allowed_tools": ["mcp__gmail__search"],
                "mcp_config": ["/tmp/mcp.json"],
            }),
        );
        let result = AgentTool::new()
            .execute(&cfg, &ExecutionContext::default())
            .await
            .unwrap();
        std::env::remove_var("ANTHROPIC_API_KEY");
        let data = result.data.unwrap();
        let text = data["text"].as_str().unwrap();
        assert!(text.contains("prompt=hello"), "{text}");
        assert!(
            text.contains("key=none"),
            "API key leaked to claude: {text}"
        );
        assert!(
            text.contains("-p --output-format json --no-session-persistence --model claude-test"),
            "{text}"
        );
        assert!(text.contains("--append-system-prompt be brief"), "{text}");
        assert!(
            text.contains("--strict-mcp-config --mcp-config /tmp/mcp.json"),
            "{text}"
        );
        assert!(
            text.contains("--allowed-tools mcp__gmail__search"),
            "{text}"
        );
        assert!(
            !text.contains("args=hello"),
            "prompt must not be an argument: {text}"
        );
        assert_eq!(data["framework"], "claude-code");
        assert_eq!(data["turns"], 2);
        assert_eq!(data["usage"]["output_tokens"], 4);
    }

    #[tokio::test]
    async fn claude_code_error_result_fails_the_step() {
        let bin = fake_claude("err", true);
        let cfg = claude_code_config(bin.to_str().unwrap(), serde_json::json!({}));
        let err = AgentTool::new()
            .execute(&cfg, &ExecutionContext::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("claude-code run failed"), "{err}");
    }

    #[tokio::test]
    async fn claude_code_missing_binary_names_the_fix() {
        let cfg = claude_code_config("/nonexistent/claude", serde_json::json!({}));
        let err = AgentTool::new()
            .execute(&cfg, &ExecutionContext::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("claude_bin"), "{err}");
    }

    #[tokio::test]
    async fn claude_code_refuses_auth_and_tools() {
        let mut with_auth = claude_code_config("claude", serde_json::json!({}));
        with_auth.auth = tool_config(serde_json::json!({}), true).auth;
        let err = AgentSession::start(&with_auth, &ctx_with_key())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("remove `auth:`"), "{err}");

        let with_tools =
            claude_code_config("claude", serde_json::json!({ "tools": [{ "name": "x" }] }));
        let err = AgentSession::start(&with_tools, &ctx_with_key())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("allowed_tools"), "{err}");
    }

    fn claude_cfg(server: &MockServer, extra: serde_json::Value) -> ToolConfig {
        let mut body = serde_json::json!({
            "framework": "claude", "model": "m", "prompt": "hi", "endpoint": server.uri(),
        });
        for (k, v) in extra.as_object().unwrap() {
            body[k] = v.clone();
        }
        tool_config(body, true)
    }

    #[tokio::test]
    async fn max_tokens_without_an_answer_fails_with_block_types() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [], "stop_reason": "max_tokens",
                "usage": { "input_tokens": 5, "output_tokens": 2048 },
            })))
            .mount(&server)
            .await;
        let mut session =
            AgentSession::start(&claude_cfg(&server, serde_json::json!({})), &ctx_with_key())
                .await
                .unwrap();
        let err = session.next_turn().await.unwrap_err().to_string();
        assert!(
            err.contains("max_tokens=4096") && err.contains("content blocks: []"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn max_tokens_with_partial_tool_calls_fails() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [{ "type": "tool_use", "id": "t", "name": "x", "input": {} }],
                "stop_reason": "max_tokens",
            })))
            .mount(&server)
            .await;
        let cfg = claude_cfg(&server, serde_json::json!({ "tools": [{ "name": "x" }] }));
        let mut session = AgentSession::start(&cfg, &ctx_with_key()).await.unwrap();
        let err = session.next_turn().await.unwrap_err().to_string();
        assert!(err.contains("tool_use"), "{err}");
    }

    #[tokio::test]
    async fn max_tokens_with_text_is_returned_flagged_truncated() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [{ "type": "text", "text": "partial" }], "stop_reason": "max_tokens",
            })))
            .mount(&server)
            .await;
        let mut session =
            AgentSession::start(&claude_cfg(&server, serde_json::json!({})), &ctx_with_key())
                .await
                .unwrap();
        match session.next_turn().await.unwrap() {
            AgentTurn::Final(v) => {
                assert_eq!(v["text"], "partial");
                assert_eq!(v["truncated"], true);
            }
            other => panic!("expected final, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn prompt_cache_marks_the_last_block_and_counts_cache_tokens() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(|req: &Request| {
                let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
                let last = body["messages"].as_array().unwrap().last().unwrap().clone();
                last["content"][0]["cache_control"]["type"] == "ephemeral"
            })
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "content": [{ "type": "text", "text": "ok" }], "stop_reason": "end_turn",
                "usage": { "input_tokens": 3, "output_tokens": 1,
                           "cache_read_input_tokens": 900, "cache_creation_input_tokens": 100 },
            })))
            .expect(1)
            .mount(&server)
            .await;
        let mut session =
            AgentSession::start(&claude_cfg(&server, serde_json::json!({})), &ctx_with_key())
                .await
                .unwrap();
        match session.next_turn().await.unwrap() {
            AgentTurn::Final(v) => {
                assert_eq!(v["usage"]["cache_read_input_tokens"], 900);
                assert_eq!(v["usage"]["cache_creation_input_tokens"], 100);
            }
            other => panic!("expected final, got {other:?}"),
        }
        // The stored history itself stays unmarked; only the request carries it.
        assert!(session.messages[0]["content"].is_string());
    }

    #[tokio::test]
    async fn thinking_and_effort_pass_through_only_when_set() {
        let server = MockServer::start().await;
        let plain =
            AgentSession::start(&claude_cfg(&server, serde_json::json!({})), &ctx_with_key())
                .await
                .unwrap()
                .request_body();
        assert!(plain.get("thinking").is_none() && plain.get("output_config").is_none());

        let set = AgentSession::start(
            &claude_cfg(
                &server,
                serde_json::json!({ "thinking": "disabled", "effort": "low" }),
            ),
            &ctx_with_key(),
        )
        .await
        .unwrap()
        .request_body();
        assert_eq!(set["thinking"], serde_json::json!({ "type": "disabled" }));
        assert_eq!(set["output_config"], serde_json::json!({ "effort": "low" }));
    }

    #[tokio::test]
    async fn prompt_cache_can_be_turned_off() {
        let server = MockServer::start().await;
        let session = AgentSession::start(
            &claude_cfg(&server, serde_json::json!({ "prompt_cache": false })),
            &ctx_with_key(),
        )
        .await
        .unwrap();
        assert!(!session.request_body().to_string().contains("cache_control"));
    }

    #[tokio::test]
    async fn api_error_surfaces_status_and_message() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
                "type": "error",
                "error": { "type": "authentication_error", "message": "invalid x-api-key" },
            })))
            .mount(&server)
            .await;
        let cfg = tool_config(
            serde_json::json!({
                "framework": "claude", "model": "m", "prompt": "hi",
                "endpoint": server.uri(),
            }),
            true,
        );
        let mut session = AgentSession::start(&cfg, &ctx_with_key()).await.unwrap();
        let err = session.next_turn().await.unwrap_err().to_string();
        assert!(
            err.contains("401") && err.contains("invalid x-api-key"),
            "{err}"
        );
        assert!(
            !err.contains("test-key"),
            "API key must not leak into errors"
        );
    }
}
