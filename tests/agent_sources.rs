//! `kind: mcp` auth/headers/on_provider_error, and `kind: agent` external
//! call-outs: `framework: remote` (NoETL agent protocol and A2A) and the
//! `mcp_servers:` / `agents:` tool sources the registry `AgentTool` runs
//! itself (so they work on the cluster worker, not only in the CLI).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use noetl_tools::registry::{AuthConfig, AuthType, Tool, ToolConfig};
use noetl_tools::tools::{AgentTool, McpClient, McpTool};
use noetl_tools::ExecutionContext;
use serde_json::{json, Value};
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn cfg(kind: &str, body: Value, auth: Option<AuthConfig>) -> ToolConfig {
    ToolConfig {
        kind: kind.into(),
        config: body,
        timeout: Some(10),
        retry: None,
        auth,
    }
}

fn bearer(alias: &str) -> AuthConfig {
    AuthConfig {
        auth_type: AuthType::Bearer,
        credential: Some(alias.into()),
        token: None,
        username: None,
        password: None,
        header: None,
        scopes: None,
    }
}

fn api_key(alias: &str) -> AuthConfig {
    AuthConfig {
        auth_type: AuthType::ApiKey,
        credential: Some(alias.into()),
        ..bearer(alias)
    }
}

fn ctx() -> ExecutionContext {
    let mut c = ExecutionContext::default();
    c.set_secret("mcp_token", "s3cret");
    c.set_secret("anthropic", "test-key");
    c.set_secret("peer_token", "peer-s3cret");
    c
}

/// A minimal stateless MCP server: initialize, tools/list (two tools),
/// tools/call echoing `lookup` and failing `broken` with isError.
async fn mcp_server(require_bearer: Option<&str>) -> MockServer {
    let server = MockServer::start().await;
    let auth = require_bearer.map(|t| format!("Bearer {t}"));
    let with_auth = |m: wiremock::MockBuilder| match &auth {
        Some(a) => m.and(header("authorization", a.as_str())),
        None => m,
    };
    with_auth(Mock::given(method("POST")).and(path("/mcp")))
        .and(body_partial_json(json!({"method": "initialize"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {"protocolVersion": "2025-03-26", "capabilities": {"tools": {}}, "serverInfo": {"name": "stub"}}
        })))
        .mount(&server)
        .await;
    with_auth(Mock::given(method("POST")).and(path("/mcp")))
        .and(body_partial_json(json!({"method": "notifications/initialized"})))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;
    with_auth(Mock::given(method("POST")).and(path("/mcp")))
        .and(body_partial_json(json!({"method": "tools/list"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 2,
            "result": {"tools": [
                {"name": "graph.lookup", "description": "Look a node up.",
                 "inputSchema": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}},
                {"name": "broken", "description": "Always fails.", "inputSchema": {"type": "object"}}
            ]}
        })))
        .mount(&server)
        .await;
    with_auth(Mock::given(method("POST")).and(path("/mcp")))
        .and(body_partial_json(json!({"method": "tools/call", "params": {"name": "graph.lookup"}})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 3,
            "result": {"content": [{"type": "text", "text": "node r1: router, up"}], "isError": false}
        })))
        .mount(&server)
        .await;
    with_auth(Mock::given(method("POST")).and(path("/mcp")))
        .and(body_partial_json(json!({"method": "tools/call", "params": {"name": "broken"}})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 3,
            "result": {"content": [{"type": "text", "text": "backend down"}], "isError": true}
        })))
        .mount(&server)
        .await;
    server
}

// ---------------------------------------------------------------------------
// kind: mcp
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mcp_step_sends_auth_and_headers_and_reports_only_header_names() {
    let server = mcp_server(Some("s3cret")).await;
    let res = McpTool::new()
        .execute(
            &cfg(
                "mcp",
                json!({
                    "endpoint": format!("{}/mcp", server.uri()),
                    "method": "tools/call", "tool": "graph.lookup", "arguments": {"id": "r1"},
                    "headers": {"X-Tenant": "acme", "X-Empty": ""}
                }),
                Some(bearer("mcp_token")),
            ),
            &ctx(),
        )
        .await
        .unwrap();
    let data = res.data.unwrap();
    assert_eq!(data["status"], "ok", "{data}");
    assert_eq!(data["text"], "node r1: router, up");
    assert_eq!(data["sent_headers"], json!(["authorization", "x-tenant"]));
    assert!(!data.to_string().contains("s3cret"), "credential leaked: {data}");
    // X-Tenant reached the server.
    let reqs = server.received_requests().await.unwrap();
    assert!(reqs
        .iter()
        .all(|r| r.headers.get("x-tenant").map(|v| v.to_str().unwrap()) == Some("acme")));
}

#[tokio::test]
async fn mcp_step_without_the_credential_is_rejected_by_the_server() {
    let server = mcp_server(Some("s3cret")).await;
    let res = McpTool::new()
        .execute(
            &cfg(
                "mcp",
                json!({"endpoint": format!("{}/mcp", server.uri()), "method": "tools/list"}),
                None,
            ),
            &ctx(),
        )
        .await
        .unwrap();
    // No auth → the stub has no matching mock → 404 → status: error (continue).
    assert_eq!(res.data.unwrap()["status"], "error");
}

#[tokio::test]
async fn on_provider_error_fail_fails_the_step_on_is_error_and_on_transport_errors() {
    let server = mcp_server(None).await;
    let tool = McpTool::new();
    let base = json!({
        "endpoint": format!("{}/mcp", server.uri()),
        "method": "tools/call", "tool": "broken", "arguments": {},
    });
    // continue (default): the result carries isError for the playbook to read.
    let ok = tool.execute(&cfg("mcp", base.clone(), None), &ctx()).await.unwrap();
    assert_eq!(ok.data.unwrap()["result"]["isError"], true);

    let mut fail = base.clone();
    fail["on_provider_error"] = json!("fail");
    let err = tool.execute(&cfg("mcp", fail, None), &ctx()).await.unwrap_err();
    assert!(err.to_string().contains("isError"), "{err}");

    let err = tool
        .execute(
            &cfg(
                "mcp",
                json!({"endpoint": "http://127.0.0.1:9/mcp", "method": "tools/list", "on_provider_error": "fail"}),
                None,
            ),
            &ctx(),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("failed"), "{err}");

    let mut typo = base;
    typo["on_provider_error"] = json!("fial");
    assert!(tool.execute(&cfg("mcp", typo, None), &ctx()).await.is_err());
}

#[tokio::test]
async fn mcp_reserved_headers_are_refused() {
    let res = McpTool::new()
        .execute(
            &cfg(
                "mcp",
                json!({"endpoint": "http://127.0.0.1:9/mcp", "headers": {"Mcp-Session-Id": "x"}}),
                None,
            ),
            &ctx(),
        )
        .await;
    assert!(res.unwrap_err().to_string().contains("set by the tool"));
}

#[tokio::test]
async fn mcp_client_lists_and_calls() {
    let server = mcp_server(None).await;
    let mut c = McpClient::new(
        &format!("{}/mcp", server.uri()),
        Default::default(),
        std::time::Duration::from_secs(5),
    )
    .unwrap();
    let tools = c.list_tools().await.unwrap();
    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0].name, "graph.lookup");
    let (text, _, is_error) = c.call_tool("graph.lookup", json!({"id": "r1"})).await.unwrap();
    assert_eq!((text.as_str(), is_error), ("node r1: router, up", false));
}

// ---------------------------------------------------------------------------
// kind: agent — tool sources run by the registry AgentTool
// ---------------------------------------------------------------------------

/// A scripted Anthropic Messages API: turn 1 asks for `tool`, turn 2 answers
/// with the tool_result content it was given.
struct ScriptedModel {
    tool: String,
    input: Value,
    calls: Arc<AtomicUsize>,
}

impl Respond for ScriptedModel {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        if n == 0 {
            return ResponseTemplate::new(200).set_body_json(json!({
                "content": [{"type": "tool_use", "id": "tu_1", "name": self.tool, "input": self.input}],
                "stop_reason": "tool_use",
                "usage": {"input_tokens": 10, "output_tokens": 5}
            }));
        }
        // Echo the last tool_result back as the final answer.
        let last = body["messages"].as_array().unwrap().last().unwrap().clone();
        let block = &last["content"][0];
        let answer = format!(
            "tool said: {} (is_error={})",
            block["content"].as_str().unwrap_or(""),
            block["is_error"]
        );
        ResponseTemplate::new(200).set_body_json(json!({
            "content": [{"type": "text", "text": answer}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 20, "output_tokens": 8}
        }))
    }
}

async fn model(tool: &str, input: Value) -> (MockServer, Arc<AtomicUsize>) {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "test-key"))
        .respond_with(ScriptedModel {
            tool: tool.into(),
            input,
            calls: calls.clone(),
        })
        .mount(&server)
        .await;
    (server, calls)
}

#[tokio::test]
async fn registry_agent_runs_mcp_server_tools_itself() {
    let mcp = mcp_server(Some("s3cret")).await;
    let (llm, calls) = model("ehg__graph_lookup", json!({"id": "r1"})).await;
    let res = AgentTool::new()
        .execute(
            &cfg(
                "agent",
                json!({
                    "framework": "claude", "model": "m", "prompt": "What is r1?",
                    "endpoint": llm.uri(),
                    "mcp_servers": [{
                        "name": "ehg", "endpoint": format!("{}/mcp", mcp.uri()),
                        "auth": {"type": "bearer", "credential": "mcp_token"},
                        "tools": ["graph.lookup"]
                    }]
                }),
                Some(api_key("anthropic")),
            ),
            &ctx(),
        )
        .await
        .unwrap();
    let data = res.data.unwrap();
    assert_eq!(data["text"], "tool said: node r1: router, up (is_error=false)", "{data}");
    assert_eq!(data["source_tool_calls"], 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    // The allowlist kept `broken` away from the model.
    let first: Value =
        serde_json::from_slice(&llm.received_requests().await.unwrap()[0].body).unwrap();
    let names: Vec<&str> = first["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["ehg__graph_lookup"]);
}

#[tokio::test]
async fn an_mcp_is_error_goes_back_to_the_model_as_a_tool_error() {
    let mcp = mcp_server(None).await;
    let (llm, _) = model("ehg__broken", json!({})).await;
    let data = AgentTool::new()
        .execute(
            &cfg(
                "agent",
                json!({
                    "framework": "claude", "model": "m", "prompt": "go", "endpoint": llm.uri(),
                    "mcp_servers": [{"name": "ehg", "endpoint": format!("{}/mcp", mcp.uri())}]
                }),
                Some(api_key("anthropic")),
            ),
            &ctx(),
        )
        .await
        .unwrap()
        .data
        .unwrap();
    assert_eq!(data["text"], "tool said: backend down (is_error=true)");
}

#[tokio::test]
async fn registry_agent_refuses_playbook_bound_tools_and_forged_bindings() {
    let base = |tools: Value| {
        cfg(
            "agent",
            json!({"framework": "claude", "model": "m", "prompt": "p", "endpoint": "http://127.0.0.1:9", "tools": tools}),
            Some(api_key("anthropic")),
        )
    };
    let err = AgentTool::new()
        .execute(&base(json!([{"name": "t", "playbook": "x.yaml"}])), &ctx())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("bound to a playbook"), "{err}");
    let err = AgentTool::new()
        .execute(
            &base(json!([{"name": "t", "mcp_server": "ehg", "mcp_tool": "x"}])),
            &ctx(),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("reserved binding key"), "{err}");
}

#[tokio::test]
async fn shared_pool_agent_refuses_claude_code() {
    let err = AgentTool::for_shared_pool()
        .execute(
            &cfg("agent", json!({"framework": "claude-code", "model": "m", "prompt": "p"}), None),
            &ctx(),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not available on this runtime"), "{err}");
}

// ---------------------------------------------------------------------------
// Remote agents
// ---------------------------------------------------------------------------

/// A NoETL agent endpoint (`noetl-server` / `noetl serve`): send → task id,
/// first poll working, second completed.
async fn noetl_agent(require_bearer: Option<&str>) -> MockServer {
    let server = MockServer::start().await;
    let auth = require_bearer.map(|t| format!("Bearer {t}"));
    let with_auth = |m: wiremock::MockBuilder| match &auth {
        Some(a) => m.and(header("authorization", a.as_str())),
        None => m,
    };
    with_auth(Mock::given(method("GET")).and(path("/api/agents/cards/peer/ask")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "name": "peer-asker", "description": "Answers questions about the peer site.",
            "agent": "peer/ask",
            "inputSchema": {"type": "object", "properties": {"question": {"type": "string"}}, "required": ["question"]}
        })))
        .mount(&server)
        .await;
    with_auth(Mock::given(method("POST")).and(path("/api/agents/send")))
        .and(body_partial_json(json!({"agent": "peer/ask"})))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({"task_id": "42", "status": "submitted"})))
        .mount(&server)
        .await;
    with_auth(Mock::given(method("GET")).and(path("/api/agents/tasks/42")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"task_id": "42", "status": "working"})))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    with_auth(Mock::given(method("GET")).and(path("/api/agents/tasks/42")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "task_id": "42", "status": "completed", "agent": "peer/ask", "result": {"answer": "link 3 is flapping"}
        })))
        .with_priority(2)
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn framework_remote_noetl_sends_polls_and_returns_the_result() {
    let peer = noetl_agent(Some("peer-s3cret")).await;
    let data = AgentTool::new()
        .execute(
            &cfg(
                "agent",
                json!({
                    "framework": "remote",
                    "remote": {
                        "endpoint": peer.uri(), "protocol": "noetl", "agent": "peer/ask",
                        "auth": {"type": "bearer", "credential": "peer_token"}
                    },
                    "input": {"question": "why is it slow?"}
                }),
                None,
            ),
            &ctx(),
        )
        .await
        .unwrap()
        .data
        .unwrap();
    assert_eq!(data["status"], "completed", "{data}");
    assert_eq!(data["result"]["answer"], "link 3 is flapping");
    assert_eq!(data["task_id"], "42");
    let sent: Value = serde_json::from_slice(
        &peer
            .received_requests()
            .await
            .unwrap()
            .iter()
            .find(|r| r.url.path() == "/api/agents/send")
            .unwrap()
            .body,
    )
    .unwrap();
    assert_eq!(sent["input"]["question"], "why is it slow?");
}

#[tokio::test]
async fn framework_remote_fails_the_step_when_the_remote_task_fails() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/agents/send"))
        .respond_with(ResponseTemplate::new(422).set_body_json(json!({"error": "input.question is required"})))
        .mount(&server)
        .await;
    let err = AgentTool::new()
        .execute(
            &cfg(
                "agent",
                json!({"framework": "remote", "remote": {"endpoint": server.uri(), "agent": "peer/ask"}, "prompt": "hi"}),
                None,
            ),
            &ctx(),
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("rejected"), "{err}");
}

#[tokio::test]
async fn framework_remote_a2a_polls_a_task_to_completion() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "message/send"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 1,
            "result": {"kind": "task", "id": "t-9", "status": {"state": "working"}}
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({"method": "tasks/get", "params": {"id": "t-9"}})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 2,
            "result": {"kind": "task", "id": "t-9", "status": {"state": "completed"},
                       "artifacts": [{"parts": [{"kind": "text", "text": "42 is the answer"}]}]}
        })))
        .mount(&server)
        .await;
    let data = AgentTool::new()
        .execute(
            &cfg(
                "agent",
                json!({"framework": "remote", "remote": {"endpoint": server.uri(), "protocol": "a2a"}, "prompt": "meaning?"}),
                None,
            ),
            &ctx(),
        )
        .await
        .unwrap()
        .data
        .unwrap();
    assert_eq!(data["text"], "42 is the answer", "{data}");
    assert_eq!(data["task_id"], "t-9");
    let first: Value = serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
    assert_eq!(first["params"]["message"]["parts"][0]["text"], "meaning?");
}

#[tokio::test]
async fn a_remote_agent_is_a_tool_the_model_can_call() {
    let peer = noetl_agent(None).await;
    let (llm, _) = model("peer_ask", json!({"question": "status?"})).await;
    let data = AgentTool::new()
        .execute(
            &cfg(
                "agent",
                json!({
                    "framework": "claude", "model": "m", "prompt": "Ask the peer.", "endpoint": llm.uri(),
                    "agents": [{"name": "peer_ask", "endpoint": peer.uri(), "agent": "peer/ask"}]
                }),
                Some(api_key("anthropic")),
            ),
            &ctx(),
        )
        .await
        .unwrap()
        .data
        .unwrap();
    assert!(data["text"].as_str().unwrap().contains("link 3 is flapping"), "{data}");
    // The card's description and input schema were offered to the model.
    let first: Value = serde_json::from_slice(&llm.received_requests().await.unwrap()[0].body).unwrap();
    assert_eq!(first["tools"][0]["description"], "Answers questions about the peer site.");
    assert_eq!(first["tools"][0]["input_schema"]["required"], json!(["question"]));
}

#[tokio::test]
async fn framework_remote_rejects_misplaced_fields() {
    let tool = AgentTool::new();
    for (body, needle) in [
        (json!({"framework": "remote", "prompt": "p"}), "needs `remote:"),
        (json!({"framework": "remote", "remote": {"endpoint": "http://x", "protocol": "noetl"}}), "needs `agent`"),
        (json!({"framework": "remote", "remote": {"endpoint": "ftp://x", "agent": "a"}}), "http(s)"),
        (json!({"framework": "claude", "model": "m", "prompt": "p", "remote": {"endpoint": "http://x"}}), "belong to `framework: remote`"),
        (json!({"framework": "claude-code", "model": "m", "prompt": "p", "mcp_servers": [{"name": "a", "endpoint": "http://x"}]}), "mcp_config"),
    ] {
        let auth = (body["framework"] == "claude").then(|| api_key("anthropic"));
        let err = tool.execute(&cfg("agent", body.clone(), auth), &ctx()).await.unwrap_err();
        assert!(err.to_string().contains(needle), "{body}: {err}");
    }
}

#[tokio::test]
async fn framework_remote_reads_input_under_the_servers_args_name() {
    let peer = noetl_agent(None).await;
    let data = AgentTool::new()
        .execute(
            &cfg(
                "agent",
                json!({
                    "framework": "remote",
                    "remote": {"endpoint": peer.uri(), "agent": "peer/ask"},
                    "args": {"question": "via args"}
                }),
                None,
            ),
            &ctx(),
        )
        .await
        .unwrap()
        .data
        .unwrap();
    assert_eq!(data["status"], "completed", "{data}");
    let sent: Value = serde_json::from_slice(
        &peer.received_requests().await.unwrap().iter().find(|r| r.url.path() == "/api/agents/send").unwrap().body,
    )
    .unwrap();
    assert_eq!(sent["input"]["question"], "via args");
}
