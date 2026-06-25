//! Playbook tool for sub-playbook composition.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

/// Playbook tool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaybookConfig {
    /// Catalog path for the child playbook.
    #[serde(default)]
    pub path: Option<String>,
    /// Catalog id for the child playbook.
    #[serde(default)]
    pub catalog_id: Option<i64>,
    /// Optional specific version.
    #[serde(default)]
    pub version: Option<i64>,
    /// Optional explicit payload.
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
    /// Optional args payload (python parity).
    #[serde(default)]
    pub args: Option<serde_json::Value>,
    /// Optional parent execution id override.
    #[serde(default)]
    pub parent_execution_id: Option<i64>,
    /// Legacy: if set, wait for completion and return the child's
    /// `/status` poll payload (`{status, current_step, progress, ...}`).
    /// Kept verbatim for back-compat — every existing caller that sets
    /// `return_step` reads the status-shaped payload.  Use
    /// `return_result` to receive the child's actual result data instead.
    #[serde(default)]
    pub return_step: Option<String>,
    /// noetl/ai-meta#136 — when `true`, block until the child reaches a
    /// terminal state and return the child's *final result payload* (the
    /// data a downstream step reads as `{{ step }}`) instead of a status
    /// envelope.  This is what MCP-composition playbooks (e.g. the travel
    /// itinerary-planner dispatching `mcp/google-places`) need: the parent
    /// step must see the child's real output, not `{status, execution_id}`.
    ///
    /// Opt-in (default `false`) so the 100+ existing `kind: playbook`
    /// callers keep their current async / status-poll semantics untouched.
    /// The env kill-switch `NOETL_PLAYBOOK_AWAIT_CHILD_RESULT` (default
    /// `true`) lets operators revert this path to the legacy status payload
    /// without a redeploy.
    #[serde(default)]
    pub return_result: Option<bool>,
    /// noetl/ai-meta#136 — optional child step name whose result is the
    /// playbook's output.  MCP child playbooks funnel their data through a
    /// dispatch step (e.g. `google_places_dispatch`) before a terminal
    /// `noop` `end` step, so naming the step makes extraction
    /// deterministic.  When unset (and `return_result` is `true`) the tool
    /// falls back to the latest non-terminal `call.done` result.
    #[serde(default)]
    pub result_step: Option<String>,
    /// Timeout while waiting for completion.
    #[serde(default)]
    pub timeout: Option<u64>,
    /// Poll interval seconds.
    #[serde(default)]
    pub poll_interval: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecuteResponse {
    execution_id: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    commands_generated: Option<i32>,
}

/// Playbook composition tool.
pub struct PlaybookTool {
    template_engine: TemplateEngine,
}

impl PlaybookTool {
    /// Create a new playbook tool.
    pub fn new() -> Self {
        Self {
            template_engine: TemplateEngine::new(),
        }
    }

    fn parse_config(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<PlaybookConfig, ToolError> {
        let rendered = self
            .template_engine
            .render_value(&config.config, &ctx.to_template_context())?;
        serde_json::from_value(rendered)
            .map_err(|e| ToolError::Configuration(format!("Invalid playbook config: {}", e)))
    }

    fn build_payload(config: &PlaybookConfig) -> serde_json::Value {
        if let Some(args) = &config.args {
            args.clone()
        } else if let Some(payload) = &config.payload {
            payload.clone()
        } else {
            serde_json::json!({})
        }
    }

    /// Returns `true` when the status-endpoint payload indicates a terminal
    /// execution state and the polling loop should stop.
    ///
    /// The `/api/executions/{id}/status` endpoint returns a JSON object whose
    /// shape is:
    ///
    /// ```json
    /// {
    ///   "execution_id": 12345,
    ///   "status": "COMPLETED",
    ///   "current_step": null,
    ///   "progress": { "total_steps": 4, "completed_steps": 4, ... },
    ///   "is_cancelled": false
    /// }
    /// ```
    ///
    /// Terminal states are `COMPLETED`, `FAILED`, and `CANCELLED`.
    /// The `is_cancelled` flag is also honoured as a secondary signal because
    /// the `status` field may briefly read `"RUNNING"` while `is_cancelled`
    /// flips first.
    pub(crate) fn is_terminal_status(payload: &serde_json::Value) -> bool {
        let status_str = payload.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let is_cancelled = payload
            .get("is_cancelled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        matches!(status_str, "COMPLETED" | "FAILED" | "CANCELLED") || is_cancelled
    }

    /// Read the terminal status string out of a `/status` (or
    /// `/api/executions/{id}`) payload.  Returns the upper-cased status
    /// (`COMPLETED` / `FAILED` / `CANCELLED`) or `"CANCELLED"` when only
    /// the `is_cancelled` flag flipped.
    pub(crate) fn terminal_status_str(payload: &serde_json::Value) -> String {
        let status_str = payload
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_ascii_uppercase();
        if status_str.is_empty()
            && payload
                .get("is_cancelled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        {
            return "CANCELLED".to_string();
        }
        status_str
    }

    /// noetl/ai-meta#136 kill-switch.  Defaults ON: a `return_result: true`
    /// step resolves and returns the child's real result.  Set
    /// `NOETL_PLAYBOOK_AWAIT_CHILD_RESULT=false` (or `0`) to revert the
    /// `return_result` path to the legacy status payload without a
    /// redeploy.
    pub(crate) fn await_child_result_enabled() -> bool {
        match std::env::var("NOETL_PLAYBOOK_AWAIT_CHILD_RESULT") {
            Ok(v) => !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"),
            Err(_) => true,
        }
    }

    /// Extract the child step's *result payload* out of the JSON the
    /// `/api/executions/{id}` endpoint reports for a `call.done` event.
    ///
    /// The worker nests a step's tool output as
    /// `{... , result: {status, context: {status, data: <PAYLOAD>, ...}}}`
    /// (see `worker/src/executor/command.rs::build_call_done_result`), but
    /// the exact outer wrapping varies (some readers see `result_obj`
    /// directly, some see the full `{command_id, call_index, result}`
    /// envelope).  Rather than hard-code one path, descend through the
    /// known wrapper keys (`result` → `context`) up to a few layers, then
    /// return the inner `data` field — which is the serialised
    /// `ToolResult.data`, i.e. the value a sibling step reads as
    /// `{{ step }}`.
    pub(crate) fn extract_child_payload(event_result: &serde_json::Value) -> serde_json::Value {
        let mut current = event_result;
        for _ in 0..5 {
            let serde_json::Value::Object(map) = current else {
                break;
            };
            // `context` is the build_call_done_result wrapper around the
            // serialised ToolResult; prefer it.
            if let Some(inner) = map.get("context").filter(|v| v.is_object()) {
                current = inner;
                continue;
            }
            // `result` is the call.done event-payload wrapper
            // (`{command_id, call_index, result}`); descend into it.
            if let Some(inner) = map.get("result").filter(|v| v.is_object()) {
                current = inner;
                continue;
            }
            break;
        }
        // `current` is now the serialised ToolResult (`{status, data, ...}`)
        // or already the payload.  The child's real output is `data`.
        if let serde_json::Value::Object(map) = current {
            if let Some(data) = map.get("data") {
                return data.clone();
            }
        }
        current.clone()
    }

    /// Select the event carrying the child's final result from an
    /// `/api/executions/{id}` detail payload.  `events` is chronological
    /// (ASC), so scan from the end for the most recent match.
    ///
    /// - When `result_step` is named, return the latest `call.done` event
    ///   for that node (deterministic — what MCP composition relies on).
    /// - Otherwise return the latest `call.done` event that isn't the
    ///   terminal `playbook` node (the dispatch step that produced the
    ///   real output, skipping the trailing `noop` `end` step's empty
    ///   result when present).
    pub(crate) fn select_result_event<'a>(
        detail: &'a serde_json::Value,
        result_step: Option<&str>,
    ) -> Option<&'a serde_json::Value> {
        let events = detail.get("events").and_then(|v| v.as_array())?;
        let is_call_done = |ev: &serde_json::Value| {
            ev.get("event_type").and_then(|v| v.as_str()) == Some("call.done")
        };
        let has_payload = |ev: &serde_json::Value| {
            ev.get("result")
                .map(|r| !r.is_null() && Self::extract_child_payload(r) != serde_json::json!(null))
                .unwrap_or(false)
        };
        if let Some(step) = result_step {
            // Deterministic: latest call.done for the named node.
            if let Some(ev) = events.iter().rev().find(|ev| {
                ev.get("node_name").and_then(|v| v.as_str()) == Some(step) && is_call_done(ev)
            }) {
                return Some(ev);
            }
            // Fallback: any latest event for the named node with a payload.
            if let Some(ev) = events.iter().rev().find(|ev| {
                ev.get("node_name").and_then(|v| v.as_str()) == Some(step) && has_payload(ev)
            }) {
                return Some(ev);
            }
        }
        // No (or unmatched) result_step: latest non-terminal call.done.
        events
            .iter()
            .rev()
            .find(|ev| {
                is_call_done(ev)
                    && ev.get("node_name").and_then(|v| v.as_str()) != Some("playbook")
                    && has_payload(ev)
            })
            .or_else(|| {
                events
                    .iter()
                    .rev()
                    .find(|ev| is_call_done(ev) && has_payload(ev))
            })
    }
}

impl Default for PlaybookTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for PlaybookTool {
    fn name(&self) -> &'static str {
        "playbook"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let playbook_config = self.parse_config(config, ctx)?;
        if playbook_config.path.is_none() && playbook_config.catalog_id.is_none() {
            return Err(ToolError::Configuration(
                "Playbook execution requires `path` or `catalog_id`".to_string(),
            ));
        }

        if ctx.server_url.trim().is_empty() {
            return Err(ToolError::Configuration(
                "ExecutionContext.server_url is required for playbook tool".to_string(),
            ));
        }

        let payload_value = Self::build_payload(&playbook_config);
        let payload_obj: HashMap<String, serde_json::Value> =
            serde_json::from_value(payload_value).unwrap_or_default();

        let mut body = serde_json::json!({
            "payload": payload_obj
        });
        if let Some(path) = &playbook_config.path {
            body["path"] = serde_json::json!(path);
        }
        if let Some(catalog_id) = playbook_config.catalog_id {
            body["catalog_id"] = serde_json::json!(catalog_id);
        }
        if let Some(version) = playbook_config.version {
            body["version"] = serde_json::json!(version);
        }

        let parent_execution_id = playbook_config
            .parent_execution_id
            .unwrap_or(ctx.execution_id);
        if parent_execution_id > 0 {
            body["parent_execution_id"] = serde_json::json!(parent_execution_id);
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout.unwrap_or(30)))
            .build()
            .map_err(|e| ToolError::Http(e.to_string()))?;

        let execute_url = format!("{}/api/execute", ctx.server_url.trim_end_matches('/'));
        let response = client
            .post(&execute_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| ToolError::Http(e.to_string()))?;

        let response_status = response.status();
        if !response_status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(ToolError::Http(format!(
                "Failed to start child playbook: {} {}",
                response_status, body
            )));
        }

        let start_result: ExecuteResponse = response
            .json()
            .await
            .map_err(|e| ToolError::Json(e.to_string()))?;
        let child_execution_id = start_result.execution_id.clone();

        // noetl/ai-meta#136 — opt-in blocking-with-result mode.  Honoured
        // only when the env kill-switch is on (default); flipping it off
        // reverts `return_result` callers to the legacy status payload.
        let want_child_result = matches!(playbook_config.return_result, Some(true))
            && Self::await_child_result_enabled();

        // Async start mode — unchanged for every caller that neither sets
        // `return_step` (legacy status poll) nor `return_result` (#136).
        if playbook_config.return_step.is_none() && !want_child_result {
            return Ok(ToolResult::success(serde_json::json!({
                "status": start_result.status.unwrap_or_else(|| "started".to_string()),
                "execution_id": child_execution_id,
                "path": playbook_config.path,
                "async": true
            })));
        }

        // Blocking wait mode.  `return_result` (#136) defaults to a longer
        // timeout than the legacy `return_step` poll because composing MCP
        // children incurs several NATS hops + an external API call.
        let timeout_seconds =
            playbook_config
                .timeout
                .unwrap_or(if want_child_result { 180 } else { 300 });
        let poll_interval = playbook_config.poll_interval.unwrap_or(2).max(1);
        let detail_url = format!(
            "{}/api/executions/{}",
            ctx.server_url.trim_end_matches('/'),
            child_execution_id
        );
        let status_url = format!("{}/status", detail_url);

        let started = std::time::Instant::now();
        loop {
            if started.elapsed().as_secs() >= timeout_seconds {
                if want_child_result {
                    // #136 — don't silently hand back synthetic-looking
                    // empty data on timeout; surface it so the parent step
                    // fails loudly instead of rendering placeholders.
                    return Err(ToolError::Timeout(timeout_seconds));
                }
                return Ok(ToolResult::success(serde_json::json!({
                    "status": "timeout",
                    "execution_id": child_execution_id,
                    "timeout_seconds": timeout_seconds
                })));
            }

            tokio::time::sleep(Duration::from_secs(poll_interval)).await;

            let status_response = client.get(&status_url).send().await;
            let status_payload = match status_response {
                Ok(res) if res.status().is_success() => res.json::<serde_json::Value>().await.ok(),
                _ => match client.get(&detail_url).send().await {
                    Ok(res) if res.status().is_success() => {
                        res.json::<serde_json::Value>().await.ok()
                    }
                    _ => None,
                },
            };

            let Some(payload) = status_payload else {
                continue;
            };
            if !Self::is_terminal_status(&payload) {
                continue;
            }

            // Legacy `return_step` path (or kill-switch off): hand back the
            // status payload exactly as before.
            if !want_child_result {
                return Ok(ToolResult::success(payload));
            }

            // #136 blocking-with-result path.
            let terminal = Self::terminal_status_str(&payload);
            if terminal == "FAILED" || terminal == "CANCELLED" {
                let detail_msg = payload
                    .get("error")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        format!("child execution {} {}", child_execution_id, terminal)
                    });
                return Err(ToolError::ExecutionFailed(format!(
                    "child playbook {} ({}): {}",
                    playbook_config.path.as_deref().unwrap_or("?"),
                    child_execution_id,
                    detail_msg
                )));
            }

            // COMPLETED — fetch the full detail (the `/status` payload
            // carries no events) and resolve the child's final result.
            let detail: serde_json::Value = match client.get(&detail_url).send().await {
                Ok(res) if res.status().is_success() => res
                    .json()
                    .await
                    .map_err(|e| ToolError::Json(e.to_string()))?,
                Ok(res) => {
                    return Err(ToolError::Http(format!(
                        "GET {} returned {}",
                        detail_url,
                        res.status()
                    )));
                }
                Err(e) => return Err(ToolError::Http(e.to_string())),
            };

            let Some(event) =
                Self::select_result_event(&detail, playbook_config.result_step.as_deref())
            else {
                return Err(ToolError::ExecutionFailed(format!(
                    "child playbook {} completed but no result event found (result_step={:?})",
                    child_execution_id, playbook_config.result_step
                )));
            };
            let event_result = event
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let payload = Self::extract_child_payload(&event_result);

            tracing::debug!(
                child_execution_id = %child_execution_id,
                result_step = ?playbook_config.result_step,
                "playbook tool resolved child result (#136)"
            );
            return Ok(ToolResult::success(payload));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Terminal-status helper ---

    #[test]
    fn test_playbook_tool_terminates_on_completed_status() {
        let payload = serde_json::json!({
            "execution_id": 1,
            "status": "COMPLETED",
            "is_cancelled": false
        });
        assert!(
            PlaybookTool::is_terminal_status(&payload),
            "COMPLETED should be terminal"
        );
    }

    #[test]
    fn test_playbook_tool_terminates_on_failed_status() {
        let payload = serde_json::json!({
            "execution_id": 2,
            "status": "FAILED",
            "is_cancelled": false
        });
        assert!(
            PlaybookTool::is_terminal_status(&payload),
            "FAILED should be terminal"
        );
    }

    #[test]
    fn test_playbook_tool_terminates_on_cancelled_status() {
        let payload = serde_json::json!({
            "execution_id": 3,
            "status": "CANCELLED",
            "is_cancelled": true
        });
        assert!(
            PlaybookTool::is_terminal_status(&payload),
            "CANCELLED should be terminal"
        );
    }

    #[test]
    fn test_playbook_tool_terminates_on_is_cancelled_flag() {
        // status field still reads RUNNING but is_cancelled has already flipped —
        // the polling loop must honour this secondary signal.
        let payload = serde_json::json!({
            "execution_id": 4,
            "status": "RUNNING",
            "is_cancelled": true
        });
        assert!(
            PlaybookTool::is_terminal_status(&payload),
            "is_cancelled=true should be terminal even when status=RUNNING"
        );
    }

    #[test]
    fn test_playbook_tool_keeps_polling_on_running_status() {
        let payload = serde_json::json!({
            "execution_id": 5,
            "status": "RUNNING",
            "is_cancelled": false
        });
        assert!(
            !PlaybookTool::is_terminal_status(&payload),
            "RUNNING should NOT be terminal"
        );
    }

    #[test]
    fn test_playbook_tool_keeps_polling_when_status_missing() {
        // A payload with no status key (e.g. unexpected server shape) must not
        // trigger a spurious terminal exit — keep polling.
        let payload = serde_json::json!({
            "execution_id": 6
        });
        assert!(
            !PlaybookTool::is_terminal_status(&payload),
            "missing status key should NOT be terminal"
        );
    }

    // --- noetl/ai-meta#136 — blocking-with-result mode ---

    #[test]
    fn config_round_trips_return_result_and_result_step() {
        let cfg: PlaybookConfig = serde_json::from_value(serde_json::json!({
            "path": "automation/agents/mcp/google-places",
            "return_result": true,
            "result_step": "google_places_dispatch",
        }))
        .unwrap();
        assert_eq!(cfg.return_result, Some(true));
        assert_eq!(cfg.result_step.as_deref(), Some("google_places_dispatch"));
    }

    #[test]
    fn config_defaults_keep_async_semantics() {
        // No return_step + no return_result → the async path is taken
        // (return_result defaults to None, not Some(true)).
        let cfg: PlaybookConfig = serde_json::from_value(serde_json::json!({
            "path": "automation/agents/mcp/firestore",
        }))
        .unwrap();
        assert!(cfg.return_step.is_none());
        assert_ne!(cfg.return_result, Some(true));
    }

    #[test]
    fn terminal_status_str_reads_status_and_cancel_flag() {
        assert_eq!(
            PlaybookTool::terminal_status_str(&serde_json::json!({"status": "completed"})),
            "COMPLETED"
        );
        assert_eq!(
            PlaybookTool::terminal_status_str(&serde_json::json!({"status": "FAILED"})),
            "FAILED"
        );
        // status absent but is_cancelled flipped → CANCELLED
        assert_eq!(
            PlaybookTool::terminal_status_str(&serde_json::json!({"is_cancelled": true})),
            "CANCELLED"
        );
    }

    /// Full nested shape exactly as `/api/executions/{id}` reports a
    /// `call.done` event's `result` field: the call.done payload wrapper
    /// (`{command_id, call_index, result}`) around the
    /// build_call_done_result wrapper (`{status, context}`) around the
    /// serialised `ToolResult` (`{status, data: PAYLOAD}`).  The extracted
    /// value must be PAYLOAD — what a sibling step reads as `{{ step }}`.
    #[test]
    fn extract_child_payload_unwraps_full_call_done_nesting() {
        let event_result = serde_json::json!({
            "command_id": "123",
            "call_index": 0,
            "result": {
                "status": "COMPLETED",
                "context": {
                    "status": "success",
                    "data": {
                        "status": "ok",
                        "data": { "items": [ {"id": "ChIJ-real-place"} ] },
                        "summary": "1 Google place(s)"
                    }
                }
            }
        });
        let payload = PlaybookTool::extract_child_payload(&event_result);
        // PAYLOAD is the google-places python `result` dict.
        assert_eq!(payload["status"], "ok");
        assert_eq!(payload["data"]["items"][0]["id"], "ChIJ-real-place");
    }

    /// Exact shape captured from a live kind `/api/executions/{id}`
    /// `call.done` event (google-places `tools/list`).  The real wrapping
    /// is one layer deeper than the minimal case: an outer `context`
    /// envelope around `{call_index, command_id, result}`, then the inner
    /// build_call_done_result `{status, context: {status, data}}`.  The
    /// iterative descent must still land on the python `data` payload.
    #[test]
    fn extract_child_payload_matches_live_kind_call_done_shape() {
        let event_result = serde_json::json!({
            "status": "COMPLETED",
            "context": {
                "call_index": 0,
                "command_id": "328447056660140032:google_places_dispatch:328447057243148289",
                "result": {
                    "status": "success",
                    "context": {
                        "status": "success",
                        "data": {
                            "status": "ok",
                            "data": { "items": [ {"id": "ChIJ-paris"} ], "calls_made": 1 },
                            "summary": "1 Google place(s)"
                        }
                    }
                }
            }
        });
        let payload = PlaybookTool::extract_child_payload(&event_result);
        // The extracted value is the google-places python `result` dict.
        assert_eq!(payload["status"], "ok");
        assert_eq!(payload["data"]["items"][0]["id"], "ChIJ-paris");
        assert_eq!(payload["data"]["calls_made"], 1);
    }

    #[test]
    fn extract_child_payload_unwraps_result_obj_only_shape() {
        // Some readers see result_obj directly (no command_id wrapper).
        let event_result = serde_json::json!({
            "status": "success",
            "context": { "status": "success", "data": { "ok": true, "items": [] } }
        });
        let payload = PlaybookTool::extract_child_payload(&event_result);
        assert_eq!(payload["ok"], true);
        assert!(payload["items"].is_array());
    }

    #[test]
    fn extract_child_payload_handles_already_flat_payload() {
        let event_result = serde_json::json!({ "data": { "x": 1 } });
        let payload = PlaybookTool::extract_child_payload(&event_result);
        assert_eq!(payload["x"], 1);
    }

    fn detail_with_events(events: serde_json::Value) -> serde_json::Value {
        serde_json::json!({ "execution_id": 42, "status": "COMPLETED", "events": events })
    }

    #[test]
    fn select_result_event_prefers_named_result_step() {
        let detail = detail_with_events(serde_json::json!([
            {"event_type": "call.done", "node_name": "google_places_dispatch",
             "result": {"status": "success", "context": {"status": "success", "data": {"hit": "places"}}}},
            {"event_type": "call.done", "node_name": "end",
             "result": {"status": "success", "context": {"status": "success", "data": {}}}},
            {"event_type": "playbook.completed", "node_name": "playbook",
             "result": {"status": "COMPLETED"}}
        ]));
        let ev =
            PlaybookTool::select_result_event(&detail, Some("google_places_dispatch")).unwrap();
        let payload = PlaybookTool::extract_child_payload(ev.get("result").unwrap());
        assert_eq!(payload["hit"], "places");
    }

    #[test]
    fn select_result_event_skips_terminal_when_no_step_named() {
        // No result_step → latest non-`playbook` call.done.  The terminal
        // playbook.completed event (just `{status: COMPLETED}`) is skipped.
        let detail = detail_with_events(serde_json::json!([
            {"event_type": "call.done", "node_name": "duffel_dispatch",
             "result": {"status": "success", "context": {"status": "success", "data": {"offers": [1, 2]}}}},
            {"event_type": "playbook.completed", "node_name": "playbook",
             "result": {"status": "COMPLETED"}}
        ]));
        let ev = PlaybookTool::select_result_event(&detail, None).unwrap();
        let payload = PlaybookTool::extract_child_payload(ev.get("result").unwrap());
        assert_eq!(payload["offers"], serde_json::json!([1, 2]));
    }

    #[test]
    fn select_result_event_none_when_no_call_done() {
        let detail = detail_with_events(serde_json::json!([
            {"event_type": "playbook.initialized", "node_name": "playbook", "result": null}
        ]));
        assert!(PlaybookTool::select_result_event(&detail, None).is_none());
    }

    #[test]
    fn await_child_result_enabled_default_on_and_kill_switch() {
        // Default (var unset) is ON.  We can't safely mutate process env
        // in parallel tests, so just assert the default branch holds when
        // the var is absent in this test process.
        if std::env::var("NOETL_PLAYBOOK_AWAIT_CHILD_RESULT").is_err() {
            assert!(PlaybookTool::await_child_result_enabled());
        }
    }

    #[test]
    fn test_playbook_tool_keeps_polling_on_started_status() {
        // STARTED is an intermediate state emitted during command dispatch —
        // not a terminal state.
        let payload = serde_json::json!({
            "execution_id": 7,
            "status": "STARTED",
            "is_cancelled": false
        });
        assert!(
            !PlaybookTool::is_terminal_status(&payload),
            "STARTED should NOT be terminal"
        );
    }
}
