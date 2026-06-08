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
    /// If set, wait for completion and return status payload.
    #[serde(default)]
    pub return_step: Option<String>,
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

        // Async start mode
        if playbook_config.return_step.is_none() {
            return Ok(ToolResult::success(serde_json::json!({
                "status": start_result.status.unwrap_or_else(|| "started".to_string()),
                "execution_id": child_execution_id,
                "path": playbook_config.path,
                "async": true
            })));
        }

        // Blocking wait mode (python parity for `return_step`)
        let timeout_seconds = playbook_config.timeout.unwrap_or(300);
        let poll_interval = playbook_config.poll_interval.unwrap_or(2).max(1);
        let status_url = format!(
            "{}/api/executions/{}/status",
            ctx.server_url.trim_end_matches('/'),
            child_execution_id
        );
        let fallback_url = format!(
            "{}/api/executions/{}",
            ctx.server_url.trim_end_matches('/'),
            child_execution_id
        );

        let started = std::time::Instant::now();
        loop {
            if started.elapsed().as_secs() >= timeout_seconds {
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
                _ => match client.get(&fallback_url).send().await {
                    Ok(res) if res.status().is_success() => {
                        res.json::<serde_json::Value>().await.ok()
                    }
                    _ => None,
                },
            };

            if let Some(payload) = status_payload {
                if Self::is_terminal_status(&payload) {
                    return Ok(ToolResult::success(payload));
                }
            }
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
