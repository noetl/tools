//! No-op tool for control-flow compatible playbooks.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

/// No-op tool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoopConfig {
    /// Optional message.
    #[serde(default)]
    pub message: Option<String>,
    /// Optional explicit result payload.
    #[serde(default)]
    pub result: Option<serde_json::Value>,
}

/// No-op tool implementation.
pub struct NoopTool {
    template_engine: TemplateEngine,
}

impl NoopTool {
    /// Create a new no-op tool.
    pub fn new() -> Self {
        Self {
            template_engine: TemplateEngine::new(),
        }
    }
}

impl Default for NoopTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for NoopTool {
    fn name(&self) -> &'static str {
        "noop"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let rendered = self
            .template_engine
            .render_value(&config.config, &ctx.to_template_context())?;
        let parsed: NoopConfig = serde_json::from_value(rendered)
            .map_err(|e| ToolError::Configuration(format!("Invalid noop config: {}", e)))?;

        let data = if let Some(result) = parsed.result {
            result
        } else {
            serde_json::json!({
                "status": "ok",
                "message": parsed.message.unwrap_or_else(|| "noop".to_string())
            })
        };

        Ok(ToolResult::success(data))
    }
}
