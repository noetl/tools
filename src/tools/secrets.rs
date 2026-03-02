//! Secrets tools (`secrets` and `secret_manager`) compatibility layer.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SecretsConfig {
    /// Secret name/key.
    pub name: String,
    /// Provider name (currently only `env`).
    #[serde(default = "default_provider")]
    pub provider: String,
}

fn default_provider() -> String {
    "env".to_string()
}

async fn execute_secret_lookup(
    config: &ToolConfig,
    ctx: &ExecutionContext,
) -> Result<ToolResult, ToolError> {
    let template_engine = TemplateEngine::new();
    let rendered = template_engine.render_value(&config.config, &ctx.to_template_context())?;
    let parsed: SecretsConfig = serde_json::from_value(rendered)
        .map_err(|e| ToolError::Configuration(format!("Invalid secrets config: {}", e)))?;

    let provider = parsed.provider.to_lowercase();
    if provider != "env" {
        return Err(ToolError::ExecutionFailed(format!(
            "Secrets provider '{}' is not implemented in Rust worker; supported: env",
            parsed.provider
        )));
    }

    let value = std::env::var(&parsed.name).map_err(|_| {
        ToolError::ExecutionFailed(format!("Secret '{}' not found in environment", parsed.name))
    })?;

    Ok(ToolResult::success(serde_json::json!({
        "name": parsed.name,
        "provider": provider,
        "value": value
    })))
}

/// `secrets` tool implementation.
pub struct SecretsTool;

impl SecretsTool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for SecretsTool {
    fn name(&self) -> &'static str {
        "secrets"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        execute_secret_lookup(config, ctx).await
    }
}

/// `secret_manager` compatibility alias.
pub struct SecretManagerTool;

impl SecretManagerTool {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Tool for SecretManagerTool {
    fn name(&self) -> &'static str {
        "secret_manager"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        execute_secret_lookup(config, ctx).await
    }
}
