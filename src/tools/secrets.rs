//! Secrets tools (`secrets` and `secret_manager`) compatibility layer.
//!
//! Dispatches on the config's `provider` field to a backend in
//! [`crate::secrets`] (Secrets Wallet Phase 3, noetl/ai-meta#61). `env` reads
//! the process environment (dev / local). `gcp` resolves against Google Secret
//! Manager via Workload Identity.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::secrets::{GcpSecretManager, SecretProvider, SecretRef};
use crate::template::TemplateEngine;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SecretsConfig {
    /// Secret name/key, or a fully-qualified provider resource path.
    pub name: String,
    /// Provider name (`env`, `gcp`).
    #[serde(default = "default_provider")]
    pub provider: String,
    /// Project / vault scope (provider-specific). For `gcp`, the project id;
    /// falls back to `GOOGLE_CLOUD_PROJECT` when omitted.
    #[serde(default)]
    pub project: Option<String>,
    /// Secret version / stage; defaults to the provider's "latest".
    #[serde(default)]
    pub version: Option<String>,
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
    match provider.as_str() {
        "env" => {
            let value = std::env::var(&parsed.name).map_err(|_| {
                ToolError::ExecutionFailed(format!(
                    "Secret '{}' not found in environment",
                    parsed.name
                ))
            })?;
            Ok(ToolResult::success(serde_json::json!({
                "name": parsed.name,
                "provider": "env",
                "value": value
            })))
        }
        "gcp" | "gcp_secret_manager" | "google_secret_manager" => {
            let sm = GcpSecretManager::from_env()?;
            let secret_ref = SecretRef {
                name: parsed.name.clone(),
                project: parsed.project.clone(),
                version: parsed.version.clone(),
            };
            let resolved = sm.fetch(&secret_ref).await?;
            let mut out = serde_json::json!({
                "name": parsed.name,
                "provider": "gcp",
                "value": resolved.value,
            });
            if let Some(v) = resolved.version {
                out["version"] = serde_json::Value::String(v);
            }
            Ok(ToolResult::success(out))
        }
        other => Err(ToolError::ExecutionFailed(format!(
            "Secrets provider '{}' is not implemented in Rust worker; supported: env, gcp",
            other
        ))),
    }
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
