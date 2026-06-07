//! `artifact` tool — get-only alias for [`super::ResultFetchTool`].
//!
//! Tracks [noetl/tools#34](https://github.com/noetl/tools/issues/34) /
//! [noetl/ai-meta#64](https://github.com/noetl/ai-meta/issues/64).
//!
//! ## Why this exists
//!
//! The Python noetl worker shipped a `kind: artifact` tool kind with
//! `action: get` + `input.result_ref` for lazy artifact retrieval (see
//! `repos/noetl/noetl/worker/nats_worker.py:2920`).  Several e2e
//! fixtures use this shape — `test_output_select.yaml`,
//! `test_gcs_storage.yaml`, `test_storage_tiers.yaml` — and the Rust
//! noetl-tools registry returned `Tool not found: artifact` for them.
//!
//! Semantically `artifact get` is the same surface as
//! [`super::ResultFetchTool`]: materialise a stored result from a
//! `noetl://` ref URI.  Rather than have two parallel implementations,
//! this tool is a thin adapter — it translates the Python-era YAML
//! shape into a [`super::ResultFetchTool`] call.
//!
//! ## Wire shape
//!
//! Playbook step (the shape the fixtures use):
//!
//! ```yaml
//! - step: lazy_load_full_data
//!   tool:
//!     - name: load_artifact
//!       kind: artifact
//!       action: get
//!       input:
//!         result_ref: "{{ start._ref }}"
//! ```
//!
//! ## Backend choice
//!
//! Same as [`super::ResultFetchTool`] — Flight first with HTTP
//! fallback by default.  All the same per-step knobs (`prefer`,
//! `flight_endpoint`, `bearer_token`, `tls_ca_path`,
//! `client_cert_path`, `client_key_path`) pass through unchanged.
//!
//! ## `action: put`
//!
//! Returns a clear configuration error.  Per
//! [`agents/rules/execution-model.md`][rule] the playbook-side push
//! surface is intentionally absent in the Rust path: a step's
//! result lands in the result store via the worker's `call.done`
//! emit (R-2.1), not via a tool kind invoked by the playbook
//! author.  Fixtures that need to "put" an artifact express it as
//! a normal step that returns the value; the worker materialises
//! it.
//!
//! [rule]: https://github.com/noetl/ai-meta/blob/main/agents/rules/execution-model.md

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

use super::ResultFetchTool;

/// Per-step config the playbook expresses.  Field names mirror the
/// Python tool's contract so the existing fixtures parse without
/// changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactConfig {
    /// `get` (supported) or `put` (returns a configuration error).
    /// Defaults to `get` to match the Python worker's default.
    #[serde(default = "default_action")]
    action: String,

    /// Required for `action: get`.  Carries the `result_ref` field
    /// alongside any other input the playbook author chose to
    /// attach (which is ignored here — only `result_ref` is read).
    input: ArtifactInput,

    /// Optional pass-throughs to [`super::ResultFetchTool`] — same
    /// names + semantics as that tool's config block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    flight_endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bearer_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tls_ca_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_cert_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    client_key_path: Option<String>,
}

fn default_action() -> String {
    "get".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ArtifactInput {
    /// `noetl://execution/<eid>/result/<step>/<id>` URI.  Same
    /// shape [`super::ResultFetchTool`] consumes under its `ref:`
    /// field; renamed here for parity with the Python fixtures.
    result_ref: String,
}

/// The artifact tool — a thin Tool impl that translates the
/// Python-era YAML shape into a [`super::ResultFetchTool`] call.
pub struct ArtifactTool {
    /// Delegate that does the actual fetch.  Cheap to hold —
    /// `ResultFetchTool::new()` builds one shared `reqwest::Client`
    /// + `TemplateEngine` at construction; per-call work is the
    /// per-call HTTP / Flight exchange.
    delegate: ResultFetchTool,
    /// Used to render the raw config (action + input.result_ref)
    /// before deserialising into [`ArtifactConfig`] — same pattern
    /// as `ResultFetchTool::parse_config`.
    template_engine: TemplateEngine,
}

impl Default for ArtifactTool {
    fn default() -> Self {
        Self::new()
    }
}

impl ArtifactTool {
    pub fn new() -> Self {
        Self {
            delegate: ResultFetchTool::new(),
            template_engine: TemplateEngine::new(),
        }
    }

    /// Translate the playbook's raw config into the synthetic
    /// `result_fetch`-shaped config the delegate consumes.
    ///
    /// On `action: get` returns `Ok(translated_config)`; on
    /// `action: put` returns `Err(ToolError::Configuration(...))`
    /// with a clear message pointing at the worker's `call.done`
    /// emit path.
    fn translate_config(&self, raw: &JsonValue) -> Result<JsonValue, ToolError> {
        // Deserialise into the typed [`ArtifactConfig`] so the
        // diagnostic message names the missing field if a fixture is
        // malformed.
        let cfg: ArtifactConfig = serde_json::from_value(raw.clone()).map_err(|e| {
            ToolError::Configuration(format!("Invalid artifact config: {e}"))
        })?;

        if cfg.action == "put" {
            return Err(ToolError::Configuration(
                "artifact action=put is not supported in the Rust path; \
                 a step's result lands in the result store via the worker's \
                 call.done emit (R-2.1), not via a tool kind invoked by the \
                 playbook author. See agents/rules/execution-model.md."
                    .to_string(),
            ));
        }
        if cfg.action != "get" {
            return Err(ToolError::Configuration(format!(
                "artifact action='{}' is not supported (only 'get' is implemented)",
                cfg.action
            )));
        }

        // Build the synthetic [`ResultFetchConfig`]-shaped JSON the
        // delegate's `parse_config` will deserialise.  Only fields
        // the operator explicitly set get copied through — defaults
        // come from `ResultFetchConfig`'s `#[serde(default)]` markers.
        let mut translated = json!({ "ref": cfg.input.result_ref });
        let obj = translated
            .as_object_mut()
            .expect("translated config is an object by construction");
        if let Some(v) = cfg.prefer {
            obj.insert("prefer".to_string(), JsonValue::String(v));
        }
        if let Some(v) = cfg.flight_endpoint {
            obj.insert("flight_endpoint".to_string(), JsonValue::String(v));
        }
        if let Some(v) = cfg.bearer_token {
            obj.insert("bearer_token".to_string(), JsonValue::String(v));
        }
        if let Some(v) = cfg.tls_ca_path {
            obj.insert("tls_ca_path".to_string(), JsonValue::String(v));
        }
        if let Some(v) = cfg.client_cert_path {
            obj.insert("client_cert_path".to_string(), JsonValue::String(v));
        }
        if let Some(v) = cfg.client_key_path {
            obj.insert("client_key_path".to_string(), JsonValue::String(v));
        }
        Ok(translated)
    }
}

#[async_trait]
impl Tool for ArtifactTool {
    fn name(&self) -> &'static str {
        "artifact"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        // Template-render first so `input.result_ref:
        // "{{ start._ref }}"` resolves to a concrete URI BEFORE we
        // deserialise (the typed deserialiser would otherwise see
        // the literal `{{ ... }}` placeholder string).
        let template_ctx = ctx.to_template_context();
        let rendered = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;
        let translated = self.translate_config(&rendered)?;

        // Synthesize a `ToolConfig` shaped for the delegate.  Kind
        // is rewritten to "result_fetch" so any diagnostic the
        // delegate emits self-describes correctly; the other
        // top-level fields (timeout / retry / auth) pass through
        // unchanged.
        let delegate_config = ToolConfig {
            kind: "result_fetch".to_string(),
            config: translated,
            timeout: config.timeout,
            retry: config.retry.clone(),
            auth: config.auth.clone(),
        };
        self.delegate.execute(&delegate_config, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ToolConfig;
    use serde_json::json;

    fn tool_config(value: JsonValue) -> ToolConfig {
        ToolConfig {
            kind: "artifact".to_string(),
            config: value,
            timeout: None,
            retry: None,
            auth: None,
        }
    }

    #[test]
    fn translate_get_with_ref_only() {
        let tool = ArtifactTool::new();
        let raw = json!({
            "action": "get",
            "input": { "result_ref": "noetl://execution/42/result/big_select/123" }
        });
        let out = tool.translate_config(&raw).expect("translate get");
        assert_eq!(out.get("ref").and_then(|v| v.as_str()),
                   Some("noetl://execution/42/result/big_select/123"));
        // No optional pass-throughs set.
        assert!(out.get("prefer").is_none());
        assert!(out.get("flight_endpoint").is_none());
        assert!(out.get("bearer_token").is_none());
    }

    #[test]
    fn translate_get_with_passthroughs() {
        let tool = ArtifactTool::new();
        let raw = json!({
            "action": "get",
            "input": { "result_ref": "noetl://execution/1/result/s/1" },
            "prefer": "http",
            "flight_endpoint": "grpc://noetl.svc.cluster.local:8083",
            "bearer_token": "flight-token-alias",
            "tls_ca_path": "/etc/noetl/ca.pem",
            "client_cert_path": "/etc/noetl/worker.crt",
            "client_key_path": "/etc/noetl/worker.key",
        });
        let out = tool.translate_config(&raw).expect("translate get");
        assert_eq!(out.get("ref").and_then(|v| v.as_str()),
                   Some("noetl://execution/1/result/s/1"));
        assert_eq!(out.get("prefer").and_then(|v| v.as_str()), Some("http"));
        assert_eq!(out.get("flight_endpoint").and_then(|v| v.as_str()),
                   Some("grpc://noetl.svc.cluster.local:8083"));
        assert_eq!(out.get("bearer_token").and_then(|v| v.as_str()),
                   Some("flight-token-alias"));
        assert_eq!(out.get("tls_ca_path").and_then(|v| v.as_str()),
                   Some("/etc/noetl/ca.pem"));
        assert_eq!(out.get("client_cert_path").and_then(|v| v.as_str()),
                   Some("/etc/noetl/worker.crt"));
        assert_eq!(out.get("client_key_path").and_then(|v| v.as_str()),
                   Some("/etc/noetl/worker.key"));
    }

    #[test]
    fn translate_defaults_action_to_get() {
        let tool = ArtifactTool::new();
        // Python worker treats missing `action:` as `"get"`.
        let raw = json!({
            "input": { "result_ref": "noetl://execution/1/result/s/1" }
        });
        let out = tool.translate_config(&raw).expect("translate default");
        assert_eq!(out.get("ref").and_then(|v| v.as_str()),
                   Some("noetl://execution/1/result/s/1"));
    }

    #[test]
    fn translate_put_returns_configuration_error_pointing_at_emit_path() {
        let tool = ArtifactTool::new();
        let raw = json!({
            "action": "put",
            "input": { "result_ref": "noetl://execution/1/result/s/1" }
        });
        let err = tool.translate_config(&raw).expect_err("put must be rejected");
        match err {
            ToolError::Configuration(msg) => {
                assert!(msg.contains("put is not supported"));
                // The message points the operator at the right place.
                assert!(msg.contains("call.done emit"));
            }
            other => panic!("expected ToolError::Configuration, got {other:?}"),
        }
    }

    #[test]
    fn translate_unknown_action_is_rejected() {
        let tool = ArtifactTool::new();
        let raw = json!({
            "action": "rotate",
            "input": { "result_ref": "noetl://execution/1/result/s/1" }
        });
        let err = tool.translate_config(&raw).expect_err("unknown action");
        match err {
            ToolError::Configuration(msg) => {
                assert!(msg.contains("rotate"));
                assert!(msg.contains("only 'get' is implemented"));
            }
            other => panic!("expected ToolError::Configuration, got {other:?}"),
        }
    }

    #[test]
    fn translate_missing_input_returns_configuration_error() {
        let tool = ArtifactTool::new();
        // No `input:` block at all — the typed deserialiser names
        // the missing field for the operator.
        let raw = json!({ "action": "get" });
        let err = tool.translate_config(&raw).expect_err("missing input");
        match err {
            ToolError::Configuration(msg) => {
                assert!(msg.contains("input"));
            }
            other => panic!("expected ToolError::Configuration, got {other:?}"),
        }
    }

    #[test]
    fn tool_name_is_artifact() {
        let tool = ArtifactTool::new();
        assert_eq!(tool.name(), "artifact");
    }

    #[test]
    fn tool_config_round_trip_translation() {
        let tool = ArtifactTool::new();
        let raw = json!({
            "action": "get",
            "input": { "result_ref": "noetl://execution/9/result/lazy/1" }
        });
        let tc = tool_config(raw.clone());
        // The tool's translate_config consumes the inner config Value,
        // not the wrapping ToolConfig — verify the wrapping doesn't
        // affect the translation shape.
        let out = tool.translate_config(&tc.config).expect("ok");
        assert_eq!(out.get("ref").and_then(|v| v.as_str()),
                   Some("noetl://execution/9/result/lazy/1"));
    }
}
