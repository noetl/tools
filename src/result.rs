//! Tool execution result types.

use serde::{Deserialize, Serialize};

/// Status of a tool execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolStatus {
    /// Tool executed successfully.
    Success,
    /// Tool execution failed.
    Error,
    /// Tool execution timed out.
    Timeout,
}

impl ToolStatus {
    /// Returns true if the status indicates success.
    pub fn is_success(&self) -> bool {
        matches!(self, ToolStatus::Success)
    }

    /// Returns true if the status indicates an error.
    pub fn is_error(&self) -> bool {
        matches!(self, ToolStatus::Error)
    }

    /// Returns true if the status indicates a timeout.
    pub fn is_timeout(&self) -> bool {
        matches!(self, ToolStatus::Timeout)
    }
}

impl std::fmt::Display for ToolStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolStatus::Success => write!(f, "success"),
            ToolStatus::Error => write!(f, "error"),
            ToolStatus::Timeout => write!(f, "timeout"),
        }
    }
}

/// Result of a tool execution.
///
/// # `#[non_exhaustive]` — noetl/ai-meta#330
///
/// ⚠ Adding a public field to a struct that downstream crates build with a
/// **struct literal** is a breaking change by Rust's rules. Nothing in the
/// release pipeline knows that: semantic-release reads the commit prefix, so
/// `feat:` ships it as a MINOR. That is exactly what happened with
/// `child_execution_id` in 3.27.0 — it did not compile in `noetl-executor`, and
/// the breakage was *latent*, waiting for whoever next resolved a caret range.
///
/// `#[non_exhaustive]` forbids literal construction from outside this crate, so
/// a field addition is genuinely non-breaking from here on. The cost is one
/// breaking release now; the builders below are the replacement for the literal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ToolResult {
    /// Execution status.
    pub status: ToolStatus,

    /// Result data (tool-specific).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,

    /// Error message if status is Error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Standard output (for shell/script tools).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stdout: Option<String>,

    /// Standard error (for shell/script tools).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stderr: Option<String>,

    /// Exit code (for shell/script tools).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,

    /// Execution duration in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,

    /// `Tool::Container` (noetl/ai-meta#43 Round 3) sets this to
    /// `Some(true)` to signal that the tool created a long-running
    /// external work item (a K8s Job) and the playbook step's
    /// `call.done` will be emitted asynchronously by a separate
    /// callback path — the worker MUST NOT emit its own `call.done`
    /// when this is set.  The current `ToolResult.data` carries the
    /// in-flight handle (Job name + UID) for forensics + idempotency
    /// matching on the callback side.  All other tools omit the
    /// field (default `None`), which the worker treats as "emit
    /// `call.done` normally" — fully backward compatible.
    ///
    /// Worker-side adoption (recognising the marker + skipping the
    /// emit) is a coordinated follow-up tracked under the same
    /// umbrella (noetl/ai-meta#43) — until that lands, the worker
    /// will emit `call.done` immediately, and the watcher's later
    /// callback will be treated as stale by the server (the call.done
    /// already arrived; the server's
    /// `noetl_container_callback_stale_total` counter records the
    /// race).  That's harmless during the transition — playbooks
    /// just see early completion.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_callback: Option<bool>,

    /// The execution this tool spawned, when it spawned one
    /// (noetl/ai-meta#328).
    ///
    /// # Why this is a field and not a key in `data`
    ///
    /// A `kind: playbook` step with `return_result: true` **unwraps** the
    /// child's async-start envelope and returns the child's own payload
    /// instead. The envelope is the only place the child's `execution_id`
    /// appeared, so unwrapping it erased the one record of *which* execution
    /// this step spawned — leaving the parent's `call.done` naming no child.
    ///
    /// That is not cosmetic. When the child's own `parent_execution_id`
    /// column was lost (noetl/ai-meta#326), the spawn relationship had **no
    /// second copy anywhere in `noetl.*`** and ten executions were permanently
    /// unrecoverable. A playbook whose parent records the child survives that;
    /// one whose parent does not, does not.
    ///
    /// Carried beside `data` rather than inside it so the value reaches the
    /// event log without altering the payload a step's consumer parses — the
    /// child's result is the contract, and widening it to fix an observability
    /// gap would make every `return_result` caller's data shape depend on this.
    ///
    /// `None` for every tool that spawns nothing, which is all of them but one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub child_execution_id: Option<String>,
}

impl ToolResult {
    /// Create a successful result with data.
    pub fn success(data: serde_json::Value) -> Self {
        Self {
            status: ToolStatus::Success,
            data: Some(data),
            error: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            duration_ms: None,
            pending_callback: None,
            child_execution_id: None,
        }
    }

    /// Record the execution this tool spawned.
    ///
    /// ⚠ Chainable and explicit: a tool that spawns a child and does not call
    /// this produces a `call.done` that names no child, which is exactly the
    /// noetl/ai-meta#328 gap.
    pub fn with_child_execution_id(mut self, id: impl Into<String>) -> Self {
        self.child_execution_id = Some(id.into());
        self
    }

    /// Create an error result with message.
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: ToolStatus::Error,
            data: None,
            error: Some(message.into()),
            stdout: None,
            stderr: None,
            exit_code: None,
            duration_ms: None,
            pending_callback: None,
            child_execution_id: None,
        }
    }

    /// Create a timeout result.
    pub fn timeout(duration_seconds: u64) -> Self {
        Self {
            status: ToolStatus::Timeout,
            data: None,
            error: Some(format!(
                "Execution timed out after {} seconds",
                duration_seconds
            )),
            stdout: None,
            stderr: None,
            exit_code: None,
            duration_ms: Some(duration_seconds * 1000),
            pending_callback: None,
            child_execution_id: None,
        }
    }

    /// Create a result from shell command execution.
    pub fn from_shell(exit_code: i32, stdout: String, stderr: String) -> Self {
        let status = if exit_code == 0 {
            ToolStatus::Success
        } else {
            ToolStatus::Error
        };

        Self {
            status,
            data: Some(serde_json::json!({
                "exit_code": exit_code,
                "stdout": stdout,
                "stderr": stderr,
            })),
            error: if exit_code != 0 {
                Some(format!("Command exited with code {}", exit_code))
            } else {
                None
            },
            stdout: Some(stdout),
            stderr: Some(stderr),
            exit_code: Some(exit_code),
            duration_ms: None,
            pending_callback: None,
            child_execution_id: None,
        }
    }

    /// Set the execution duration.
    pub fn with_duration(mut self, duration_ms: u64) -> Self {
        self.duration_ms = Some(duration_ms);
        self
    }

    /// Set additional data on the result.
    /// Set the process exit code (noetl/ai-meta#330).
    ///
    /// Exists so a downstream crate can build any `ToolResult` it needs without
    /// a struct literal — which `#[non_exhaustive]` forbids, and which is the
    /// whole point: a field added here must stop being a breaking change.
    pub fn with_exit_code(mut self, exit_code: i32) -> Self {
        self.exit_code = Some(exit_code);
        self
    }

    /// Set the marker that suppresses the worker's own `call.done`
    /// (noetl/ai-meta#43 Round 4).
    pub fn with_pending_callback(mut self, pending: bool) -> Self {
        self.pending_callback = Some(pending);
        self
    }

    /// Attach an error message without changing the status.
    pub fn with_error(mut self, message: impl Into<String>) -> Self {
        self.error = Some(message.into());
        self
    }

    pub fn with_data(mut self, data: serde_json::Value) -> Self {
        self.data = Some(data);
        self
    }

    /// Returns true if the result indicates success.
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }
}

impl Default for ToolResult {
    fn default() -> Self {
        Self {
            status: ToolStatus::Success,
            data: None,
            error: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            duration_ms: None,
            pending_callback: None,
            child_execution_id: None,
        }
    }
}

#[cfg(test)]
mod tests {

    /// ⚠⚠ noetl/ai-meta#330. This is the guard, and it is a SOURCE guard because
    /// the property is about what *other crates* may do — which no test inside
    /// this crate can exercise. Inside the defining crate a literal is still
    /// legal, so a unit test would pass with or without the attribute.
    ///
    /// Removing `#[non_exhaustive]` silently restores the old hazard: a later
    /// `feat:` adding a field ships as a MINOR and fails to compile downstream,
    /// latently, for whoever next resolves a caret range.
    #[test]
    fn tool_result_is_sealed_against_downstream_literals() {
        let src = include_str!("result.rs");
        let body = src.split("#[cfg(test)]").next().unwrap();
        let marker = format!("#[non{}exhaustive]", "_");
        let at = body
            .find("pub struct ToolResult {")
            .expect("ToolResult not found — the extraction broke");
        // ⚠ Strip comments first. The doc comment on this very struct EXPLAINS
        // the attribute, so a substring search finds it whether or not the
        // attribute is there — this guard passed a mutation that removed it,
        // which is "comments counting as callers" in the guard itself.
        // Require the attribute on its own line instead.
        let window: Vec<&str> = body[at.saturating_sub(600)..at]
            .lines()
            .map(str::trim)
            .filter(|l| !l.starts_with("//"))
            .collect();
        assert!(
            window.iter().any(|l| *l == marker),
            "ToolResult must stay non_exhaustive: without it, adding a public \
             field is a BREAKING change that semantic-release publishes as a \
             minor (noetl/ai-meta#330)."
        );
    }

    /// Every field must be reachable through a builder, or sealing the struct
    /// just moves the breakage from "downstream cannot compile" to "downstream
    /// cannot express what it needs".
    #[test]
    fn every_field_is_reachable_without_a_literal() {
        let r = ToolResult::error("boom")
            .with_data(serde_json::json!({"k": "v"}))
            .with_exit_code(1)
            .with_duration(5)
            .with_pending_callback(true)
            .with_child_execution_id("42")
            .with_error("boom");
        assert_eq!(r.exit_code, Some(1));
        assert_eq!(r.duration_ms, Some(5));
        assert_eq!(r.pending_callback, Some(true));
        assert_eq!(r.child_execution_id.as_deref(), Some("42"));
        assert_eq!(r.error.as_deref(), Some("boom"));
        assert!(r.data.is_some());
        // stdout/stderr come from the shell constructor.
        let sh = ToolResult::from_shell(0, "out".into(), "err".into());
        assert_eq!(sh.stdout.as_deref(), Some("out"));
        assert_eq!(sh.stderr.as_deref(), Some("err"));
    }
    use super::*;

    #[test]
    fn test_tool_status_display() {
        assert_eq!(ToolStatus::Success.to_string(), "success");
        assert_eq!(ToolStatus::Error.to_string(), "error");
        assert_eq!(ToolStatus::Timeout.to_string(), "timeout");
    }

    #[test]
    fn test_tool_status_methods() {
        assert!(ToolStatus::Success.is_success());
        assert!(!ToolStatus::Success.is_error());
        assert!(ToolStatus::Error.is_error());
        assert!(ToolStatus::Timeout.is_timeout());
    }

    #[test]
    fn test_tool_result_success() {
        let result = ToolResult::success(serde_json::json!({"key": "value"}));
        assert!(result.is_success());
        assert!(result.data.is_some());
        assert!(result.error.is_none());
    }

    #[test]
    fn test_tool_result_error() {
        let result = ToolResult::error("something went wrong");
        assert!(!result.is_success());
        assert!(result.data.is_none());
        assert_eq!(result.error, Some("something went wrong".to_string()));
    }

    #[test]
    fn test_tool_result_from_shell() {
        let result = ToolResult::from_shell(0, "output".to_string(), "".to_string());
        assert!(result.is_success());
        assert_eq!(result.stdout, Some("output".to_string()));
        assert_eq!(result.exit_code, Some(0));

        let result = ToolResult::from_shell(1, "".to_string(), "error".to_string());
        assert!(!result.is_success());
        assert_eq!(result.exit_code, Some(1));
    }

    #[test]
    fn test_tool_result_serialization() {
        let result = ToolResult::success(serde_json::json!({"count": 42}));
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"status\":\"success\""));
        assert!(json.contains("\"count\":42"));
    }
}
