//! Python script execution tool.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::{ToolResult, ToolStatus};
use crate::template::TemplateEngine;

/// Sentinel string the wrapper script uses to mark the JSON line
/// carrying the user-set `result` global.  Chosen to be improbable
/// in real user output.  See `extract_result_from_stdout` for the
/// parsing side.
const NOETL_RESULT_MARKER: &str = "@@__NOETL_RESULT__@@";

/// Find the wrapper-emitted result line in stdout and return
/// `(captured_result, cleaned_stdout)`.  If no marker is present
/// (e.g. the wrapper failed before emit), returns `(None, stdout)`.
///
/// Side effect: the marker line is stripped from the cleaned stdout
/// so the user-visible output stays clean.
fn extract_result_from_stdout(stdout: &str) -> (Option<serde_json::Value>, String) {
    let mut kept_lines = Vec::new();
    let mut captured: Option<serde_json::Value> = None;
    for line in stdout.lines() {
        if let Some(rest) = line.strip_prefix(NOETL_RESULT_MARKER) {
            // Last marker wins if the user code is weird enough to
            // emit multiple — keeps the wrapper's emit authoritative.
            captured = serde_json::from_str(rest).ok();
            continue;
        }
        kept_lines.push(line);
    }
    // Preserve original trailing-newline shape.
    let mut cleaned = kept_lines.join("\n");
    if stdout.ends_with('\n') && !cleaned.is_empty() {
        cleaned.push('\n');
    }
    (captured, cleaned)
}

/// Python tool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonConfig {
    /// Python code to execute.
    pub code: String,

    /// Arguments passed to the script (available as 'args' dict).
    #[serde(default)]
    pub args: HashMap<String, serde_json::Value>,

    /// Python interpreter to use (default: "python3").
    #[serde(default = "default_python")]
    pub python: String,

    /// Additional environment variables.
    #[serde(default)]
    pub env: HashMap<String, String>,

    /// Timeout in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,

    /// Working directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}

fn default_python() -> String {
    std::env::var("PYTHON_PATH").unwrap_or_else(|_| "python3".to_string())
}

/// Python script execution tool.
///
/// Executes Python code in a subprocess with JSON protocol:
/// - Script receives context on stdin as JSON
/// - Script should print result as JSON to stdout
/// - Exit code determines success/failure
pub struct PythonTool {
    template_engine: TemplateEngine,
}

impl PythonTool {
    /// Create a new Python tool.
    pub fn new() -> Self {
        Self {
            template_engine: TemplateEngine::new(),
        }
    }

    /// Execute Python code.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_code(
        &self,
        code: &str,
        args: &HashMap<String, serde_json::Value>,
        env: &HashMap<String, String>,
        python: &str,
        cwd: Option<&str>,
        timeout_duration: Option<Duration>,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let start = std::time::Instant::now();

        // Create wrapper script that handles JSON I/O.
        //
        // The wrapper emits the user-set `result` global as a JSON
        // line prefixed with the `NOETL_RESULT_MARKER` sentinel.
        // The tool's stdout parser (below) finds that line, parses
        // the rest as JSON, and strips it from the visible stdout
        // before exposing the result to downstream consumers.  This
        // preserves user-side `print(...)` calls (visible in stdout
        // for debugging) while also capturing the structured
        // `result` value the orchestrator needs for
        // `{{ step_name.field }}` references in next.arcs.
        //
        // The marker string is intentionally improbable in real
        // output; user code that legitimately wants to print this
        // exact substring would conflict, but the probability is
        // low enough not to warrant a more complex out-of-band
        // channel (e.g. fd 3) at this stage.  See noetl/ai-meta#60
        // for the surfacing finding.
        let wrapper_code = format!(
            r#"
import sys
import json

# Read context from stdin
context = json.loads(sys.stdin.read())
args = context.get('args', {{}})
variables = context.get('variables', {{}})
execution_id = context.get('execution_id')
step = context.get('step')

# Make args available as globals for convenience
globals().update(args)

# User code
{}

# Emit the user-set `result` global as JSON on a single line
# prefixed with the noetl marker.  Missing / non-JSON-serializable
# results fall back to `null` so the tool can still complete
# successfully.
try:
    __noetl_captured = globals().get('result', None)
    sys.stdout.write('{marker}' + json.dumps(__noetl_captured, default=str) + '\n')
    sys.stdout.flush()
except Exception as __noetl_err:
    sys.stdout.write('{marker}null\n')
    sys.stderr.write('noetl result capture failed: ' + repr(__noetl_err) + '\n')
"#,
            code,
            marker = NOETL_RESULT_MARKER
        );

        // Write script to temp file
        let temp_file = NamedTempFile::new()
            .map_err(|e| ToolError::Process(format!("Failed to create temp file: {}", e)))?;

        tokio::fs::write(temp_file.path(), wrapper_code.as_bytes())
            .await
            .map_err(|e| ToolError::Process(format!("Failed to write script: {}", e)))?;

        // Build command
        let mut cmd = Command::new(python);
        cmd.arg(temp_file.path());

        if let Some(dir) = cwd {
            cmd.current_dir(dir);
        }

        // Set environment variables
        for (k, v) in env {
            cmd.env(k, v);
        }

        // Setup stdin/stdout/stderr
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        // Spawn process
        let mut child = cmd
            .spawn()
            .map_err(|e| ToolError::Process(format!("Failed to spawn Python process: {}", e)))?;

        // Write context to stdin
        let context_json = serde_json::json!({
            "args": args,
            "variables": ctx.variables,
            "execution_id": ctx.execution_id,
            "step": ctx.step,
            "server_url": ctx.server_url,
        });

        let stdin = child.stdin.take();
        if let Some(mut stdin) = stdin {
            let _ = stdin.write_all(context_json.to_string().as_bytes()).await;
            let _ = stdin.shutdown().await;
        }

        // Wait for completion with timeout
        let output = if let Some(duration) = timeout_duration {
            // Take the child id before we potentially consume the process
            let child_id = child.id();

            match timeout(duration, child.wait_with_output()).await {
                Ok(result) => result.map_err(|e| {
                    ToolError::Process(format!("Failed to wait for process: {}", e))
                })?,
                Err(_) => {
                    // Timeout occurred - try to kill the process by ID
                    if let Some(pid) = child_id {
                        #[cfg(unix)]
                        {
                            let _ = std::process::Command::new("kill")
                                .args(["-9", &pid.to_string()])
                                .spawn();
                        }
                        #[cfg(windows)]
                        {
                            let _ = std::process::Command::new("taskkill")
                                .args(["/F", "/PID", &pid.to_string()])
                                .spawn();
                        }
                    }
                    let duration_ms = start.elapsed().as_millis() as u64;
                    return Ok(ToolResult::timeout(duration.as_secs()).with_duration(duration_ms));
                }
            }
        } else {
            child
                .wait_with_output()
                .await
                .map_err(|e| ToolError::Process(format!("Failed to wait for process: {}", e)))?
        };

        let exit_code = output.status.code().unwrap_or(-1);
        let raw_stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        // Pick out the wrapper's marked result line; the visible
        // stdout (user `print(...)` calls) is what's left after
        // stripping that line out.  See `extract_result_from_stdout`
        // + the wrapper script's emit section.
        let (captured_result, stdout) = extract_result_from_stdout(&raw_stdout);

        let duration_ms = start.elapsed().as_millis() as u64;

        // `result = {...}` global captured by the wrapper is the
        // authoritative tool result.  Fall back to the legacy
        // parse-stdout-as-JSON path only when no marker was found
        // (e.g. wrapper exited before emit) so back-compat with
        // tooling that printed JSON-only on stdout stays intact.
        let data = if let Some(value) = captured_result {
            // Treat a null capture (user didn't set `result`) as a
            // no-data successful run rather than a confusing
            // `data: null`.
            if value.is_null() {
                Some(serde_json::json!({
                    "stdout": stdout,
                    "stderr": stderr,
                }))
            } else {
                Some(value)
            }
        } else if !stdout.trim().is_empty() {
            serde_json::from_str(&stdout).ok()
        } else {
            None
        };

        let status = if exit_code == 0 {
            ToolStatus::Success
        } else {
            ToolStatus::Error
        };

        Ok(ToolResult {
            status,
            data: data.or_else(|| {
                Some(serde_json::json!({
                    "stdout": stdout,
                    "stderr": stderr,
                }))
            }),
            error: if exit_code != 0 {
                Some(format!("Python script exited with code {}", exit_code))
            } else {
                None
            },
            stdout: Some(stdout),
            stderr: Some(stderr),
            exit_code: Some(exit_code),
            duration_ms: Some(duration_ms),
        })
    }

    /// Parse Python config from tool config.
    fn parse_config(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<PythonConfig, ToolError> {
        let template_ctx = ctx.to_template_context();
        let rendered_config = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;

        serde_json::from_value(rendered_config)
            .map_err(|e| ToolError::Configuration(format!("Invalid python config: {}", e)))
    }
}

impl Default for PythonTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for PythonTool {
    fn name(&self) -> &'static str {
        "python"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let python_config = self.parse_config(config, ctx)?;

        let timeout_duration = python_config
            .timeout_seconds
            .or(config.timeout)
            .map(Duration::from_secs);

        tracing::debug!(
            code_len = python_config.code.len(),
            python = %python_config.python,
            timeout = ?timeout_duration,
            "Executing Python script"
        );

        self.execute_code(
            &python_config.code,
            &python_config.args,
            &python_config.env,
            &python_config.python,
            python_config.cwd.as_deref(),
            timeout_duration,
            ctx,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_python_config_deserialization() {
        let json = serde_json::json!({
            "code": "print('hello')",
            "args": {"name": "world"},
            "python": "python3"
        });

        let config: PythonConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.code, "print('hello')");
        assert!(config.args.contains_key("name"));
    }

    #[test]
    fn test_python_config_defaults() {
        let json = serde_json::json!({
            "code": "print(1)"
        });

        let config: PythonConfig = serde_json::from_value(json).unwrap();
        assert!(config.args.is_empty());
        assert!(config.env.is_empty());
        assert_eq!(config.python, default_python());
    }

    #[test]
    fn test_extract_result_strips_marker_line() {
        // Wrapper-emitted result line at the end gets stripped from
        // visible stdout; the JSON after the marker is parsed.
        let raw = "hello from user\n@@__NOETL_RESULT__@@{\"is_hot\":true}\n";
        let (captured, cleaned) = extract_result_from_stdout(raw);
        assert_eq!(
            captured,
            Some(serde_json::json!({"is_hot": true})),
            "marker JSON should parse"
        );
        assert_eq!(cleaned, "hello from user\n", "marker line stripped");
    }

    #[test]
    fn test_extract_result_no_marker_returns_none() {
        // Legacy scripts without the wrapper (or scripts whose
        // wrapper failed before emit) — no marker means no capture.
        let raw = "just user output\n";
        let (captured, cleaned) = extract_result_from_stdout(raw);
        assert!(captured.is_none(), "no marker = no capture");
        assert_eq!(cleaned, "just user output\n", "stdout unchanged");
    }

    #[test]
    fn test_extract_result_handles_null_capture() {
        // User script that didn't set `result` — wrapper emits the
        // marker with `null` as the JSON payload.  Capture is
        // Some(Null), not None — the caller distinguishes the two
        // cases (no marker = legacy, marker+null = explicit
        // "user ran but set no result").
        let raw = "@@__NOETL_RESULT__@@null\n";
        let (captured, cleaned) = extract_result_from_stdout(raw);
        assert_eq!(captured, Some(serde_json::Value::Null));
        assert_eq!(cleaned, "");
    }

    #[tokio::test]
    async fn test_python_captures_result_global() {
        // The whole point of the noetl-tools change — user code that
        // assigns `result = {...}` should expose that dict as the
        // tool's data field so the orchestrator can resolve
        // `{{ step_name.field }}` references in next.arcs.
        let tool = PythonTool::new();
        let args = HashMap::new();
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let result = tool
            .execute_code(
                "result = {'is_hot': True, 'message': 'hot'}",
                &args,
                &env,
                "python3",
                None,
                None,
                &ctx,
            )
            .await
            .unwrap();

        assert!(result.is_success());
        let data = result.data.expect("data should be the captured result");
        assert_eq!(
            data.get("is_hot").and_then(|v| v.as_bool()),
            Some(true),
            "captured result must expose `is_hot`"
        );
        assert_eq!(
            data.get("message").and_then(|v| v.as_str()),
            Some("hot"),
            "captured result must expose `message`"
        );
    }

    #[tokio::test]
    async fn test_python_capture_preserves_user_stdout() {
        // User `print(...)` calls and the captured result coexist —
        // the marker line is stripped, but the user's print stays
        // visible in stdout for debugging.
        let tool = PythonTool::new();
        let args = HashMap::new();
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let result = tool
            .execute_code(
                "print('debug: starting'); result = {'ok': True}",
                &args,
                &env,
                "python3",
                None,
                None,
                &ctx,
            )
            .await
            .unwrap();

        assert!(result.is_success());
        // stdout has the user print, NOT the marker line.
        let stdout = result.stdout.as_ref().unwrap();
        assert!(stdout.contains("debug: starting"), "user print preserved");
        assert!(
            !stdout.contains(NOETL_RESULT_MARKER),
            "marker line must be stripped from visible stdout"
        );
        // data has the result.
        let data = result.data.expect("captured result");
        assert_eq!(data.get("ok").and_then(|v| v.as_bool()), Some(true));
    }

    #[tokio::test]
    async fn test_python_simple_script() {
        let tool = PythonTool::new();
        let args = HashMap::new();
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let result = tool
            .execute_code(
                "print('hello from python')",
                &args,
                &env,
                "python3",
                None,
                None,
                &ctx,
            )
            .await
            .unwrap();

        assert!(result.is_success());
        assert!(result
            .stdout
            .as_ref()
            .unwrap()
            .contains("hello from python"));
    }

    #[tokio::test]
    async fn test_python_json_output() {
        let tool = PythonTool::new();
        let args = HashMap::new();
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let result = tool
            .execute_code(
                r#"import json; print(json.dumps({"result": 42}))"#,
                &args,
                &env,
                "python3",
                None,
                None,
                &ctx,
            )
            .await
            .unwrap();

        assert!(result.is_success());
        if let Some(data) = result.data {
            // Either parsed JSON or raw output
            assert!(data.to_string().contains("42"));
        }
    }

    #[tokio::test]
    async fn test_python_with_args() {
        let tool = PythonTool::new();
        let mut args = HashMap::new();
        args.insert("x".to_string(), serde_json::json!(10));
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let result = tool
            .execute_code(
                "print(args.get('x', 0) * 2)",
                &args,
                &env,
                "python3",
                None,
                None,
                &ctx,
            )
            .await
            .unwrap();

        assert!(result.is_success());
        assert!(result.stdout.as_ref().unwrap().contains("20"));
    }

    #[tokio::test]
    async fn test_python_error() {
        let tool = PythonTool::new();
        let args = HashMap::new();
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let result = tool
            .execute_code(
                "raise ValueError('test error')",
                &args,
                &env,
                "python3",
                None,
                None,
                &ctx,
            )
            .await
            .unwrap();

        assert!(!result.is_success());
        assert!(result.exit_code.unwrap() != 0);
    }

    #[tokio::test]
    async fn test_python_timeout() {
        let tool = PythonTool::new();
        let args = HashMap::new();
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let result = tool
            .execute_code(
                "import time; time.sleep(10)",
                &args,
                &env,
                "python3",
                None,
                Some(Duration::from_millis(100)),
                &ctx,
            )
            .await
            .unwrap();

        assert_eq!(result.status, ToolStatus::Timeout);
    }

    #[tokio::test]
    async fn test_python_tool_interface() {
        let tool = PythonTool::new();
        assert_eq!(tool.name(), "python");

        let config = ToolConfig {
            kind: "python".to_string(),
            config: serde_json::json!({
                "code": "print('test')"
            }),
            timeout: None,
            retry: None,
            auth: None,
        };

        let ctx = ExecutionContext::default();
        let result = tool.execute(&config, &ctx).await.unwrap();
        assert!(result.is_success());
    }
}
