//! Python script execution tool.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::time::timeout;

use crate::auth::GcpAuth;
use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::{ToolResult, ToolStatus};
use crate::template::TemplateEngine;

/// GCS read scope — narrower than full cloud-platform.
const GCS_READ_SCOPES: &[&str] = &["https://www.googleapis.com/auth/devstorage.read_only"];

/// Default per-fetch timeout (seconds) for `gcs` / `http` script loaders.
const DEFAULT_LOADER_TIMEOUT_SECS: u64 = 30;

/// Sentinel string the wrapper script uses to mark the JSON line
/// carrying the user-set `result` global.  Chosen to be improbable
/// in real user output.  See `extract_result_from_stdout` for the
/// parsing side.
const NOETL_RESULT_MARKER: &str = "@@__NOETL_RESULT__@@";

/// Detect whether `code` contains a top-level `return` statement and,
/// if so, wrap the entire code body in an implicit
/// `def __noetl_step__(args, input_data, **kw):` function so the
/// `return` is syntactically valid Python.  noetl/ai-meta#71.
///
/// Detection heuristic (deliberately simple — no AST):
/// Walk lines in order.  Skip blank lines and comment-only lines
/// (`#…`).  If the first non-trivial token we see is `return ` (or
/// `return\n`), treat it as a top-level return.  Stop scanning once
/// a `def ` or `class ` statement is encountered — those mean the
/// `return` belongs to a nested function/class, not the top level.
///
/// False-negative: a `return` that lives inside a nested
/// `def`/`class` won't be mis-wrapped (the scan stops).
/// False-positive: a `return` that is the FIRST statement but is
/// actually unreachable dead code after a prior def — acceptable
/// edge case; the wrapper still executes correctly.
///
/// When wrapping:
/// - Every line of `code` is indented by 4 spaces.
/// - A call `result = __noetl_step__(args, input_data)` is appended
///   at module level so the return value is captured as `result`.
fn wrap_top_level_return(code: &str) -> String {
    // Scan for a top-level `return` before any `def`/`class`.
    //
    // A "top-level return" is a `return` statement that is NOT indented
    // (i.e. the line starts with `return` after stripping leading
    // whitespace, AND the raw line itself starts with `return` — not
    // inside a function body whose indentation would reveal it).
    //
    // Two conditions must both hold for a `return` to be "top-level":
    //   1. The raw line is unindented (starts with `return` or optional
    //      comment-leading whitespace — but we require no indent for
    //      safety).
    //   2. No `def ` / `async def ` / `class ` statement has been seen
    //      yet in the scan (unindented lines only — we stop at the first
    //      function/class definition).
    //
    // This handles `async def main():\n    return X` correctly: the
    // scan stops at `async def main():` before reaching the indented
    // `return`.

    let mut found_return = false;
    for line in code.lines() {
        // Only consider unindented lines — indented lines are inside a
        // block and can never be "top-level" by definition.
        if line.starts_with(' ') || line.starts_with('\t') {
            continue;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        // An unindented `def `, `async def `, or `class ` opens a
        // function/class body — stop scanning; any `return` after
        // this point belongs to that body.
        if trimmed.starts_with("def ")
            || trimmed.starts_with("async def ")
            || trimmed.starts_with("class ")
        {
            break;
        }
        if trimmed.starts_with("return ") || trimmed == "return" {
            found_return = true;
            break;
        }
    }

    if !found_return {
        return code.to_string();
    }

    // Wrap: indent every code line by 4 spaces and add the call.
    let indented: String = code
        .lines()
        .map(|l| format!("    {l}"))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "def __noetl_step__(args, input_data, **kw):\n{indented}\n\nresult = __noetl_step__(args, input_data)"
    )
}

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

/// Source of a Python script inside the richer `script:` block.
///
/// `type: inline` carries the code directly — the nested analog of the flat
/// `code:` field.  `type: file` / `gcs` / `http` name an external script,
/// loaded by [`PythonTool::load_script_code`]:
///
/// - **file** — the script's `uri` is a local filesystem path; read it.
/// - **gcs** — the script's `uri` is a `gs://bucket/object` URL; download
///   via the GCS JSON API with a GCP ADC token (workload identity on
///   GKE, `GOOGLE_APPLICATION_CREDENTIALS` / `gcloud` locally).
/// - **http** — GET the URL in `source.endpoint` (falling back to the
///   script's `uri`), honoring `source.method` (default GET) +
///   `source.timeout` (seconds).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PythonScriptSource {
    /// `inline` (default) / `file` / `gcs` / `http`.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub source_type: Option<String>,
    /// Inline code (when `type: inline`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Credential alias for `gcs` / `http` sources that need auth.
    /// On GKE, GCS uses workload-identity ADC regardless; this field
    /// is accepted for forward-compat + parity with the YAML shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<String>,
    /// HTTP source endpoint URL (when `type: http`).  When absent, the
    /// loader falls back to the script's `uri`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// HTTP method for `type: http` (default `GET`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    /// Per-fetch timeout in seconds for `gcs` / `http` sources.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
}

/// The `script:` block — the richer script-loading shape canonical v10
/// fixtures use (`script: { uri, source: { type, code } }`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PythonScript {
    /// Where the script lives: `inline`, a file path, or a gcs/http URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<PythonScriptSource>,
}

/// Python tool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonConfig {
    /// Python code to execute (flat form).  Either this or `script` must
    /// supply the code — see [`PythonConfig::resolve_code`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,

    /// Richer script-loading block.  The `inline` source type is the nested
    /// analog of `code:`; external sources aren't loaded yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script: Option<PythonScript>,

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

/// Where a Python script's body should come from.  Produced by
/// [`PythonConfig::resolve_source`]; loaded by
/// [`PythonTool::load_script_code`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PythonSource {
    /// Code is already present (flat `code:` or `script.source.code`).
    Inline(String),
    /// Read the body from a local filesystem path.
    File { path: String },
    /// Download `gs://bucket/object` via the GCS JSON API.
    Gcs { uri: String },
    /// HTTP GET the body from `endpoint` (method default GET).
    Http {
        endpoint: String,
        method: String,
        timeout_secs: u64,
    },
}

impl PythonConfig {
    /// Resolve the effective Python source.
    ///
    /// Accepts the flat `code:` field, the nested `script.source.code`
    /// (inline) shape, and the three external shapes (`file` / `gcs` /
    /// `http`).  The flat `code:` wins when both inline forms are
    /// present.  External sources are loaded by
    /// [`PythonTool::load_script_code`]; this method only classifies
    /// the source (it does no I/O).
    ///
    /// noetl/ai-meta#65 implemented the external loaders that #63's
    /// inline-only resolver returned a "not yet supported" error for.
    pub fn resolve_source(&self) -> Result<PythonSource, ToolError> {
        if let Some(code) = self.code.as_deref() {
            return Ok(PythonSource::Inline(code.to_string()));
        }
        let script = self.script.as_ref();
        let source = script.and_then(|s| s.source.as_ref());
        let kind = source
            .and_then(|s| s.source_type.as_deref())
            .unwrap_or("inline");

        match kind {
            "inline" => source
                .and_then(|s| s.code.as_deref())
                .map(|c| PythonSource::Inline(c.to_string()))
                .ok_or_else(|| {
                    ToolError::Configuration(
                        "python script `source.type: inline` has no `code`".to_string(),
                    )
                }),
            "file" => {
                let path = script.and_then(|s| s.uri.as_deref()).ok_or_else(|| {
                    ToolError::Configuration(
                        "python script `source.type: file` requires `script.uri` (the file path)"
                            .to_string(),
                    )
                })?;
                Ok(PythonSource::File {
                    path: path.to_string(),
                })
            }
            "gcs" => {
                let uri = script.and_then(|s| s.uri.as_deref()).ok_or_else(|| {
                    ToolError::Configuration(
                        "python script `source.type: gcs` requires `script.uri` (a gs:// URL)"
                            .to_string(),
                    )
                })?;
                Ok(PythonSource::Gcs {
                    uri: uri.to_string(),
                })
            }
            "http" => {
                // The actual URL lives in `source.endpoint`; the script
                // `uri` is a label in the canonical fixtures.  Fall back
                // to `uri` when `endpoint` is absent.
                let endpoint = source
                    .and_then(|s| s.endpoint.as_deref())
                    .or_else(|| script.and_then(|s| s.uri.as_deref()))
                    .ok_or_else(|| {
                        ToolError::Configuration(
                            "python script `source.type: http` requires `source.endpoint` \
                             (the URL to GET) or a `script.uri`"
                                .to_string(),
                        )
                    })?;
                let method = source
                    .and_then(|s| s.method.as_deref())
                    .unwrap_or("GET")
                    .to_uppercase();
                let timeout_secs = source
                    .and_then(|s| s.timeout)
                    .unwrap_or(DEFAULT_LOADER_TIMEOUT_SECS);
                Ok(PythonSource::Http {
                    endpoint: endpoint.to_string(),
                    method,
                    timeout_secs,
                })
            }
            other => Err(ToolError::Configuration(format!(
                "python script `source.type: {other}` is unknown; expected one of \
                 inline | file | gcs | http"
            ))),
        }
    }

    /// Back-compat accessor for the inline fast path.  Returns the
    /// borrowed inline code, or an error for external sources (which
    /// require async I/O — use [`PythonTool::load_script_code`]).
    ///
    /// Retained so existing call sites / tests that only ever pass
    /// inline code keep working without an async context.
    pub fn resolve_code(&self) -> Result<&str, ToolError> {
        if let Some(code) = self.code.as_deref() {
            return Ok(code);
        }
        if let Some(source) = self.script.as_ref().and_then(|s| s.source.as_ref()) {
            let kind = source.source_type.as_deref().unwrap_or("inline");
            if kind == "inline" {
                return source.code.as_deref().ok_or_else(|| {
                    ToolError::Configuration(
                        "python script `source.type: inline` has no `code`".to_string(),
                    )
                });
            }
            return Err(ToolError::Configuration(format!(
                "python script `source.type: {kind}` requires async loading; \
                 call PythonTool::load_script_code"
            )));
        }
        Err(ToolError::Configuration(
            "python config has no code: set `code:` or `script.source.code`".to_string(),
        ))
    }
}

/// Parse a `gs://bucket/object/path` URI into `(bucket, object)`.
/// The object key is returned URL-unencoded; the caller is
/// responsible for percent-encoding it into the GCS JSON API path.
fn parse_gcs_uri(uri: &str) -> Result<(String, String), ToolError> {
    let rest = uri.strip_prefix("gs://").ok_or_else(|| {
        ToolError::Configuration(format!(
            "gcs script uri must start with `gs://`, got `{uri}`"
        ))
    })?;
    let (bucket, object) = rest.split_once('/').ok_or_else(|| {
        ToolError::Configuration(format!(
            "gcs script uri `{uri}` has no object path after the bucket"
        ))
    })?;
    if bucket.is_empty() || object.is_empty() {
        return Err(ToolError::Configuration(format!(
            "gcs script uri `{uri}` has an empty bucket or object"
        )));
    }
    Ok((bucket.to_string(), object.to_string()))
}

/// Percent-encode a GCS object key for the JSON API path segment.
/// Slashes and other reserved chars must be encoded so the object
/// name lands in a single path segment.
fn encode_gcs_object(object: &str) -> String {
    let mut out = String::with_capacity(object.len() * 2);
    for byte in object.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// Python script execution tool.
///
/// Executes Python code in a subprocess with JSON protocol:
/// - Script receives context on stdin as JSON
/// - Script should print result as JSON to stdout
/// - Exit code determines success/failure
pub struct PythonTool {
    template_engine: TemplateEngine,
    /// HTTP client for `type: http` script loading.
    http_client: reqwest::Client,
    /// GCP ADC auth for `type: gcs` script loading.
    gcp_auth: GcpAuth,
}

impl PythonTool {
    /// Create a new Python tool.
    pub fn new() -> Self {
        Self {
            template_engine: TemplateEngine::new(),
            http_client: reqwest::Client::new(),
            gcp_auth: GcpAuth::new(),
        }
    }

    /// Load the Python script body from whichever source the config
    /// names — inline / file / gcs / http.  noetl/ai-meta#65.
    pub async fn load_script_code(&self, cfg: &PythonConfig) -> Result<String, ToolError> {
        match cfg.resolve_source()? {
            PythonSource::Inline(code) => Ok(code),
            PythonSource::File { path } => self.load_from_file(&path).await,
            PythonSource::Gcs { uri } => self.load_from_gcs(&uri).await,
            PythonSource::Http {
                endpoint,
                method,
                timeout_secs,
            } => self.load_from_http(&endpoint, &method, timeout_secs).await,
        }
    }

    /// Read a script body from a local filesystem path.
    async fn load_from_file(&self, path: &str) -> Result<String, ToolError> {
        tokio::fs::read_to_string(path).await.map_err(|e| {
            ToolError::Io(format!(
                "python script file `{path}` could not be read: {e}"
            ))
        })
    }

    /// Download a script body from `gs://bucket/object` via the GCS
    /// JSON API, authenticating with a GCP ADC token.  On GKE this
    /// rides workload identity; locally it uses
    /// `GOOGLE_APPLICATION_CREDENTIALS` / `gcloud`.
    async fn load_from_gcs(&self, uri: &str) -> Result<String, ToolError> {
        let (bucket, object) = parse_gcs_uri(uri)?;
        let token = self.gcp_auth.get_token(GCS_READ_SCOPES).await?;
        let url = format!(
            "https://storage.googleapis.com/storage/v1/b/{bucket}/o/{}?alt=media",
            encode_gcs_object(&object)
        );
        let resp = self
            .http_client
            .get(&url)
            .bearer_auth(token)
            .timeout(Duration::from_secs(DEFAULT_LOADER_TIMEOUT_SECS))
            .send()
            .await
            .map_err(|e| ToolError::Http(format!("gcs fetch `{uri}` failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ToolError::Http(format!(
                "gcs fetch `{uri}` returned HTTP {status}: {}",
                body.chars().take(300).collect::<String>()
            )));
        }
        resp.text()
            .await
            .map_err(|e| ToolError::Http(format!("gcs fetch `{uri}` body read failed: {e}")))
    }

    /// HTTP GET (or other method) a script body from `endpoint`.
    async fn load_from_http(
        &self,
        endpoint: &str,
        method: &str,
        timeout_secs: u64,
    ) -> Result<String, ToolError> {
        let req_method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|_| {
            ToolError::Configuration(format!("python http script: invalid method `{method}`"))
        })?;
        let resp = self
            .http_client
            .request(req_method, endpoint)
            .timeout(Duration::from_secs(timeout_secs))
            .send()
            .await
            .map_err(|e| ToolError::Http(format!("http fetch `{endpoint}` failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ToolError::Http(format!(
                "http fetch `{endpoint}` returned HTTP {status}: {}",
                body.chars().take(300).collect::<String>()
            )));
        }
        resp.text()
            .await
            .map_err(|e| ToolError::Http(format!("http fetch `{endpoint}` body read failed: {e}")))
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
        //
        // Style C (noetl/ai-meta#71): if the user code contains a
        // top-level `return` statement (and no `def`/`class` precedes
        // it), wrap it in an implicit function so the return is valid
        // Python.  The `input_data` global (also injected uniformly
        // below) is passed as a parameter so the wrapped function body
        // can reference it even before the globals are merged in.
        let effective_code = wrap_top_level_return(code);

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

# Expose args as `input_data` for fixture parity (noetl/ai-meta#71).
# Legacy Python executor injected args under both the individual
# key names AND as the dict `input_data` so fixtures that call
# `input_data.get('foo')` work regardless of which style they use.
input_data = dict(args)

# User code (possibly wrapped by wrap_top_level_return for Style C)
{}

# Legacy `main()` convention (noetl/ai-meta#65): the
# script_execution/* fixtures + the Python (legacy) tool support
# function-based scripts that define a `main(...)` callable instead
# of setting the `result` global directly.  When the user code
# didn't set a non-None `result` AND defines a callable `main`,
# call it with the matching args and capture its return value.
# Mirrors noetl/tools/python/executor.py:_invoke_main — binds
# main's named params from `args` by name, forwards all args when
# main accepts `**kwargs`, and awaits async `main` via asyncio.run.
if globals().get('result', None) is None:
    __noetl_main = globals().get('main', None)
    if callable(__noetl_main):
        import inspect as __noetl_inspect
        __noetl_sig = __noetl_inspect.signature(__noetl_main)
        __noetl_kwargs = {{}}
        __noetl_var_kw = any(
            __p.kind == __p.VAR_KEYWORD
            for __p in __noetl_sig.parameters.values()
        )
        for __pname, __p in __noetl_sig.parameters.items():
            if __p.kind in (__p.VAR_POSITIONAL, __p.VAR_KEYWORD):
                continue
            if __pname in args:
                __noetl_kwargs[__pname] = args[__pname]
        if __noetl_var_kw:
            for __k, __v in args.items():
                __noetl_kwargs.setdefault(__k, __v)
        if __noetl_inspect.iscoroutinefunction(__noetl_main):
            import asyncio as __noetl_asyncio
            result = __noetl_asyncio.run(__noetl_main(**__noetl_kwargs))
        else:
            result = __noetl_main(**__noetl_kwargs)

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
            effective_code,
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
            pending_callback: None,
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
        let code = self.load_script_code(&python_config).await?;

        let timeout_duration = python_config
            .timeout_seconds
            .or(config.timeout)
            .map(Duration::from_secs);

        tracing::debug!(
            code_len = code.len(),
            python = %python_config.python,
            timeout = ?timeout_duration,
            "Executing Python script"
        );

        self.execute_code(
            &code,
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
        assert_eq!(config.resolve_code().unwrap(), "print('hello')");
        assert!(config.args.contains_key("name"));
    }

    #[test]
    fn test_python_inline_script_shape_resolves_code() {
        // Canonical v10 script-loading shape (root_scripts/test_script_loading):
        // script: { uri: inline, source: { type: inline, code } }.
        let json = serde_json::json!({
            "script": {
                "uri": "inline",
                "source": { "type": "inline", "code": "result = {'ok': 1}" }
            }
        });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.resolve_code().unwrap(), "result = {'ok': 1}");
    }

    #[test]
    fn test_python_source_without_type_defaults_inline() {
        let json = serde_json::json!({
            "script": { "source": { "code": "x = 1" } }
        });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.resolve_code().unwrap(), "x = 1");
    }

    #[test]
    fn test_flat_code_wins_over_script() {
        let json = serde_json::json!({
            "code": "flat",
            "script": { "source": { "type": "inline", "code": "nested" } }
        });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.resolve_code().unwrap(), "flat");
    }

    #[test]
    fn test_resolve_source_classifies_file() {
        let json = serde_json::json!({
            "script": { "uri": "scripts/run.py", "source": { "type": "file" } }
        });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        assert_eq!(
            config.resolve_source().unwrap(),
            PythonSource::File {
                path: "scripts/run.py".to_string()
            },
        );
    }

    #[test]
    fn test_resolve_source_classifies_gcs() {
        let json = serde_json::json!({
            "script": {
                "uri": "gs://my-bucket/scripts/run.py",
                "source": { "type": "gcs", "auth": "gcp_cred" }
            }
        });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        assert_eq!(
            config.resolve_source().unwrap(),
            PythonSource::Gcs {
                uri: "gs://my-bucket/scripts/run.py".to_string()
            },
        );
    }

    #[test]
    fn test_resolve_source_classifies_http_endpoint_over_uri() {
        // The HTTP loader prefers `source.endpoint`; the script `uri`
        // is just a label in the canonical fixtures.
        let json = serde_json::json!({
            "script": {
                "uri": "hello.py",
                "source": {
                    "type": "http",
                    "endpoint": "https://example.com/hello.py",
                    "method": "get",
                    "timeout": 15
                }
            }
        });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        assert_eq!(
            config.resolve_source().unwrap(),
            PythonSource::Http {
                endpoint: "https://example.com/hello.py".to_string(),
                method: "GET".to_string(),
                timeout_secs: 15,
            },
        );
    }

    #[test]
    fn test_resolve_source_http_falls_back_to_uri() {
        let json = serde_json::json!({
            "script": {
                "uri": "https://example.com/from-uri.py",
                "source": { "type": "http" }
            }
        });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        assert_eq!(
            config.resolve_source().unwrap(),
            PythonSource::Http {
                endpoint: "https://example.com/from-uri.py".to_string(),
                method: "GET".to_string(),
                timeout_secs: DEFAULT_LOADER_TIMEOUT_SECS,
            },
        );
    }

    #[test]
    fn test_resolve_source_unknown_type_errors() {
        let json = serde_json::json!({
            "script": { "uri": "x", "source": { "type": "ftp" } }
        });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        let err = config.resolve_source().unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("ftp"), "got: {msg}");
        assert!(msg.contains("inline | file | gcs | http"), "got: {msg}");
    }

    #[test]
    fn test_resolve_source_file_requires_uri() {
        let json = serde_json::json!({
            "script": { "source": { "type": "file" } }
        });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        let err = config.resolve_source().unwrap_err();
        assert!(format!("{err}").contains("script.uri"), "got: {err}");
    }

    #[test]
    fn test_parse_gcs_uri() {
        assert_eq!(
            parse_gcs_uri("gs://bucket/a/b/c.py").unwrap(),
            ("bucket".to_string(), "a/b/c.py".to_string()),
        );
        assert!(parse_gcs_uri("https://x").is_err());
        assert!(parse_gcs_uri("gs://bucket-only").is_err());
        assert!(parse_gcs_uri("gs:///object").is_err());
    }

    #[test]
    fn test_encode_gcs_object_percent_encodes_slashes() {
        assert_eq!(encode_gcs_object("a/b/c.py"), "a%2Fb%2Fc.py");
        assert_eq!(encode_gcs_object("plain.py"), "plain.py");
        assert_eq!(encode_gcs_object("with space.py"), "with%20space.py");
    }

    #[tokio::test]
    async fn test_load_from_file_reads_body() {
        let mut tmp = NamedTempFile::new().unwrap();
        use std::io::Write;
        write!(tmp, "result = {{'loaded': 'from_file'}}").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();

        let tool = PythonTool::new();
        let code = tool.load_from_file(&path).await.unwrap();
        assert_eq!(code, "result = {'loaded': 'from_file'}");
    }

    #[tokio::test]
    async fn test_load_from_file_missing_path_errors() {
        let tool = PythonTool::new();
        let err = tool
            .load_from_file("/nonexistent/path/to/script.py")
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("could not be read"), "got: {err}");
    }

    #[tokio::test]
    async fn test_load_script_code_inline_path() {
        let json = serde_json::json!({ "code": "x = 1" });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        let tool = PythonTool::new();
        assert_eq!(tool.load_script_code(&config).await.unwrap(), "x = 1");
    }

    #[test]
    fn test_no_code_anywhere_errors() {
        let json = serde_json::json!({ "args": {} });
        let config: PythonConfig = serde_json::from_value(json).unwrap();
        let err = config.resolve_code().unwrap_err();
        assert!(format!("{err}").contains("no code"), "got: {err}");
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
    async fn test_python_main_function_convention() {
        // The script_execution/* fixtures + the Python (legacy) tool
        // support function-based scripts: a `main(...)` callable whose
        // return value becomes the result.  noetl/ai-meta#65.
        let tool = PythonTool::new();
        let mut args = HashMap::new();
        args.insert("name".to_string(), serde_json::json!("NoETL"));
        args.insert("count".to_string(), serde_json::json!(3));
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let code = r#"
def main(name="World", count=1):
    messages = [f"Hello, {name}! (#{i+1})" for i in range(count)]
    return {"status": "success", "messages": messages, "total": count}
"#;
        let result = tool
            .execute_code(code, &args, &env, "python3", None, None, &ctx)
            .await
            .unwrap();

        assert!(result.is_success(), "stderr: {:?}", result.stderr);
        let data = result.data.expect("main() return becomes data");
        assert_eq!(data.get("status").and_then(|v| v.as_str()), Some("success"));
        assert_eq!(data.get("total").and_then(|v| v.as_i64()), Some(3));
        assert_eq!(
            data.get("messages")
                .and_then(|v| v.as_array())
                .map(|a| a.len()),
            Some(3),
        );
    }

    #[tokio::test]
    async fn test_python_explicit_result_wins_over_main() {
        // When the user code sets `result` directly AND defines a
        // `main`, the explicit `result` wins — main() is not called.
        let tool = PythonTool::new();
        let args = HashMap::new();
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let code = r#"
def main():
    return {"from": "main"}
result = {"from": "explicit"}
"#;
        let result = tool
            .execute_code(code, &args, &env, "python3", None, None, &ctx)
            .await
            .unwrap();

        assert!(result.is_success());
        let data = result.data.expect("explicit result");
        assert_eq!(data.get("from").and_then(|v| v.as_str()), Some("explicit"));
    }

    #[tokio::test]
    async fn test_python_async_main_function() {
        // Async `main` is awaited via asyncio.run, matching the
        // legacy `_invoke_main` coroutine path.
        let tool = PythonTool::new();
        let args = HashMap::new();
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let code = r#"
async def main():
    return {"async": True, "value": 42}
"#;
        let result = tool
            .execute_code(code, &args, &env, "python3", None, None, &ctx)
            .await
            .unwrap();

        assert!(result.is_success(), "stderr: {:?}", result.stderr);
        let data = result.data.expect("async main() return becomes data");
        assert_eq!(data.get("async").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(data.get("value").and_then(|v| v.as_i64()), Some(42));
    }

    #[tokio::test]
    async fn test_python_main_with_var_kwargs() {
        // A `main(**kwargs)` receives all args.
        let tool = PythonTool::new();
        let mut args = HashMap::new();
        args.insert("a".to_string(), serde_json::json!(1));
        args.insert("b".to_string(), serde_json::json!(2));
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let code = r#"
def main(**kwargs):
    return {"sum": sum(v for v in kwargs.values() if isinstance(v, int))}
"#;
        let result = tool
            .execute_code(code, &args, &env, "python3", None, None, &ctx)
            .await
            .unwrap();

        assert!(result.is_success(), "stderr: {:?}", result.stderr);
        assert_eq!(
            result.data.unwrap().get("sum").and_then(|v| v.as_i64()),
            Some(3),
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

    // ── noetl/ai-meta#71 tests ────────────────────────────────────────────

    /// `wrap_top_level_return` must NOT wrap code that has no top-level return.
    #[test]
    fn test_wrap_top_level_return_noop_when_no_return() {
        let code = "result = {'x': 1}";
        assert_eq!(wrap_top_level_return(code), code);
    }

    /// `wrap_top_level_return` must NOT wrap code where `return` lives
    /// inside a `def` body (the scan stops at `def `).
    #[test]
    fn test_wrap_top_level_return_noop_inside_def() {
        let code = "def main():\n    return {'x': 1}";
        assert_eq!(wrap_top_level_return(code), code);
    }

    /// `wrap_top_level_return` must NOT wrap code where `return` lives
    /// inside an `async def` body (the scan stops at `async def `).
    #[test]
    fn test_wrap_top_level_return_noop_inside_async_def() {
        let code = "async def main():\n    return {'x': 1}";
        assert_eq!(wrap_top_level_return(code), code);
    }

    /// `wrap_top_level_return` must wrap code where the first non-comment
    /// non-blank statement is `return X`.
    #[test]
    fn test_wrap_top_level_return_wraps_bare_return() {
        let code = "return {'x': 1}";
        let wrapped = wrap_top_level_return(code);
        assert!(
            wrapped.contains("def __noetl_step__(args, input_data, **kw):"),
            "expected wrapper function, got:\n{wrapped}"
        );
        assert!(
            wrapped.contains("result = __noetl_step__(args, input_data)"),
            "expected call line, got:\n{wrapped}"
        );
        assert!(
            wrapped.contains("    return {'x': 1}"),
            "expected indented user code, got:\n{wrapped}"
        );
    }

    /// `input_data` global is injected — user code can call
    /// `input_data.get('foo')` (noetl/ai-meta#71, actions_test pattern).
    #[tokio::test]
    async fn test_input_data_global_is_injected() {
        let tool = PythonTool::new();
        let mut args = HashMap::new();
        args.insert("foo".to_string(), serde_json::json!("bar"));
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let result = tool
            .execute_code(
                "result = {\"got\": input_data.get(\"foo\")}",
                &args,
                &env,
                "python3",
                None,
                None,
                &ctx,
            )
            .await
            .unwrap();

        assert!(result.is_success(), "stderr: {:?}", result.stderr);
        let data = result.data.expect("data should be the captured result");
        assert_eq!(
            data.get("got").and_then(|v| v.as_str()),
            Some("bar"),
            "input_data.get('foo') must return 'bar'"
        );
    }

    /// Top-level `return` is wrapped so it yields the returned value as
    /// the step result (noetl/ai-meta#71, loop_test pattern).
    #[tokio::test]
    async fn test_top_level_return_wraps_user_code() {
        let tool = PythonTool::new();
        let mut args = HashMap::new();
        args.insert("n".to_string(), serde_json::json!(5));
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let result = tool
            .execute_code(
                "return {\"echoed\": input_data.get(\"n\", 0) * 2}",
                &args,
                &env,
                "python3",
                None,
                None,
                &ctx,
            )
            .await
            .unwrap();

        assert!(result.is_success(), "stderr: {:?}", result.stderr);
        let data = result.data.expect("return value becomes data");
        assert_eq!(
            data.get("echoed").and_then(|v| v.as_i64()),
            Some(10),
            "top-level return must yield echoed: 10"
        );
    }

    /// Top-level `return` with no args — covers the empty-args path.
    #[tokio::test]
    async fn test_top_level_return_with_no_input_data() {
        let tool = PythonTool::new();
        let args = HashMap::new();
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let result = tool
            .execute_code(
                "return {\"ok\": True}",
                &args,
                &env,
                "python3",
                None,
                None,
                &ctx,
            )
            .await
            .unwrap();

        assert!(result.is_success(), "stderr: {:?}", result.stderr);
        let data = result.data.expect("return value becomes data");
        assert_eq!(
            data.get("ok").and_then(|v| v.as_bool()),
            Some(true),
            "top-level return with no args must yield ok: true"
        );
    }

    /// Style B (main() convention) continues to work alongside the new
    /// `input_data` global and top-level-return detection.
    #[tokio::test]
    async fn test_main_function_convention_still_works_with_input_data_global() {
        let tool = PythonTool::new();
        let mut args = HashMap::new();
        args.insert("value".to_string(), serde_json::json!(7));
        let env = HashMap::new();
        let ctx = ExecutionContext::default();

        let code = r#"
def main(value=0):
    return {"doubled": value * 2, "from_input_data": input_data.get("value", -1)}
"#;
        let result = tool
            .execute_code(code, &args, &env, "python3", None, None, &ctx)
            .await
            .unwrap();

        assert!(result.is_success(), "stderr: {:?}", result.stderr);
        let data = result.data.expect("main() return becomes data");
        assert_eq!(
            data.get("doubled").and_then(|v| v.as_i64()),
            Some(14),
            "main() called with value=7"
        );
        assert_eq!(
            data.get("from_input_data").and_then(|v| v.as_i64()),
            Some(7),
            "input_data accessible inside main() body via global"
        );
    }
}
