//! Task sequence tool — runs a list of sub-tasks in order through
//! the registry, threading each task's result into the context for
//! the next task to reference.
//!
//! Wire format from noetl-server (`ToolDefinition::Pipeline` after
//! the `noetl/ai-meta#57` fix): the `config` payload is a JSON
//! array of single-key objects, where the key is the task label
//! and the value is a `ToolSpec`-shaped map carrying the task's
//! tool kind + its tool-specific config fields:
//!
//! ```json
//! [
//!   {"transform": {"kind": "python", "code": "...", "input": {...}}},
//!   {"save":      {"kind": "postgres", "query": "INSERT ..."}}
//! ]
//! ```
//!
//! Behavior:
//!
//! - Each task is dispatched through a fresh `default_registry()`
//!   instance so a sub-task can be any registered tool kind
//!   (python, postgres, shell, etc.).
//! - The running map of `{label: result_data}` is exposed under
//!   the `_results` variable for downstream tasks; the most recent
//!   task's data is also exposed under `_prev` (the Python
//!   reference impl's convention).
//! - Templates inside each task's config are rendered with the
//!   updated context so `{{ _prev.is_hot }}` etc. resolve to the
//!   previous task's data.
//! - The aggregated result data is a JSON object keyed by task
//!   label.  Status is `Success` if every sub-task succeeded,
//!   `Error` on the first sub-task failure (the rest are skipped
//!   so the orchestrator's failure-termination logic — noetl/server
//!   #63 — emits `playbook.failed` cleanly).
//!
//! Tracks noetl/tools#15.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::{ToolResult, ToolStatus};
use crate::template::TemplateEngine;

/// Task sequence configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSequenceConfig {
    /// Pipeline tasks.  Accepts the bare-array wire shape the
    /// server emits as `config: [{label: spec}, ...]`; the custom
    /// `From<serde_json::Value>` path below also handles a nested
    /// `{tasks: [...]}` form should a future producer wrap it.
    #[serde(default)]
    pub tasks: Vec<HashMap<String, serde_json::Value>>,
}

/// Task sequence tool implementation.
pub struct TaskSequenceTool {
    template_engine: TemplateEngine,
}

impl TaskSequenceTool {
    /// Create a new task sequence tool.
    pub fn new() -> Self {
        Self {
            template_engine: TemplateEngine::new(),
        }
    }

    /// Parse the task list from the incoming config.  Three
    /// acceptable shapes:
    ///
    /// 1. **Bare array** — `[{label: spec}, ...]`.  Pre-worker-
    ///    envelope shape; useful for unit tests and any caller
    ///    that hands a raw task list to the tool.
    /// 2. **Object with `tasks` field** — `{"tasks": [...]}`.
    ///    Future-proof wrap shape; no current emitter uses it but
    ///    the parser accepts it so producers can adopt without a
    ///    coordinated change.
    /// 3. **Worker envelope** — `{"args": ..., "tool_config":
    ///    [...], "render_context": ...}`.  The actual shape the
    ///    Rust worker's command-dispatch path produces: the
    ///    rendered tool config gets wrapped in `args` +
    ///    `tool_config` + `render_context` before being handed to
    ///    the registry.  Most other tools (PythonTool, etc.)
    ///    deserialize from this envelope via serde-default-skip
    ///    on the unknown fields; task_sequence needs the actual
    ///    list, which lives under `tool_config`.
    fn parse_tasks(
        &self,
        config: &ToolConfig,
    ) -> Result<Vec<HashMap<String, serde_json::Value>>, ToolError> {
        match &config.config {
            serde_json::Value::Array(_) => {
                let tasks: Vec<HashMap<String, serde_json::Value>> =
                    serde_json::from_value(config.config.clone()).map_err(|e| {
                        ToolError::Configuration(format!(
                            "task_sequence: bare-array config did not decode as Vec<HashMap<String, Value>>: {e}"
                        ))
                    })?;
                Ok(tasks)
            }
            serde_json::Value::Object(map) => {
                // Prefer `tool_config` (worker envelope shape) over
                // `tasks` (forward-compat wrap shape) so callers can
                // mix and match without ambiguity in this parser.
                let array_value = map
                    .get("tool_config")
                    .or_else(|| map.get("tasks"))
                    .ok_or_else(|| {
                        ToolError::Configuration(
                            "task_sequence config must be an array, or an object containing a `tool_config` field (worker envelope) or a `tasks` field".to_string(),
                        )
                    })?;
                let tasks: Vec<HashMap<String, serde_json::Value>> =
                    serde_json::from_value(array_value.clone()).map_err(|e| {
                        ToolError::Configuration(format!(
                            "task_sequence: pipeline field did not decode as Vec<HashMap<String, Value>>: {e}"
                        ))
                    })?;
                Ok(tasks)
            }
            other => Err(ToolError::Configuration(format!(
                "task_sequence: config must be array or object, got {}",
                other
            ))),
        }
    }

    /// Convert a single labeled task entry into a `ToolConfig` the
    /// registry can dispatch.  The label is dropped; the embedded
    /// `kind` field on the spec becomes the registry lookup key.
    fn build_task_config(
        label: &str,
        spec: &serde_json::Value,
    ) -> Result<ToolConfig, ToolError> {
        let spec_obj = spec.as_object().ok_or_else(|| {
            ToolError::Configuration(format!(
                "task_sequence: task '{label}' spec must be a JSON object, got {spec}"
            ))
        })?;

        let kind = spec_obj
            .get("kind")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                ToolError::Configuration(format!(
                    "task_sequence: task '{label}' missing required 'kind' field"
                ))
            })?
            .to_string();

        // Strip the `kind` field from the inner config so the sub-
        // tool's deserializer doesn't trip on it as an unknown
        // field.  Everything else (code, query, input, auth, ...)
        // passes through verbatim — the sub-tool decodes from its
        // ToolConfig.config payload exactly as it would for a top-
        // level step.
        let mut inner = spec_obj.clone();
        inner.remove("kind");

        Ok(ToolConfig {
            kind,
            config: serde_json::Value::Object(inner),
            timeout: None,
            retry: None,
            auth: None,
        })
    }
}

impl Default for TaskSequenceTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for TaskSequenceTool {
    fn name(&self) -> &'static str {
        "task_sequence"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let start = std::time::Instant::now();

        let tasks = self.parse_tasks(config)?;

        // Fresh registry instance for sub-task dispatch.  See module
        // docs — the back-reference avoidance keeps task_sequence
        // standalone, at the cost of one registry build per execute.
        // Acceptable in practice (~µs); a OnceLock-backed singleton
        // is a follow-up if real-world usage shows hot pipelines.
        let registry = crate::tools::create_default_registry();

        let mut labeled_results: HashMap<String, serde_json::Value> = HashMap::new();
        let mut last_data: serde_json::Value = serde_json::Value::Null;
        let mut last_stdout = String::new();
        let mut last_stderr = String::new();
        let mut total_exit_code: i32 = 0;

        tracing::debug!(
            task_count = tasks.len(),
            "task_sequence: starting pipeline"
        );

        for (idx, task_entry) in tasks.into_iter().enumerate() {
            if task_entry.len() != 1 {
                return Err(ToolError::Configuration(format!(
                    "task_sequence: task[{idx}] must have exactly one labeled entry (got {})",
                    task_entry.len()
                )));
            }
            let (label, spec) = task_entry.into_iter().next().unwrap();

            // Augment the execution context with the running results
            // map + the previous task's data, so the next task's
            // templates (`{{ _prev.is_hot }}`, `{{ _results.transform.x }}`)
            // resolve.  The augmentation is per-task — sub-tasks see
            // the most up-to-date snapshot.
            let mut task_ctx = ctx.clone();
            task_ctx
                .variables
                .insert("_prev".to_string(), last_data.clone());
            task_ctx.variables.insert(
                "_results".to_string(),
                serde_json::Value::Object(
                    labeled_results
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                ),
            );

            let raw_task_config = Self::build_task_config(&label, &spec)?;

            // Render templates in the task config against the
            // augmented context so `_prev` / `_results` references
            // resolve before the sub-tool sees them.
            let rendered = self
                .template_engine
                .render_value(&raw_task_config.config, &task_ctx.to_template_context())?;
            let task_config = ToolConfig {
                kind: raw_task_config.kind,
                config: rendered,
                timeout: raw_task_config.timeout,
                retry: raw_task_config.retry,
                auth: raw_task_config.auth,
            };

            tracing::debug!(
                index = idx,
                label = %label,
                kind = %task_config.kind,
                "task_sequence: dispatching sub-task"
            );

            let task_result = registry
                .execute_from_config(&task_config, &task_ctx)
                .await?;

            // Carry forward stdout / stderr / exit code from each
            // sub-task — the pipeline's final ToolResult collects
            // the last task's I/O streams so logs / debugging see
            // the tail of the pipeline.  Aggregated structured data
            // lives under `labeled_results`.
            if let Some(stdout) = task_result.stdout.as_ref() {
                last_stdout = stdout.clone();
            }
            if let Some(stderr) = task_result.stderr.as_ref() {
                last_stderr = stderr.clone();
            }
            total_exit_code = task_result.exit_code.unwrap_or(0);

            if let Some(data) = &task_result.data {
                labeled_results.insert(label, data.clone());
                last_data = data.clone();
            } else {
                last_data = serde_json::Value::Null;
            }

            // Failure short-circuit: the orchestrator's
            // command.failed handler (noetl/ai-meta#58) emits
            // playbook.failed cleanly when the worker reports a
            // failed sub-task, so we don't run the rest of the
            // pipeline — the user's expectation of "first failure
            // stops the pipeline" matches the Python reference.
            if task_result.status == ToolStatus::Error {
                let duration_ms = start.elapsed().as_millis() as u64;
                return Ok(ToolResult {
                    status: ToolStatus::Error,
                    data: Some(serde_json::json!({
                        "labeled_results": labeled_results,
                        "failed_task": idx,
                    })),
                    error: task_result.error.clone().or_else(|| {
                        Some(format!("task_sequence task[{idx}] failed"))
                    }),
                    stdout: Some(last_stdout),
                    stderr: Some(last_stderr),
                    exit_code: Some(total_exit_code),
                    duration_ms: Some(duration_ms),
                });
            }
        }

        let duration_ms = start.elapsed().as_millis() as u64;
        Ok(ToolResult {
            status: ToolStatus::Success,
            data: Some(serde_json::Value::Object(
                labeled_results.into_iter().collect(),
            )),
            error: None,
            stdout: Some(last_stdout),
            stderr: Some(last_stderr),
            exit_code: Some(total_exit_code),
            duration_ms: Some(duration_ms),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tasks_bare_array_shape() {
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {"transform": {"kind": "python", "code": "result={}"}},
                {"save": {"kind": "python", "code": "result={}"}},
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let tasks = tool.parse_tasks(&config).expect("bare array parses");
        assert_eq!(tasks.len(), 2);
        assert!(tasks[0].contains_key("transform"));
        assert!(tasks[1].contains_key("save"));
    }

    #[test]
    fn test_parse_tasks_worker_envelope_shape() {
        // The Rust worker's command-dispatch path wraps the
        // rendered tool config in `{args, tool_config,
        // render_context}` before handing it to the registry.
        // task_sequence must walk into `tool_config` to find the
        // actual task list — without this, every flat-pipeline
        // playbook fixture (start_with_action, end_with_action,
        // iterator_save_test, http_test, postgres_test, ...)
        // failed with "config must be array or object with
        // 'tasks' field" in local kind.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!({
                "args": {},
                "tool_config": [
                    {"init_action": {"kind": "python", "code": "result={}"}},
                    {"finish":      {"kind": "python", "code": "result={}"}},
                ],
                "render_context": {"workload": {"test_value": "hello"}}
            }),
            timeout: None,
            retry: None,
            auth: None,
        };
        let tasks = tool.parse_tasks(&config).expect("worker envelope parses");
        assert_eq!(tasks.len(), 2);
        assert!(tasks[0].contains_key("init_action"));
        assert!(tasks[1].contains_key("finish"));
    }

    #[tokio::test]
    async fn test_task_sequence_duckdb_multi_statement() {
        // Regression (noetl/ai-meta#54): duckdb_test.yaml wraps a duckdb
        // tool in a single-item `tool:` list, so it dispatches through
        // task_sequence. A multi-statement query (CREATE; INSERT; SELECT)
        // must run end-to-end via the sub-task registry dispatch, the same
        // way it does for a direct single-tool duckdb step.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {"run_query": {
                    "kind": "duckdb",
                    "database": ":memory:",
                    "query": "CREATE TABLE u (id INTEGER, name VARCHAR);\nINSERT INTO u VALUES (1, 'Alice'), (2, 'Bob');\nSELECT name FROM u ORDER BY id;"
                }},
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool
            .execute(&config, &ctx)
            .await
            .expect("task_sequence duckdb sub-task runs");
        assert!(
            result.is_success(),
            "expected success, got: {:?}",
            result.error
        );
    }

    #[test]
    fn test_parse_tasks_nested_object_shape() {
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!({
                "tasks": [
                    {"a": {"kind": "python", "code": "result={}"}},
                ]
            }),
            timeout: None,
            retry: None,
            auth: None,
        };
        let tasks = tool.parse_tasks(&config).expect("nested object parses");
        assert_eq!(tasks.len(), 1);
    }

    #[test]
    fn test_parse_tasks_rejects_non_array_non_object() {
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!("not a pipeline"),
            timeout: None,
            retry: None,
            auth: None,
        };
        assert!(tool.parse_tasks(&config).is_err());
    }

    #[test]
    fn test_build_task_config_extracts_kind() {
        let spec = serde_json::json!({
            "kind": "python",
            "code": "result = {'x': 1}",
            "input": {"y": 2}
        });
        let cfg = TaskSequenceTool::build_task_config("compute", &spec)
            .expect("build_task_config succeeds");
        assert_eq!(cfg.kind, "python");
        // `kind` is stripped from inner config so the sub-tool's
        // deserializer doesn't see it.
        assert!(cfg.config.get("kind").is_none(), "kind must be stripped");
        assert!(cfg.config.get("code").is_some(), "code preserved");
        assert!(cfg.config.get("input").is_some(), "input preserved");
    }

    #[test]
    fn test_build_task_config_errors_on_missing_kind() {
        let spec = serde_json::json!({"code": "result = {}"});
        let err =
            TaskSequenceTool::build_task_config("anon", &spec).expect_err("missing kind rejected");
        assert!(format!("{:?}", err).contains("missing required 'kind'"));
    }

    #[tokio::test]
    async fn test_task_sequence_end_to_end_two_python_tasks() {
        // Two python tasks: first sets `result = {'value': 10}`;
        // second reads the first task's value via the
        // `{{ _prev.value }}` template substitution path.  Cross-
        // task references in v10 pipelines flow through the
        // template layer, not raw Python globals — the running
        // results are exposed under `_prev` + `_results` in the
        // Jinja context, and each task's config is rendered
        // against that context before dispatch.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "compute": {
                        "kind": "python",
                        "code": "result = {'value': 10}"
                    }
                },
                {
                    "double": {
                        "kind": "python",
                        "code": "result = {'doubled': {{ _prev.value }} * 2}"
                    }
                },
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool.execute(&config, &ctx).await.expect("execute ok");

        assert!(result.is_success(), "pipeline completes successfully");
        let data = result.data.expect("aggregated data present");
        assert!(
            data.get("compute").is_some(),
            "labeled result for first task"
        );
        assert!(
            data.get("double").is_some(),
            "labeled result for second task"
        );
        let doubled = data
            .get("double")
            .and_then(|v| v.get("doubled"))
            .and_then(|v| v.as_i64());
        assert_eq!(
            doubled,
            Some(20),
            "second task should see _prev.value = 10 and double it"
        );
    }

    #[tokio::test]
    async fn test_task_sequence_short_circuits_on_failure() {
        // First task succeeds; second task raises an exception
        // (exit_code != 0).  Pipeline should stop after the second
        // task and return Error status without running the third.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {"ok":   {"kind": "python", "code": "result = {'k': 1}"}},
                {"boom": {"kind": "python", "code": "raise ValueError('intentional')"}},
                {"never_runs": {"kind": "python", "code": "result = {'should_not': True}"}},
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool.execute(&config, &ctx).await.expect("execute returns Ok with Error status");

        assert_eq!(result.status, ToolStatus::Error);
        let data = result.data.expect("partial data present");
        // The error envelope marks which task failed; the
        // `never_runs` task did not contribute.
        let failed_idx = data.get("failed_task").and_then(|v| v.as_i64());
        assert_eq!(failed_idx, Some(1), "second task is the failure point");
        let labeled = data.get("labeled_results").and_then(|v| v.as_object()).unwrap();
        assert!(labeled.contains_key("ok"), "first task's result recorded");
        assert!(
            !labeled.contains_key("never_runs"),
            "third task was skipped after failure"
        );
    }

    #[test]
    fn test_task_sequence_tool_name() {
        let tool = TaskSequenceTool::new();
        assert_eq!(tool.name(), "task_sequence");
    }
}
