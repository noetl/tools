//! Task sequence tool — runs a list of sub-tasks in order through
//! the registry, using forward-only data propagation via explicit
//! `input:` / `set:` bindings per the NoETL DSL convention.
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
//! Data flow between tool items follows the DSL's established
//! forward-only convention (noetl_dsl_assignment_and_reference_spec
//! §7, noetl_dsl_refactoring_spec §4.2):
//!
//! - **`set:`** on a tool item evaluates expressions against the
//!   tool's `output` after execution, then merges the resolved
//!   key-value pairs into a running context shared across items.
//! - **`input:`** on a tool item resolves its values from the
//!   running context, then injects them as local template
//!   variables for the tool's own templates (`command:`, `query:`,
//!   `code:`, etc.).  The resolved `input:` also passes through
//!   to tools that read it natively (e.g. python's `input_data`).
//! - **`output`** is available inside `set:` expressions as the
//!   executed tool's result data (`{{ output.field }}`).
//!
//! The aggregated result data is a JSON object keyed by task
//! label.  Status is `Success` if every sub-task succeeded,
//! `Error` on the first sub-task failure (the rest are skipped
//! so the orchestrator's failure-termination logic — noetl/server
//! #63 — emits `playbook.failed` cleanly).
//!
//! Tracks noetl/tools#15, noetl/ai-meta#77.

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
#[allow(dead_code)]
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
    fn build_task_config(label: &str, spec: &serde_json::Value) -> Result<ToolConfig, ToolError> {
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

        // Strip `kind` (registry lookup key), `set` (forward-
        // propagation directive), and `spec` (policy rules — handled
        // by task_sequence, not by the sub-tool) from the inner
        // config.  Everything else (code, query, input, auth, ...)
        // passes through verbatim — the sub-tool decodes from its
        // ToolConfig.config payload exactly as it would for a
        // top-level step.
        let mut inner = spec_obj.clone();
        inner.remove("kind");
        inner.remove("set");
        inner.remove("spec");

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
        let mut last_stdout = String::new();
        let mut last_stderr = String::new();
        let mut total_exit_code: i32 = 0;

        // Running context that accumulates `set:` values across
        // tool items — the forward-only propagation surface.
        let mut running_ctx = ctx.clone();

        // Track context mutations from policy-rule `set:` so the
        // server can propagate them to subsequent steps.  Worker-side
        // task_sequence only sees one step's pipeline; cross-step
        // propagation requires the server to apply these mutations
        // to the execution state.  Keyed by the `set:` key exactly
        // as written in the playbook (e.g. `ctx.counter`).
        let mut context_updates: HashMap<String, serde_json::Value> = HashMap::new();

        // Build a label → index map so a `do: jump, to: <label>`
        // policy action can re-enter the pipeline at the named
        // sub-task.  The wire shape is an ordered list of single-key
        // `{label: spec}` entries, so positional index doubles as the
        // jump address.  Last definition wins on a duplicate label.
        let label_to_idx: HashMap<String, usize> = tasks
            .iter()
            .enumerate()
            .filter_map(|(i, entry)| entry.keys().next().map(|k| (k.clone(), i)))
            .collect();

        // Bound total sub-task executions so a `do: jump` loop whose
        // `do: break` condition is never reached can't spin forever
        // (noetl/ai-meta#125 acceptance: guard against infinite jump
        // loops).  Generous default; override via
        // NOETL_TASK_SEQUENCE_MAX_ITERATIONS for pathological drains.
        let max_iterations: u64 = std::env::var("NOETL_TASK_SEQUENCE_MAX_ITERATIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(1_000_000);

        // Per-label retry counter (noetl/ai-meta#125 `do: retry`).
        let mut retry_counts: HashMap<String, u32> = HashMap::new();
        let mut current_idx: usize = 0;
        let mut iterations: u64 = 0;

        tracing::debug!(task_count = tasks.len(), "task_sequence: starting pipeline");

        while current_idx < tasks.len() {
            iterations += 1;
            if iterations > max_iterations {
                let duration_ms = start.elapsed().as_millis() as u64;
                tracing::error!(
                    max_iterations,
                    "task_sequence: exceeded max iterations (possible infinite jump loop)"
                );
                return Ok(ToolResult {
                    status: ToolStatus::Error,
                    data: Some(serde_json::json!({
                        "labeled_results": labeled_results,
                        "error": "max_iterations_exceeded",
                    })),
                    error: Some(format!(
                        "task_sequence exceeded max iterations ({max_iterations}); possible infinite `do: jump` loop without a reachable `do: break`"
                    )),
                    stdout: Some(last_stdout),
                    stderr: Some(last_stderr),
                    exit_code: Some(1),
                    duration_ms: Some(duration_ms),
                    pending_callback: None,
                });
            }

            let idx = current_idx;
            let task_entry = &tasks[current_idx];
            if task_entry.len() != 1 {
                return Err(ToolError::Configuration(format!(
                    "task_sequence: task[{idx}] must have exactly one labeled entry (got {})",
                    task_entry.len()
                )));
            }
            // Clone label + spec: a sub-task may be re-executed by a
            // `do: jump` / `do: retry`, so the task list is borrowed,
            // not consumed.
            let (label, spec) = {
                let (l, s) = task_entry.iter().next().unwrap();
                (l.clone(), s.clone())
            };

            let spec_obj = spec.as_object().ok_or_else(|| {
                ToolError::Configuration(format!(
                    "task_sequence: task '{label}' spec must be a JSON object"
                ))
            })?;

            // Extract `set:` directive — not a tool config field;
            // evaluated post-execution for forward propagation.
            let set_block = spec_obj.get("set").cloned();

            // Extract `spec.policy.rules` — DSL policy rules whose
            // `then.set` directives are evaluated post-execution,
            // mirroring the Python server's per-tool policy handling.
            let policy_rules = spec_obj
                .get("spec")
                .and_then(|s| s.get("policy"))
                .and_then(|p| p.get("rules"))
                .and_then(|r| r.as_array())
                .cloned();

            // Build per-tool context from the running context
            // (includes vars from prior items' `set:` blocks).
            let mut task_ctx = running_ctx.clone();

            // Resolve `input:` values against the running context,
            // then inject the resolved key-value pairs as local
            // template vars so the tool's own templates (command,
            // query, code) can reference them by name.  The server
            // emits `input` (renamed from ToolSpec's `args` field);
            // fall back to `args` for robustness if `input` is
            // absent.
            let input_val = spec_obj.get("input").or_else(|| spec_obj.get("args"));
            if let Some(input_obj) = input_val.and_then(|v| v.as_object()) {
                // Build the proxied context once (pre-injection
                // snapshot) and reuse it across every `input:` value —
                // each input renders against the same frozen context,
                // matching the prior per-call `to_template_context()`
                // snapshot semantics without the repeated deep-clone
                // (noetl/ai-meta#127).
                let input_ctx = TemplateEngine::build_context_with_overlay(
                    &task_ctx.variables,
                    task_ctx.template_metadata(),
                );
                for (key, val) in input_obj {
                    let rendered = self.template_engine.render_value_with(val, &input_ctx)?;
                    task_ctx.variables.insert(key.clone(), rendered);
                }
            }

            let raw_task_config = Self::build_task_config(&label, &spec)?;

            // Render templates in the task config against the
            // augmented context (running vars + resolved input).  Build
            // the proxied context once and reuse it across the config
            // tree's templated fields (noetl/ai-meta#127).
            let task_ctx_value = TemplateEngine::build_context_with_overlay(
                &task_ctx.variables,
                task_ctx.template_metadata(),
            );
            let rendered = self
                .template_engine
                .render_value_with(&raw_task_config.config, &task_ctx_value)?;
            let task_config = ToolConfig {
                kind: raw_task_config.kind,
                config: rendered,
                timeout: raw_task_config.timeout,
                retry: raw_task_config.retry,
                auth: raw_task_config.auth,
            };

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

            let result_data = task_result.data.clone().unwrap_or(serde_json::Value::Null);
            labeled_results.insert(label.clone(), result_data.clone());

            // noetl/ai-meta#87: expose this sub-tool's result under its
            // label in the running context so a LATER sibling sub-tool
            // in the same multi-tool step can reference it via
            // `{{ <label>.<field> }}`.  Before this, only
            // `set:`-propagated values and the immediately-preceding
            // `output` were visible; a direct sibling reference rendered
            // empty — e.g. `{{ generate_large.metadata.record_count }}`
            // collapsed to `VALUES ('large_payload_test', , ...)` and
            // DuckDB raised `syntax error at or near ","`.  The bug was
            // masked wherever the reference sat in a quoted position (an
            // empty render is a valid `''`).
            //
            // Mirror the server's `build_context` shape (state.rs): when
            // the result is an object without its own `data` key, add a
            // synthetic `data` self-reference so both `{{ label.field }}`
            // and `{{ label.data.field }}` resolve — matching how a
            // single-tool step's result is exposed to next.arcs /
            // step.when downstream.  The result is also visible to a
            // later python sub-tool via the stdin `variables` map.
            let sibling_value = match &result_data {
                serde_json::Value::Object(map) if !map.contains_key("data") => {
                    let mut m = map.clone();
                    m.insert("data".to_string(), result_data.clone());
                    serde_json::Value::Object(m)
                }
                _ => result_data.clone(),
            };
            running_ctx.variables.insert(label.clone(), sibling_value);

            // Forward propagation: evaluate `set:` expressions
            // against the running context augmented with `output`
            // (this tool's result data), then merge the resolved
            // values into the running context for subsequent items.
            if let Some(set_val) = set_block {
                if let Some(set_obj) = set_val.as_object() {
                    // Build the proxied eval context once — base
                    // (running vars + resolved input) overlaid with
                    // `output` (this tool's raw result) — and reuse it
                    // across every `set:` expression, instead of cloning
                    // the whole ExecutionContext + a `to_template_context`
                    // deep-clone per block (noetl/ai-meta#127).
                    let set_eval_ctx = TemplateEngine::build_context_with_overlay(
                        &task_ctx.variables,
                        task_ctx
                            .template_metadata()
                            .into_iter()
                            .chain(std::iter::once(("output", result_data.clone()))),
                    );
                    for (key, expr) in set_obj {
                        let rendered = self.template_engine.render_value_with(expr, &set_eval_ctx)?;
                        set_nested_var(&mut running_ctx.variables, key, rendered);
                    }
                }
            }

            // Policy-rule evaluation: `spec.policy.rules` on a
            // tool item allows conditional `set:` and `do: fail`
            // based on the tool output.  The `output` context wraps
            // the raw result in a `{status, data}` envelope so
            // templates like `{{ output.data.counter }}` resolve
            // correctly (matches the Python server's convention).
            // The control action chosen by the first matching policy
            // rule.  `None` means no rule matched — the default action
            // is then derived from the sub-task outcome (error → fail,
            // success → continue), matching the Python reference.
            let mut matched_action: Option<ControlAction> = None;

            // Running attempt number for this sub-task (1-based), used
            // for `do: retry` accounting and exposed to policy
            // templates as `_attempt` (Python parity).
            let attempt = retry_counts.get(&label).copied().unwrap_or(0) + 1;

            if let Some(ref rules) = policy_rules {
                // Build the output envelope for policy evaluation
                let output_envelope = serde_json::json!({
                    "status": if task_result.status == ToolStatus::Success { "success" } else { "error" },
                    "data": result_data.clone(),
                    "error": {
                        "retryable": false
                    }
                });
                // Build the proxied policy-eval context once — base
                // (running vars + resolved input) overlaid with the
                // `output` envelope + `_attempt` — and reuse it across
                // every rule's `when:` / `set:` render, instead of a
                // per-step ExecutionContext clone + `to_template_context`
                // deep-clone (noetl/ai-meta#127).
                let policy_template_ctx = TemplateEngine::build_context_with_overlay(
                    &task_ctx.variables,
                    task_ctx.template_metadata().into_iter().chain([
                        ("output", output_envelope),
                        ("_attempt", serde_json::json!(attempt)),
                    ]),
                );

                for rule in rules {
                    let rule_obj = match rule.as_object() {
                        Some(o) => o,
                        None => continue,
                    };

                    // Determine if the rule matches and get the `then:` block.
                    let (matched, then_block) = if let Some(when_val) = rule_obj.get("when") {
                        // Conditional rule — render `when:` expression.
                        // YAML `when: true` / `when: false` arrives as
                        // a JSON boolean, not a string — handle it
                        // directly before falling through to template
                        // rendering for string expressions.
                        if let Some(b) = when_val.as_bool() {
                            (b, rule_obj.get("then"))
                        } else {
                            let condition_str = when_val.as_str().unwrap_or("");
                            let rendered = self
                                .template_engine
                                .render_with(condition_str, &policy_template_ctx)
                                .unwrap_or_default();
                            let is_truthy = !rendered.is_empty()
                                && rendered != "false"
                                && rendered != "False"
                                && rendered != "0"
                                && rendered != "none"
                                && rendered != "None"
                                && rendered != "null";
                            (is_truthy, rule_obj.get("then"))
                        }
                    } else if let Some(else_val) = rule_obj.get("else") {
                        // `else:` catch-all — always matches;
                        // `then:` is nested under the `else` key.
                        let then_val = else_val
                            .as_object()
                            .and_then(|o| o.get("then"));
                        (true, then_val)
                    } else {
                        (false, None)
                    };

                    if !matched {
                        continue;
                    }

                    if let Some(then_val) = then_block {
                        let then_obj = then_val.as_object();

                        // Apply `set:` mutations from the matching rule
                        if let Some(set_val) = then_obj.and_then(|o| o.get("set")) {
                            if let Some(set_obj) = set_val.as_object() {
                                for (key, expr) in set_obj {
                                    let rendered = self
                                        .template_engine
                                        .render_value_with(expr, &policy_template_ctx)?;
                                    set_nested_var(
                                        &mut running_ctx.variables,
                                        key,
                                        rendered.clone(),
                                    );
                                    // Record the mutation for cross-step
                                    // propagation via `_context_updates`
                                    // in the result payload.
                                    context_updates
                                        .insert(key.clone(), rendered);
                                }
                            }
                        }

                        // Capture the `do:` control action for the
                        // first matching rule.  Dispatch happens after
                        // the rule loop so `set:` mutations above are
                        // applied first (Python applies set, then the
                        // action).  Recognised verbs: continue / fail /
                        // break / jump / retry; an unrecognised verb
                        // falls back to `continue`.
                        if let Some(then_map) = then_obj {
                            matched_action = Some(parse_control_action(then_map));
                        }
                    }
                    break; // First matching rule wins
                }
            }

            // Resolve the effective control action: an explicit
            // matched rule wins; otherwise default by outcome
            // (error → fail, success → continue) — Python parity.
            let task_errored = task_result.status == ToolStatus::Error;
            let action = matched_action.unwrap_or(if task_errored {
                ControlAction::Fail
            } else {
                ControlAction::Continue
            });

            match action {
                ControlAction::Continue => {
                    current_idx += 1;
                }

                ControlAction::Break => {
                    // Clean stop (e.g. drain loop's `claim` returned 0
                    // rows).  Success with the accumulated results.
                    let duration_ms = start.elapsed().as_millis() as u64;
                    tracing::debug!(task = %label, "task_sequence: break");
                    return Ok(build_pipeline_result(
                        ToolStatus::Success,
                        labeled_results,
                        context_updates,
                        None,
                        last_stdout,
                        last_stderr,
                        total_exit_code,
                        duration_ms,
                    ));
                }

                ControlAction::Jump { to } => {
                    let target = to
                        .as_deref()
                        .and_then(|t| resolve_jump_target(t, idx, &label_to_idx));
                    match target {
                        Some(t) => {
                            tracing::debug!(from = %label, to = ?to, "task_sequence: jump");
                            current_idx = t;
                        }
                        None => {
                            let duration_ms = start.elapsed().as_millis() as u64;
                            return Ok(ToolResult {
                                status: ToolStatus::Error,
                                data: Some(serde_json::json!({
                                    "labeled_results": labeled_results,
                                    "failed_task": idx,
                                })),
                                error: Some(format!(
                                    "task_sequence: jump target {to:?} from task[{idx}] '{label}' not found"
                                )),
                                stdout: Some(last_stdout),
                                stderr: Some(last_stderr),
                                exit_code: Some(1),
                                duration_ms: Some(duration_ms),
                                pending_callback: None,
                            });
                        }
                    }
                }

                ControlAction::Retry {
                    attempts,
                    backoff,
                    delay,
                } => {
                    let count = retry_counts.entry(label.clone()).or_insert(0);
                    *count += 1;
                    if *count >= attempts {
                        tracing::error!(
                            task = %label,
                            attempts,
                            "task_sequence: retry exhausted"
                        );
                        let duration_ms = start.elapsed().as_millis() as u64;
                        return Ok(ToolResult {
                            status: ToolStatus::Error,
                            data: Some(serde_json::json!({
                                "labeled_results": labeled_results,
                                "failed_task": idx,
                            })),
                            error: task_result.error.clone().or_else(|| {
                                Some(format!(
                                    "task_sequence task[{idx}] '{label}' exceeded retry attempts ({attempts})"
                                ))
                            }),
                            stdout: Some(last_stdout),
                            stderr: Some(last_stderr),
                            exit_code: Some(total_exit_code),
                            duration_ms: Some(duration_ms),
                            pending_callback: None,
                        });
                    }
                    let delay_secs = calc_retry_delay(&backoff, delay, *count);
                    if delay_secs > 0.0 {
                        tracing::debug!(
                            task = %label,
                            delay_secs,
                            backoff = %backoff,
                            "task_sequence: retry backoff"
                        );
                        tokio::time::sleep(std::time::Duration::from_secs_f64(delay_secs)).await;
                    }
                    tracing::debug!(
                        task = %label,
                        attempt = *count + 1,
                        attempts,
                        "task_sequence: retrying"
                    );
                    // current_idx unchanged → re-execute this sub-task.
                }

                ControlAction::Fail => {
                    let duration_ms = start.elapsed().as_millis() as u64;
                    return Ok(ToolResult {
                        status: ToolStatus::Error,
                        data: Some(serde_json::json!({
                            "labeled_results": labeled_results,
                            "failed_task": idx,
                        })),
                        error: task_result.error.clone().or_else(|| {
                            Some(format!(
                                "policy rule triggered fail for task[{idx}] '{label}'"
                            ))
                        }),
                        stdout: Some(last_stdout),
                        stderr: Some(last_stderr),
                        exit_code: Some(if task_errored { total_exit_code } else { 1 }),
                        duration_ms: Some(duration_ms),
                        pending_callback: None,
                    });
                }
            }

            // Sub-task failure with no policy rule to handle it is
            // covered above: the default action for an errored task is
            // `Fail`, which returns from the `match` arm.  Reaching
            // here means the action was Continue / Jump / Retry, so we
            // loop with the (already-updated) `current_idx`.
        }

        // Pipeline drained normally (ran off the end of the task list).
        let duration_ms = start.elapsed().as_millis() as u64;
        Ok(build_pipeline_result(
            ToolStatus::Success,
            labeled_results,
            context_updates,
            None,
            last_stdout,
            last_stderr,
            total_exit_code,
            duration_ms,
        ))
    }
}

/// Control-flow verb produced by a matching `spec.policy.rules`
/// `then.do` directive (noetl/ai-meta#125).  Mirrors the Python
/// `task_sequence_executor` `ControlAction`.
#[derive(Debug, Clone)]
enum ControlAction {
    /// Advance to the next sub-task.
    Continue,
    /// Re-run the current sub-task up to `attempts` times, sleeping
    /// `calc_retry_delay(backoff, delay, attempt)` seconds between.
    Retry {
        attempts: u32,
        backoff: String,
        delay: f64,
    },
    /// Re-enter the pipeline at the sub-task named by `to` (or the
    /// previous sub-task for the special `previous`/`prev` targets).
    Jump { to: Option<String> },
    /// Stop the pipeline cleanly and report success.
    Break,
    /// Stop the pipeline and report failure.
    Fail,
}

/// Parse a policy rule's `then` block into a [`ControlAction`].
/// Defaults mirror the Python reference: `attempts` 3, `backoff`
/// "none", `delay` 1.0.  An unrecognised `do:` verb (or a missing
/// one) falls back to `Continue` so a malformed rule never wedges
/// the pipeline.
fn parse_control_action(then_obj: &serde_json::Map<String, serde_json::Value>) -> ControlAction {
    let do_str = then_obj.get("do").and_then(|v| v.as_str()).unwrap_or("continue");
    match do_str {
        "break" => ControlAction::Break,
        "fail" => ControlAction::Fail,
        "jump" => ControlAction::Jump {
            to: then_obj
                .get("to")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        },
        "retry" => {
            let attempts = then_obj
                .get("attempts")
                .and_then(json_to_u32)
                .unwrap_or(3)
                .max(1);
            let backoff = then_obj
                .get("backoff")
                .and_then(|v| v.as_str())
                .unwrap_or("none")
                .to_string();
            let delay = then_obj
                .get("delay")
                .and_then(json_to_f64)
                .unwrap_or(1.0);
            ControlAction::Retry {
                attempts,
                backoff,
                delay,
            }
        }
        // "continue" and anything unrecognised.
        _ => ControlAction::Continue,
    }
}

/// Coerce a JSON number/string into u32 (YAML scalars can arrive
/// either way after templating).
fn json_to_u32(v: &serde_json::Value) -> Option<u32> {
    v.as_u64()
        .map(|n| n as u32)
        .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
}

/// Coerce a JSON number/string into f64.
fn json_to_f64(v: &serde_json::Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
}

/// Retry backoff (seconds) for a given attempt, matching the Python
/// `_calculate_delay`: `none` → 0, `linear` → delay·attempt,
/// `exponential` → delay·2^(attempt-1), otherwise → delay.
fn calc_retry_delay(backoff: &str, delay: f64, attempt: u32) -> f64 {
    match backoff {
        "none" => 0.0,
        "linear" => delay * attempt as f64,
        "exponential" => delay * 2f64.powi((attempt as i32) - 1),
        _ => delay,
    }
}

/// Resolve a `do: jump` target label to a task index.  Supports the
/// special `previous` / `prev` / `_previous` / `@previous` targets
/// (jump one sub-task back); returns `None` if the label is unknown
/// or `previous` is requested from the first sub-task.
fn resolve_jump_target(
    to: &str,
    current_idx: usize,
    label_to_idx: &HashMap<String, usize>,
) -> Option<usize> {
    let trimmed = to.trim();
    if matches!(
        trimmed.to_ascii_lowercase().as_str(),
        "previous" | "prev" | "_previous" | "@previous"
    ) {
        return if current_idx == 0 {
            None
        } else {
            Some(current_idx - 1)
        };
    }
    label_to_idx.get(trimmed).copied()
}

/// Build the aggregated pipeline [`ToolResult`].  Merges
/// `_context_updates` into the data payload only when policy rules
/// produced mutations, so existing consumers are unaffected when the
/// key is absent.
#[allow(clippy::too_many_arguments)]
fn build_pipeline_result(
    status: ToolStatus,
    labeled_results: HashMap<String, serde_json::Value>,
    context_updates: HashMap<String, serde_json::Value>,
    error: Option<String>,
    stdout: String,
    stderr: String,
    exit_code: i32,
    duration_ms: u64,
) -> ToolResult {
    let mut result_map: serde_json::Map<String, serde_json::Value> =
        labeled_results.into_iter().collect();
    if !context_updates.is_empty() {
        result_map.insert(
            "_context_updates".to_string(),
            serde_json::to_value(&context_updates).unwrap_or(serde_json::Value::Null),
        );
    }

    ToolResult {
        status,
        data: Some(serde_json::Value::Object(result_map)),
        error,
        stdout: Some(stdout),
        stderr: Some(stderr),
        exit_code: Some(exit_code),
        duration_ms: Some(duration_ms),
        pending_callback: None,
    }
}

/// Insert a value into a context map, supporting dotted keys.
///
/// `"foo"` inserts a flat key.  `"a.b.c"` builds nested objects:
/// `ctx["a"]["b"]["c"] = value`.  This mirrors the DSL's scoped
/// variable convention (`iter.page`, `ctx.stats_ref`, `step.x`)
/// where the dot is a namespace separator.
fn set_nested_var(
    ctx: &mut HashMap<String, serde_json::Value>,
    key: &str,
    value: serde_json::Value,
) {
    let parts: Vec<&str> = key.split('.').collect();
    if parts.len() == 1 {
        ctx.insert(key.to_string(), value);
        return;
    }
    // Build nested: "a.b.c" → ctx["a"]["b"]["c"] = value
    let root = parts[0].to_string();
    let entry = ctx.entry(root).or_insert_with(|| serde_json::json!({}));
    let mut current = entry;
    for part in &parts[1..parts.len() - 1] {
        if !current.is_object() {
            *current = serde_json::json!({});
        }
        current = current
            .as_object_mut()
            .unwrap()
            .entry(part.to_string())
            .or_insert_with(|| serde_json::json!({}));
    }
    if let Some(obj) = current.as_object_mut() {
        obj.insert(parts.last().unwrap().to_string(), value);
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
    fn test_build_task_config_extracts_kind_and_strips_set() {
        let spec = serde_json::json!({
            "kind": "python",
            "code": "result = {'x': 1}",
            "input": {"y": 2},
            "set": {"val": "{{ output.x }}"}
        });
        let cfg = TaskSequenceTool::build_task_config("compute", &spec)
            .expect("build_task_config succeeds");
        assert_eq!(cfg.kind, "python");
        // `kind` and `set` are stripped from inner config so the
        // sub-tool's deserializer doesn't see them.
        assert!(cfg.config.get("kind").is_none(), "kind must be stripped");
        assert!(cfg.config.get("set").is_none(), "set must be stripped");
        assert!(cfg.config.get("code").is_some(), "code preserved");
        assert!(cfg.config.get("input").is_some(), "input preserved");
    }

    #[test]
    fn test_set_nested_var_flat_key() {
        let mut ctx = HashMap::new();
        set_nested_var(&mut ctx, "name", serde_json::json!("hello"));
        assert_eq!(ctx.get("name"), Some(&serde_json::json!("hello")));
    }

    #[test]
    fn test_set_nested_var_dotted_key() {
        let mut ctx = HashMap::new();
        set_nested_var(&mut ctx, "data.id", serde_json::json!(42));
        set_nested_var(&mut ctx, "data.name", serde_json::json!("test"));
        let data = ctx.get("data").expect("data key exists");
        assert_eq!(data.get("id"), Some(&serde_json::json!(42)));
        assert_eq!(data.get("name"), Some(&serde_json::json!("test")));
    }

    #[test]
    fn test_set_nested_var_deep_dotted_key() {
        let mut ctx = HashMap::new();
        set_nested_var(&mut ctx, "a.b.c", serde_json::json!(true));
        let a = ctx.get("a").unwrap();
        let b = a.get("b").unwrap();
        assert_eq!(b.get("c"), Some(&serde_json::json!(true)));
    }

    #[test]
    fn test_build_task_config_errors_on_missing_kind() {
        let spec = serde_json::json!({"code": "result = {}"});
        let err =
            TaskSequenceTool::build_task_config("anon", &spec).expect_err("missing kind rejected");
        assert!(format!("{:?}", err).contains("missing required 'kind'"));
    }

    #[tokio::test]
    async fn test_task_sequence_forward_propagation_via_set_and_input() {
        // Two python tasks: first produces `{'value': 10}` and
        // publishes it via `set:`.  Second reads it via `input:`
        // from the running context — forward-only, no `_prev`.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "compute": {
                        "kind": "python",
                        "code": "result = {'value': 10}",
                        "set": {
                            "computed_value": "{{ output.value }}"
                        }
                    }
                },
                {
                    "double": {
                        "kind": "python",
                        "code": "result = {'doubled': {{ computed_value }} * 2}",
                        "input": {
                            "computed_value": "{{ computed_value }}"
                        }
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
            "second task should see computed_value = 10 via set:/input: and double it"
        );
    }

    #[tokio::test]
    async fn test_task_sequence_sibling_reference_without_set() {
        // noetl/ai-meta#87: a later sub-tool references an earlier
        // sibling's output directly by label — `{{ <label>.<field> }}`
        // — with NO `set:` and NO `input:` plumbing.  Before the fix
        // the reference rendered empty; here `record_count` collapsing
        // to nothing would make the consumer's `result = {'count': }`
        // a Python syntax error (the same failure shape as the
        // unquoted-SQL `VALUES (..., , ...)` in save_edge_cases).
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "generate_large": {
                        "kind": "python",
                        "code": "result = {'metadata': {'record_count': 100}, 'records': []}"
                    }
                },
                {
                    "save_large_payload": {
                        "kind": "python",
                        // Unquoted numeric position — mirrors the DuckDB
                        // `VALUES ('x', {{ generate_large.metadata.record_count }}, ...)`.
                        "code": "result = {'count': {{ generate_large.metadata.record_count }}}"
                    }
                },
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool.execute(&config, &ctx).await.expect("execute ok");

        assert!(
            result.is_success(),
            "sibling reference must resolve; pipeline failed: {:?}",
            result.error,
        );
        let data = result.data.expect("data present");
        let count = data
            .get("save_large_payload")
            .and_then(|v| v.get("count"))
            .and_then(|v| v.as_i64());
        assert_eq!(
            count,
            Some(100),
            "later sub-tool must read the earlier sibling's nested field",
        );
    }

    #[tokio::test]
    async fn test_task_sequence_sibling_reference_synthetic_data_accessor() {
        // The sibling result is exposed under both `{{ label.field }}`
        // and `{{ label.data.field }}` (the synthetic `.data`
        // self-reference mirrors the server's build_context shape), and
        // a producer that already returns a `data` key keeps its real
        // nested shape under `{{ label.data.field }}`.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "flat_producer": {
                        "kind": "python",
                        "code": "result = {'n': 7}"
                    }
                },
                {
                    "wrapped_producer": {
                        "kind": "python",
                        // Already carries its own `data` key.
                        "code": "result = {'status': 'ok', 'data': {'m': 9}}"
                    }
                },
                {
                    "consume": {
                        "kind": "python",
                        // Synthetic `.data` on the flat producer +
                        // real `.data` on the wrapped one.
                        "code": "result = {'flat': {{ flat_producer.data.n }}, 'wrapped': {{ wrapped_producer.data.m }}}"
                    }
                },
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool.execute(&config, &ctx).await.expect("execute ok");

        assert!(
            result.is_success(),
            "both data accessors must resolve; failed: {:?}",
            result.error,
        );
        let data = result.data.expect("data present");
        let consume = data.get("consume").expect("consume result");
        assert_eq!(consume.get("flat").and_then(|v| v.as_i64()), Some(7));
        assert_eq!(consume.get("wrapped").and_then(|v| v.as_i64()), Some(9));
    }

    #[tokio::test]
    async fn test_task_sequence_set_with_dotted_keys() {
        // Verify that `set:` with dotted keys like `data.id`
        // builds nested objects in the running context.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "produce": {
                        "kind": "python",
                        "code": "result = {'id': 42, 'name': 'test'}",
                        "set": {
                            "data.id": "{{ output.id }}",
                            "data.name": "{{ output.name }}"
                        }
                    }
                },
                {
                    "consume": {
                        "kind": "python",
                        "code": "result = {'got_id': {{ data.id }}, 'got_name': '{{ data.name }}'}"
                    }
                },
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool.execute(&config, &ctx).await.expect("execute ok");

        assert!(result.is_success(), "pipeline completes");
        let data = result.data.expect("data present");
        let got_id = data
            .get("consume")
            .and_then(|v| v.get("got_id"))
            .and_then(|v| v.as_i64());
        assert_eq!(got_id, Some(42));
    }

    #[tokio::test]
    async fn test_task_sequence_input_as_tool_input_data() {
        // Verify that `input:` passes through to the python tool
        // as `input_data` while also making values available as
        // template vars.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "produce": {
                        "kind": "python",
                        "code": "result = {'x': 99}",
                        "set": {
                            "x_val": "{{ output.x }}"
                        }
                    }
                },
                {
                    "consume": {
                        "kind": "python",
                        "input": {
                            "my_x": "{{ x_val }}"
                        },
                        "code": "result = {'from_input': input_data.get('my_x', 'missing')}"
                    }
                },
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool.execute(&config, &ctx).await.expect("execute ok");

        assert!(result.is_success());
        let data = result.data.expect("data present");
        let from_input = data
            .get("consume")
            .and_then(|v| v.get("from_input"));
        assert_eq!(from_input, Some(&serde_json::json!(99)));
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
        let result = tool
            .execute(&config, &ctx)
            .await
            .expect("execute returns Ok with Error status");

        assert_eq!(result.status, ToolStatus::Error);
        let data = result.data.expect("partial data present");
        // The error envelope marks which task failed; the
        // `never_runs` task did not contribute.
        let failed_idx = data.get("failed_task").and_then(|v| v.as_i64());
        assert_eq!(failed_idx, Some(1), "second task is the failure point");
        let labeled = data
            .get("labeled_results")
            .and_then(|v| v.as_object())
            .unwrap();
        assert!(labeled.contains_key("ok"), "first task's result recorded");
        assert!(
            !labeled.contains_key("never_runs"),
            "third task was skipped after failure"
        );
    }

    #[tokio::test]
    async fn test_task_sequence_policy_rule_set_else() {
        // Verify `spec.policy.rules` with an `else` catch-all
        // applies `set:` mutations using the `output.data.*`
        // envelope convention.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "produce": {
                        "kind": "python",
                        "code": "result = {'counter': 42, 'message': 'works'}",
                        "spec": {
                            "policy": {
                                "rules": [{
                                    "else": {
                                        "then": {
                                            "do": "continue",
                                            "set": {
                                                "ctx.counter": "{{ output.data.counter }}",
                                                "ctx.message": "{{ output.data.message }}"
                                            }
                                        }
                                    }
                                }]
                            }
                        }
                    }
                },
                {
                    "consume": {
                        "kind": "python",
                        "input": {
                            "counter": "{{ ctx.counter }}",
                            "msg": "{{ ctx.message }}"
                        },
                        "code": "result = {'got_counter': counter, 'got_msg': msg}"
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
        let consume = data.get("consume").expect("consume result present");
        assert_eq!(
            consume.get("got_counter").and_then(|v| v.as_i64()),
            Some(42),
            "policy-rule set: propagated counter via output.data.counter"
        );
        assert_eq!(
            consume.get("got_msg").and_then(|v| v.as_str()),
            Some("works"),
            "policy-rule set: propagated message via output.data.message"
        );
    }

    #[test]
    fn test_task_sequence_tool_name() {
        let tool = TaskSequenceTool::new();
        assert_eq!(tool.name(), "task_sequence");
    }

    #[test]
    fn test_render_value_roundtrip_complex_object() {
        // Regression test for iterator_save_test: verify that
        // rendering {{ output.data }} where output.data is a complex
        // object round-trips correctly through render_value so that
        // subsequent access to nested fields (.item_name) works.
        use crate::template::TemplateEngine;

        let engine = TemplateEngine::new();

        // Simulate the output envelope the policy evaluation builds
        let output = serde_json::json!({
            "status": "success",
            "data": {"item_name": "item1", "item_value": 100},
            "error": {"retryable": false}
        });
        let mut ctx: HashMap<String, serde_json::Value> = HashMap::new();
        ctx.insert("output".to_string(), output);
        ctx.insert("iter".to_string(), serde_json::json!({
            "item": {"name": "item1", "value": 100},
            "_index": 0,
            "_total": 3,
        }));

        // Step 1: Render {{ output.data }} — this is what the set: block does
        let expr = serde_json::json!("{{ output.data }}");
        let rendered = engine.render_value(&expr, &ctx).expect("render_value ok");

        // The rendered value MUST be a JSON object, not a string
        assert!(
            rendered.is_object(),
            "expected object, got: {:?} (type: {})",
            rendered,
            match &rendered {
                serde_json::Value::String(_) => "string",
                serde_json::Value::Object(_) => "object",
                serde_json::Value::Null => "null",
                _ => "other",
            }
        );

        // Step 2: Apply the set mutation: iter.processed_item = rendered
        set_nested_var(&mut ctx, "iter.processed_item", rendered);

        // Step 3: Verify we can access iter.processed_item.item_name
        let result = engine.render("{{ iter.processed_item.item_name }}", &ctx)
            .expect("nested field resolves");
        assert_eq!(result, "item1", "nested field access should work");
    }

    #[test]
    fn test_policy_rules_extraction_from_wire_format() {
        // Reproduce the exact wire format the server produces for
        // iterator_save_test and verify that parse_tasks + policy
        // rules extraction works correctly.
        let tool = TaskSequenceTool::new();

        // This is the wire format: tool_config array wrapped in
        // a worker envelope with kind injected.
        let wire_config = serde_json::json!({
            "kind": "task_sequence",
            "tool_config": [
                {
                    "process_item": {
                        "kind": "python",
                        "code": "result = {'item_name': item.get('name', 'unknown'), 'item_value': item.get('value', 0)}",
                        "input": {"item": "{{ iter.item }}"},
                        "name": "process_item",
                        "spec": {
                            "policy": {
                                "rules": [
                                    {
                                        "when": true,
                                        "then": {
                                            "do": "continue",
                                            "set": {
                                                "iter.processed_item": "{{ output.data }}"
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                },
                {
                    "save_item": {
                        "kind": "postgres",
                        "command": "INSERT INTO test (item_name) VALUES ('{{ iter.processed_item.item_name }}')",
                        "name": "save_item",
                        "host": "localhost",
                        "port": 5432,
                        "database": "test",
                        "user": "test",
                        "password": "test"
                    }
                }
            ]
        });

        let tool_config = crate::registry::ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!({"tool_config": wire_config["tool_config"].clone()}),
            timeout: None,
            retry: None,
            auth: None,
        };

        let tasks = tool.parse_tasks(&tool_config).expect("parse_tasks should work");
        assert_eq!(tasks.len(), 2, "should have 2 tasks");

        // Check process_item has spec.policy.rules
        let process_item_entry = &tasks[0];
        assert_eq!(process_item_entry.len(), 1, "single-entry map");
        let (label, spec) = process_item_entry.iter().next().unwrap();
        assert_eq!(label, "process_item");

        let spec_obj = spec.as_object().expect("spec should be object");
        let policy_rules = spec_obj
            .get("spec")
            .and_then(|s| s.get("policy"))
            .and_then(|p| p.get("rules"))
            .and_then(|r| r.as_array());

        assert!(
            policy_rules.is_some(),
            "process_item should have policy rules; spec_obj keys = {:?}, spec_obj = {}",
            spec_obj.keys().collect::<Vec<_>>(),
            serde_json::to_string_pretty(spec_obj).unwrap()
        );
        assert_eq!(policy_rules.unwrap().len(), 1, "should have 1 policy rule");

        // Verify the rule has a set block
        let rule = &policy_rules.unwrap()[0];
        let set_block = rule
            .get("then")
            .and_then(|t| t.get("set"));
        assert!(set_block.is_some(), "rule should have a set block");
    }

    // ----------------------------------------------------------------
    // noetl/ai-meta#125 — control-flow verbs: jump / break / retry
    // ----------------------------------------------------------------

    #[test]
    fn test_parse_control_action_verbs() {
        let mk = |s: &str| -> serde_json::Map<String, serde_json::Value> {
            s.parse::<serde_json::Value>()
                .unwrap()
                .as_object()
                .unwrap()
                .clone()
        };

        assert!(matches!(
            parse_control_action(&mk(r#"{"do":"continue"}"#)),
            ControlAction::Continue
        ));
        assert!(matches!(
            parse_control_action(&mk(r#"{"do":"break"}"#)),
            ControlAction::Break
        ));
        assert!(matches!(
            parse_control_action(&mk(r#"{"do":"fail"}"#)),
            ControlAction::Fail
        ));
        // Unknown verb falls back to continue (never wedges).
        assert!(matches!(
            parse_control_action(&mk(r#"{"do":"frobnicate"}"#)),
            ControlAction::Continue
        ));
        // Missing `do` defaults to continue.
        assert!(matches!(
            parse_control_action(&mk(r#"{"set":{"a":"b"}}"#)),
            ControlAction::Continue
        ));

        match parse_control_action(&mk(r#"{"do":"jump","to":"claim_batch"}"#)) {
            ControlAction::Jump { to } => assert_eq!(to.as_deref(), Some("claim_batch")),
            other => panic!("expected jump, got {other:?}"),
        }

        // Retry defaults: attempts 3, backoff none, delay 1.0.
        match parse_control_action(&mk(r#"{"do":"retry"}"#)) {
            ControlAction::Retry {
                attempts,
                backoff,
                delay,
            } => {
                assert_eq!(attempts, 3);
                assert_eq!(backoff, "none");
                assert_eq!(delay, 1.0);
            }
            other => panic!("expected retry, got {other:?}"),
        }
        // Retry with explicit fields; attempts as a string coerces.
        match parse_control_action(&mk(r#"{"do":"retry","attempts":"5","backoff":"exponential","delay":2}"#)) {
            ControlAction::Retry {
                attempts,
                backoff,
                delay,
            } => {
                assert_eq!(attempts, 5);
                assert_eq!(backoff, "exponential");
                assert_eq!(delay, 2.0);
            }
            other => panic!("expected retry, got {other:?}"),
        }
    }

    #[test]
    fn test_calc_retry_delay_backoff() {
        assert_eq!(calc_retry_delay("none", 1.0, 3), 0.0);
        assert_eq!(calc_retry_delay("linear", 1.5, 3), 4.5);
        assert_eq!(calc_retry_delay("exponential", 1.0, 1), 1.0);
        assert_eq!(calc_retry_delay("exponential", 1.0, 3), 4.0);
        assert_eq!(calc_retry_delay("unknown", 2.0, 9), 2.0);
    }

    #[test]
    fn test_resolve_jump_target() {
        let mut map = HashMap::new();
        map.insert("claim_batch".to_string(), 0usize);
        map.insert("fetch_batch".to_string(), 1usize);
        map.insert("save_batch".to_string(), 2usize);

        assert_eq!(resolve_jump_target("claim_batch", 2, &map), Some(0));
        assert_eq!(resolve_jump_target(" save_batch ", 0, &map), Some(2));
        assert_eq!(resolve_jump_target("nope", 0, &map), None);
        // `previous` jumps one back; invalid from the first task.
        assert_eq!(resolve_jump_target("previous", 2, &map), Some(1));
        assert_eq!(resolve_jump_target("PREV", 1, &map), Some(0));
        assert_eq!(resolve_jump_target("previous", 0, &map), None);
    }

    #[tokio::test]
    async fn test_task_sequence_jump_drains_loop() {
        // Mirrors the pft batch drain loop: `claim` yields work until a
        // counter hits zero (then `do: break`); `save` jumps back to
        // `claim`.  Before the fix `do: jump` was a no-op, so the
        // pipeline ran claim→save exactly once.  With the fix it loops
        // until the break condition, draining the counter to -1.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "claim": {
                        "kind": "python",
                        "input": {"counter": "{{ iter.counter }}"},
                        "code": "result = {'remaining': {{ counter }} - 1, 'work': (1 if {{ counter }} > 0 else 0)}",
                        "spec": {"policy": {"rules": [
                            {"when": "{{ output.data.work == 0 }}", "then": {"do": "break"}},
                            {"else": {"then": {"do": "continue", "set": {
                                "iter.counter": "{{ output.data.remaining }}"
                            }}}}
                        ]}}
                    }
                },
                {
                    "save": {
                        "kind": "python",
                        "code": "result = {'saved': True}",
                        "spec": {"policy": {"rules": [
                            {"else": {"then": {"do": "jump", "to": "claim"}}}
                        ]}}
                    }
                },
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let mut ctx = ExecutionContext::default();
        ctx.variables
            .insert("iter".to_string(), serde_json::json!({"counter": 3}));

        let result = tool.execute(&config, &ctx).await.expect("execute ok");
        assert!(
            result.is_success(),
            "drain loop should break cleanly: {:?}",
            result.error
        );
        let data = result.data.expect("data present");
        // Last claim ran at counter=0 → remaining=-1 → work=0 → break.
        // If `jump` were a no-op, claim would have run once (remaining=2).
        let remaining = data
            .get("claim")
            .and_then(|c| c.get("remaining"))
            .and_then(|v| v.as_i64());
        assert_eq!(
            remaining,
            Some(-1),
            "claim must have re-run via jump until the break condition"
        );
    }

    #[tokio::test]
    async fn test_task_sequence_break_stops_cleanly() {
        // `do: break` returns success and skips the rest of the pipeline.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "gate": {
                        "kind": "python",
                        "code": "result = {'go': 0}",
                        "spec": {"policy": {"rules": [
                            {"when": "{{ output.data.go == 0 }}", "then": {"do": "break"}}
                        ]}}
                    }
                },
                {
                    "after": {
                        "kind": "python",
                        "code": "result = {'ran': True}"
                    }
                },
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool.execute(&config, &ctx).await.expect("execute ok");
        assert!(result.is_success(), "break is a clean success");
        let data = result.data.expect("data present");
        assert!(data.get("gate").is_some(), "gate ran");
        assert!(
            data.get("after").is_none(),
            "tasks after a break must not run"
        );
    }

    #[tokio::test]
    async fn test_task_sequence_retry_counts_attempts() {
        // A task that retries while `_attempt < 3`, then continues.
        // Proves retry re-executes the same task and increments the
        // attempt counter; the final `_attempt` is captured via `set`.
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "flaky": {
                        "kind": "python",
                        "code": "result = {'x': 1}",
                        "spec": {"policy": {"rules": [
                            {"when": "{{ _attempt < 3 }}", "then": {"do": "retry", "attempts": 5}},
                            {"else": {"then": {"do": "continue", "set": {
                                "ctx.final_attempt": "{{ _attempt }}"
                            }}}}
                        ]}}
                    }
                },
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool.execute(&config, &ctx).await.expect("execute ok");
        assert!(result.is_success(), "retries then continues: {:?}", result.error);
        let data = result.data.expect("data present");
        let final_attempt = data
            .get("_context_updates")
            .and_then(|u| u.get("ctx.final_attempt"))
            .and_then(|v| v.as_i64());
        assert_eq!(
            final_attempt,
            Some(3),
            "task should have run 3 times (retried twice) before continuing"
        );
    }

    #[tokio::test]
    async fn test_task_sequence_retry_exhausted_fails() {
        // A task that always errors with `do: retry, attempts: 2`
        // exhausts its budget and the pipeline fails (no infinite loop).
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "always_fail": {
                        "kind": "python",
                        "code": "raise ValueError('nope')",
                        "spec": {"policy": {"rules": [
                            {"when": "{{ output.status == \"error\" }}", "then": {"do": "retry", "attempts": 2}}
                        ]}}
                    }
                },
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool
            .execute(&config, &ctx)
            .await
            .expect("execute returns Ok with Error status");
        assert_eq!(result.status, ToolStatus::Error, "retry budget exhausted");
        let failed = result
            .data
            .and_then(|d| d.get("failed_task").and_then(|v| v.as_i64()));
        assert_eq!(failed, Some(0));
    }

    #[tokio::test]
    async fn test_task_sequence_jump_to_unknown_label_errors() {
        let tool = TaskSequenceTool::new();
        let config = ToolConfig {
            kind: "task_sequence".to_string(),
            config: serde_json::json!([
                {
                    "t": {
                        "kind": "python",
                        "code": "result = {'ok': True}",
                        "spec": {"policy": {"rules": [
                            {"else": {"then": {"do": "jump", "to": "does_not_exist"}}}
                        ]}}
                    }
                },
            ]),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let result = tool.execute(&config, &ctx).await.expect("execute ok");
        assert_eq!(result.status, ToolStatus::Error);
        assert!(
            result
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("not found"),
            "error should name the missing jump target: {:?}",
            result.error
        );
    }
}
