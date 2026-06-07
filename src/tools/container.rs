//! `container` tool — dispatch a long-running K8s Job and return
//! immediately via the [Container Tool Callback][umbrella] pattern.
//!
//! Round 3 of the umbrella ([noetl/ai-meta#43](https://github.com/noetl/ai-meta/issues/43)).
//! Tracks [noetl/tools#36](https://github.com/noetl/tools/issues/36).
//!
//! ## Why this exists
//!
//! Other compute tools (`python`, `http`, `shell`, `duckdb`,
//! `postgres`) finish in seconds.  A container tool kind can run for
//! minutes or hours (training jobs, ETL pipelines, long-form
//! inference).  Per `agents/rules/execution-model.md` the worker slot
//! MUST NOT block waiting for the container to finish — that breaks
//! the atomic-block model and starves real-time playbooks.
//!
//! This tool creates the K8s Job, labels it with the execution
//! id + step name, and returns immediately.  The worker slot frees
//! as soon as the create-Job RPC returns.  The
//! [noetl-k8s-watcher][watcher] (Round 1) observes Job state
//! transitions and POSTs terminal-state events to noetl-server's
//! container-callback endpoint (Round 2); the orchestrator's
//! resume event lands from there.
//!
//! ## Wire shape
//!
//! Playbook step:
//!
//! ```yaml
//! - step: train_model
//!   tool:
//!     kind: container
//!     image: gcr.io/my-project/trainer:v1.2.3
//!     command: ["./train"]
//!     args: ["--epochs", "100"]
//!     env:
//!       - name: API_KEY
//!         value_from:
//!           secret_name: ml-secrets
//!           secret_key:  openai_api_key
//!       - name: TRAINING_RUN
//!         value: "{{ start.run_id }}"
//!     resources:
//!       requests: { cpu: "2", memory: "4Gi" }
//!       limits:   { cpu: "4", memory: "8Gi" }
//!     timeout_seconds: 7200
//!     service_account: noetl-container-job
//!     namespace: noetl
//!     backoff_limit: 0
//!     restart_policy: Never
//! ```
//!
//! ## Returned [`ToolResult`]
//!
//! `Tool::Container::execute` returns a `Success` result with the
//! `pending_callback: Some(true)` marker set per
//! [`crate::result::ToolResult`].  The worker recognises the marker
//! and suppresses its own `call.done` emit — the watcher's
//! callback path emits the resume event when the Job terminates.
//! `data` carries the Job handle for forensics:
//!
//! ```ignore
//! {
//!   "job_name": "noetl-container-train-eid-12345-suffix",
//!   "job_uid":  "abc12345-...",
//!   "namespace": "noetl",
//!   "labels": { "noetl.execution-id": "...", "noetl.step-name": "..." }
//! }
//! ```
//!
//! Worker-side adoption of the `pending_callback` marker is a
//! coordinated follow-up; until then the worker emits `call.done`
//! immediately, and the watcher's later callback is treated as
//! stale by the server (recorded by
//! `noetl_container_callback_stale_total`).  That race is harmless
//! during the transition — playbooks just see early completion.
//!
//! ## RBAC
//!
//! The worker pod's ServiceAccount needs
//! `batch/jobs.create` in the target namespace.  In the kind +
//! GKE deployments the existing `noetl-worker` SA already has
//! this (it's used by the `script` tool too).
//!
//! ## What this tool is NOT
//!
//! - Not a stream — the Job runs entirely on the K8s side; the
//!   tool's return value is the Job handle, not output.
//! - Not a `kubectl exec` — for short-lived single-pod workloads,
//!   use the `script` tool kind instead (it polls and returns the
//!   pod's stdout/stderr inline, which is fine for seconds-scale
//!   work).
//! - Not multi-container — round 3 ships single-container Jobs.
//!   Init containers + sidecars are out of scope; can land in a
//!   follow-up.
//!
//! [umbrella]: https://github.com/noetl/ai-meta/wiki/Umbrella-Container-Tool-Callback
//! [watcher]: https://github.com/noetl/ops/blob/main/ci/manifests/k8s-watcher/README.md

use std::collections::HashMap;

use async_trait::async_trait;
use k8s_openapi::api::batch::v1::{Job, JobSpec};
use k8s_openapi::api::core::v1::{
    Container, EnvVar, EnvVarSource, PodSpec, PodTemplateSpec, ResourceRequirements,
    SecretKeySelector,
};
use kube::api::{Api, ObjectMeta, PostParams};
use kube::Client;
use serde::{Deserialize, Serialize};

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

/// Default namespace for dispatched Jobs when the playbook step doesn't
/// override.  The watcher Deployment is namespaced to `noetl` too.
const DEFAULT_NAMESPACE: &str = "noetl";
/// Default `RestartPolicy` for the Pod spec.  `Never` matches Job
/// semantics — the Job controller handles retries via `backoffLimit`.
const DEFAULT_RESTART_POLICY: &str = "Never";
/// Default `backoffLimit` on the Job.  `0` means "don't retry on
/// failure" — the playbook layer handles retries via its own
/// `retry:` block.  Setting a higher number would muddle the
/// terminal-state mapping the watcher does.
const DEFAULT_BACKOFF_LIMIT: i32 = 0;

/// Per-step config for `Tool::Container`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerConfig {
    /// Container image (required).  Fully-qualified, including
    /// registry + tag (`gcr.io/my-project/trainer:v1.2.3`).  Pinning
    /// to a digest is recommended in production.
    pub image: String,

    /// Override the container's entrypoint.  Defaults to the image's
    /// `ENTRYPOINT`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,

    /// Arguments passed to the container.  These can reference
    /// playbook variables via `{{ ... }}` — the template engine
    /// renders them at dispatch time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,

    /// Environment variables.  Each carries either a literal value
    /// or a `value_from.secret_name` + `secret_key` pair pointing at
    /// a Secret already on the cluster.
    #[serde(default)]
    pub env: Vec<ContainerEnvVar>,

    /// Pod resource requests + limits.  Pass-through to the K8s API
    /// (`requests` / `limits` maps of CPU + memory + custom).
    #[serde(default)]
    pub resources: ContainerResources,

    /// Job's `activeDeadlineSeconds`.  When the deadline is exceeded
    /// the watcher classifies the terminal state as
    /// `failed_timeout`.  `None` means no deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<i64>,

    /// Pod's `serviceAccountName`.  `None` falls back to the
    /// namespace default — usually fine for read-only workloads;
    /// jobs that need cloud-platform impersonation should set this
    /// explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account: Option<String>,

    /// Target namespace.  Defaults to `noetl` (the same namespace
    /// the watcher Deployment lives in).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Job's `backoffLimit`.  Defaults to `0` so the Job doesn't
    /// retry on its own — the playbook's own `retry:` block is the
    /// right place to express retry semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_limit: Option<i32>,

    /// Pod's `restartPolicy`.  Must be `Never` or `OnFailure` for a
    /// Job; defaults to `Never`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<String>,
}

/// One env var the playbook author declared.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerEnvVar {
    pub name: String,
    /// Literal value.  Templating runs against this before the
    /// Job spec is built.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Reference to a Secret already on the cluster.  When set,
    /// `value` MUST be `None`; the playbook spec is invalid
    /// otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_from: Option<EnvValueFrom>,
}

/// `valueFrom` subset we support today.  Future rounds may add
/// configmap-key / field-ref / resource-field sources.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvValueFrom {
    pub secret_name: String,
    pub secret_key: String,
}

/// Resource requests / limits.  Mirrors the K8s shape verbatim so
/// the playbook author writes the same YAML they'd write in a
/// hand-rolled Job spec.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerResources {
    #[serde(default)]
    pub requests: HashMap<String, String>,
    #[serde(default)]
    pub limits: HashMap<String, String>,
}

/// The container tool.  Holds a kube `Client` lazily (built on
/// first execute; reused thereafter) so unit tests can drive the
/// translation logic without a cluster.
pub struct ContainerTool {
    template_engine: TemplateEngine,
}

impl Default for ContainerTool {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainerTool {
    pub fn new() -> Self {
        Self {
            template_engine: TemplateEngine::new(),
        }
    }

    /// Parse the raw `ToolConfig` into a typed `ContainerConfig`
    /// after template-rendering Jinja-style expressions.  This
    /// resolves `{{ start.input_path }}` into a concrete string
    /// BEFORE the typed deserialiser sees the placeholder.
    fn parse_config(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ContainerConfig, ToolError> {
        let template_ctx = ctx.to_template_context();
        let rendered = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;
        serde_json::from_value(rendered).map_err(|e| {
            ToolError::Configuration(format!("Invalid container config: {e}"))
        })
    }

    /// Translate a [`ContainerConfig`] into a K8s [`Job`] resource.
    ///
    /// Pulled out as a free-standing method so the unit tests can
    /// drive it without a kube client.
    fn build_job(cfg: &ContainerConfig, ctx: &ExecutionContext) -> Result<Job, ToolError> {
        if cfg.image.trim().is_empty() {
            return Err(ToolError::Configuration(
                "container: image is required".to_string(),
            ));
        }

        // Build env vars.  `value` + `value_from` are mutually exclusive.
        let mut env_vars = Vec::with_capacity(cfg.env.len());
        for ev in &cfg.env {
            if ev.value.is_some() && ev.value_from.is_some() {
                return Err(ToolError::Configuration(format!(
                    "container env var '{}': value and value_from are mutually exclusive",
                    ev.name
                )));
            }
            let env = if let Some(vf) = &ev.value_from {
                EnvVar {
                    name: ev.name.clone(),
                    value: None,
                    value_from: Some(EnvVarSource {
                        secret_key_ref: Some(SecretKeySelector {
                            name: vf.secret_name.clone(),
                            key: vf.secret_key.clone(),
                            optional: Some(false),
                        }),
                        ..Default::default()
                    }),
                }
            } else {
                EnvVar {
                    name: ev.name.clone(),
                    value: ev.value.clone().or_else(|| Some(String::new())),
                    value_from: None,
                }
            };
            env_vars.push(env);
        }

        // Resource requirements.  Empty maps → None so the JSON
        // wire shape stays minimal.
        let resources = if cfg.resources.requests.is_empty()
            && cfg.resources.limits.is_empty()
        {
            None
        } else {
            use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
            let to_qty_map = |m: &HashMap<String, String>| {
                m.iter()
                    .map(|(k, v)| (k.clone(), Quantity(v.clone())))
                    .collect::<std::collections::BTreeMap<_, _>>()
            };
            Some(ResourceRequirements {
                requests: if cfg.resources.requests.is_empty() {
                    None
                } else {
                    Some(to_qty_map(&cfg.resources.requests))
                },
                limits: if cfg.resources.limits.is_empty() {
                    None
                } else {
                    Some(to_qty_map(&cfg.resources.limits))
                },
                ..Default::default()
            })
        };

        let container = Container {
            name: "main".to_string(),
            image: Some(cfg.image.clone()),
            command: cfg.command.clone(),
            args: cfg.args.clone(),
            env: if env_vars.is_empty() {
                None
            } else {
                Some(env_vars)
            },
            resources,
            ..Default::default()
        };

        let pod_spec = PodSpec {
            containers: vec![container],
            restart_policy: Some(
                cfg.restart_policy
                    .clone()
                    .unwrap_or_else(|| DEFAULT_RESTART_POLICY.to_string()),
            ),
            service_account_name: cfg.service_account.clone(),
            ..Default::default()
        };

        let labels = std::collections::BTreeMap::from([
            ("noetl.execution-id".to_string(), ctx.execution_id.to_string()),
            ("noetl.step-name".to_string(), ctx.step.clone()),
            ("noetl.tool-kind".to_string(), "container".to_string()),
        ]);

        // `generateName` keeps Job names unique across retries
        // without us having to mint a fresh suffix.  K8s appends a
        // 5-char random suffix.  Truncate the step name to fit the
        // 63-char DNS label limit comfortably.
        let step_slug: String = ctx
            .step
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
            .take(20)
            .collect();
        let generate_name = format!(
            "noetl-container-{step}-{eid}-",
            step = if step_slug.is_empty() {
                "step".to_string()
            } else {
                step_slug.to_lowercase()
            },
            eid = ctx.execution_id
        );

        let job = Job {
            metadata: ObjectMeta {
                generate_name: Some(generate_name),
                namespace: Some(
                    cfg.namespace
                        .clone()
                        .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string()),
                ),
                labels: Some(labels.clone()),
                ..Default::default()
            },
            spec: Some(JobSpec {
                backoff_limit: Some(cfg.backoff_limit.unwrap_or(DEFAULT_BACKOFF_LIMIT)),
                active_deadline_seconds: cfg.timeout_seconds,
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(labels),
                        ..Default::default()
                    }),
                    spec: Some(pod_spec),
                },
                ..Default::default()
            }),
            status: None,
        };

        Ok(job)
    }
}

#[async_trait]
impl Tool for ContainerTool {
    fn name(&self) -> &'static str {
        "container"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let cfg = self.parse_config(config, ctx)?;
        let namespace = cfg
            .namespace
            .clone()
            .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string());
        let job_spec = Self::build_job(&cfg, ctx)?;

        // Build the kube client from in-cluster config.  The worker
        // pod's ServiceAccount mounts a token + the cluster CA; the
        // default `Client::try_default()` reads both.
        let client = Client::try_default().await.map_err(|e| {
            ToolError::ExecutionFailed(format!("container: kube client init failed: {e}"))
        })?;
        let api: Api<Job> = Api::namespaced(client, &namespace);

        tracing::info!(
            execution_id = ctx.execution_id,
            step = %ctx.step,
            namespace = %namespace,
            image = %cfg.image,
            "container.dispatch"
        );

        let created = api
            .create(&PostParams::default(), &job_spec)
            .await
            .map_err(|e| {
                ToolError::ExecutionFailed(format!(
                    "container: Job create failed in namespace '{namespace}': {e}"
                ))
            })?;

        let job_name = created
            .metadata
            .name
            .clone()
            .unwrap_or_default();
        let job_uid = created.metadata.uid.clone();

        tracing::info!(
            execution_id = ctx.execution_id,
            step = %ctx.step,
            job_name = %job_name,
            job_uid = job_uid.as_deref().unwrap_or(""),
            "container.dispatched"
        );

        // Return immediately with the PendingCallback marker.  The
        // worker recognises the marker and suppresses its own
        // call.done emit; the watcher's callback path delivers the
        // resume event when the Job terminates.
        let mut result = ToolResult::success(serde_json::json!({
            "job_name": job_name,
            "job_uid":  job_uid,
            "namespace": namespace,
            "labels": {
                "noetl.execution-id": ctx.execution_id.to_string(),
                "noetl.step-name":    ctx.step,
                "noetl.tool-kind":    "container",
            },
        }));
        result.pending_callback = Some(true);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> ExecutionContext {
        ExecutionContext::new(900000000000000001_i64, "train_model", "http://test")
    }

    fn minimal_config() -> ContainerConfig {
        ContainerConfig {
            image: "alpine:3.19".to_string(),
            command: None,
            args: None,
            env: Vec::new(),
            resources: ContainerResources::default(),
            timeout_seconds: None,
            service_account: None,
            namespace: None,
            backoff_limit: None,
            restart_policy: None,
        }
    }

    #[test]
    fn build_job_sets_required_labels() {
        let cfg = minimal_config();
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        let labels = job
            .metadata
            .labels
            .as_ref()
            .expect("labels present on metadata");
        assert_eq!(
            labels.get("noetl.execution-id").map(String::as_str),
            Some("900000000000000001")
        );
        assert_eq!(
            labels.get("noetl.step-name").map(String::as_str),
            Some("train_model")
        );
        assert_eq!(
            labels.get("noetl.tool-kind").map(String::as_str),
            Some("container")
        );
    }

    #[test]
    fn build_job_uses_default_namespace() {
        let cfg = minimal_config();
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        assert_eq!(job.metadata.namespace.as_deref(), Some("noetl"));
    }

    #[test]
    fn build_job_honours_explicit_namespace() {
        let mut cfg = minimal_config();
        cfg.namespace = Some("ml-platform".to_string());
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        assert_eq!(job.metadata.namespace.as_deref(), Some("ml-platform"));
    }

    #[test]
    fn build_job_generate_name_includes_step_and_eid() {
        let cfg = minimal_config();
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        let gn = job.metadata.generate_name.as_deref().unwrap_or("");
        // Underscores are stripped from the step slug — only `[a-z0-9-]`
        // survives so the generated name stays a valid K8s DNS-1123
        // label.  "train_model" becomes "trainmodel".
        assert!(gn.starts_with("noetl-container-trainmodel-"), "got {gn}");
        assert!(gn.ends_with("-"), "generateName should end with hyphen for K8s suffix; got {gn}");
        assert!(gn.contains("900000000000000001"), "got {gn}");
    }

    #[test]
    fn build_job_propagates_command_and_args() {
        let mut cfg = minimal_config();
        cfg.command = Some(vec!["/bin/train".to_string()]);
        cfg.args = Some(vec!["--epochs".to_string(), "100".to_string()]);
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        let container = &job.spec.unwrap().template.spec.unwrap().containers[0];
        assert_eq!(container.command.as_deref(), Some(&["/bin/train".to_string()][..]));
        assert_eq!(
            container.args.as_deref(),
            Some(&["--epochs".to_string(), "100".to_string()][..])
        );
    }

    #[test]
    fn build_job_propagates_env_literal() {
        let mut cfg = minimal_config();
        cfg.env.push(ContainerEnvVar {
            name: "TRAINING_RUN".to_string(),
            value: Some("run-42".to_string()),
            value_from: None,
        });
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        let env = job.spec.unwrap().template.spec.unwrap().containers[0]
            .env
            .clone()
            .expect("env present");
        assert_eq!(env.len(), 1);
        assert_eq!(env[0].name, "TRAINING_RUN");
        assert_eq!(env[0].value.as_deref(), Some("run-42"));
        assert!(env[0].value_from.is_none());
    }

    #[test]
    fn build_job_propagates_env_secret_ref() {
        let mut cfg = minimal_config();
        cfg.env.push(ContainerEnvVar {
            name: "API_KEY".to_string(),
            value: None,
            value_from: Some(EnvValueFrom {
                secret_name: "ml-secrets".to_string(),
                secret_key: "openai_api_key".to_string(),
            }),
        });
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        let env = job.spec.unwrap().template.spec.unwrap().containers[0]
            .env
            .clone()
            .expect("env present");
        let v = env[0].value_from.as_ref().expect("value_from set");
        let secret = v.secret_key_ref.as_ref().expect("secret_key_ref set");
        assert_eq!(secret.name, "ml-secrets");
        assert_eq!(secret.key, "openai_api_key");
        // value MUST be None when value_from is set.
        assert!(env[0].value.is_none());
    }

    #[test]
    fn build_job_rejects_env_with_both_value_and_value_from() {
        let mut cfg = minimal_config();
        cfg.env.push(ContainerEnvVar {
            name: "AMBIGUOUS".to_string(),
            value: Some("literal".to_string()),
            value_from: Some(EnvValueFrom {
                secret_name: "s".to_string(),
                secret_key: "k".to_string(),
            }),
        });
        let err = ContainerTool::build_job(&cfg, &ctx()).expect_err("must reject");
        let msg = match err {
            ToolError::Configuration(m) => m,
            other => panic!("expected Configuration, got {other:?}"),
        };
        assert!(msg.contains("value and value_from are mutually exclusive"), "got {msg}");
    }

    #[test]
    fn build_job_rejects_empty_image() {
        let mut cfg = minimal_config();
        cfg.image = "".to_string();
        let err = ContainerTool::build_job(&cfg, &ctx()).expect_err("must reject");
        let msg = match err {
            ToolError::Configuration(m) => m,
            other => panic!("expected Configuration, got {other:?}"),
        };
        assert!(msg.contains("image is required"), "got {msg}");
    }

    #[test]
    fn build_job_propagates_resources() {
        let mut cfg = minimal_config();
        cfg.resources.requests.insert("cpu".to_string(), "500m".to_string());
        cfg.resources.requests.insert("memory".to_string(), "1Gi".to_string());
        cfg.resources.limits.insert("cpu".to_string(), "2".to_string());
        cfg.resources.limits.insert("memory".to_string(), "4Gi".to_string());
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        let res = job.spec.unwrap().template.spec.unwrap().containers[0]
            .resources
            .clone()
            .expect("resources present");
        let requests = res.requests.expect("requests present");
        assert_eq!(requests.get("cpu").map(|q| q.0.as_str()), Some("500m"));
        assert_eq!(requests.get("memory").map(|q| q.0.as_str()), Some("1Gi"));
        let limits = res.limits.expect("limits present");
        assert_eq!(limits.get("cpu").map(|q| q.0.as_str()), Some("2"));
        assert_eq!(limits.get("memory").map(|q| q.0.as_str()), Some("4Gi"));
    }

    #[test]
    fn build_job_empty_resources_means_none() {
        let cfg = minimal_config();
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        let container = &job.spec.unwrap().template.spec.unwrap().containers[0];
        assert!(container.resources.is_none(), "empty resources should serialise as None");
    }

    #[test]
    fn build_job_sets_backoff_and_deadline() {
        let mut cfg = minimal_config();
        cfg.backoff_limit = Some(3);
        cfg.timeout_seconds = Some(3600);
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        let spec = job.spec.unwrap();
        assert_eq!(spec.backoff_limit, Some(3));
        assert_eq!(spec.active_deadline_seconds, Some(3600));
    }

    #[test]
    fn build_job_default_backoff_is_zero() {
        let cfg = minimal_config();
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        // Default 0 — the playbook's own retry: block is the right
        // place to express retry semantics; the Job controller's
        // built-in retry would muddle the terminal-state mapping.
        assert_eq!(job.spec.unwrap().backoff_limit, Some(0));
    }

    #[test]
    fn build_job_default_restart_policy_is_never() {
        let cfg = minimal_config();
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        let policy = job.spec.unwrap().template.spec.unwrap().restart_policy;
        assert_eq!(policy.as_deref(), Some("Never"));
    }

    #[test]
    fn build_job_step_slug_is_lowercased_and_truncated() {
        let mut c = ctx();
        c.step = "VeryLongStepNameWithLotsOfCharactersExceedingTwentyChars".to_string();
        let cfg = minimal_config();
        let job = ContainerTool::build_job(&cfg, &c).expect("build");
        let gn = job.metadata.generate_name.as_deref().unwrap_or("");
        // 20-char limit on the step slug; lowercased.
        let between = gn.trim_start_matches("noetl-container-");
        let step_part = between.split('-').next().unwrap();
        assert!(step_part.len() <= 20, "step slug too long: {step_part}");
        assert_eq!(step_part, step_part.to_lowercase(), "step slug not lowercase: {step_part}");
    }

    #[test]
    fn build_job_empty_step_falls_back_to_step_word() {
        let mut c = ctx();
        c.step = "".to_string();
        let cfg = minimal_config();
        let job = ContainerTool::build_job(&cfg, &c).expect("build");
        let gn = job.metadata.generate_name.as_deref().unwrap_or("");
        assert!(gn.starts_with("noetl-container-step-"), "got {gn}");
    }

    #[test]
    fn build_job_propagates_service_account() {
        let mut cfg = minimal_config();
        cfg.service_account = Some("noetl-container-job".to_string());
        let job = ContainerTool::build_job(&cfg, &ctx()).expect("build");
        let sa = job.spec.unwrap().template.spec.unwrap().service_account_name;
        assert_eq!(sa.as_deref(), Some("noetl-container-job"));
    }
}
