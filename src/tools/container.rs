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
    ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvVar, EnvVarSource,
    PersistentVolumeClaimVolumeSource, PodSpec, PodTemplateSpec, ResourceRequirements,
    SecretKeySelector, SecretVolumeSource, Toleration, Volume, VolumeMount,
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

    /// Pod's `nodeSelector` (noetl/ai-meta#144 G1).  The knob that
    /// places a GPU Job on a GPU node pool — e.g.
    /// `{ "cloud.google.com/gke-accelerator": "nvidia-l4" }`.  Empty
    /// map → omitted from the spec.
    #[serde(default)]
    pub node_selector: std::collections::BTreeMap<String, String>,

    /// Pod's `tolerations` (G1).  GPU node pools are tainted
    /// (`nvidia.com/gpu`), so a GPU Job must tolerate the taint to
    /// schedule there.  Empty → omitted.
    #[serde(default)]
    pub tolerations: Vec<ContainerToleration>,

    /// Pod-level `volumes` (G1).  Round-1 subset: `empty_dir`,
    /// `persistent_volume_claim`, `config_map`, `secret`.  A training
    /// Job uses these for scratch space + a mounted artifact volume.
    #[serde(default)]
    pub volumes: Vec<ContainerVolume>,

    /// Container `volumeMounts` (G1).  Each references a `volumes[]`
    /// entry by name.  Empty → omitted.
    #[serde(default)]
    pub volume_mounts: Vec<ContainerVolumeMount>,
}

/// One pod toleration.  Mirrors the K8s `Toleration` shape in
/// snake_case to match the rest of [`ContainerConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerToleration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// `Exists` or `Equal`.  `Exists` (with no `value`) tolerates any
    /// value for the key — the common GPU-taint shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// `NoSchedule` / `PreferNoSchedule` / `NoExecute`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toleration_seconds: Option<i64>,
}

/// One pod volume.  Exactly one source field should be set; the round-1
/// subset covers the sources a batch Job realistically needs.  When more
/// than one is set, the first non-`None` in declaration order wins.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerVolume {
    pub name: String,
    /// Ephemeral scratch space (`emptyDir: {}`).  Value is the optional
    /// `{ medium, size_limit }`; an empty object means defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub empty_dir: Option<EmptyDirSource>,
    /// Mount an existing PersistentVolumeClaim (e.g. a shared dataset /
    /// model-artifact volume).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistent_volume_claim: Option<PvcSource>,
    /// Mount a ConfigMap as files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_map: Option<NamedSource>,
    /// Mount a Secret as files.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretSource>,
}

/// `emptyDir` volume source.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmptyDirSource {
    /// `""` (node disk) or `"Memory"` (tmpfs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub medium: Option<String>,
    /// Size cap, e.g. `"10Gi"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_limit: Option<String>,
}

/// `persistentVolumeClaim` volume source.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PvcSource {
    pub claim_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
}

/// `configMap` volume source.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamedSource {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// `secret` volume source.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecretSource {
    pub secret_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optional: Option<bool>,
}

/// One container `volumeMount`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContainerVolumeMount {
    pub name: String,
    pub mount_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_path: Option<String>,
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

        // noetl/ai-meta#144 G1 — container volumeMounts.
        let volume_mounts: Vec<VolumeMount> = cfg
            .volume_mounts
            .iter()
            .map(|vm| VolumeMount {
                name: vm.name.clone(),
                mount_path: vm.mount_path.clone(),
                read_only: vm.read_only,
                sub_path: vm.sub_path.clone(),
                ..Default::default()
            })
            .collect();

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
            volume_mounts: if volume_mounts.is_empty() {
                None
            } else {
                Some(volume_mounts)
            },
            ..Default::default()
        };

        // noetl/ai-meta#144 G1 — pod-level volumes (round-1 source subset).
        let volumes: Vec<Volume> = cfg
            .volumes
            .iter()
            .map(|v| Volume {
                name: v.name.clone(),
                empty_dir: v.empty_dir.as_ref().map(|e| EmptyDirVolumeSource {
                    medium: e.medium.clone(),
                    size_limit: e.size_limit.as_ref().map(|s| {
                        k8s_openapi::apimachinery::pkg::api::resource::Quantity(s.clone())
                    }),
                }),
                persistent_volume_claim: v.persistent_volume_claim.as_ref().map(|p| {
                    PersistentVolumeClaimVolumeSource {
                        claim_name: p.claim_name.clone(),
                        read_only: p.read_only,
                    }
                }),
                config_map: v.config_map.as_ref().map(|c| ConfigMapVolumeSource {
                    name: c.name.clone(),
                    optional: c.optional,
                    ..Default::default()
                }),
                secret: v.secret.as_ref().map(|s| SecretVolumeSource {
                    secret_name: Some(s.secret_name.clone()),
                    optional: s.optional,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect();

        // noetl/ai-meta#144 G1 — tolerations (GPU node-pool taint).
        let tolerations: Vec<Toleration> = cfg
            .tolerations
            .iter()
            .map(|t| Toleration {
                key: t.key.clone(),
                operator: t.operator.clone(),
                value: t.value.clone(),
                effect: t.effect.clone(),
                toleration_seconds: t.toleration_seconds,
            })
            .collect();

        let pod_spec = PodSpec {
            containers: vec![container],
            restart_policy: Some(
                cfg.restart_policy
                    .clone()
                    .unwrap_or_else(|| DEFAULT_RESTART_POLICY.to_string()),
            ),
            service_account_name: cfg.service_account.clone(),
            // noetl/ai-meta#144 G1 — GPU placement + volumes.  Empty
            // collections serialise as None so the wire shape stays minimal.
            node_selector: if cfg.node_selector.is_empty() {
                None
            } else {
                Some(cfg.node_selector.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            },
            tolerations: if tolerations.is_empty() {
                None
            } else {
                Some(tolerations)
            },
            volumes: if volumes.is_empty() {
                None
            } else {
                Some(volumes)
            },
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

// ===========================================================================
// noetl/ai-meta#145 G2 — poll-based completion fallback.
//
// The durable async-resume path is the external `noetl-k8s-watcher`
// (#43 Round 1).  This helper is the *poll* fallback for environments
// that don't run the watcher: the worker calls it from a detached task
// (slot already freed) to observe a dispatched Job to its terminal
// state, then emits the resume `call.done` itself.  Returning a plain
// [`JobTerminalOutcome`] keeps `kube`/`k8s-openapi` types from crossing
// the crate boundary, so the worker gains no new direct dependency.
// ===========================================================================

/// Knobs for [`poll_job_to_terminal`].  All have sensible defaults via
/// [`PollOptions::default`]; the worker overrides from env.
#[derive(Debug, Clone)]
pub struct PollOptions {
    /// First poll interval.  Doubles up to `max_interval` on each tick.
    pub interval: std::time::Duration,
    /// Backoff cap for the poll interval.
    pub max_interval: std::time::Duration,
    /// Overall deadline backstop.  If the Job hasn't reached a terminal
    /// state by then the poller returns [`JobTerminalOutcome`] with
    /// `state = "poll_timeout"`.  This is a safety net *above* the Job's
    /// own `activeDeadlineSeconds` (which is authoritative); it only
    /// fires if the watch itself wedges.
    pub max_wait: std::time::Duration,
}

impl Default for PollOptions {
    fn default() -> Self {
        Self {
            interval: std::time::Duration::from_secs(5),
            max_interval: std::time::Duration::from_secs(30),
            max_wait: std::time::Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// Terminal outcome of a polled Job.  `state` is one of `succeeded`,
/// `failed`, or `poll_timeout` (the poll-deadline backstop).  Finer
/// pod-level classification (OOM / image-pull / node-lost) is the
/// watcher's job; the poll fallback reports the Job-condition outcome
/// only (round-1 scope).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobTerminalOutcome {
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl JobTerminalOutcome {
    /// `true` only for the `succeeded` state — the worker maps this to a
    /// `call.done` `status="COMPLETED"`; everything else is `FAILED`.
    pub fn is_success(&self) -> bool {
        self.state == "succeeded"
    }
}

/// Classify a [`JobStatus`] into a terminal outcome, or `None` if the
/// Job is still running.  Pure function over the status so it unit-tests
/// without a cluster.  A Job is terminal when it carries a `Complete`
/// (→ `succeeded`) or `Failed` (→ `failed`) condition with
/// `status == "True"`.
fn classify_job_status(
    status: &k8s_openapi::api::batch::v1::JobStatus,
) -> Option<JobTerminalOutcome> {
    let conditions = status.conditions.as_ref()?;
    for c in conditions {
        if c.status != "True" {
            continue;
        }
        let completed_at = c.last_transition_time.as_ref().map(|t| t.0);
        match c.type_.as_str() {
            "Complete" => {
                return Some(JobTerminalOutcome {
                    state: "succeeded".to_string(),
                    reason: c.reason.clone(),
                    completed_at,
                })
            }
            "Failed" => {
                return Some(JobTerminalOutcome {
                    state: "failed".to_string(),
                    reason: c
                        .reason
                        .clone()
                        .or_else(|| c.message.clone()),
                    completed_at,
                })
            }
            _ => {}
        }
    }
    None
}

/// Poll a K8s Job to its terminal state.  Builds an in-cluster kube
/// client, polls the Job's status with exponential backoff until a
/// terminal condition appears or `opts.max_wait` elapses.
///
/// Errors (`ToolError::ExecutionFailed`) only on infrastructure
/// failures — kube client init or a Job `get` that fails.  A Job that
/// never finishes within `max_wait` is *not* an error; it returns
/// `state = "poll_timeout"` so the worker can emit a FAILED resume
/// rather than leaking a poller forever.
pub async fn poll_job_to_terminal(
    namespace: &str,
    job_name: &str,
    opts: PollOptions,
) -> Result<JobTerminalOutcome, ToolError> {
    use k8s_openapi::api::batch::v1::Job as K8sJob;

    let client = Client::try_default().await.map_err(|e| {
        ToolError::ExecutionFailed(format!("container.poll: kube client init failed: {e}"))
    })?;
    let api: Api<K8sJob> = Api::namespaced(client, namespace);

    let started = std::time::Instant::now();
    let mut interval = opts.interval;
    loop {
        let job = api.get(job_name).await.map_err(|e| {
            ToolError::ExecutionFailed(format!(
                "container.poll: get Job '{job_name}' in '{namespace}' failed: {e}"
            ))
        })?;
        if let Some(status) = job.status.as_ref() {
            if let Some(outcome) = classify_job_status(status) {
                return Ok(outcome);
            }
        }
        if started.elapsed() >= opts.max_wait {
            return Ok(JobTerminalOutcome {
                state: "poll_timeout".to_string(),
                reason: Some(format!(
                    "poll deadline {}s exceeded without terminal Job condition",
                    opts.max_wait.as_secs()
                )),
                completed_at: None,
            });
        }
        tokio::time::sleep(interval).await;
        interval = std::cmp::min(interval.saturating_mul(2), opts.max_interval);
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
            node_selector: std::collections::BTreeMap::new(),
            tolerations: Vec::new(),
            volumes: Vec::new(),
            volume_mounts: Vec::new(),
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

    // --- noetl/ai-meta#144 G1 — GPU placement + volumes ---

    #[test]
    fn build_job_empty_placement_means_none() {
        let cfg = minimal_config();
        let pod = job_pod_spec(&cfg);
        assert!(pod.node_selector.is_none(), "empty node_selector → None");
        assert!(pod.tolerations.is_none(), "empty tolerations → None");
        assert!(pod.volumes.is_none(), "empty volumes → None");
        assert!(
            pod.containers[0].volume_mounts.is_none(),
            "empty volume_mounts → None"
        );
    }

    #[test]
    fn build_job_propagates_node_selector() {
        let mut cfg = minimal_config();
        cfg.node_selector.insert(
            "cloud.google.com/gke-accelerator".to_string(),
            "nvidia-l4".to_string(),
        );
        let pod = job_pod_spec(&cfg);
        let ns = pod.node_selector.expect("node_selector present");
        assert_eq!(
            ns.get("cloud.google.com/gke-accelerator").map(String::as_str),
            Some("nvidia-l4")
        );
    }

    #[test]
    fn build_job_propagates_gpu_toleration() {
        let mut cfg = minimal_config();
        cfg.tolerations.push(ContainerToleration {
            key: Some("nvidia.com/gpu".to_string()),
            operator: Some("Exists".to_string()),
            effect: Some("NoSchedule".to_string()),
            ..Default::default()
        });
        let pod = job_pod_spec(&cfg);
        let tols = pod.tolerations.expect("tolerations present");
        assert_eq!(tols.len(), 1);
        assert_eq!(tols[0].key.as_deref(), Some("nvidia.com/gpu"));
        assert_eq!(tols[0].operator.as_deref(), Some("Exists"));
        assert_eq!(tols[0].effect.as_deref(), Some("NoSchedule"));
    }

    #[test]
    fn build_job_propagates_gpu_resource_request() {
        // The GPU *resource* request rides the existing resources
        // passthrough — no new field.  Pin that it still works.
        let mut cfg = minimal_config();
        cfg.resources
            .limits
            .insert("nvidia.com/gpu".to_string(), "1".to_string());
        let pod = job_pod_spec(&cfg);
        let limits = pod.containers[0]
            .resources
            .clone()
            .expect("resources")
            .limits
            .expect("limits");
        assert_eq!(limits.get("nvidia.com/gpu").map(|q| q.0.as_str()), Some("1"));
    }

    #[test]
    fn build_job_propagates_empty_dir_volume_and_mount() {
        let mut cfg = minimal_config();
        cfg.volumes.push(ContainerVolume {
            name: "scratch".to_string(),
            empty_dir: Some(EmptyDirSource {
                size_limit: Some("10Gi".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        });
        cfg.volume_mounts.push(ContainerVolumeMount {
            name: "scratch".to_string(),
            mount_path: "/scratch".to_string(),
            ..Default::default()
        });
        let pod = job_pod_spec(&cfg);
        let vols = pod.volumes.expect("volumes present");
        assert_eq!(vols.len(), 1);
        assert_eq!(vols[0].name, "scratch");
        let ed = vols[0].empty_dir.as_ref().expect("empty_dir source");
        assert_eq!(ed.size_limit.as_ref().map(|q| q.0.as_str()), Some("10Gi"));
        let mounts = pod.containers[0]
            .volume_mounts
            .clone()
            .expect("volume_mounts present");
        assert_eq!(mounts[0].name, "scratch");
        assert_eq!(mounts[0].mount_path, "/scratch");
    }

    #[test]
    fn build_job_propagates_pvc_volume() {
        let mut cfg = minimal_config();
        cfg.volumes.push(ContainerVolume {
            name: "models".to_string(),
            persistent_volume_claim: Some(PvcSource {
                claim_name: "slm-artifacts".to_string(),
                read_only: Some(false),
            }),
            ..Default::default()
        });
        let pod = job_pod_spec(&cfg);
        let vols = pod.volumes.expect("volumes present");
        let pvc = vols[0]
            .persistent_volume_claim
            .as_ref()
            .expect("pvc source");
        assert_eq!(pvc.claim_name, "slm-artifacts");
        assert_eq!(pvc.read_only, Some(false));
    }

    #[test]
    fn config_deserialises_gpu_shape_from_yaml_json() {
        // Pins the playbook-author wire shape (snake_case) for the new
        // G1 fields — the contract the RFC examples document.
        let raw = serde_json::json!({
            "image": "gcr.io/noetl/slm-trainer:v1",
            "resources": { "limits": { "nvidia.com/gpu": "1" } },
            "node_selector": { "cloud.google.com/gke-accelerator": "nvidia-l4" },
            "tolerations": [
                { "key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule" }
            ],
            "volumes": [ { "name": "scratch", "empty_dir": {} } ],
            "volume_mounts": [ { "name": "scratch", "mount_path": "/scratch" } ]
        });
        let cfg: ContainerConfig = serde_json::from_value(raw).expect("deserialise");
        assert_eq!(cfg.node_selector.len(), 1);
        assert_eq!(cfg.tolerations.len(), 1);
        assert_eq!(cfg.volumes.len(), 1);
        assert!(cfg.volumes[0].empty_dir.is_some());
        assert_eq!(cfg.volume_mounts[0].mount_path, "/scratch");
    }

    // --- noetl/ai-meta#145 G2 — poll classifier ---

    fn job_status_with_condition(
        type_: &str,
        status: &str,
        reason: Option<&str>,
    ) -> k8s_openapi::api::batch::v1::JobStatus {
        use k8s_openapi::api::batch::v1::{JobCondition, JobStatus};
        JobStatus {
            conditions: Some(vec![JobCondition {
                type_: type_.to_string(),
                status: status.to_string(),
                reason: reason.map(String::from),
                ..Default::default()
            }]),
            ..Default::default()
        }
    }

    #[test]
    fn classify_complete_condition_is_succeeded() {
        let st = job_status_with_condition("Complete", "True", None);
        let outcome = classify_job_status(&st).expect("terminal");
        assert_eq!(outcome.state, "succeeded");
        assert!(outcome.is_success());
    }

    #[test]
    fn classify_failed_condition_is_failed_with_reason() {
        let st = job_status_with_condition("Failed", "True", Some("BackoffLimitExceeded"));
        let outcome = classify_job_status(&st).expect("terminal");
        assert_eq!(outcome.state, "failed");
        assert!(!outcome.is_success());
        assert_eq!(outcome.reason.as_deref(), Some("BackoffLimitExceeded"));
    }

    #[test]
    fn classify_running_job_is_none() {
        use k8s_openapi::api::batch::v1::JobStatus;
        // active pods, no terminal condition.
        let st = JobStatus {
            active: Some(1),
            ..Default::default()
        };
        assert!(classify_job_status(&st).is_none());
    }

    #[test]
    fn classify_condition_status_false_is_not_terminal() {
        // A `Complete` condition that hasn't flipped to True yet.
        let st = job_status_with_condition("Complete", "False", None);
        assert!(classify_job_status(&st).is_none());
    }

    #[test]
    fn poll_options_default_is_sane() {
        let o = PollOptions::default();
        assert!(o.interval <= o.max_interval);
        assert!(o.max_wait.as_secs() >= 3600, "deadline should be generous");
    }

    /// Helper: build the Job and return its PodSpec for placement assertions.
    fn job_pod_spec(cfg: &ContainerConfig) -> k8s_openapi::api::core::v1::PodSpec {
        let job = ContainerTool::build_job(cfg, &ctx()).expect("build");
        job.spec.unwrap().template.spec.unwrap()
    }
}
