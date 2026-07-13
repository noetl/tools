//! Cloud provider operation tool (`kind: provider`).
//!
//! Executes typed cloud-provider operation specs emitted by infrastructure
//! playbooks (the ShastaraTech `gcp-org-playbooks` are the first consumer) so
//! that provisioning no longer shells out to `gcloud`.  A playbook step looks
//! like:
//!
//! ```yaml
//! tool:
//!   kind: provider
//!   provider: google              # provider family — google (aws / azure later)
//!   runtime: rest                 # rest (round-1 backend) | rust-sdk (deferred → rest)
//!   action: google.cloudresourcemanager.projects.ensure
//!   dry_run: "{{ workload.action != 'apply' }}"
//!   input:
//!     project_id: shastaratech-youtube-prod
//!     parent: folders/by-display-name/20-media
//!   auth:                         # REQUIRED for a real (apply-mode) call
//!     type: gcp_adc
//!     scopes: ["https://www.googleapis.com/auth/cloud-platform"]
//!   poll:                         # optional — bounds the LRO wait (create ops)
//!     max_attempts: 30
//!     interval_ms: 2000
//!     max_wait_secs: 120
//! ```
//!
//! ## Scope (noetl/ai-meta#189 cloud-provider-tools)
//!
//! - **REST-first.** The Google backend calls Cloud Resource Manager v3, Cloud
//!   Billing v1, and Service Usage v1 over `reqwest` + the existing
//!   [`GcpAuth`](crate::auth) ADC credential path — zero new heavy deps.  The
//!   `runtime: rust-sdk` label is accepted and mapped to the REST backend for
//!   now (the `google-cloud-rust` gRPC SDK is deferred behind the same YAML
//!   surface; adopting it later is a backend swap, not a playbook change).  The
//!   backend that actually ran is echoed as `result.data.backend`.
//! - **Explicit auth for mutations (user decision, round-01 review).** A real
//!   call (`dry_run: false`) REQUIRES an explicit `auth:` block; there is no
//!   ambient-ADC fallback.  Omitting `auth:` on a real call yields a
//!   [`ToolError::Configuration`] and makes **no** network request.  Plan /
//!   dry-run mode stays credential-free — it mints no token and issues no call.
//! - **Plan vs apply.** `dry_run` (default **true** — never silently mutate)
//!   returns the request the tool *would* send (`would_call`) without touching
//!   the network.  `dry_run: false` executes it.
//! - **Idempotent GET-first converge (round-2).** Every mutating action reads
//!   the actual state first and only writes when the desired state is absent:
//!   - `folders.ensure` — list folders under `parent`, create only if no ACTIVE
//!     folder with the target `displayName` exists.
//!   - `projects.ensure` — GET the project, create under `parent` only on 404.
//!   - `services.enable` — GET the service state, enable only if not `ENABLED`.
//!   - `projects.link` (billing) — GET current `billingInfo`, PUT only if the
//!     linked account differs.
//!   - `*.iam.ensure_binding` — `getIamPolicy`, add the `{role, member}` binding
//!     only if absent, then `setIamPolicy` with the full modified policy
//!     (preserving `etag` for optimistic concurrency).
//! - **No-op re-runs (round-2).** Re-running any ensure action against
//!   already-converged state issues the read but **no** write and reports
//!   `changed: false`.  This is the stateless converge — it holds under any
//!   future ownership/state model because it derives desired-vs-actual from the
//!   live API, not a local state store.
//! - **Bounded LRO polling (round-2).** `folders`/`projects` create return a
//!   long-running [Operation]; the tool polls `GET {op.name}` until `done` or a
//!   configurable bound (`poll.max_attempts` / `poll.interval_ms` /
//!   `poll.max_wait_secs`, whichever trips first).  On timeout it returns a
//!   descriptive [`ToolError::ExecutionFailed`] naming the operation and the
//!   resume path — it does **not** leave the caller guessing.  The
//!   callback/hook overflow path for operations that exceed any sane inline
//!   bound is documented below (not built in this round).
//! - **No credential logging.** The bearer token lives only on the outbound
//!   `reqwest` builder — never in `ToolResult`, the `would_call` echo, spans,
//!   or errors.  A field allowlist redaction ([`redact_sensitive`]) scrubs any
//!   sensitive-looking key from echoed input / request bodies as defence in
//!   depth.
//!
//! ## LRO overflow: the callback/hook path (documented, not built)
//!
//! The inline poll is bounded on purpose — a worker slot must not be held for a
//! multi-minute cloud operation (the execution-model callback rule).  When an
//! operation legitimately exceeds the inline bound (org-policy-gated project
//! creates can take minutes), the intended shape — a **follow-up**, not part of
//! this round — is:
//!
//! 1. The create call captures `operation.name` + the step's `execution_id`.
//! 2. The tool returns a `pending_operation` result and frees the slot instead
//!    of blocking.
//! 3. A `system/provider_operation_watch` playbook (or a Cloud Operations
//!    webhook) polls the operation out-of-band and, on completion, emits a
//!    resume event carrying the `execution_id`.
//! 4. The next block claims off NATS and continues from the recorded state.
//!
//! Until that lands, the bounded inline poll with a clear timeout error is the
//! contract: a timed-out operation is still progressing server-side and the
//! ensure action is safe to re-run (the GET-first converge makes the retry a
//! no-op once the operation completes).
//!
//! ## Cross-cloud shape
//!
//! [`ProviderFamily`] keeps `aws` / `azure` as explicit, not-yet-implemented
//! arms behind the same `kind: provider` surface — a later backend attaches
//! without changing the YAML contract.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};

use crate::auth::AuthResolver;
use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

/// Google API base URLs (round-1 services).
const CRM_V3: &str = "https://cloudresourcemanager.googleapis.com/v3";
const BILLING_V1: &str = "https://cloudbilling.googleapis.com/v1";
const SERVICEUSAGE_V1: &str = "https://serviceusage.googleapis.com/v1";

/// Keys whose values are scrubbed from any echoed input / request body.
/// Case-insensitive substring match.  The resolved bearer token never reaches
/// these structures in the first place; this is defence-in-depth against a
/// playbook author placing a secret in `input`.
const SENSITIVE_KEY_FRAGMENTS: &[&str] = &[
    "token",
    "authorization",
    "secret",
    "password",
    "passwd",
    "credential",
    "private_key",
    "client_secret",
    "api_key",
    "apikey",
    "access_key",
];

/// Provider family — the cloud whose API the operation targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderFamily {
    /// Google Cloud — the only implemented family in round 1.
    Google,
    /// AWS — parsed but not yet implemented (explicit cross-cloud seam).
    Aws,
    /// Azure — parsed but not yet implemented (explicit cross-cloud seam).
    Azure,
}

/// Backend that executes the operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum Backend {
    /// Direct REST-over-`reqwest` — the round-1 backend.
    #[default]
    Rest,
    /// Official `google-cloud-rust` gRPC SDK — deferred; maps to [`Backend::Rest`].
    RustSdk,
}

/// Bounds for the inline long-running-Operation poll.  The poll stops on the
/// first of: operation `done`, `max_attempts` reached, or `max_wait_secs`
/// elapsed.  Defaults are tuned for the fast CRM operations (folder/project
/// create usually completes in a few seconds) while staying inside a sane
/// worker-slot hold.
#[derive(Debug, Clone, Deserialize)]
pub struct PollConfig {
    /// Maximum number of poll iterations (including the initial check).
    #[serde(default = "default_poll_max_attempts")]
    pub max_attempts: u32,
    /// Delay between poll iterations, in milliseconds.
    #[serde(default = "default_poll_interval_ms")]
    pub interval_ms: u64,
    /// Wall-clock ceiling for the whole poll, in seconds.
    #[serde(default = "default_poll_max_wait_secs")]
    pub max_wait_secs: u64,
}

fn default_poll_max_attempts() -> u32 {
    30
}
fn default_poll_interval_ms() -> u64 {
    2000
}
fn default_poll_max_wait_secs() -> u64 {
    120
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_poll_max_attempts(),
            interval_ms: default_poll_interval_ms(),
            max_wait_secs: default_poll_max_wait_secs(),
        }
    }
}

/// The three Google service base URLs the tool talks to.  Instance-held (rather
/// than the `const`s directly) so tests can point the tool at a mock server.
#[derive(Debug, Clone)]
struct ApiEndpoints {
    crm: String,
    billing: String,
    serviceusage: String,
}

impl Default for ApiEndpoints {
    fn default() -> Self {
        Self {
            crm: CRM_V3.to_string(),
            billing: BILLING_V1.to_string(),
            serviceusage: SERVICEUSAGE_V1.to_string(),
        }
    }
}

/// Parsed `kind: provider` tool config (deserialized from the flattened,
/// template-rendered tool block).
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderSpec {
    /// Provider family.
    pub provider: ProviderFamily,

    /// Execution backend (defaults to `rest`).
    #[serde(default)]
    pub runtime: Backend,

    /// Operation action.  Accepts the fully-qualified
    /// `google.<service>.<resource>.<verb>` form, or the short
    /// `<resource>.<verb>` form the playbooks emit today alongside a
    /// `service` key (see [`ProviderSpec::canonical_action`]).
    pub action: String,

    /// Emitted-spec `service` domain (e.g. `cloudresourcemanager.googleapis.com`),
    /// used to normalize a short `action` into the canonical form.
    #[serde(default)]
    pub service: Option<String>,

    /// Whether to plan only (default **true** — never silently mutate).
    /// Accepts a real bool or the strings `"true"`/`"false"`/`""`.
    #[serde(
        default = "default_dry_run",
        deserialize_with = "deserialize_flexible_bool"
    )]
    pub dry_run: bool,

    /// Operation-specific parameters.
    #[serde(default)]
    pub input: serde_json::Value,

    /// Bounds for the inline long-running-Operation poll (apply mode).
    #[serde(default)]
    pub poll: PollConfig,
}

fn default_dry_run() -> bool {
    true
}

/// Deserialize a bool that may arrive as a JSON bool or as a template-rendered
/// string (`"true"` / `"false"` / `""`).  An empty string means "unset" →
/// falls back to the safe plan default (`true`).
fn deserialize_flexible_bool<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    match serde_json::Value::deserialize(deserializer)? {
        serde_json::Value::Bool(b) => Ok(b),
        serde_json::Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(true),
            "false" | "0" | "no" => Ok(false),
            "" => Ok(default_dry_run()),
            other => Err(serde::de::Error::custom(format!(
                "invalid dry_run value: {other:?} (expected true/false)"
            ))),
        },
        serde_json::Value::Null => Ok(default_dry_run()),
        other => Err(serde::de::Error::custom(format!(
            "invalid dry_run type: {other} (expected bool or string)"
        ))),
    }
}

impl ProviderSpec {
    /// Normalize `action` to the canonical `<provider>.<service>.<resource>.<verb>`
    /// key.  If `action` already starts with the provider prefix it is returned
    /// as-is; otherwise the `service` domain is mapped to its short name and
    /// prepended (`projects.ensure` + `cloudresourcemanager.googleapis.com` →
    /// `google.cloudresourcemanager.projects.ensure`).
    pub fn canonical_action(&self) -> Result<String, ToolError> {
        let provider_prefix = match self.provider {
            ProviderFamily::Google => "google",
            ProviderFamily::Aws => "aws",
            ProviderFamily::Azure => "azure",
        };

        if self.action.starts_with(&format!("{provider_prefix}.")) {
            return Ok(self.action.clone());
        }

        // Short form: need the service domain to build the canonical key.
        let service_short = self
            .service
            .as_deref()
            .and_then(short_service_name)
            .ok_or_else(|| {
                ToolError::Configuration(format!(
                    "provider action {:?} is not fully-qualified and no known `service` \
                     was supplied to normalize it (expected e.g. \
                     `{provider_prefix}.cloudresourcemanager.folders.list`)",
                    self.action
                ))
            })?;

        Ok(format!("{provider_prefix}.{service_short}.{}", self.action))
    }
}

/// Map an emitted-spec service domain to its short canonical segment.
fn short_service_name(service: &str) -> Option<&'static str> {
    match service {
        "cloudresourcemanager.googleapis.com" => Some("cloudresourcemanager"),
        "cloudbilling.googleapis.com" => Some("cloudbilling"),
        "serviceusage.googleapis.com" => Some("serviceusage"),
        _ => None,
    }
}

/// A concrete REST request the tool would issue for an action.  This is what
/// `dry_run` echoes (never carrying credentials) and what apply mode sends.
#[derive(Debug, Clone)]
struct PlannedRequest {
    method: Method,
    url: String,
    body: Option<serde_json::Value>,
}

impl PlannedRequest {
    fn get(url: String) -> Self {
        Self {
            method: Method::GET,
            url,
            body: None,
        }
    }
    fn post(url: String, body: Option<serde_json::Value>) -> Self {
        Self {
            method: Method::POST,
            url,
            body,
        }
    }
    fn put(url: String, body: Option<serde_json::Value>) -> Self {
        Self {
            method: Method::PUT,
            url,
            body,
        }
    }

    /// Redacted JSON echo for the `would_call` plan (no credentials present).
    fn to_echo(&self) -> serde_json::Value {
        serde_json::json!({
            "method": self.method.as_str(),
            "url": self.url,
            "body": self.body.as_ref().map(redact_sensitive),
        })
    }
}

/// How apply mode converges a given action.  The dry-run echo always uses the
/// [`ActionPlan::request`] (the primary write); apply mode dispatches on this.
#[derive(Debug, Clone)]
enum ApplyKind {
    /// Execute `request` directly; the response *is* the resource (reads).
    Single,
    /// List folders under `parent`; create (LRO) only if no ACTIVE folder with
    /// `display_name` exists.
    EnsureFolder {
        parent: String,
        display_name: String,
    },
    /// GET the project; create (LRO) under `parent` only on 404.
    EnsureProject {
        project_id: String,
        parent: String,
        display_name: String,
    },
    /// GET the service state; enable (LRO) only if not already `ENABLED`.
    EnsureService { project: String, service: String },
    /// GET current billing info; PUT the link only if the account differs.
    EnsureBillingLink {
        project: String,
        billing_account: String,
    },
    /// `getIamPolicy` → add `{role, member}` if absent → `setIamPolicy`.
    EnsureIamBinding {
        get_url: String,
        set_url: String,
        role: String,
        member: String,
    },
}

/// Round-2 action plan: the primary request (dry-run echo + `Single` apply),
/// whether it mutates state, and how apply mode converges it.
struct ActionPlan {
    /// The primary request (for a read, the whole op; for a mutation, the
    /// write shown in the dry-run echo).
    request: PlannedRequest,
    /// True if the action changes cloud state.
    mutates: bool,
    /// How apply mode converges the action.
    apply: ApplyKind,
}

/// Outcome of classifying a long-running-Operation envelope.
enum OpOutcome {
    /// Operation is complete; carries the resolved resource.
    Done(serde_json::Value),
    /// Operation is still running.
    Pending,
    /// Operation finished with an error; carries the stringified error.
    Failed(String),
}

/// Cloud provider operation tool.
pub struct ProviderTool {
    client: reqwest::Client,
    auth_resolver: AuthResolver,
    template_engine: TemplateEngine,
    endpoints: ApiEndpoints,
}

impl ProviderTool {
    /// Create a new provider tool pointed at the real Google API endpoints.
    pub fn new() -> Self {
        Self::with_endpoints(ApiEndpoints::default())
    }

    fn with_endpoints(endpoints: ApiEndpoints) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .unwrap_or_default();
        Self {
            client,
            auth_resolver: AuthResolver::new(),
            template_engine: TemplateEngine::new(),
            endpoints,
        }
    }

    /// Parse + render the tool config into a [`ProviderSpec`].
    fn parse_spec(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ProviderSpec, ToolError> {
        let template_ctx = ctx.to_template_context();
        let rendered = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;
        serde_json::from_value(rendered)
            .map_err(|e| ToolError::Configuration(format!("invalid provider config: {e}")))
    }

    /// Build the action plan for a canonical Google action.
    fn plan_google(
        &self,
        action: &str,
        input: &serde_json::Value,
    ) -> Result<ActionPlan, ToolError> {
        let crm = &self.endpoints.crm;
        let billing = &self.endpoints.billing;
        let serviceusage = &self.endpoints.serviceusage;
        let get = |k: &str| input_str(input, k);

        match action {
            "google.cloudresourcemanager.folders.list" => {
                let parent = require(&get("parent"), "parent", action)?;
                Ok(ActionPlan {
                    request: PlannedRequest::get(format!(
                        "{crm}/folders?parent={}",
                        urlencode(&parent)
                    )),
                    mutates: false,
                    apply: ApplyKind::Single,
                })
            }
            "google.cloudresourcemanager.folders.ensure" => {
                let parent = require(&get("parent"), "parent", action)?;
                let display_name = require(
                    &get("display_name").or_else(|| get("displayName")),
                    "display_name",
                    action,
                )?;
                Ok(ActionPlan {
                    // Primary (write) request echoed in dry-run; apply mode runs
                    // the list-then-create converge in `ApplyKind::EnsureFolder`.
                    request: PlannedRequest::post(
                        format!("{crm}/folders"),
                        Some(serde_json::json!({ "parent": parent, "displayName": display_name })),
                    ),
                    mutates: true,
                    apply: ApplyKind::EnsureFolder {
                        parent,
                        display_name,
                    },
                })
            }
            "google.cloudresourcemanager.organizations.iam.get_policy" => {
                let org = require(
                    &get("organization").or_else(|| org_from_input(input)),
                    "organization",
                    action,
                )?;
                Ok(ActionPlan {
                    request: PlannedRequest::post(
                        format!("{crm}/{org}:getIamPolicy"),
                        Some(serde_json::json!({})),
                    ),
                    mutates: false,
                    apply: ApplyKind::Single,
                })
            }
            "google.cloudresourcemanager.organizations.iam.ensure_binding" => {
                let org = require(
                    &get("organization").or_else(|| org_from_input(input)),
                    "organization",
                    action,
                )?;
                let role = require(&get("role"), "role", action)?;
                let member = require(&get("member"), "member", action)?;
                Ok(ActionPlan {
                    request: PlannedRequest::post(
                        format!("{crm}/{org}:setIamPolicy"),
                        Some(serde_json::json!({
                            "policy": { "bindings": [ { "role": role, "members": [member] } ] }
                        })),
                    ),
                    mutates: true,
                    apply: ApplyKind::EnsureIamBinding {
                        get_url: format!("{crm}/{org}:getIamPolicy"),
                        set_url: format!("{crm}/{org}:setIamPolicy"),
                        role,
                        member,
                    },
                })
            }
            "google.cloudresourcemanager.projects.describe" => {
                let project = require(
                    &get("project_id").or_else(|| get("projectId")),
                    "project_id",
                    action,
                )?;
                Ok(ActionPlan {
                    request: PlannedRequest::get(format!("{crm}/projects/{project}")),
                    mutates: false,
                    apply: ApplyKind::Single,
                })
            }
            "google.cloudresourcemanager.projects.ensure" => {
                let project = require(
                    &get("project_id").or_else(|| get("projectId")),
                    "project_id",
                    action,
                )?;
                let parent = require(&get("parent"), "parent", action)?;
                let display_name = get("display_name")
                    .or_else(|| get("displayName"))
                    .unwrap_or_else(|| project.clone());
                Ok(ActionPlan {
                    request: PlannedRequest::post(
                        format!("{crm}/projects"),
                        Some(serde_json::json!({
                            "projectId": project,
                            "displayName": display_name,
                            "parent": parent,
                        })),
                    ),
                    mutates: true,
                    apply: ApplyKind::EnsureProject {
                        project_id: project,
                        parent,
                        display_name,
                    },
                })
            }
            "google.cloudbilling.projects.link" => {
                let project = require(
                    &get("project_id").or_else(|| get("projectId")),
                    "project_id",
                    action,
                )?;
                let billing_account = require(
                    &get("billing_account")
                        .or_else(|| get("billingAccountName"))
                        .or_else(|| get("billing_account_name")),
                    "billing_account",
                    action,
                )?;
                Ok(ActionPlan {
                    request: PlannedRequest::put(
                        format!("{billing}/projects/{project}/billingInfo"),
                        Some(serde_json::json!({ "billingAccountName": billing_account })),
                    ),
                    mutates: true,
                    apply: ApplyKind::EnsureBillingLink {
                        project,
                        billing_account,
                    },
                })
            }
            "google.cloudbilling.billing_accounts.iam.get_policy" => {
                let ba = require(
                    &get("billing_account").or_else(|| get("billingAccount")),
                    "billing_account",
                    action,
                )?;
                Ok(ActionPlan {
                    request: PlannedRequest::post(
                        format!("{billing}/{ba}:getIamPolicy"),
                        Some(serde_json::json!({})),
                    ),
                    mutates: false,
                    apply: ApplyKind::Single,
                })
            }
            "google.cloudbilling.billing_accounts.iam.ensure_binding" => {
                let ba = require(
                    &get("billing_account").or_else(|| get("billingAccount")),
                    "billing_account",
                    action,
                )?;
                let role = require(&get("role"), "role", action)?;
                let member = require(&get("member"), "member", action)?;
                Ok(ActionPlan {
                    request: PlannedRequest::post(
                        format!("{billing}/{ba}:setIamPolicy"),
                        Some(serde_json::json!({
                            "policy": { "bindings": [ { "role": role, "members": [member] } ] }
                        })),
                    ),
                    mutates: true,
                    apply: ApplyKind::EnsureIamBinding {
                        get_url: format!("{billing}/{ba}:getIamPolicy"),
                        set_url: format!("{billing}/{ba}:setIamPolicy"),
                        role,
                        member,
                    },
                })
            }
            "google.serviceusage.services.list_enabled" => {
                let project = require(
                    &get("project_id").or_else(|| get("projectId")),
                    "project_id",
                    action,
                )?;
                Ok(ActionPlan {
                    request: PlannedRequest::get(format!(
                        "{serviceusage}/projects/{project}/services?filter=state:ENABLED"
                    )),
                    mutates: false,
                    apply: ApplyKind::Single,
                })
            }
            "google.serviceusage.services.enable" => {
                let project = require(
                    &get("project_id").or_else(|| get("projectId")),
                    "project_id",
                    action,
                )?;
                let service = require(
                    &get("service_name").or_else(|| get("service")),
                    "service_name",
                    action,
                )?;
                Ok(ActionPlan {
                    request: PlannedRequest::post(
                        format!("{serviceusage}/projects/{project}/services/{service}:enable"),
                        Some(serde_json::json!({})),
                    ),
                    mutates: true,
                    apply: ApplyKind::EnsureService { project, service },
                })
            }
            other => Err(ToolError::Configuration(format!(
                "unknown google provider action: {other:?}"
            ))),
        }
    }

    /// Send a planned request and return `(status, body)` WITHOUT erroring on a
    /// non-2xx status — the converge callers branch on 404 vs other failures.
    async fn send(
        &self,
        plan: &PlannedRequest,
        creds: &crate::auth::AuthCredentials,
    ) -> Result<(StatusCode, serde_json::Value), ToolError> {
        let mut req = self.client.request(plan.method.clone(), &plan.url);
        if let Some(ref body) = plan.body {
            req = req.json(body);
        }
        req = creds.apply_to_request(req);

        let resp = req.send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        let json: serde_json::Value =
            serde_json::from_str(&text).unwrap_or_else(|_| serde_json::json!(text));
        Ok((status, json))
    }

    /// Execute the planned request against the live API, erroring on non-2xx.
    async fn execute_request(
        &self,
        plan: &PlannedRequest,
        creds: &crate::auth::AuthCredentials,
    ) -> Result<serde_json::Value, ToolError> {
        let (status, json) = self.send(plan, creds).await?;
        if !status.is_success() {
            // The API error body may reference the resource but never the token.
            return Err(ToolError::Http(format!(
                "google API {} for {} {}: {}",
                status.as_u16(),
                plan.method.as_str(),
                plan.url,
                redact_sensitive(&json)
            )));
        }
        Ok(json)
    }

    /// Apply-mode dispatch: run the converge strategy for the planned action and
    /// return `(resource, changed)`.
    async fn apply_action(
        &self,
        plan: &ActionPlan,
        creds: &crate::auth::AuthCredentials,
        poll: &PollConfig,
    ) -> Result<(serde_json::Value, bool), ToolError> {
        match &plan.apply {
            ApplyKind::Single => {
                let resource = self.execute_request(&plan.request, creds).await?;
                Ok((resource, plan.mutates))
            }
            ApplyKind::EnsureFolder {
                parent,
                display_name,
            } => {
                self.apply_ensure_folder(parent, display_name, creds, poll)
                    .await
            }
            ApplyKind::EnsureProject {
                project_id,
                parent,
                display_name,
            } => {
                self.apply_ensure_project(project_id, parent, display_name, creds, poll)
                    .await
            }
            ApplyKind::EnsureService { project, service } => {
                self.apply_ensure_service(project, service, creds, poll)
                    .await
            }
            ApplyKind::EnsureBillingLink {
                project,
                billing_account,
            } => {
                self.apply_ensure_billing_link(project, billing_account, creds)
                    .await
            }
            ApplyKind::EnsureIamBinding {
                get_url,
                set_url,
                role,
                member,
            } => {
                self.apply_ensure_iam_binding(get_url, set_url, role, member, creds)
                    .await
            }
        }
    }

    /// `folders.ensure` — list under `parent`, create (LRO) only if absent.
    async fn apply_ensure_folder(
        &self,
        parent: &str,
        display_name: &str,
        creds: &crate::auth::AuthCredentials,
        poll: &PollConfig,
    ) -> Result<(serde_json::Value, bool), ToolError> {
        let list = self
            .execute_request(
                &PlannedRequest::get(format!(
                    "{}/folders?parent={}",
                    self.endpoints.crm,
                    urlencode(parent)
                )),
                creds,
            )
            .await?;
        if let Some(existing) = find_active_folder(&list, display_name) {
            return Ok((existing, false));
        }
        let op = self
            .execute_request(
                &PlannedRequest::post(
                    format!("{}/folders", self.endpoints.crm),
                    Some(serde_json::json!({ "parent": parent, "displayName": display_name })),
                ),
                creds,
            )
            .await?;
        let resource = self.await_operation(op, creds, poll).await?;
        Ok((resource, true))
    }

    /// `projects.ensure` — GET the project, create (LRO) under `parent` on 404.
    async fn apply_ensure_project(
        &self,
        project_id: &str,
        parent: &str,
        display_name: &str,
        creds: &crate::auth::AuthCredentials,
        poll: &PollConfig,
    ) -> Result<(serde_json::Value, bool), ToolError> {
        let (status, body) = self
            .send(
                &PlannedRequest::get(format!("{}/projects/{project_id}", self.endpoints.crm)),
                creds,
            )
            .await?;
        if status.is_success() {
            // Already exists (any lifecycle state) → no-op converge.  A
            // DELETE_REQUESTED project is intentionally left alone; un-deleting
            // is a destructive-adjacent verb out of round-2 scope.
            return Ok((body, false));
        }
        if status != StatusCode::NOT_FOUND {
            return Err(ToolError::Http(format!(
                "google API {} for GET {}/projects/{project_id}: {}",
                status.as_u16(),
                self.endpoints.crm,
                redact_sensitive(&body)
            )));
        }
        // 404 → create.
        let op = self
            .execute_request(
                &PlannedRequest::post(
                    format!("{}/projects", self.endpoints.crm),
                    Some(serde_json::json!({
                        "projectId": project_id,
                        "displayName": display_name,
                        "parent": parent,
                    })),
                ),
                creds,
            )
            .await?;
        let resource = self.await_operation(op, creds, poll).await?;
        Ok((resource, true))
    }

    /// `services.enable` — GET the service state, enable (LRO) only if not ENABLED.
    async fn apply_ensure_service(
        &self,
        project: &str,
        service: &str,
        creds: &crate::auth::AuthCredentials,
        poll: &PollConfig,
    ) -> Result<(serde_json::Value, bool), ToolError> {
        let current = self
            .execute_request(
                &PlannedRequest::get(format!(
                    "{}/projects/{project}/services/{service}",
                    self.endpoints.serviceusage
                )),
                creds,
            )
            .await?;
        if current.get("state").and_then(|s| s.as_str()) == Some("ENABLED") {
            return Ok((current, false));
        }
        let op = self
            .execute_request(
                &PlannedRequest::post(
                    format!(
                        "{}/projects/{project}/services/{service}:enable",
                        self.endpoints.serviceusage
                    ),
                    Some(serde_json::json!({})),
                ),
                creds,
            )
            .await?;
        let resource = self.await_operation(op, creds, poll).await?;
        Ok((resource, true))
    }

    /// `projects.link` (billing) — GET current billingInfo, PUT only if differs.
    async fn apply_ensure_billing_link(
        &self,
        project: &str,
        billing_account: &str,
        creds: &crate::auth::AuthCredentials,
    ) -> Result<(serde_json::Value, bool), ToolError> {
        let current = self
            .execute_request(
                &PlannedRequest::get(format!(
                    "{}/projects/{project}/billingInfo",
                    self.endpoints.billing
                )),
                creds,
            )
            .await?;
        let linked = current
            .get("billingAccountName")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if linked == billing_account {
            return Ok((current, false));
        }
        let updated = self
            .execute_request(
                &PlannedRequest::put(
                    format!("{}/projects/{project}/billingInfo", self.endpoints.billing),
                    Some(serde_json::json!({ "billingAccountName": billing_account })),
                ),
                creds,
            )
            .await?;
        Ok((updated, true))
    }

    /// `*.iam.ensure_binding` — read-modify-write the IAM policy idempotently.
    async fn apply_ensure_iam_binding(
        &self,
        get_url: &str,
        set_url: &str,
        role: &str,
        member: &str,
        creds: &crate::auth::AuthCredentials,
    ) -> Result<(serde_json::Value, bool), ToolError> {
        let policy = self
            .execute_request(
                &PlannedRequest::post(get_url.to_string(), Some(serde_json::json!({}))),
                creds,
            )
            .await?;
        if binding_present(&policy, role, member) {
            return Ok((policy, false));
        }
        let new_policy = upsert_binding(&policy, role, member);
        let updated = self
            .execute_request(
                &PlannedRequest::post(
                    set_url.to_string(),
                    Some(serde_json::json!({ "policy": new_policy })),
                ),
                creds,
            )
            .await?;
        Ok((updated, true))
    }

    /// Poll a long-running Operation until `done` or the configured bound.
    ///
    /// `initial` is the create call's response (which may itself already be
    /// `done: true` for a fast operation, in which case no polling happens).  On
    /// timeout returns a descriptive [`ToolError::ExecutionFailed`] naming the
    /// operation and the resume path — never a bare timeout.
    async fn await_operation(
        &self,
        initial: serde_json::Value,
        creds: &crate::auth::AuthCredentials,
        poll: &PollConfig,
    ) -> Result<serde_json::Value, ToolError> {
        let op_name = operation_name(&initial);
        let mut current = initial;
        let start = std::time::Instant::now();

        for _ in 0..poll.max_attempts.max(1) {
            match classify_operation(&current) {
                OpOutcome::Done(resource) => return Ok(resource),
                OpOutcome::Failed(msg) => {
                    return Err(ToolError::ExecutionFailed(format!(
                        "provider operation {} failed: {}",
                        op_name.as_deref().unwrap_or("<unknown>"),
                        msg
                    )));
                }
                OpOutcome::Pending => {}
            }

            if start.elapsed().as_secs() >= poll.max_wait_secs {
                break;
            }

            let name = match op_name.as_deref() {
                Some(n) => n,
                // Pending but no operation name to poll — cannot make progress.
                None => break,
            };
            tokio::time::sleep(Duration::from_millis(poll.interval_ms)).await;
            let (status, body) = self
                .send(
                    &PlannedRequest::get(format!("{}/{name}", self.endpoints.crm)),
                    creds,
                )
                .await?;
            if !status.is_success() {
                return Err(ToolError::Http(format!(
                    "google API {} polling operation {name}: {}",
                    status.as_u16(),
                    redact_sensitive(&body)
                )));
            }
            current = body;
        }

        Err(ToolError::ExecutionFailed(format!(
            "provider long-running operation {} did not complete within the bounded \
             inline poll (max_attempts={}, interval_ms={}, max_wait_secs={}). The \
             operation is still progressing server-side — re-run this ensure action \
             to resume (the GET-first converge makes the retry a no-op once the \
             operation completes), or raise poll.max_wait_secs. The callback/hook \
             overflow path for operations that legitimately exceed any inline bound \
             is documented in the provider tool module docs (not built this round).",
            op_name.as_deref().unwrap_or("<unknown>"),
            poll.max_attempts,
            poll.interval_ms,
            poll.max_wait_secs,
        )))
    }
}

impl Default for ProviderTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for ProviderTool {
    fn name(&self) -> &'static str {
        "provider"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let start = std::time::Instant::now();
        let spec = self.parse_spec(config, ctx)?;

        // Cross-cloud seam: only Google is implemented in round 1.
        if spec.provider != ProviderFamily::Google {
            return Err(ToolError::Configuration(format!(
                "provider {:?} is not yet implemented (round 1 is google-only)",
                spec.provider
            )));
        }

        let action = spec.canonical_action()?;
        let plan = self.plan_google(&action, &spec.input)?;
        let backend = match spec.runtime {
            Backend::Rest => "rest",
            // Deferred SDK path maps to REST for now (documented in module docs).
            Backend::RustSdk => "rest",
        };

        let span = tracing::info_span!(
            "tool.dispatch.provider",
            execution_id = ctx.execution_id,
            provider = "google",
            action = %action,
            dry_run = spec.dry_run,
        );
        let _guard = span.enter();

        // ---- Plan / dry-run: no credentials, no network. ----
        if spec.dry_run {
            let data = serde_json::json!({
                "provider": "google",
                "action": action,
                "dry_run": true,
                "changed": false,
                "backend": backend,
                "converge": apply_strategy_label(&plan.apply),
                "would_call": plan.request.to_echo(),
                "input": redact_sensitive(&spec.input),
            });
            return Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64));
        }

        // ---- Apply mode: explicit auth REQUIRED (no ambient ADC fallback). ----
        let auth_config = config.auth.as_ref().ok_or_else(|| {
            ToolError::Configuration(format!(
                "provider action {action:?} runs in apply mode (dry_run=false) and \
                 requires an explicit `auth:` alias; refusing to fall back to ambient \
                 ADC. Add an `auth:` block or set dry_run=true to plan."
            ))
        })?;

        let creds = self.auth_resolver.resolve(auth_config, ctx).await?;
        let (resource, changed) = self.apply_action(&plan, &creds, &spec.poll).await?;

        let data = serde_json::json!({
            "provider": "google",
            "action": action,
            "dry_run": false,
            // `changed` is the real convergence signal: true only when a write
            // was issued (create / enable / link / setIamPolicy); a no-op
            // re-run against already-converged state reports false.
            "changed": changed,
            "backend": backend,
            "converge": apply_strategy_label(&plan.apply),
            "resource": resource,
        });
        Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64))
    }
}

// ---- helpers ----

/// Short label for the converge strategy, surfaced in the result so operators
/// can see whether a read-first path ran.
fn apply_strategy_label(apply: &ApplyKind) -> &'static str {
    match apply {
        ApplyKind::Single => "single",
        ApplyKind::EnsureFolder { .. } => "ensure_folder",
        ApplyKind::EnsureProject { .. } => "ensure_project",
        ApplyKind::EnsureService { .. } => "ensure_service",
        ApplyKind::EnsureBillingLink { .. } => "ensure_billing_link",
        ApplyKind::EnsureIamBinding { .. } => "ensure_iam_binding",
    }
}

/// Find an ACTIVE folder with the target `displayName` in a `folders.list`
/// response.  A folder with no `state` field is treated as active (the field is
/// omitted in some API projections).  Returns the matching folder resource.
fn find_active_folder(list: &serde_json::Value, display_name: &str) -> Option<serde_json::Value> {
    let folders = list.get("folders")?.as_array()?;
    folders
        .iter()
        .find(|f| {
            let name_matches = f.get("displayName").and_then(|v| v.as_str()) == Some(display_name);
            let active = match f.get("state").and_then(|v| v.as_str()) {
                Some(s) => s == "ACTIVE",
                None => true,
            };
            name_matches && active
        })
        .cloned()
}

/// True if the IAM policy already grants `member` on `role`.
fn binding_present(policy: &serde_json::Value, role: &str, member: &str) -> bool {
    policy
        .get("bindings")
        .and_then(|b| b.as_array())
        .map(|bindings| {
            bindings.iter().any(|b| {
                b.get("role").and_then(|v| v.as_str()) == Some(role)
                    && b.get("members")
                        .and_then(|m| m.as_array())
                        .map(|members| members.iter().any(|m| m.as_str() == Some(member)))
                        .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

/// Return a copy of `policy` with `member` added to `role` — adding the member
/// to the existing binding for `role`, or appending a new binding if none
/// exists.  Preserves `etag`, `version`, and every other binding so the
/// `setIamPolicy` write is a minimal, optimistic-concurrency-safe update.
fn upsert_binding(policy: &serde_json::Value, role: &str, member: &str) -> serde_json::Value {
    let mut out = policy.clone();
    if !out.is_object() {
        out = serde_json::json!({});
    }
    let obj = out.as_object_mut().unwrap();

    let bindings = obj
        .entry("bindings")
        .or_insert_with(|| serde_json::Value::Array(vec![]));
    if !bindings.is_array() {
        *bindings = serde_json::Value::Array(vec![]);
    }
    let arr = bindings.as_array_mut().unwrap();

    if let Some(binding) = arr
        .iter_mut()
        .find(|b| b.get("role").and_then(|v| v.as_str()) == Some(role))
    {
        let members = binding
            .as_object_mut()
            .unwrap()
            .entry("members")
            .or_insert_with(|| serde_json::Value::Array(vec![]));
        if !members.is_array() {
            *members = serde_json::Value::Array(vec![]);
        }
        let members = members.as_array_mut().unwrap();
        if !members.iter().any(|m| m.as_str() == Some(member)) {
            members.push(serde_json::Value::String(member.to_string()));
        }
    } else {
        arr.push(serde_json::json!({
            "role": role,
            "members": [member],
        }));
    }

    out
}

/// Extract the pollable operation name (`operations/...`) from an envelope.
fn operation_name(op: &serde_json::Value) -> Option<String> {
    op.get("name")
        .and_then(|v| v.as_str())
        .filter(|s| s.starts_with("operations/"))
        .map(|s| s.to_string())
}

/// Classify a long-running-Operation envelope.  A value that is not an
/// operation envelope (no `operations/` name and no `done` field) is treated as
/// a resource returned directly (`Done`).
fn classify_operation(op: &serde_json::Value) -> OpOutcome {
    let looks_like_op = operation_name(op).is_some() || op.get("done").is_some();
    if !looks_like_op {
        return OpOutcome::Done(op.clone());
    }

    if let Some(err) = op.get("error") {
        if !err.is_null() {
            return OpOutcome::Failed(redact_sensitive(err).to_string());
        }
    }

    let done = op.get("done").and_then(|v| v.as_bool()).unwrap_or(false);
    if done {
        // Completed: the resolved resource is under `response`; fall back to the
        // whole envelope if the API omitted it.
        let resource = op.get("response").cloned().unwrap_or_else(|| op.clone());
        OpOutcome::Done(resource)
    } else {
        OpOutcome::Pending
    }
}

/// Read a string field from the `input` object, coercing scalars to string.
fn input_str(input: &serde_json::Value, key: &str) -> Option<String> {
    match input.get(key)? {
        serde_json::Value::String(s) if !s.is_empty() => Some(s.clone()),
        serde_json::Value::String(_) => None,
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Derive an `organizations/<id>` resource name from `org_id`/`organization_id`.
fn org_from_input(input: &serde_json::Value) -> Option<String> {
    input_str(input, "org_id")
        .or_else(|| input_str(input, "organization_id"))
        .map(|id| {
            if id.starts_with("organizations/") {
                id
            } else {
                format!("organizations/{id}")
            }
        })
}

/// Require a field, mapping absence to a clear config error.
fn require(v: &Option<String>, field: &str, action: &str) -> Result<String, ToolError> {
    v.clone().ok_or_else(|| {
        ToolError::Configuration(format!(
            "provider action {action:?} requires input field {field:?}"
        ))
    })
}

/// Minimal URL query-component encoding (path-segment-safe subset).
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char)
            }
            b' ' => out.push_str("%20"),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Recursively mask values whose key looks sensitive.  Used on echoed input and
/// request bodies so a secret a playbook author placed in `input` never leaves
/// the tool in cleartext.  (The resolved bearer token is never placed in these
/// structures at all — this is defence in depth.)
pub fn redact_sensitive(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                let lk = k.to_ascii_lowercase();
                if SENSITIVE_KEY_FRAGMENTS.iter().any(|frag| lk.contains(frag)) {
                    out.insert(k.clone(), serde_json::json!("***redacted***"));
                } else {
                    out.insert(k.clone(), redact_sensitive(v));
                }
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(redact_sensitive).collect())
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_config(value: serde_json::Value) -> ToolConfig {
        ToolConfig {
            kind: "provider".to_string(),
            config: value,
            timeout: None,
            retry: None,
            auth: None,
        }
    }

    fn gcp_auth() -> crate::registry::AuthConfig {
        crate::registry::AuthConfig {
            auth_type: crate::registry::AuthType::Bearer,
            credential: None,
            token: Some("test-bearer-token".to_string()),
            username: None,
            password: None,
            header: None,
            scopes: None,
        }
    }

    // ---- action normalization / parsing ----

    #[test]
    fn canonical_action_passthrough_when_qualified() {
        let spec: ProviderSpec = serde_json::from_value(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.folders.list",
        }))
        .unwrap();
        assert_eq!(
            spec.canonical_action().unwrap(),
            "google.cloudresourcemanager.folders.list"
        );
    }

    #[test]
    fn canonical_action_normalizes_short_form_with_service() {
        // The shape the gcp-org-playbooks emit today.
        let spec: ProviderSpec = serde_json::from_value(serde_json::json!({
            "provider": "google",
            "service": "cloudresourcemanager.googleapis.com",
            "action": "projects.ensure",
        }))
        .unwrap();
        assert_eq!(
            spec.canonical_action().unwrap(),
            "google.cloudresourcemanager.projects.ensure"
        );
    }

    #[test]
    fn canonical_action_short_form_without_service_errors() {
        let spec: ProviderSpec = serde_json::from_value(serde_json::json!({
            "provider": "google",
            "action": "projects.ensure",
        }))
        .unwrap();
        assert!(matches!(
            spec.canonical_action(),
            Err(ToolError::Configuration(_))
        ));
    }

    #[test]
    fn dry_run_defaults_true_and_accepts_string() {
        let unset: ProviderSpec = serde_json::from_value(serde_json::json!({
            "provider": "google", "action": "google.serviceusage.services.list_enabled",
        }))
        .unwrap();
        assert!(
            unset.dry_run,
            "dry_run must default to true (never silently apply)"
        );

        let templated: ProviderSpec = serde_json::from_value(serde_json::json!({
            "provider": "google", "action": "x", "dry_run": "false",
        }))
        .unwrap();
        assert!(!templated.dry_run);

        let empty: ProviderSpec = serde_json::from_value(serde_json::json!({
            "provider": "google", "action": "x", "dry_run": "",
        }))
        .unwrap();
        assert!(
            empty.dry_run,
            "empty rendered dry_run falls back to the safe default"
        );
    }

    #[test]
    fn poll_config_defaults_and_overrides() {
        let spec: ProviderSpec = serde_json::from_value(serde_json::json!({
            "provider": "google", "action": "x",
        }))
        .unwrap();
        assert_eq!(spec.poll.max_attempts, 30);
        assert_eq!(spec.poll.interval_ms, 2000);
        assert_eq!(spec.poll.max_wait_secs, 120);

        let spec: ProviderSpec = serde_json::from_value(serde_json::json!({
            "provider": "google", "action": "x",
            "poll": { "max_attempts": 3, "interval_ms": 10, "max_wait_secs": 5 },
        }))
        .unwrap();
        assert_eq!(spec.poll.max_attempts, 3);
        assert_eq!(spec.poll.interval_ms, 10);
        assert_eq!(spec.poll.max_wait_secs, 5);
    }

    // ---- dry-run makes no network call and echoes the plan ----

    #[tokio::test]
    async fn dry_run_echoes_would_call_no_network() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.serviceusage.services.enable",
            "dry_run": true,
            "input": { "project_id": "shastaratech-youtube-prod", "service_name": "youtube.googleapis.com" }
        }));

        let result = tool.execute(&cfg, &ctx).await.unwrap();
        assert!(result.is_success());
        let data = result.data.unwrap();
        assert_eq!(data["dry_run"], serde_json::json!(true));
        assert_eq!(data["changed"], serde_json::json!(false));
        assert_eq!(data["backend"], serde_json::json!("rest"));
        assert_eq!(data["converge"], serde_json::json!("ensure_service"));
        assert_eq!(data["would_call"]["method"], serde_json::json!("POST"));
        assert_eq!(
            data["would_call"]["url"],
            serde_json::json!(
                "https://serviceusage.googleapis.com/v1/projects/shastaratech-youtube-prod/services/youtube.googleapis.com:enable"
            )
        );
    }

    #[tokio::test]
    async fn dry_run_folders_list_builds_parent_query() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.folders.list",
            "dry_run": true,
            "input": { "parent": "organizations/561323743912" }
        }));
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["would_call"]["method"], serde_json::json!("GET"));
        assert_eq!(
            data["would_call"]["url"],
            serde_json::json!(
                "https://cloudresourcemanager.googleapis.com/v3/folders?parent=organizations/561323743912"
            )
        );
    }

    // ---- explicit-auth decision: apply mode without auth: → Configuration, no network ----

    #[tokio::test]
    async fn apply_without_auth_is_config_error_no_network() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        // dry_run:false + no auth block on the ToolConfig.
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.serviceusage.services.enable",
            "dry_run": false,
            "input": { "project_id": "p", "service_name": "youtube.googleapis.com" }
        }));
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        match err {
            ToolError::Configuration(msg) => {
                assert!(
                    msg.contains("apply mode"),
                    "message names apply mode: {msg}"
                );
                assert!(
                    msg.contains("auth"),
                    "message names the missing auth: {msg}"
                );
            }
            other => panic!("expected Configuration error, got {other:?}"),
        }
    }

    // ---- unknown provider / unknown action ----

    #[tokio::test]
    async fn unknown_provider_errors() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "aws",
            "action": "aws.something.do",
            "dry_run": true,
        }));
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        assert!(matches!(err, ToolError::Configuration(m) if m.contains("not yet implemented")));
    }

    #[tokio::test]
    async fn unknown_action_errors() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.folders.teleport",
            "dry_run": true,
        }));
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        assert!(
            matches!(err, ToolError::Configuration(m) if m.contains("unknown google provider action"))
        );
    }

    // ---- secret redaction in emitted results ----

    #[test]
    fn redact_sensitive_masks_secret_keys() {
        let v = serde_json::json!({
            "project_id": "p",
            "access_token": "SUPERSECRET",
            "nested": { "client_secret": "HUNTER2", "keep": "ok" },
            "list": [ { "api_key": "AKIA-LEAK" } ],
        });
        let red = redact_sensitive(&v);
        let s = serde_json::to_string(&red).unwrap();
        assert!(
            !s.contains("SUPERSECRET"),
            "access_token must be masked: {s}"
        );
        assert!(!s.contains("HUNTER2"), "client_secret must be masked: {s}");
        assert!(
            !s.contains("AKIA-LEAK"),
            "nested api_key must be masked: {s}"
        );
        assert_eq!(red["project_id"], serde_json::json!("p"));
        assert_eq!(red["nested"]["keep"], serde_json::json!("ok"));
    }

    #[tokio::test]
    async fn dry_run_output_redacts_secret_in_input() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.projects.describe",
            "dry_run": true,
            "input": { "project_id": "p", "oauth_token": "LEAKME" }
        }));
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        let s = serde_json::to_string(&data).unwrap();
        assert!(
            !s.contains("LEAKME"),
            "input secret must be redacted in echo: {s}"
        );
    }

    #[test]
    fn short_service_name_maps_known_domains() {
        assert_eq!(
            short_service_name("cloudbilling.googleapis.com"),
            Some("cloudbilling")
        );
        assert_eq!(short_service_name("unknown.googleapis.com"), None);
    }

    // ---- converge pure-logic units ----

    #[test]
    fn find_active_folder_matches_display_name_and_state() {
        let list = serde_json::json!({
            "folders": [
                { "name": "folders/1", "displayName": "10-shared", "state": "ACTIVE" },
                { "name": "folders/2", "displayName": "20-media", "state": "DELETE_REQUESTED" },
                { "name": "folders/3", "displayName": "20-media", "state": "ACTIVE" },
            ]
        });
        let found = find_active_folder(&list, "20-media").unwrap();
        assert_eq!(found["name"], serde_json::json!("folders/3"));
        // A folder that only exists in DELETE_REQUESTED is not a match.
        let list2 = serde_json::json!({
            "folders": [
                { "name": "folders/2", "displayName": "20-media", "state": "DELETE_REQUESTED" },
            ]
        });
        assert!(find_active_folder(&list2, "20-media").is_none());
        // Empty / missing list.
        assert!(find_active_folder(&serde_json::json!({}), "x").is_none());
    }

    #[test]
    fn binding_present_detects_existing_grant() {
        let policy = serde_json::json!({
            "etag": "BwXyz",
            "bindings": [
                { "role": "roles/owner", "members": ["user:a@x.com", "user:b@x.com"] },
                { "role": "roles/viewer", "members": ["user:c@x.com"] },
            ]
        });
        assert!(binding_present(&policy, "roles/owner", "user:b@x.com"));
        assert!(!binding_present(&policy, "roles/owner", "user:z@x.com"));
        assert!(!binding_present(&policy, "roles/editor", "user:a@x.com"));
        assert!(!binding_present(
            &serde_json::json!({}),
            "roles/owner",
            "user:a@x.com"
        ));
    }

    #[test]
    fn upsert_binding_adds_member_preserving_etag_and_other_bindings() {
        let policy = serde_json::json!({
            "version": 1,
            "etag": "BwXyz",
            "bindings": [
                { "role": "roles/owner", "members": ["user:a@x.com"] },
                { "role": "roles/viewer", "members": ["user:c@x.com"] },
            ]
        });
        // Add to an existing role binding.
        let updated = upsert_binding(&policy, "roles/owner", "user:b@x.com");
        assert_eq!(updated["etag"], serde_json::json!("BwXyz"));
        assert_eq!(updated["version"], serde_json::json!(1));
        assert!(binding_present(&updated, "roles/owner", "user:a@x.com"));
        assert!(binding_present(&updated, "roles/owner", "user:b@x.com"));
        assert!(binding_present(&updated, "roles/viewer", "user:c@x.com"));

        // Add a brand-new role binding.
        let updated2 = upsert_binding(&policy, "roles/editor", "group:eng@x.com");
        assert!(binding_present(
            &updated2,
            "roles/editor",
            "group:eng@x.com"
        ));
        // Original bindings preserved.
        assert!(binding_present(&updated2, "roles/owner", "user:a@x.com"));

        // Re-adding an existing member does not duplicate it.
        let same = upsert_binding(&policy, "roles/owner", "user:a@x.com");
        let owner = same["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["role"] == serde_json::json!("roles/owner"))
            .unwrap();
        assert_eq!(owner["members"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn classify_operation_handles_done_pending_failed_and_direct_resource() {
        // Direct resource (not an operation envelope).
        let direct = serde_json::json!({ "name": "projects/p", "projectId": "p" });
        assert!(matches!(classify_operation(&direct), OpOutcome::Done(_)));

        // Pending operation.
        let pending = serde_json::json!({ "name": "operations/op1", "done": false });
        assert!(matches!(classify_operation(&pending), OpOutcome::Pending));

        // Done with response resource.
        let done = serde_json::json!({
            "name": "operations/op1", "done": true,
            "response": { "name": "folders/9", "displayName": "20-media" }
        });
        match classify_operation(&done) {
            OpOutcome::Done(r) => assert_eq!(r["name"], serde_json::json!("folders/9")),
            _ => panic!("expected Done"),
        }

        // Failed operation.
        let failed = serde_json::json!({
            "name": "operations/op1", "done": true,
            "error": { "code": 7, "message": "permission denied" }
        });
        assert!(matches!(classify_operation(&failed), OpOutcome::Failed(_)));
    }

    // ---- apply-mode integration (wiremock): idempotency + LRO ----

    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn tool_for(server: &MockServer) -> ProviderTool {
        ProviderTool::with_endpoints(ApiEndpoints {
            crm: format!("{}/v3", server.uri()),
            billing: format!("{}/v1", server.uri()),
            serviceusage: format!("{}/v1", server.uri()),
        })
    }

    fn apply_cfg(action: &str, input: serde_json::Value) -> ToolConfig {
        let mut cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": action,
            "dry_run": false,
            "input": input,
            // Fast poll so the timeout test doesn't sleep for real time.
            "poll": { "max_attempts": 3, "interval_ms": 5, "max_wait_secs": 2 },
        }));
        cfg.auth = Some(gcp_auth());
        cfg
    }

    #[tokio::test]
    async fn ensure_folder_creates_then_polls_operation_to_done() {
        let server = MockServer::start().await;
        // List → no matching folder.
        Mock::given(method("GET"))
            .and(path("/v3/folders"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "folders": [ { "name": "folders/1", "displayName": "10-other", "state": "ACTIVE" } ]
            })))
            .mount(&server)
            .await;
        // Create → returns a pending operation.
        Mock::given(method("POST"))
            .and(path("/v3/folders"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "operations/cp.folder-create", "done": false
            })))
            .mount(&server)
            .await;
        // Poll → done with the created folder.
        Mock::given(method("GET"))
            .and(path("/v3/operations/cp.folder-create"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "operations/cp.folder-create", "done": true,
                "response": { "name": "folders/99", "displayName": "20-media", "state": "ACTIVE" }
            })))
            .mount(&server)
            .await;

        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = apply_cfg(
            "google.cloudresourcemanager.folders.ensure",
            serde_json::json!({ "parent": "organizations/1", "display_name": "20-media" }),
        );
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["changed"], serde_json::json!(true));
        assert_eq!(data["resource"]["name"], serde_json::json!("folders/99"));
    }

    #[tokio::test]
    async fn ensure_folder_is_noop_when_already_present() {
        let server = MockServer::start().await;
        // List → the folder already exists → no create should be issued.
        Mock::given(method("GET"))
            .and(path("/v3/folders"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "folders": [ { "name": "folders/99", "displayName": "20-media", "state": "ACTIVE" } ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        // A POST would hit no mounted mock → 404 → error, proving the converge
        // did not attempt a create.

        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = apply_cfg(
            "google.cloudresourcemanager.folders.ensure",
            serde_json::json!({ "parent": "organizations/1", "display_name": "20-media" }),
        );
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(
            data["changed"],
            serde_json::json!(false),
            "re-running ensure against an existing folder must be a no-op"
        );
        assert_eq!(data["resource"]["name"], serde_json::json!("folders/99"));
    }

    #[tokio::test]
    async fn ensure_project_creates_on_404_then_noop_on_second_run() {
        // First run: GET 404 → create → poll done.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/shastaratech-prod"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": { "code": 404, "message": "not found" }
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v3/projects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "operations/pc.1", "done": true,
                "response": { "name": "projects/shastaratech-prod", "projectId": "shastaratech-prod" }
            })))
            .mount(&server)
            .await;

        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = apply_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "shastaratech-prod", "parent": "folders/20" }),
        );
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["changed"], serde_json::json!(true));

        // Second run against a fresh server where GET now returns the project →
        // no-op, no POST mounted.
        let server2 = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/shastaratech-prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "projects/shastaratech-prod", "projectId": "shastaratech-prod",
                "state": "ACTIVE"
            })))
            .mount(&server2)
            .await;
        let tool2 = tool_for(&server2);
        let cfg2 = apply_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "shastaratech-prod", "parent": "folders/20" }),
        );
        let data2 = tool2.execute(&cfg2, &ctx).await.unwrap().data.unwrap();
        assert_eq!(
            data2["changed"],
            serde_json::json!(false),
            "re-running projects.ensure against an existing project must be a no-op"
        );
    }

    #[tokio::test]
    async fn ensure_iam_binding_writes_when_absent_and_noops_when_present() {
        // Absent → getIamPolicy (no binding) → setIamPolicy.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/organizations/1:getIamPolicy"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "etag": "BwABC",
                "bindings": [ { "role": "roles/viewer", "members": ["user:v@x.com"] } ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v3/organizations/1:setIamPolicy"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "etag": "BwDEF",
                "bindings": [
                    { "role": "roles/viewer", "members": ["user:v@x.com"] },
                    { "role": "roles/resourcemanager.folderAdmin", "members": ["group:eng@x.com"] }
                ]
            })))
            .mount(&server)
            .await;

        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = apply_cfg(
            "google.cloudresourcemanager.organizations.iam.ensure_binding",
            serde_json::json!({
                "organization": "organizations/1",
                "role": "roles/resourcemanager.folderAdmin",
                "member": "group:eng@x.com"
            }),
        );
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["changed"], serde_json::json!(true));

        // Present → getIamPolicy already has the binding → no setIamPolicy.
        let server2 = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v3/organizations/1:getIamPolicy"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "etag": "BwXYZ",
                "bindings": [
                    { "role": "roles/resourcemanager.folderAdmin", "members": ["group:eng@x.com"] }
                ]
            })))
            .mount(&server2)
            .await;
        // No setIamPolicy mock → if the converge tried to write it would 404.
        let tool2 = tool_for(&server2);
        let cfg2 = apply_cfg(
            "google.cloudresourcemanager.organizations.iam.ensure_binding",
            serde_json::json!({
                "organization": "organizations/1",
                "role": "roles/resourcemanager.folderAdmin",
                "member": "group:eng@x.com"
            }),
        );
        let data2 = tool2.execute(&cfg2, &ctx).await.unwrap().data.unwrap();
        assert_eq!(
            data2["changed"],
            serde_json::json!(false),
            "re-running ensure_binding with the grant present must be a no-op"
        );
    }

    #[tokio::test]
    async fn ensure_service_noop_when_already_enabled() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/projects/p/services/youtube.googleapis.com"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "projects/p/services/youtube.googleapis.com", "state": "ENABLED"
            })))
            .mount(&server)
            .await;
        // No :enable mock → a write attempt would fail.
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = apply_cfg(
            "google.serviceusage.services.enable",
            serde_json::json!({ "project_id": "p", "service_name": "youtube.googleapis.com" }),
        );
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["changed"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn ensure_billing_link_noop_when_already_linked() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/projects/p/billingInfo"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "projects/p/billingInfo",
                "billingAccountName": "billingAccounts/ABC-123"
            })))
            .mount(&server)
            .await;
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = apply_cfg(
            "google.cloudbilling.projects.link",
            serde_json::json!({ "project_id": "p", "billing_account": "billingAccounts/ABC-123" }),
        );
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["changed"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn lro_poll_times_out_with_descriptive_error() {
        let server = MockServer::start().await;
        // List → empty so a create is issued.
        Mock::given(method("GET"))
            .and(path("/v3/folders"))
            .and(query_param("parent", "organizations/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "folders": []
            })))
            .mount(&server)
            .await;
        // Create → pending operation.
        Mock::given(method("POST"))
            .and(path("/v3/folders"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "operations/never-done", "done": false
            })))
            .mount(&server)
            .await;
        // Poll → always pending → forces the bounded timeout.
        Mock::given(method("GET"))
            .and(path("/v3/operations/never-done"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "operations/never-done", "done": false
            })))
            .mount(&server)
            .await;

        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = apply_cfg(
            "google.cloudresourcemanager.folders.ensure",
            serde_json::json!({ "parent": "organizations/1", "display_name": "20-media" }),
        );
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        match err {
            ToolError::ExecutionFailed(msg) => {
                assert!(
                    msg.contains("operations/never-done"),
                    "timeout names the operation: {msg}"
                );
                assert!(
                    msg.contains("re-run") && msg.contains("bounded"),
                    "timeout describes the resume path: {msg}"
                );
            }
            other => panic!("expected ExecutionFailed timeout, got {other:?}"),
        }
    }
}
