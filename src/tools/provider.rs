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
//! ## Reconcile policy: report / enforce / adopt (round-4 — drift adoption)
//!
//! When infra is changed out-of-band (the GCP console, another tool), the tool
//! must be able to **accept** the change rather than revert it or
//! destroy-and-recreate.  A [`ReconcilePolicy`] on the step selects the
//! response for the mutating ensure actions:
//!
//! - **`report`** (DEFAULT) — detect + report drift; change **nothing** (no
//!   cloud mutation, no ownership fact).  A plain run is always safe.  This is
//!   the Terraform-`plan` shape and it is now the default, so converging
//!   requires an explicit `reconcile: enforce`.
//! - **`enforce`** — push desired → actual (the round-2 GET-first converge);
//!   `dry_run` still gates plan-vs-apply, apply still requires `auth:`.
//! - **`adopt`** — accept the live actual as the NEW desired.  Makes **no**
//!   cloud mutation (read/GET only); writes a new `provider_fact`
//!   (`verb: "adopt"`, `outcome: "adopted"`) recording actual-as-desired.
//!   Covers both an already-owned resource tweaked out-of-band (refresh
//!   last-known-desired) and a resource created entirely outside NoETL
//!   (`import`: `Untracked → Owned`).
//!
//! **`adopt` is confirm-gated, identical to the destroy gate:** a dry-run emits
//! the field-by-field drift diff (desired vs actual) + a `plan_digest`; an apply
//! requires `confirm:<plan_digest>`; a **stale digest is refused** if the
//! resource drifted *again* since the human reviewed it.  Ungated adoption would
//! launder an accidental or malicious out-of-band change into "desired"
//! permanently — human review is the point.  The last-known-desired side of the
//! diff comes from the caller's query adapter (`known_desired`, sourced from the
//! EHDB raw-eventlog-tier fold — see [`state::extract_facts_from_eventlog`]);
//! the live-actual side is [`normalize_live`]'d from a read-only GET.
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

impl ApiEndpoints {
    /// Resolve the endpoints from an optional config-level override.  With no
    /// override the real Google hosts are used.  A base-URL override derives the
    /// three service URLs by appending the same version segments the real hosts
    /// carry (`/v3` for CRM, `/v1` for billing + service usage) — this is the
    /// exact shape the wiremock/emulator tests point at, so a playbook validated
    /// offline exercises the identical URL construction as production.
    fn resolve(override_cfg: Option<&EndpointOverride>) -> Self {
        match override_cfg {
            None => Self::default(),
            Some(EndpointOverride::Base(base)) => {
                let base = base.trim_end_matches('/');
                Self {
                    crm: format!("{base}/v3"),
                    billing: format!("{base}/v1"),
                    serviceusage: format!("{base}/v1"),
                }
            }
            Some(EndpointOverride::PerService {
                crm,
                billing,
                serviceusage,
            }) => {
                let d = ApiEndpoints::default();
                Self {
                    crm: crm.clone().unwrap_or(d.crm),
                    billing: billing.clone().unwrap_or(d.billing),
                    serviceusage: serviceusage.clone().unwrap_or(d.serviceusage),
                }
            }
        }
    }
}

/// Config-level API endpoint override.  **For testing / emulators only** — it
/// points the tool at a mock server (wiremock) or a local Google API emulator so
/// a playbook can be validated offline in `noetl exec --runtime local` without a
/// single live cloud call.  Never set this against a real workload.
///
/// Two shapes:
/// - a single base URL string (`endpoint: http://127.0.0.1:8089`) → the three
///   service URLs are derived by appending `/v3` (CRM) and `/v1` (billing,
///   service usage), matching the real hosts' version segments;
/// - an object with any of `crm` / `billing` / `serviceusage` for full control
///   (unset services fall back to the real Google host).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum EndpointOverride {
    /// Single base URL; service URLs derived by appending version segments.
    Base(String),
    /// Explicit per-service override; unset services use the real host.
    PerService {
        #[serde(default)]
        crm: Option<String>,
        #[serde(default)]
        billing: Option<String>,
        #[serde(default)]
        serviceusage: Option<String>,
    },
}

/// Reconciliation policy for a drifted resource (Round 4 — noetl/ai-meta#189).
///
/// Selects how the tool responds when last-known-desired ≠ live actual.  The
/// default is [`ReconcilePolicy::Report`] — a plain run detects drift and
/// changes **nothing** (no cloud mutation, no state write).  The two mutating
/// policies are explicit opt-ins; `adopt` is additionally confirm-gated exactly
/// like a destroy.  The tool never silently reverts and never silently accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReconcilePolicy {
    /// DEFAULT.  Detect + report drift.  Reads live state only (needs `auth:`
    /// to read the live actual — without it the report is `undetermined`, still
    /// safe); makes **no** cloud mutation and writes **no** ownership fact.
    #[default]
    Report,
    /// Push desired → actual — the round-2 GET-first idempotent converge (the
    /// Terraform-`apply` shape).  Explicit opt-in; `dry_run` still gates
    /// plan-vs-apply within enforce, and apply mode still REQUIRES `auth:`.
    Enforce,
    /// Accept the live actual as the NEW desired.  Makes **no** cloud mutation
    /// (read/GET only) — records a new `provider_fact` (`verb: "adopt"`) marking
    /// the live actual as desired.  Covers both an already-owned resource tweaked
    /// out-of-band (update last-known-desired) and a resource created entirely
    /// outside NoETL (import: `Untracked → Owned`).  Confirm-gated exactly like
    /// destroy: dry-run emits the field-by-field diff + a `plan_digest`; apply
    /// requires `confirm:<digest>`; a stale digest (the resource drifted again
    /// since review) is refused so an accidental / malicious out-of-band change
    /// cannot be laundered into "desired" without human review.
    Adopt,
}

/// Wrong-target safety guard (see [`ProviderSpec::guard`]).  Pins the
/// organization and billing account a run is allowed to touch.  Mirrors the
/// confirm-gate discipline: make a wrong-target write structurally impossible,
/// not merely unlikely.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GuardSpec {
    /// Organization the run is pinned to (`organizations/<id>` or bare `<id>`).
    /// Any action whose input references a *different* `organizations/<id>` is
    /// refused offline, before any request is built or credential minted.
    #[serde(default)]
    pub require_org: Option<String>,

    /// Expected `displayName` of `require_org`.  Checked live (read-only GET) in
    /// apply mode as belt-and-braces against a re-pointed / typo'd numeric id
    /// that happens to exist elsewhere.
    #[serde(default)]
    pub require_org_display_name: Option<String>,

    /// Billing account the run is pinned to (`billingAccounts/<id>` or bare
    /// `<id>`).  Any action referencing a different billing account is refused
    /// offline; in apply mode the account's `parent` is checked to equal
    /// `require_org`.
    #[serde(default)]
    pub require_billing_account: Option<String>,
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

    /// Config-level API endpoint override — **testing / emulators only**.  Lets
    /// a playbook be validated offline against wiremock or a Google API emulator
    /// in `noetl exec --runtime local` without any live cloud call.  See
    /// [`EndpointOverride`].
    #[serde(default)]
    pub endpoint: Option<EndpointOverride>,

    /// Ownership / stack label (Fork 1).  Scopes the resource-ownership
    /// projection so drift + orphan detection can answer "what does THIS stack
    /// own".  When unset, ownership facts are still emitted but carry an
    /// `<unscoped>` stack — set an explicit label to make orphan detection
    /// meaningful across playbook runs.
    #[serde(default)]
    pub stack: Option<String>,

    /// Destroy confirmation digest (Fork 2).  Required to *apply* a destroy
    /// action (`dry_run: false` on a `*.delete` / `*.disable` / `*.remove_binding`
    /// verb).  Must equal the `plan_digest` a human reviewed from the destroy
    /// dry-run.  A blind apply (no `confirm`) is refused; a stale `confirm` (the
    /// live plan no longer matches the reviewed digest) is refused.  Ignored by
    /// non-destroy actions.
    #[serde(default)]
    pub confirm: Option<String>,

    /// Reconciliation policy (Round 4).  Governs the mutating **ensure** actions
    /// only — reads (`folders.list`, `*.describe`, `*.get_policy`) and the
    /// confirm-gated destroy verbs are unaffected.  Defaults to
    /// [`ReconcilePolicy::Report`]: an unqualified run of a mutating action
    /// reports drift and changes nothing.  Set `enforce` to converge (the
    /// round-2 behavior) or `adopt` to accept live actual as desired.
    #[serde(default)]
    pub reconcile: ReconcilePolicy,

    /// Last-known-desired spec for this resource's URN, supplied by the caller's
    /// query adapter (the EHDB raw-eventlog-tier fold — see
    /// [`state::extract_facts_from_eventlog`] / [`state::fold_facts`]).  Used by
    /// `report` and `adopt` to compute the desired-vs-actual diff.  When absent,
    /// the resource has no ownership record: `report` reports it `untracked` and
    /// `adopt` performs an import (`Untracked → Owned`).  Ignored by `enforce`
    /// (which derives desired from the step's own `input`).
    #[serde(default)]
    pub known_desired: Option<serde_json::Value>,

    /// Multi-org / multi-billing safety guard.  When set, every action is
    /// checked structurally offline (no network, fires in dry-run) and, in apply
    /// mode, against live state — so a run can only touch the pinned
    /// organization and billing account.  A mismatch is refused before any
    /// request is built.
    #[serde(default)]
    pub guard: Option<GuardSpec>,
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
    fn delete(url: String) -> Self {
        Self {
            method: Method::DELETE,
            url,
            body: None,
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
    /// Confirm-gated destroy (Fork 2).  Resolved against live state to produce a
    /// digest-confirmable plan; apply requires a matching `confirm` digest.
    Destroy(DestroyKind),
}

/// A destroy verb, resolved against live state before any deletion.  Each kind
/// targets exactly **one explicit** resource — there are no wildcard, glob, or
/// bulk deletes.  The resolve step reads live state (requires `auth:` even in
/// dry-run) so the emitted plan digest reflects what would *actually* be deleted
/// right now; a `confirm` digest that no longer matches the live-resolved plan
/// is refused (stale-review guard).
#[derive(Debug, Clone)]
enum DestroyKind {
    /// DELETE an ACTIVE folder resolved by `display_name` under `parent` (LRO).
    DeleteFolder {
        parent: String,
        display_name: String,
    },
    /// DELETE a project resolved by id (LRO).
    DeleteProject { project_id: String },
    /// `services/{service}:disable` an ENABLED service.
    DisableService { project: String, service: String },
    /// `getIamPolicy` → remove `{role, member}` if present → `setIamPolicy`.
    RemoveIamBinding {
        get_url: String,
        set_url: String,
        role: String,
        member: String,
        /// Human-facing resource label for the plan / URN (e.g. `organizations/1`).
        resource: String,
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
            "google.cloudresourcemanager.organizations.get" => {
                // Read-only org describe.  Backs the guard's live display-name
                // check and is a first-class action a playbook can call directly.
                let org = require(
                    &get("organization").or_else(|| org_from_input(input)),
                    "organization",
                    action,
                )?;
                Ok(ActionPlan {
                    request: PlannedRequest::get(format!("{crm}/{org}")),
                    mutates: false,
                    apply: ApplyKind::Single,
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

            // ---- destroy verbs (Fork 2 — confirm-gated; resolved against live state) ----
            "google.cloudresourcemanager.folders.delete" => {
                let parent = require(&get("parent"), "parent", action)?;
                let display_name = require(
                    &get("display_name").or_else(|| get("displayName")),
                    "display_name",
                    action,
                )?;
                reject_wildcard(&display_name, "display_name", action)?;
                Ok(ActionPlan {
                    // Echo request is illustrative; the real target folders/<id> is
                    // resolved live (the folder id is not known until the list read).
                    request: PlannedRequest::delete(format!("{crm}/folders/<resolved>")),
                    mutates: true,
                    apply: ApplyKind::Destroy(DestroyKind::DeleteFolder {
                        parent,
                        display_name,
                    }),
                })
            }
            "google.cloudresourcemanager.projects.delete" => {
                let project = require(
                    &get("project_id").or_else(|| get("projectId")),
                    "project_id",
                    action,
                )?;
                reject_wildcard(&project, "project_id", action)?;
                Ok(ActionPlan {
                    request: PlannedRequest::delete(format!("{crm}/projects/{project}")),
                    mutates: true,
                    apply: ApplyKind::Destroy(DestroyKind::DeleteProject {
                        project_id: project,
                    }),
                })
            }
            "google.serviceusage.services.disable" => {
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
                reject_wildcard(&service, "service_name", action)?;
                Ok(ActionPlan {
                    request: PlannedRequest::post(
                        format!("{serviceusage}/projects/{project}/services/{service}:disable"),
                        Some(serde_json::json!({})),
                    ),
                    mutates: true,
                    apply: ApplyKind::Destroy(DestroyKind::DisableService { project, service }),
                })
            }
            "google.cloudresourcemanager.organizations.iam.remove_binding" => {
                let org = require(
                    &get("organization").or_else(|| org_from_input(input)),
                    "organization",
                    action,
                )?;
                let role = require(&get("role"), "role", action)?;
                let member = require(&get("member"), "member", action)?;
                reject_wildcard(&member, "member", action)?;
                reject_wildcard(&role, "role", action)?;
                Ok(ActionPlan {
                    request: PlannedRequest::post(
                        format!("{crm}/{org}:setIamPolicy"),
                        Some(serde_json::json!({})),
                    ),
                    mutates: true,
                    apply: ApplyKind::Destroy(DestroyKind::RemoveIamBinding {
                        get_url: format!("{crm}/{org}:getIamPolicy"),
                        set_url: format!("{crm}/{org}:setIamPolicy"),
                        role,
                        member,
                        resource: org,
                    }),
                })
            }
            "google.cloudbilling.billing_accounts.iam.remove_binding" => {
                let ba = require(
                    &get("billing_account").or_else(|| get("billingAccount")),
                    "billing_account",
                    action,
                )?;
                let role = require(&get("role"), "role", action)?;
                let member = require(&get("member"), "member", action)?;
                reject_wildcard(&member, "member", action)?;
                reject_wildcard(&role, "role", action)?;
                Ok(ActionPlan {
                    request: PlannedRequest::post(
                        format!("{billing}/{ba}:setIamPolicy"),
                        Some(serde_json::json!({})),
                    ),
                    mutates: true,
                    apply: ApplyKind::Destroy(DestroyKind::RemoveIamBinding {
                        get_url: format!("{billing}/{ba}:getIamPolicy"),
                        set_url: format!("{billing}/{ba}:setIamPolicy"),
                        role,
                        member,
                        resource: ba,
                    }),
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

    /// Live (read-only) belt-and-braces pass of the wrong-target guard.  Runs in
    /// apply mode after credentials resolve.  Confirms the pinned org's
    /// `displayName` and, when a billing account is pinned, that its `parent`
    /// equals the pinned org.  A mismatch is refused before any mutating call.
    async fn verify_guard_live(
        &self,
        guard: &GuardSpec,
        creds: &crate::auth::AuthCredentials,
    ) -> Result<(), ToolError> {
        if let (Some(org), Some(want_name)) = (
            guard.require_org.as_deref(),
            guard.require_org_display_name.as_deref(),
        ) {
            let org = normalize_resource(org, "organizations/");
            let req = PlannedRequest::get(format!("{}/{}", self.endpoints.crm, org));
            let (status, body) = self.send(&req, creds).await?;
            if !status.is_success() {
                return Err(ToolError::Configuration(format!(
                    "org guard live check could not verify {org}: GET returned {status}; \
                     refusing to proceed"
                )));
            }
            let got = body
                .get("displayName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if got != want_name {
                return Err(ToolError::Configuration(format!(
                    "org guard REFUSED (live): {org} displayName is {got:?} but \
                     require_org_display_name pins {want_name:?}"
                )));
            }
        }

        if let (Some(ba), Some(org)) = (
            guard.require_billing_account.as_deref(),
            guard.require_org.as_deref(),
        ) {
            let ba = normalize_resource(ba, "billingAccounts/");
            let org = normalize_resource(org, "organizations/");
            let req = PlannedRequest::get(format!("{}/{}", self.endpoints.billing, ba));
            let (status, body) = self.send(&req, creds).await?;
            if !status.is_success() {
                return Err(ToolError::Configuration(format!(
                    "billing guard live check could not verify {ba}: GET returned \
                     {status}; refusing to proceed"
                )));
            }
            if let Some(parent) = body.get("parent").and_then(|v| v.as_str()) {
                if parent != org {
                    return Err(ToolError::Configuration(format!(
                        "billing guard REFUSED (live): {ba} parent is {parent:?} but \
                         require_org pins {org:?}"
                    )));
                }
            }
        }

        Ok(())
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
            // Destroy is confirm-gated and dispatched via `run_destroy`, never
            // through the ensure/read apply path.
            ApplyKind::Destroy(_) => Err(ToolError::ExecutionFailed(
                "internal error: destroy action reached the ensure apply path".to_string(),
            )),
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

        // Config-level endpoint override (testing / emulators only): dispatch on
        // a tool instance pointed at the override so every URL construction is
        // exercised exactly as production, but against a mock / emulator.  With
        // no override this is `self` — production hits the real Google hosts.
        let endpoint_override = spec.endpoint.clone();
        match endpoint_override {
            None => self.run_spec(config, ctx, spec, start).await,
            Some(over) => {
                let scoped = ProviderTool::with_endpoints(ApiEndpoints::resolve(Some(&over)));
                scoped.run_spec(config, ctx, spec, start).await
            }
        }
    }
}

impl ProviderTool {
    /// Dispatch a parsed spec (post endpoint-scoping).  Splits into the
    /// destroy-gated path (Fork 2) and the ensure/read path, both of which emit
    /// an ownership fact (Fork 1) for mutating actions.
    async fn run_spec(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
        spec: ProviderSpec,
        start: std::time::Instant,
    ) -> Result<ToolResult, ToolError> {
        // Cross-cloud seam: only Google is implemented in round 1.
        if spec.provider != ProviderFamily::Google {
            return Err(ToolError::Configuration(format!(
                "provider {:?} is not yet implemented (round 1 is google-only)",
                spec.provider
            )));
        }

        let action = spec.canonical_action()?;

        // ---- Wrong-target guard, structural pass (Stage-1 safety) ----
        // Purely offline: fires here, before any request is built or credential
        // minted, so it protects dry-run, report, adopt, destroy, and apply
        // alike.  Refuses if any input references an org / billing account other
        // than the pinned one.
        enforce_guard_offline(spec.guard.as_ref(), &action, &spec.input)?;

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
            destroy = matches!(plan.apply, ApplyKind::Destroy(_)),
        );
        let _guard = span.enter();

        // ---- Confirm-gated destroy (Fork 2) ----
        if let ApplyKind::Destroy(kind) = &plan.apply {
            return self
                .run_destroy(&action, kind, &spec, config, ctx, backend, start)
                .await;
        }

        // ---- Reconciliation policy (Round 4) ----
        // Governs the mutating ensure actions only.  Reads (`plan.mutates ==
        // false`) have no desired-vs-actual to reconcile — they always execute
        // as before.  Destroy (handled above) carries its own confirm gate.
        if plan.mutates {
            match spec.reconcile {
                ReconcilePolicy::Report => {
                    return self
                        .run_report(&action, &plan, &spec, config, ctx, backend, start)
                        .await;
                }
                ReconcilePolicy::Adopt => {
                    return self
                        .run_adopt(&action, &plan, &spec, config, ctx, backend, start)
                        .await;
                }
                // Enforce is the round-2 converge — falls through to the existing
                // dry-run-echo / apply path below.
                ReconcilePolicy::Enforce => {}
            }
        }

        let stack = spec.stack.as_deref().unwrap_or("<unscoped>");
        let (service, resource_type) = service_and_type(&plan.apply);
        let urn = urn_for_apply(&plan.apply);

        // ---- Plan / dry-run: no credentials, no network. ----
        if spec.dry_run {
            let mut data = serde_json::json!({
                "provider": "google",
                "action": action,
                "dry_run": true,
                "changed": false,
                "backend": backend,
                "converge": apply_strategy_label(&plan.apply),
                "would_call": plan.request.to_echo(),
                "input": redact_sensitive(&spec.input),
            });
            if plan.mutates {
                if let Some(urn) = urn {
                    data["provider_fact"] = build_fact(
                        &urn,
                        service,
                        resource_type,
                        "ensure",
                        stack,
                        redact_sensitive(&spec.input),
                        ctx.execution_id,
                        "planned",
                    );
                }
            }
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

        // ---- Wrong-target guard, live pass (belt-and-braces) ----
        // Read-only GETs confirm the pinned org's displayName and the pinned
        // billing account's parent before any mutating call for this step.
        if let Some(g) = spec.guard.as_ref() {
            self.verify_guard_live(g, &creds).await?;
        }

        let (resource, changed) = self.apply_action(&plan, &creds, &spec.poll).await?;

        let mut data = serde_json::json!({
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
        if plan.mutates {
            if let Some(urn) = urn {
                let outcome = if changed { "changed" } else { "noop" };
                data["provider_fact"] = build_fact(
                    &urn,
                    service,
                    resource_type,
                    "ensure",
                    stack,
                    redact_sensitive(&spec.input),
                    ctx.execution_id,
                    outcome,
                );
            }
        }
        Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64))
    }

    /// Confirm-gated destroy dispatch (Fork 2).
    ///
    /// Destroy needs `auth:` even to *plan*, because the plan digest must be
    /// resolved against live state — that is the only way a stale confirmation
    /// (cloud drifted since review) can be detected and refused.
    #[allow(clippy::too_many_arguments)]
    async fn run_destroy(
        &self,
        action: &str,
        kind: &DestroyKind,
        spec: &ProviderSpec,
        config: &ToolConfig,
        ctx: &ExecutionContext,
        backend: &str,
        start: std::time::Instant,
    ) -> Result<ToolResult, ToolError> {
        let stack = spec.stack.as_deref().unwrap_or("<unscoped>");
        let (service, resource_type) = service_and_type(&ApplyKind::Destroy(kind.clone()));

        let auth_config = config.auth.as_ref().ok_or_else(|| {
            ToolError::Configuration(format!(
                "destroy action {action:?} requires an explicit `auth:` block even to plan: \
                 the deletion plan is resolved against live cloud state so a stale \
                 confirmation can be refused. Add an `auth:` block (dry_run:true still \
                 performs no deletion — only the read needed to resolve the plan)."
            ))
        })?;
        let creds = self.auth_resolver.resolve(auth_config, ctx).await?;

        // Resolve the plan against live state → the confirmable digest.
        let resolved = self.resolve_destroy(action, kind, &creds).await?;
        let digest = destroy_plan_digest(action, &resolved);
        let urn = resolved.urn.clone();

        // ---- Destroy dry-run: emit the explicit plan + digest for review. ----
        if spec.dry_run {
            let data = serde_json::json!({
                "provider": "google",
                "action": action,
                "dry_run": true,
                "changed": false,
                "destroy": true,
                "backend": backend,
                "converge": "destroy",
                "plan_digest": digest,
                "plan": resolved.to_plan_echo(),
                "present": resolved.present,
                "confirm_hint": "review the plan above, then re-run with dry_run:false and \
                                 confirm:<plan_digest> to apply this exact reviewed deletion. \
                                 If the cloud state changes before you apply, the digest will \
                                 no longer match and the apply will be refused.",
                "provider_fact": build_fact(
                    &urn, service, resource_type, "delete", stack,
                    serde_json::Value::Null, ctx.execution_id, "planned",
                ),
            });
            return Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64));
        }

        // ---- Destroy apply: require a confirm digest bound to THIS live plan. ----
        let confirm = spec.confirm.as_deref().ok_or_else(|| {
            ToolError::Configuration(format!(
                "destroy apply REFUSED for {urn}: no `confirm` digest supplied. A destroy \
                 apply must echo the `plan_digest` from a reviewed dry-run — blind applies are \
                 not permitted. Run dry_run:true first, review the plan, then re-run with \
                 confirm:<plan_digest>. No deletion was performed."
            ))
        })?;
        if confirm != digest {
            return Err(ToolError::ExecutionFailed(format!(
                "destroy apply REFUSED for {urn}: the supplied confirm digest ({confirm}) does \
                 not match the live-resolved plan digest ({digest}). The cloud state changed \
                 since the plan was reviewed (or the confirm value is wrong) — re-run \
                 dry_run:true, review the fresh plan, and confirm its new digest. No deletion \
                 was performed."
            )));
        }

        // Digest matched.  If the resource is already absent, the reviewed plan
        // itself was a no-op deletion — honor it as such (idempotent).
        if !resolved.present {
            let data = serde_json::json!({
                "provider": "google",
                "action": action,
                "dry_run": false,
                "changed": false,
                "destroy": true,
                "already_absent": true,
                "backend": backend,
                "converge": "destroy",
                "resource": serde_json::Value::Null,
                "provider_fact": build_fact(
                    &urn, service, resource_type, "delete", stack,
                    serde_json::Value::Null, ctx.execution_id, "absent",
                ),
            });
            return Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64));
        }

        // Execute the resolved deletion.
        let (resource, changed) = self
            .execute_destroy(&resolved, kind, &creds, &spec.poll)
            .await?;
        let data = serde_json::json!({
            "provider": "google",
            "action": action,
            "dry_run": false,
            "changed": changed,
            "destroy": true,
            "backend": backend,
            "converge": "destroy",
            "resource": resource,
            "provider_fact": build_fact(
                &urn, service, resource_type, "delete", stack,
                serde_json::Value::Null, ctx.execution_id, "deleted",
            ),
        });
        Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64))
    }

    /// Resolve a destroy target against live cloud state.  Reads only — issues no
    /// deletion.  Refuses an ambiguous target (a folder display-name matching
    /// more than one ACTIVE folder is not "explicit").
    async fn resolve_destroy(
        &self,
        action: &str,
        kind: &DestroyKind,
        creds: &crate::auth::AuthCredentials,
    ) -> Result<ResolvedDestroy, ToolError> {
        let urn = urn_for_destroy(kind);
        match kind {
            DestroyKind::DeleteFolder {
                parent,
                display_name,
            } => {
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
                let matches = active_folders_named(&list, display_name);
                if matches.len() > 1 {
                    return Err(ToolError::Configuration(format!(
                        "destroy action {action:?} is ambiguous: {} ACTIVE folders match \
                         display_name {display_name:?} under {parent}. Destroy targets must \
                         resolve to exactly one explicit resource — no bulk deletes.",
                        matches.len()
                    )));
                }
                match matches.into_iter().next() {
                    None => Ok(ResolvedDestroy {
                        urn,
                        resolved_name: "<absent>".to_string(),
                        etag: None,
                        state: None,
                        present: false,
                        request: PlannedRequest::delete(format!(
                            "{}/folders/<absent>",
                            self.endpoints.crm
                        )),
                    }),
                    Some(folder) => {
                        let name = folder
                            .get("name")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        Ok(ResolvedDestroy {
                            urn,
                            resolved_name: name.clone(),
                            etag: folder
                                .get("etag")
                                .and_then(|v| v.as_str())
                                .map(String::from),
                            state: folder
                                .get("state")
                                .and_then(|v| v.as_str())
                                .map(String::from),
                            present: true,
                            request: PlannedRequest::delete(format!(
                                "{}/{name}",
                                self.endpoints.crm
                            )),
                        })
                    }
                }
            }
            DestroyKind::DeleteProject { project_id } => {
                let (status, body) = self
                    .send(
                        &PlannedRequest::get(format!(
                            "{}/projects/{project_id}",
                            self.endpoints.crm
                        )),
                        creds,
                    )
                    .await?;
                if status == StatusCode::NOT_FOUND {
                    return Ok(ResolvedDestroy {
                        urn,
                        resolved_name: "<absent>".to_string(),
                        etag: None,
                        state: None,
                        present: false,
                        request: PlannedRequest::delete(format!(
                            "{}/projects/{project_id}",
                            self.endpoints.crm
                        )),
                    });
                }
                if !status.is_success() {
                    return Err(ToolError::Http(format!(
                        "google API {} resolving destroy target projects/{project_id}: {}",
                        status.as_u16(),
                        redact_sensitive(&body)
                    )));
                }
                Ok(ResolvedDestroy {
                    urn,
                    resolved_name: format!("projects/{project_id}"),
                    etag: body.get("etag").and_then(|v| v.as_str()).map(String::from),
                    state: body.get("state").and_then(|v| v.as_str()).map(String::from),
                    present: true,
                    request: PlannedRequest::delete(format!(
                        "{}/projects/{project_id}",
                        self.endpoints.crm
                    )),
                })
            }
            DestroyKind::DisableService { project, service } => {
                let current = self
                    .execute_request(
                        &PlannedRequest::get(format!(
                            "{}/projects/{project}/services/{service}",
                            self.endpoints.serviceusage
                        )),
                        creds,
                    )
                    .await?;
                let state = current
                    .get("state")
                    .and_then(|v| v.as_str())
                    .unwrap_or("STATE_UNSPECIFIED");
                let present = state == "ENABLED";
                Ok(ResolvedDestroy {
                    urn,
                    resolved_name: format!("projects/{project}/services/{service}"),
                    etag: None,
                    state: Some(state.to_string()),
                    present,
                    request: PlannedRequest::post(
                        format!(
                            "{}/projects/{project}/services/{service}:disable",
                            self.endpoints.serviceusage
                        ),
                        Some(serde_json::json!({})),
                    ),
                })
            }
            DestroyKind::RemoveIamBinding {
                get_url,
                set_url,
                role,
                member,
                ..
            } => {
                let policy = self
                    .execute_request(
                        &PlannedRequest::post(get_url.clone(), Some(serde_json::json!({}))),
                        creds,
                    )
                    .await?;
                let present = binding_present(&policy, role, member);
                Ok(ResolvedDestroy {
                    urn,
                    resolved_name: format!("{role} → {member}"),
                    etag: policy
                        .get("etag")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    state: Some(if present { "bound" } else { "absent" }.to_string()),
                    present,
                    request: PlannedRequest::post(set_url.clone(), Some(serde_json::json!({}))),
                })
            }
        }
    }

    /// Execute a resolved deletion (only reached after the digest matched and the
    /// resource is present).  Returns `(resource, changed)`.
    async fn execute_destroy(
        &self,
        resolved: &ResolvedDestroy,
        kind: &DestroyKind,
        creds: &crate::auth::AuthCredentials,
        poll: &PollConfig,
    ) -> Result<(serde_json::Value, bool), ToolError> {
        match kind {
            DestroyKind::DeleteFolder { .. } | DestroyKind::DeleteProject { .. } => {
                let op = self.execute_request(&resolved.request, creds).await?;
                let resource = self.await_operation(op, creds, poll).await?;
                Ok((resource, true))
            }
            DestroyKind::DisableService { .. } => {
                // Disable returns an LRO on serviceusage; poll it to done.
                let op = self.execute_request(&resolved.request, creds).await?;
                let resource = self.await_operation(op, creds, poll).await?;
                Ok((resource, true))
            }
            DestroyKind::RemoveIamBinding {
                get_url,
                set_url,
                role,
                member,
                ..
            } => {
                // Read-modify-write: re-read the policy (preserving etag) and
                // write it back without the {role, member} binding.
                let policy = self
                    .execute_request(
                        &PlannedRequest::post(get_url.clone(), Some(serde_json::json!({}))),
                        creds,
                    )
                    .await?;
                if !binding_present(&policy, role, member) {
                    return Ok((policy, false));
                }
                let new_policy = remove_binding(&policy, role, member);
                let updated = self
                    .execute_request(
                        &PlannedRequest::post(
                            set_url.clone(),
                            Some(serde_json::json!({ "policy": new_policy })),
                        ),
                        creds,
                    )
                    .await?;
                Ok((updated, true))
            }
        }
    }

    /// Round-4 `reconcile: report` — read-only drift report.  Reads the live
    /// actual, normalizes it into the desired key space, and reports the drift
    /// verdict against last-known-desired (`spec.known_desired`, supplied by the
    /// caller's query adapter).  Makes **no** cloud mutation and writes **no**
    /// ownership fact — a `report` run is always safe.  Without `auth:` the live
    /// actual can't be read, so the drift is `undetermined` (still no mutation).
    #[allow(clippy::too_many_arguments)]
    async fn run_report(
        &self,
        action: &str,
        plan: &ActionPlan,
        spec: &ProviderSpec,
        config: &ToolConfig,
        ctx: &ExecutionContext,
        backend: &str,
        start: std::time::Instant,
    ) -> Result<ToolResult, ToolError> {
        let urn = urn_for_apply(&plan.apply).unwrap_or_default();
        let desired = spec
            .known_desired
            .as_ref()
            .map(|d| canonicalize_desired(&plan.apply, d));

        // No auth → we cannot read live actual.  Return a safe, mutation-free
        // "undetermined" report rather than erroring: a plain run never fails.
        let Some(auth_config) = config.auth.as_ref() else {
            let data = serde_json::json!({
                "provider": "google",
                "action": action,
                "reconcile": "report",
                "backend": backend,
                "converge": apply_strategy_label(&plan.apply),
                "urn": urn,
                "drift": "undetermined",
                "changed": false,
                "desired": desired,
                "note": "no `auth:` supplied — cannot read live actual to detect drift. \
                         Add an `auth:` block to compute the real desired-vs-actual report. \
                         (report never mutates cloud state regardless.)",
            });
            return Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64));
        };

        let creds = self.auth_resolver.resolve(auth_config, ctx).await?;
        let resolved = self.resolve_actual(action, &plan.apply, &creds).await?;
        let actual_opt = if resolved.present {
            Some(&resolved.actual)
        } else {
            None
        };
        let drift = state::drift_between(desired.as_ref(), actual_opt);

        let data = serde_json::json!({
            "provider": "google",
            "action": action,
            "reconcile": "report",
            "backend": backend,
            "converge": apply_strategy_label(&plan.apply),
            "urn": urn,
            // The verdict (in_sync / modified{fields} / missing / untracked / not_managed).
            "drift": drift,
            // Field-by-field desired-vs-actual (empty when in sync / untracked).
            "diff": adopt_diff_echo(desired.as_ref(), &resolved.actual),
            "desired": desired,
            "actual": resolved.actual,
            "present": resolved.present,
            // A report NEVER mutates and NEVER writes an ownership fact.
            "changed": false,
        });
        Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64))
    }

    /// Round-4 `reconcile: adopt` — accept the live actual as the NEW desired.
    ///
    /// Confirm-gated exactly like [`ProviderTool::run_destroy`]: the diff +
    /// digest are resolved against live state (so `adopt` needs `auth:` even to
    /// plan), a dry-run emits the field-by-field diff + a `plan_digest`, and an
    /// apply requires `confirm:<digest>`.  A blind apply (no confirm) and a stale
    /// digest (the resource drifted again since review) are both refused — this
    /// is the guard that stops an accidental / malicious out-of-band change from
    /// being laundered into "desired" without human review.
    ///
    /// **Adopt makes no cloud mutation.**  Structurally: this method only ever
    /// calls [`ProviderTool::resolve_actual`], which issues reads
    /// (GET / getIamPolicy) — it has no path to a create / update / delete /
    /// setIamPolicy.  On apply it writes only the `provider_fact` (state), never
    /// the cloud.
    #[allow(clippy::too_many_arguments)]
    async fn run_adopt(
        &self,
        action: &str,
        plan: &ActionPlan,
        spec: &ProviderSpec,
        config: &ToolConfig,
        ctx: &ExecutionContext,
        backend: &str,
        start: std::time::Instant,
    ) -> Result<ToolResult, ToolError> {
        let stack = spec.stack.as_deref().unwrap_or("<unscoped>");
        let (service, resource_type) = service_and_type(&plan.apply);
        let desired = spec
            .known_desired
            .as_ref()
            .map(|d| canonicalize_desired(&plan.apply, d));
        let import = desired.is_none();

        // Adopt needs auth even to plan — the diff + digest are resolved against
        // live actual so a stale confirmation (drifted again) can be refused.
        let auth_config = config.auth.as_ref().ok_or_else(|| {
            ToolError::Configuration(format!(
                "adopt action {action:?} requires an explicit `auth:` block even to plan: the \
                 adopt diff is resolved against live cloud state so a stale confirmation can be \
                 refused. Add an `auth:` block (dry_run:true still performs no mutation — only \
                 the read needed to resolve the live actual)."
            ))
        })?;
        let creds = self.auth_resolver.resolve(auth_config, ctx).await?;
        let resolved = self.resolve_actual(action, &plan.apply, &creds).await?;
        let urn = resolved.urn.clone();
        let digest = adopt_plan_digest(action, &resolved);

        // Nothing to adopt if the resource does not exist live.
        if !resolved.present {
            return Err(ToolError::ExecutionFailed(format!(
                "adopt REFUSED for {urn}: the target resource was not found in live cloud state, \
                 so there is nothing to adopt. Create it (reconcile:enforce) or fix the target. \
                 No state was written."
            )));
        }

        let actual_opt = Some(&resolved.actual);
        let drift = state::drift_between(desired.as_ref(), actual_opt);

        // ---- Adopt dry-run: emit the explicit diff + digest for review. ----
        if spec.dry_run {
            let data = serde_json::json!({
                "provider": "google",
                "action": action,
                "reconcile": "adopt",
                "dry_run": true,
                "changed": false,
                "adopt": true,
                "import": import,
                "backend": backend,
                "converge": apply_strategy_label(&plan.apply),
                "urn": urn,
                "drift": drift,
                "diff": adopt_diff_echo(desired.as_ref(), &resolved.actual),
                "desired": desired,
                "actual": resolved.actual,
                "present": true,
                "plan_digest": digest,
                "confirm_hint": "review the diff above, then re-run with dry_run:false and \
                                 confirm:<plan_digest> to accept this exact reviewed live actual \
                                 as the new desired. No cloud state is ever modified by adopt. If \
                                 the resource changes again before you apply, the digest will no \
                                 longer match and the adopt will be refused.",
                // A dry-run adopt fact is `planned` intent — the fold ignores it,
                // so no ownership moves until a confirmed apply.
                "provider_fact": build_fact(
                    &urn, service, resource_type, "adopt", stack,
                    resolved.actual.clone(), ctx.execution_id, "planned",
                ),
            });
            return Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64));
        }

        // ---- Adopt apply: require a confirm digest bound to THIS live actual. ----
        let confirm = spec.confirm.as_deref().ok_or_else(|| {
            ToolError::Configuration(format!(
                "adopt apply REFUSED for {urn}: no `confirm` digest supplied. An adopt apply must \
                 echo the `plan_digest` from a reviewed dry-run — blind adoptions are not \
                 permitted (they would launder an out-of-band change into desired without human \
                 review). Run dry_run:true first, review the diff, then re-run with \
                 confirm:<plan_digest>. No state was written."
            ))
        })?;
        if confirm != digest {
            return Err(ToolError::ExecutionFailed(format!(
                "adopt apply REFUSED for {urn}: the supplied confirm digest ({confirm}) does not \
                 match the live-resolved actual digest ({digest}). The resource changed again \
                 since the diff was reviewed (or the confirm value is wrong) — re-run \
                 dry_run:true, review the fresh diff, and confirm its new digest. No state was \
                 written."
            )));
        }

        // Digest matched → accept live actual as the new desired.  NO cloud call
        // beyond the read already issued by `resolve_actual`.
        let data = serde_json::json!({
            "provider": "google",
            "action": action,
            "reconcile": "adopt",
            "dry_run": false,
            // No cloud state changed — adopt only rewrites last-known-desired.
            "changed": false,
            "adopt": true,
            "adopted": true,
            "import": import,
            "backend": backend,
            "converge": apply_strategy_label(&plan.apply),
            "urn": urn,
            // The accepted live actual is now the desired.
            "desired": resolved.actual,
            "actual": resolved.actual,
            "present": true,
            "provider_fact": build_fact(
                &urn, service, resource_type, "adopt", stack,
                resolved.actual.clone(), ctx.execution_id, "adopted",
            ),
        });
        Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64))
    }

    /// Resolve the live actual state of an **ensure** resource, read-only, for
    /// `report` / `adopt`.  Issues only reads (GET / getIamPolicy) — it has no
    /// path to a mutation, which is what makes `adopt` provably non-mutating.
    /// The returned `actual` is normalized into the desired key space so it can
    /// be diffed directly against last-known-desired.
    async fn resolve_actual(
        &self,
        action: &str,
        apply: &ApplyKind,
        creds: &crate::auth::AuthCredentials,
    ) -> Result<ResolvedActual, ToolError> {
        let urn = urn_for_apply(apply).unwrap_or_default();
        match apply {
            ApplyKind::EnsureFolder {
                parent,
                display_name,
            } => {
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
                match find_active_folder(&list, display_name) {
                    Some(folder) => Ok(ResolvedActual {
                        urn,
                        present: true,
                        etag: folder
                            .get("etag")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                        state: folder
                            .get("state")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                        actual: normalize_live(apply, &folder),
                    }),
                    None => Ok(ResolvedActual::absent(urn)),
                }
            }
            ApplyKind::EnsureProject { project_id, .. } => {
                let (status, body) = self
                    .send(
                        &PlannedRequest::get(format!(
                            "{}/projects/{project_id}",
                            self.endpoints.crm
                        )),
                        creds,
                    )
                    .await?;
                if status == StatusCode::NOT_FOUND {
                    return Ok(ResolvedActual::absent(urn));
                }
                if !status.is_success() {
                    return Err(ToolError::Http(format!(
                        "google API {} resolving actual for projects/{project_id}: {}",
                        status.as_u16(),
                        redact_sensitive(&body)
                    )));
                }
                Ok(ResolvedActual {
                    urn,
                    present: true,
                    etag: body.get("etag").and_then(|v| v.as_str()).map(String::from),
                    state: body.get("state").and_then(|v| v.as_str()).map(String::from),
                    actual: normalize_live(apply, &body),
                })
            }
            ApplyKind::EnsureService { project, service } => {
                let current = self
                    .execute_request(
                        &PlannedRequest::get(format!(
                            "{}/projects/{project}/services/{service}",
                            self.endpoints.serviceusage
                        )),
                        creds,
                    )
                    .await?;
                let state = current
                    .get("state")
                    .and_then(|v| v.as_str())
                    .unwrap_or("STATE_UNSPECIFIED");
                // A service is "present to adopt" when it is ENABLED (the ensure
                // desired); a DISABLED service has no enabled-state to accept.
                let present = state == "ENABLED";
                Ok(ResolvedActual {
                    urn,
                    present,
                    etag: None,
                    state: Some(state.to_string()),
                    actual: if present {
                        normalize_live(apply, &current)
                    } else {
                        serde_json::json!({})
                    },
                })
            }
            ApplyKind::EnsureBillingLink { project, .. } => {
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
                let present = !linked.is_empty();
                Ok(ResolvedActual {
                    urn,
                    present,
                    etag: None,
                    state: Some(if present { "linked" } else { "unlinked" }.to_string()),
                    actual: if present {
                        normalize_live(apply, &current)
                    } else {
                        serde_json::json!({})
                    },
                })
            }
            ApplyKind::EnsureIamBinding {
                get_url,
                role,
                member,
                ..
            } => {
                let policy = self
                    .execute_request(
                        &PlannedRequest::post(get_url.clone(), Some(serde_json::json!({}))),
                        creds,
                    )
                    .await?;
                let present = binding_present(&policy, role, member);
                Ok(ResolvedActual {
                    urn,
                    present,
                    etag: policy
                        .get("etag")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    state: Some(if present { "bound" } else { "absent" }.to_string()),
                    actual: serde_json::json!({ "role": role, "member": member, "bound": present }),
                })
            }
            // A pure read has no desired/actual to adopt; guarded by the caller.
            ApplyKind::Single | ApplyKind::Destroy(_) => Err(ToolError::Configuration(format!(
                "action {action:?} is not an ensure resource and cannot be reconciled (report / \
                 adopt apply only to mutating ensure actions)"
            ))),
        }
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
        ApplyKind::Destroy(_) => "destroy",
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

/// Return a copy of `policy` with the `{role, member}` grant removed — dropping
/// the member from the role's binding, and dropping the binding entirely if it
/// becomes empty.  Preserves `etag` and every other binding for a minimal,
/// optimistic-concurrency-safe `setIamPolicy`.  The inverse of [`upsert_binding`].
fn remove_binding(policy: &serde_json::Value, role: &str, member: &str) -> serde_json::Value {
    let mut out = policy.clone();
    let Some(obj) = out.as_object_mut() else {
        return out;
    };
    let Some(bindings) = obj.get_mut("bindings").and_then(|b| b.as_array_mut()) else {
        return out;
    };
    for binding in bindings.iter_mut() {
        if binding.get("role").and_then(|v| v.as_str()) == Some(role) {
            if let Some(members) = binding.get_mut("members").and_then(|m| m.as_array_mut()) {
                members.retain(|m| m.as_str() != Some(member));
            }
        }
    }
    // Drop any binding whose members list is now empty.
    bindings.retain(|b| {
        b.get("members")
            .and_then(|m| m.as_array())
            .map(|m| !m.is_empty())
            .unwrap_or(true)
    });
    out
}

/// All ACTIVE folders in a `folders.list` response matching `display_name`.
/// A folder with no `state` field is treated as ACTIVE (some API projections
/// omit it).  Used by destroy resolution to detect an ambiguous (>1) match.
fn active_folders_named(list: &serde_json::Value, display_name: &str) -> Vec<serde_json::Value> {
    list.get("folders")
        .and_then(|f| f.as_array())
        .map(|folders| {
            folders
                .iter()
                .filter(|f| {
                    let name_matches =
                        f.get("displayName").and_then(|v| v.as_str()) == Some(display_name);
                    let active = match f.get("state").and_then(|v| v.as_str()) {
                        Some(s) => s == "ACTIVE",
                        None => true,
                    };
                    name_matches && active
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// Build the secret-free ownership fact embedded in a converge result and later
/// folded by the EHDB ownership projection (Fork 1).  `desired` is already
/// redacted; the fact carries only resource identity + desired spec + outcome.
#[allow(clippy::too_many_arguments)]
fn build_fact(
    urn: &str,
    service: &str,
    resource_type: &str,
    verb: &str,
    stack: &str,
    desired: serde_json::Value,
    execution_id: i64,
    outcome: &str,
) -> serde_json::Value {
    serde_json::json!({
        "urn": urn,
        "provider": "google",
        "service": service,
        "resource_type": resource_type,
        "verb": verb,
        "stack": stack,
        "desired": desired,
        "execution_id": execution_id,
        "outcome": outcome,
    })
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

/// Normalize a resource reference to the `<prefix><id>` form, accepting either a
/// bare id or an already-prefixed value (`561323743912` or
/// `organizations/561323743912` → `organizations/561323743912`).
fn normalize_resource(v: &str, prefix: &str) -> String {
    let v = v.trim();
    if v.starts_with(prefix) {
        v.to_string()
    } else {
        format!("{prefix}{v}")
    }
}

/// First path segment following `prefix` in `s`, if present (stops at `/` or
/// `:`).  `organizations/561323743912:getIamPolicy` → `561323743912`.
fn segment_after(s: &str, prefix: &str) -> Option<String> {
    let idx = s.find(prefix)?;
    let rest = &s[idx + prefix.len()..];
    let seg = rest.split(['/', ':']).next().unwrap_or("").trim();
    if seg.is_empty() {
        None
    } else {
        Some(seg.to_string())
    }
}

/// Structural (offline, no-network) pass of the wrong-target guard.  Scans an
/// action's input for `organizations/<id>` and `billingAccounts/<id>` references
/// and refuses if any disagrees with the pinned guard values.  Returns `Ok(())`
/// when there is no guard or every reference matches.  Because it needs no
/// network, it fires in dry-run and before any credential is minted.
fn enforce_guard_offline(
    guard: Option<&GuardSpec>,
    action: &str,
    input: &serde_json::Value,
) -> Result<(), ToolError> {
    let Some(g) = guard else {
        return Ok(());
    };

    // Candidate resource strings: every top-level string input value, plus the
    // normalized `org_id` form (a bare `org_id` becomes `organizations/<id>`).
    let mut refs: Vec<String> = Vec::new();
    if let Some(obj) = input.as_object() {
        for v in obj.values() {
            if let serde_json::Value::String(s) = v {
                if !s.is_empty() {
                    refs.push(s.clone());
                }
            }
        }
    }
    if let Some(org) = org_from_input(input) {
        refs.push(org);
    }

    if let Some(want) = g.require_org.as_deref() {
        let want_id = want.trim().trim_start_matches("organizations/");
        for r in &refs {
            if let Some(id) = segment_after(r, "organizations/") {
                if id != want_id {
                    return Err(ToolError::Configuration(format!(
                        "org guard REFUSED: action {action:?} targets \
                         organizations/{id} but require_org pins \
                         organizations/{want_id}; no request built, no network call"
                    )));
                }
            }
        }
    }

    if let Some(want) = g.require_billing_account.as_deref() {
        let want_id = want.trim().trim_start_matches("billingAccounts/");
        for r in &refs {
            if let Some(id) = segment_after(r, "billingAccounts/") {
                if id != want_id {
                    return Err(ToolError::Configuration(format!(
                        "billing guard REFUSED: action {action:?} targets \
                         billingAccounts/{id} but require_billing_account pins \
                         billingAccounts/{want_id}; no request built"
                    )));
                }
            }
        }
    }

    Ok(())
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

/// Reject a destroy target that carries a wildcard / glob metacharacter.  There
/// are **no** bulk / wildcard deletes — every destroy target is an explicit,
/// literal resource identifier.
fn reject_wildcard(value: &str, field: &str, action: &str) -> Result<(), ToolError> {
    if value.contains('*')
        || value.contains('?')
        || value.contains('[')
        || value.contains(']')
        || value.trim().is_empty()
    {
        return Err(ToolError::Configuration(format!(
            "provider destroy action {action:?} refuses field {field:?} value {value:?}: \
             destroy targets must be explicit literal identifiers — no wildcard, glob, or \
             bulk deletes are permitted."
        )));
    }
    Ok(())
}

/// Resource URN for the ownership projection — a stable identity derived from
/// the *declared* desired identity (not the post-create resource id, which is
/// unknown at plan time).  Same shape for an ensure and its matching delete so
/// the fold links create → drift → orphan → destroy for one resource.
fn urn_for_apply(apply: &ApplyKind) -> Option<String> {
    match apply {
        ApplyKind::Single => None,
        ApplyKind::EnsureFolder {
            parent,
            display_name,
        } => Some(format!(
            "google:cloudresourcemanager:folder:{parent}/{display_name}"
        )),
        ApplyKind::EnsureProject { project_id, .. } => {
            Some(format!("google:cloudresourcemanager:project:{project_id}"))
        }
        ApplyKind::EnsureService { project, service } => {
            Some(format!("google:serviceusage:service:{project}/{service}"))
        }
        ApplyKind::EnsureBillingLink { project, .. } => {
            Some(format!("google:cloudbilling:billing-link:{project}"))
        }
        ApplyKind::EnsureIamBinding {
            get_url,
            role,
            member,
            ..
        } => Some(format!("google:iam:binding:{get_url}/{role}/{member}")),
        ApplyKind::Destroy(kind) => Some(urn_for_destroy(kind)),
    }
}

fn urn_for_destroy(kind: &DestroyKind) -> String {
    match kind {
        DestroyKind::DeleteFolder {
            parent,
            display_name,
        } => format!("google:cloudresourcemanager:folder:{parent}/{display_name}"),
        DestroyKind::DeleteProject { project_id } => {
            format!("google:cloudresourcemanager:project:{project_id}")
        }
        DestroyKind::DisableService { project, service } => {
            format!("google:serviceusage:service:{project}/{service}")
        }
        DestroyKind::RemoveIamBinding {
            resource,
            role,
            member,
            ..
        } => format!("google:iam:binding:{resource}/{role}/{member}"),
    }
}

/// The `(service, resource_type)` pair for a URN, for the ownership fact.
fn service_and_type(apply: &ApplyKind) -> (&'static str, &'static str) {
    match apply {
        ApplyKind::Single => ("", ""),
        ApplyKind::EnsureFolder { .. } => ("cloudresourcemanager", "folder"),
        ApplyKind::EnsureProject { .. } => ("cloudresourcemanager", "project"),
        ApplyKind::EnsureService { .. } => ("serviceusage", "service"),
        ApplyKind::EnsureBillingLink { .. } => ("cloudbilling", "billing-link"),
        ApplyKind::EnsureIamBinding { .. } => ("iam", "binding"),
        ApplyKind::Destroy(kind) => match kind {
            DestroyKind::DeleteFolder { .. } => ("cloudresourcemanager", "folder"),
            DestroyKind::DeleteProject { .. } => ("cloudresourcemanager", "project"),
            DestroyKind::DisableService { .. } => ("serviceusage", "service"),
            DestroyKind::RemoveIamBinding { .. } => ("iam", "binding"),
        },
    }
}

/// One destroy target, resolved against live cloud state.  The digest is
/// computed over this so a `confirm` reviewed at one point in time is refused if
/// live state has drifted (etag / lifecycle-state / presence changed).
#[derive(Debug, Clone)]
struct ResolvedDestroy {
    urn: String,
    /// The real resource id resolved from live state (e.g. `folders/12345`), or
    /// `<absent>` when the resource is already gone.
    resolved_name: String,
    /// Optimistic-concurrency token where the API exposes one (folders /
    /// projects / IAM policy) — changes on any modification.
    etag: Option<String>,
    /// Lifecycle state fingerprint (`ACTIVE` / `ENABLED` / `bound` / …).
    state: Option<String>,
    /// Whether the target currently exists (and would actually be deleted).
    present: bool,
    /// The concrete request(s) apply mode would issue.  For IAM removal this is
    /// re-derived at execute time (read-modify-write); the echo is the write.
    request: PlannedRequest,
}

impl ResolvedDestroy {
    /// The exact, human-reviewable plan echoed in a destroy dry-run.
    fn to_plan_echo(&self) -> serde_json::Value {
        serde_json::json!({
            "urn": self.urn,
            "resolved_name": self.resolved_name,
            "etag": self.etag,
            "state": self.state,
            "present": self.present,
            "request": self.request.to_echo(),
        })
    }
}

/// SHA-256 (hex) over the canonical live-resolved destroy plan.  Every field
/// that would make the deletion "a different deletion than the one reviewed" is
/// folded in: the URN, the resolved resource id, the etag, the lifecycle state,
/// presence, and the request method + url.  A change in any of them yields a
/// different digest, so a stale `confirm` is refused.
fn destroy_plan_digest(action: &str, resolved: &ResolvedDestroy) -> String {
    use sha2::{Digest, Sha256};
    // Fixed-order canonical string (independent of serde_json map ordering).
    let canonical = format!(
        "v1|action={}|urn={}|name={}|etag={}|state={}|present={}|method={}|url={}",
        action,
        resolved.urn,
        resolved.resolved_name,
        resolved.etag.as_deref().unwrap_or(""),
        resolved.state.as_deref().unwrap_or(""),
        resolved.present,
        resolved.request.method.as_str(),
        resolved.request.url,
    );
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// One ensure resource's live actual, resolved read-only for `report` / `adopt`
/// (Round 4).  `actual` is normalized into the desired key space so it diffs
/// directly against last-known-desired.  The digest is computed over this so a
/// `confirm` reviewed at one point in time is refused if the live actual drifted
/// again (a changed field, etag, lifecycle state, or presence flip).
#[derive(Debug, Clone)]
struct ResolvedActual {
    urn: String,
    /// Whether the target currently exists live (and can be adopted).
    present: bool,
    /// Live actual normalized into the desired key space (`{}` when absent).
    actual: serde_json::Value,
    /// Optimistic-concurrency token where the API exposes one — folded into the
    /// digest so a drift the normalized `actual` didn't surface still refuses a
    /// stale confirm.
    etag: Option<String>,
    /// Lifecycle-state fingerprint (`ACTIVE` / `ENABLED` / `bound` / …).
    state: Option<String>,
}

impl ResolvedActual {
    fn absent(urn: String) -> Self {
        Self {
            urn,
            present: false,
            actual: serde_json::json!({}),
            etag: None,
            state: None,
        }
    }
}

/// SHA-256 (hex) over the canonical live-resolved adopt plan.  Every field that
/// would make the adoption "a different adoption than the one reviewed" is
/// folded in: the URN, presence, the normalized actual (canonical JSON — object
/// keys are sorted because `serde_json::Map` is a `BTreeMap` in this build), and
/// the live etag / lifecycle-state fingerprint.  A change in any of them yields a
/// different digest, so a stale `confirm` is refused.
fn adopt_plan_digest(action: &str, resolved: &ResolvedActual) -> String {
    use sha2::{Digest, Sha256};
    let canonical = format!(
        "v1|adopt|action={}|urn={}|present={}|actual={}|etag={}|state={}",
        action,
        resolved.urn,
        resolved.present,
        serde_json::to_string(&resolved.actual).unwrap_or_default(),
        resolved.etag.as_deref().unwrap_or(""),
        resolved.state.as_deref().unwrap_or(""),
    );
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// Field-by-field desired-vs-actual diff echoed in a report / adopt dry-run.
/// Emits one entry per field where the two differ, plus fields that are new in
/// actual (the import case, where there is no prior desired).  `{}` when in sync.
fn adopt_diff_echo(
    desired: Option<&serde_json::Value>,
    actual: &serde_json::Value,
) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    let empty = serde_json::Map::new();
    let desired_obj = desired.and_then(|d| d.as_object()).unwrap_or(&empty);
    let actual_obj = actual.as_object().unwrap_or(&empty);

    // Union of keys, so a field present on only one side still shows.
    let mut keys: std::collections::BTreeSet<&String> = std::collections::BTreeSet::new();
    keys.extend(desired_obj.keys());
    keys.extend(actual_obj.keys());
    for k in keys {
        let want = desired_obj.get(k);
        let got = actual_obj.get(k);
        if want != got {
            out.insert(
                k.clone(),
                serde_json::json!({
                    "desired": want,
                    "actual": got,
                }),
            );
        }
    }
    serde_json::Value::Object(out)
}

/// Normalize a live GCP GET response into the desired key space for a given
/// ensure action, so the live actual diffs directly against last-known-desired.
/// This is the "normalize the live GET" half of the query I/O adapter (the other
/// half — reading the EHDB raw eventlog tier — is [`state::extract_facts_from_eventlog`]).
fn normalize_live(apply: &ApplyKind, live: &serde_json::Value) -> serde_json::Value {
    let s = |k: &str| live.get(k).and_then(|v| v.as_str()).map(String::from);
    match apply {
        ApplyKind::EnsureFolder { .. } => {
            let mut o = serde_json::Map::new();
            if let Some(v) = s("parent") {
                o.insert("parent".into(), v.into());
            }
            if let Some(v) = s("displayName") {
                o.insert("display_name".into(), v.into());
            }
            serde_json::Value::Object(o)
        }
        ApplyKind::EnsureProject { .. } => {
            let mut o = serde_json::Map::new();
            if let Some(v) = s("projectId") {
                o.insert("project_id".into(), v.into());
            }
            // CRM v3 exposes `parent` as a string (`folders/123`).
            if let Some(v) = s("parent") {
                o.insert("parent".into(), v.into());
            }
            if let Some(v) = s("displayName") {
                o.insert("display_name".into(), v.into());
            }
            serde_json::Value::Object(o)
        }
        ApplyKind::EnsureService { .. } => {
            serde_json::json!({ "state": s("state").unwrap_or_else(|| "ENABLED".into()) })
        }
        ApplyKind::EnsureBillingLink { .. } => {
            serde_json::json!({ "billing_account": s("billingAccountName").unwrap_or_default() })
        }
        // IAM actual is synthesized at resolve time; nothing to normalize here.
        ApplyKind::EnsureIamBinding { .. } | ApplyKind::Single | ApplyKind::Destroy(_) => {
            live.clone()
        }
    }
}

/// Canonicalize a last-known-desired object (whatever key spellings the emitting
/// playbook used) into the same snake_case key space [`normalize_live`] targets,
/// so the report / adopt diff compares like against like.  Only the fields that
/// are meaningful for the resource type are carried; unknown keys are dropped.
fn canonicalize_desired(apply: &ApplyKind, desired: &serde_json::Value) -> serde_json::Value {
    // First present of the candidate keys, coerced to string.
    let pick =
        |keys: &[&str]| -> Option<String> { keys.iter().find_map(|k| input_str(desired, k)) };
    match apply {
        ApplyKind::EnsureFolder { .. } => {
            let mut o = serde_json::Map::new();
            if let Some(v) = pick(&["parent"]) {
                o.insert("parent".into(), v.into());
            }
            if let Some(v) = pick(&["display_name", "displayName"]) {
                o.insert("display_name".into(), v.into());
            }
            serde_json::Value::Object(o)
        }
        ApplyKind::EnsureProject { .. } => {
            let mut o = serde_json::Map::new();
            if let Some(v) = pick(&["project_id", "projectId"]) {
                o.insert("project_id".into(), v.into());
            }
            if let Some(v) = pick(&["parent"]) {
                o.insert("parent".into(), v.into());
            }
            if let Some(v) = pick(&["display_name", "displayName"]) {
                o.insert("display_name".into(), v.into());
            }
            serde_json::Value::Object(o)
        }
        ApplyKind::EnsureService { .. } => {
            let mut o = serde_json::Map::new();
            if let Some(v) = pick(&["state"]) {
                o.insert("state".into(), v.into());
            }
            serde_json::Value::Object(o)
        }
        ApplyKind::EnsureBillingLink { .. } => {
            let mut o = serde_json::Map::new();
            if let Some(v) = pick(&[
                "billing_account",
                "billingAccountName",
                "billing_account_name",
            ]) {
                o.insert("billing_account".into(), v.into());
            }
            serde_json::Value::Object(o)
        }
        ApplyKind::EnsureIamBinding { .. } => {
            let mut o = serde_json::Map::new();
            if let Some(v) = pick(&["role"]) {
                o.insert("role".into(), v.into());
            }
            if let Some(v) = pick(&["member"]) {
                o.insert("member".into(), v.into());
            }
            // A known binding is, by definition, bound.
            o.insert("bound".into(), serde_json::Value::Bool(true));
            serde_json::Value::Object(o)
        }
        ApplyKind::Single | ApplyKind::Destroy(_) => desired.clone(),
    }
}

/// Ownership / drift / orphan projection over the provider event stream
/// (Fork 1).  This is the **pure fold** half — it consumes an ordered slice of
/// [`ProviderFact`]s (the shape a converge emits and the EHDB *raw eventlog
/// tier* later surfaces) and answers ownership / drift / orphan questions
/// without any I/O.  The I/O adapter (query the tier, GET live state) lives at
/// the call boundary and is documented in the provider-tool wiki.
///
/// ## Why the raw eventlog tier, not `/api/ehdb/events`
///
/// The #178 projection read-model (`/api/ehdb/events`) is **secret-free by
/// construction** — it deliberately excludes `result` / `context` payload
/// bodies from its DTOs.  A provider fact rides *inside* a step result payload,
/// so folding it requires the **raw eventlog tier** (`/api/ehdb/tiers/eventlog`,
/// worker-served) which carries full records.  This reuses the #178 surface
/// (it is one of its tiers) and keeps the control-plane read-model secret-free
/// — no parallel store, no relaxation of the secret-free guarantee.  See the
/// design note recorded on the umbrella issue.
pub mod state {
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

    /// A provider ownership fact — emitted in a converge result's
    /// `provider_fact` field, and (per the design) surfaced from the EHDB raw
    /// eventlog tier for folding.  Secret-free by construction: it carries only
    /// resource identity + desired spec + outcome, never a credential.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct ProviderFact {
        pub urn: String,
        pub provider: String,
        pub service: String,
        pub resource_type: String,
        /// `ensure` | `delete` | `adopt`.
        pub verb: String,
        pub stack: String,
        /// Declared desired spec (normalized).  Empty for a delete; for an
        /// `adopt` this is the live actual accepted as the new desired.
        #[serde(default)]
        pub desired: serde_json::Value,
        pub execution_id: i64,
        /// `planned` (dry-run intent) | `changed` | `noop` | `deleted` |
        /// `absent` | `adopted` (Round 4 — actual accepted as desired).
        pub outcome: String,
    }

    /// Current ownership record for one URN after folding its fact history.
    #[derive(Debug, Clone, PartialEq)]
    pub struct Owned {
        pub urn: String,
        pub stack: String,
        pub resource_type: String,
        pub last_desired: serde_json::Value,
        pub last_execution_id: i64,
    }

    /// The folded ownership state: what a stack currently owns (live) and what it
    /// has tombstoned (deleted).
    #[derive(Debug, Default, Clone)]
    pub struct OwnershipModel {
        pub owned: BTreeMap<String, Owned>,
        pub tombstoned: BTreeMap<String, Owned>,
    }

    impl OwnershipModel {
        /// Narrow this model to a single ownership stack (Round 5 — scoping fix).
        ///
        /// Ownership facts carry a `stack` label; a fold over a multi-stack fact
        /// stream mixes them.  Orphan detection **must** scope to one stack or it
        /// reports another stack's resources as orphans of the current run.
        /// `<unscoped>` (the default when a step sets no `stack:`) behaves as a
        /// single global stack — `for_stack("<unscoped>")` returns exactly the
        /// unscoped resources.
        ///
        /// Note on contention: the fold is last-writer-wins per URN, so if two
        /// stacks own the same URN with no intervening delete the folded model
        /// attributes it to the most-recent owning stack; `for_stack` follows
        /// that attribution.  Use [`detect_stack_conflicts`] on the fact stream
        /// to surface such contention before it bites.
        pub fn for_stack(&self, stack: &str) -> OwnershipModel {
            let keep = |m: &BTreeMap<String, Owned>| -> BTreeMap<String, Owned> {
                m.iter()
                    .filter(|(_, o)| o.stack == stack)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            OwnershipModel {
                owned: keep(&self.owned),
                tombstoned: keep(&self.tombstoned),
            }
        }

        /// The distinct stacks present in this model's live-owned set.
        pub fn stacks(&self) -> Vec<String> {
            self.owned
                .values()
                .map(|o| o.stack.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect()
        }
    }

    /// Drift verdict for a single owned URN vs its live actual state.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    #[serde(tag = "drift", rename_all = "snake_case")]
    pub enum Drift {
        /// Live actual matches last-known-desired.
        InSync,
        /// We own it (believe it live) but the live GET found nothing.
        Missing,
        /// Desired vs actual differ on these normalized fields.
        Modified { fields: Vec<String> },
        /// A live resource exists that no ownership record covers.
        Untracked,
        /// Neither owned nor live — nothing to reconcile.
        NotManaged,
    }

    /// Fold an ordered (oldest→newest) fact stream into an ownership model.
    /// Only **applied** facts move the live model — a `planned` dry-run fact is
    /// intent, not ownership.  A terminal `delete`/`absent` tombstones the URN.
    pub fn fold_facts(facts: &[ProviderFact]) -> OwnershipModel {
        let mut model = OwnershipModel::default();
        for f in facts {
            match (f.verb.as_str(), f.outcome.as_str()) {
                // An applied ensure asserts (or refreshes) live ownership.  An
                // applied `adopt` does the same — accepting the live actual as
                // desired — and so covers BOTH cases: refreshing last-known-
                // desired for an already-owned URN, and importing a
                // previously-untracked one (`Untracked → Owned`).
                ("ensure", "changed") | ("ensure", "noop") | ("adopt", "adopted") => {
                    model.tombstoned.remove(&f.urn);
                    model.owned.insert(
                        f.urn.clone(),
                        Owned {
                            urn: f.urn.clone(),
                            stack: f.stack.clone(),
                            resource_type: f.resource_type.clone(),
                            last_desired: f.desired.clone(),
                            last_execution_id: f.execution_id,
                        },
                    );
                }
                // An applied delete (or a confirmed already-absent) tombstones it.
                ("delete", "deleted") | ("delete", "absent") => {
                    let prior = model.owned.remove(&f.urn);
                    let rec = prior.unwrap_or(Owned {
                        urn: f.urn.clone(),
                        stack: f.stack.clone(),
                        resource_type: f.resource_type.clone(),
                        last_desired: serde_json::Value::Null,
                        last_execution_id: f.execution_id,
                    });
                    model.tombstoned.insert(f.urn.clone(), rec);
                }
                // planned / other outcomes are intent only — no live-model change.
                _ => {}
            }
        }
        model
    }

    /// Compute drift for one URN.  `live_actual` is the live GET already
    /// **normalized into the desired key space** by the adapter (the GCP→desired
    /// field mapping is the adapter's job; this fold stays pure and generic).
    pub fn compute_drift(
        model: &OwnershipModel,
        urn: &str,
        live_actual: Option<&serde_json::Value>,
    ) -> Drift {
        drift_between(model.owned.get(urn).map(|o| &o.last_desired), live_actual)
    }

    /// The drift primitive shared by the fold's [`compute_drift`] and the tool's
    /// `report` / `adopt` paths.  Compares a last-known-desired object against a
    /// live actual (both already normalized into the same key space):
    ///
    /// - both present → `InSync`, or `Modified { fields }` naming every desired
    ///   key whose actual value differs;
    /// - desired present, actual absent → `Missing` (we own it, it's gone);
    /// - desired absent, actual present → `Untracked` (live, no ownership — the
    ///   adopt-import case);
    /// - neither → `NotManaged`.
    pub fn drift_between(
        desired: Option<&serde_json::Value>,
        actual: Option<&serde_json::Value>,
    ) -> Drift {
        match (desired, actual) {
            (Some(desired), Some(actual)) => {
                let mut diffs = Vec::new();
                if let Some(obj) = desired.as_object() {
                    for (k, want) in obj {
                        if actual.get(k) != Some(want) {
                            diffs.push(k.clone());
                        }
                    }
                }
                if diffs.is_empty() {
                    Drift::InSync
                } else {
                    diffs.sort();
                    Drift::Modified { fields: diffs }
                }
            }
            (Some(_), None) => Drift::Missing,
            (None, Some(_)) => Drift::Untracked,
            (None, None) => Drift::NotManaged,
        }
    }

    /// URNs the model owns (live) that are **not** in the currently-declared set
    /// → orphaned (declared in a prior run, no longer declared).  `declared_urns`
    /// is the union of URNs the current stack converge run declares.
    pub fn detect_orphans(model: &OwnershipModel, declared_urns: &[String]) -> Vec<Owned> {
        let declared: std::collections::BTreeSet<&String> = declared_urns.iter().collect();
        model
            .owned
            .values()
            .filter(|o| !declared.contains(&o.urn))
            .cloned()
            .collect()
    }

    /// Extract provider ownership facts from a slice of raw EHDB eventlog-tier
    /// records (oldest→newest).  This is the pure half of the query I/O adapter
    /// (Round 4): the caller fetches the raw eventlog tier
    /// (`/api/ehdb/tiers/eventlog`, which carries full result payloads — unlike
    /// the secret-free `/api/ehdb/events` projection that deliberately drops
    /// them), and this fold pulls each step result's `provider_fact` out.  The
    /// HTTP fetch itself is server-dependent and lives in the worker / CLI
    /// adapter (out of scope for this pure crate); feeding the fetched records
    /// here yields the same fact stream a converge emits.
    ///
    /// A `provider_fact` rides *inside* a step result payload, so the common
    /// nesting shapes are probed: `result.data.provider_fact`,
    /// `data.provider_fact`, and a bare top-level `provider_fact`.
    pub fn extract_facts_from_eventlog(records: &[serde_json::Value]) -> Vec<ProviderFact> {
        records.iter().filter_map(fact_in_record).collect()
    }

    /// Convenience: extract + fold raw eventlog-tier records into an ownership
    /// model in one call.
    pub fn fold_eventlog(records: &[serde_json::Value]) -> OwnershipModel {
        fold_facts(&extract_facts_from_eventlog(records))
    }

    /// Extract facts from **wire-shape** EHDB eventlog-tier records — the
    /// `EventLogRecordView` rows served by `GET /api/ehdb/tiers/eventlog`
    /// (Round 5 — the query I/O adapter's server-facing half).
    ///
    /// Two things differ from [`extract_facts_from_eventlog`], and both bite in
    /// production:
    ///
    /// 1. **The event body is a JSON *string* in `payload`.**  The tier row is
    ///    `{ global_sequence, execution_id, transaction_id, byte_len, payload }`
    ///    where `payload` is the caller's whole event body serialized to a
    ///    string.  This unwraps and re-parses it before probing.
    /// 2. **The real nesting is deeper than the fixed paths.**  A `call.done`
    ///    event nests the tool result as
    ///    `context.result.context.data.provider_fact` (worker
    ///    `emit_call_done` → `build_call_done_result` → `ToolResult.data`), which
    ///    the fixed [`fact_in_record`] paths do **not** reach.  So this falls
    ///    back to a bounded recursive search for a `provider_fact` object that
    ///    deserializes to a strict [`ProviderFact`] — robust to the exact
    ///    envelope nesting (and to future envelope changes) without guessing a
    ///    path.  A stray key merely *named* `provider_fact` that isn't a real
    ///    fact is rejected because it fails the strict deserialize.
    ///
    /// Feeding already-decoded records (a bare body, or the shapes
    /// [`extract_facts_from_eventlog`] handles) still works — the payload-string
    /// unwrap is best-effort and the fixed-path probe runs first.
    pub fn extract_facts_from_tier_records(records: &[serde_json::Value]) -> Vec<ProviderFact> {
        records.iter().filter_map(fact_in_tier_record).collect()
    }

    /// Convenience: extract + fold wire-shape tier records into an ownership
    /// model in one call.
    pub fn fold_tier_records(records: &[serde_json::Value]) -> OwnershipModel {
        fold_facts(&extract_facts_from_tier_records(records))
    }

    /// Orphan detection **scoped to one stack** (Round 5 — scoping fix).
    ///
    /// The correct multi-stack shape: owned-by-`stack` URNs that the current run
    /// no longer declares.  Plain [`detect_orphans`] over a multi-stack model
    /// would report *another* stack's resources as orphans of this run, then a
    /// `--prune` built on that would delete live infra another stack owns.  This
    /// narrows to `stack` first (see [`OwnershipModel::for_stack`]).
    pub fn detect_orphans_scoped(
        model: &OwnershipModel,
        stack: &str,
        declared_urns: &[String],
    ) -> Vec<Owned> {
        detect_orphans(&model.for_stack(stack), declared_urns)
    }

    /// A URN asserted-owned by more than one stack with no intervening delete —
    /// two stacks are managing the same resource and will fight over it (each
    /// `enforce` reconciles it to its own desired; each stack-scoped orphan scan
    /// sees only its own view).  Surfaced as a warning so an operator can split
    /// ownership before the conflict bites.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct StackConflict {
        pub urn: String,
        /// The distinct stacks observed contending for this URN (sorted).
        pub stacks: Vec<String>,
    }

    /// Detect cross-stack ownership contention from an ordered (oldest→newest)
    /// fact stream (Round 5).  A conflict is recorded when an owning fact from
    /// stack X lands while the URN is already owned by stack Y ≠ X **without a
    /// tombstoning delete in between** — so a legitimate ownership *transfer*
    /// (stack A deletes, stack B adopts) is **not** flagged, only genuine
    /// simultaneous ownership.  A single-stack (or all-`<unscoped>`) stream
    /// yields no conflicts.
    pub fn detect_stack_conflicts(facts: &[ProviderFact]) -> Vec<StackConflict> {
        use std::collections::{BTreeMap, BTreeSet};
        let mut current: BTreeMap<String, String> = BTreeMap::new();
        let mut contended: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for f in facts {
            match (f.verb.as_str(), f.outcome.as_str()) {
                ("ensure", "changed") | ("ensure", "noop") | ("adopt", "adopted") => {
                    if let Some(prev) = current.get(&f.urn) {
                        if prev != &f.stack {
                            let set = contended.entry(f.urn.clone()).or_default();
                            set.insert(prev.clone());
                            set.insert(f.stack.clone());
                        }
                    }
                    current.insert(f.urn.clone(), f.stack.clone());
                }
                ("delete", "deleted") | ("delete", "absent") => {
                    current.remove(&f.urn);
                }
                _ => {}
            }
        }
        contended
            .into_iter()
            .map(|(urn, stacks)| StackConflict {
                urn,
                stacks: stacks.into_iter().collect(),
            })
            .collect()
    }

    fn fact_in_record(record: &serde_json::Value) -> Option<ProviderFact> {
        const PATHS: [&[&str]; 3] = [
            &["result", "data", "provider_fact"],
            &["data", "provider_fact"],
            &["provider_fact"],
        ];
        for path in PATHS {
            let mut cur = record;
            let mut ok = true;
            for key in path {
                match cur.get(key) {
                    Some(v) => cur = v,
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                if let Ok(fact) = serde_json::from_value::<ProviderFact>(cur.clone()) {
                    return Some(fact);
                }
            }
        }
        None
    }

    /// Extract a fact from one wire-shape tier record: unwrap the `payload`
    /// JSON string when present, then probe (fixed paths first, recursive search
    /// as the fallback for the real deeper envelope nesting).
    fn fact_in_tier_record(record: &serde_json::Value) -> Option<ProviderFact> {
        if let Some(body) = record
            .get("payload")
            .and_then(|v| v.as_str())
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        {
            if let Some(fact) = fact_in_record(&body).or_else(|| fact_in_value_recursive(&body, 0))
            {
                return Some(fact);
            }
        }
        fact_in_record(record).or_else(|| fact_in_value_recursive(record, 0))
    }

    /// Bounded recursive search for a `provider_fact` object anywhere in a value.
    /// A match must deserialize to the strict [`ProviderFact`] (all required
    /// fields present), so a stray key merely named `provider_fact` is rejected.
    /// Depth-bounded to avoid pathological nesting; the real `call.done`
    /// envelope reaches it at `context.result.context.data.provider_fact`.
    fn fact_in_value_recursive(value: &serde_json::Value, depth: usize) -> Option<ProviderFact> {
        const MAX_DEPTH: usize = 12;
        if depth > MAX_DEPTH {
            return None;
        }
        match value {
            serde_json::Value::Object(map) => {
                if let Some(pf) = map.get("provider_fact") {
                    if let Ok(fact) = serde_json::from_value::<ProviderFact>(pf.clone()) {
                        return Some(fact);
                    }
                }
                map.values()
                    .find_map(|v| fact_in_value_recursive(v, depth + 1))
            }
            serde_json::Value::Array(arr) => arr
                .iter()
                .find_map(|v| fact_in_value_recursive(v, depth + 1)),
            _ => None,
        }
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
            "reconcile": "enforce",
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

    // ---- wrong-target guard (Stage-1 safety) ----

    #[tokio::test]
    async fn guard_wrong_org_id_refused_offline() {
        // The acting identity can see three orgs; a wrong ORG_ID must be refused
        // structurally, before any request is built (dry-run → no network).
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        for wrong in ["103794563683", "712221118891"] {
            let cfg = spec_config(serde_json::json!({
                "provider": "google",
                "action": "google.cloudresourcemanager.folders.ensure",
                "dry_run": true,
                "reconcile": "enforce",
                "guard": { "require_org": "organizations/561323743912",
                           "require_org_display_name": "shastaratech-org" },
                "input": { "parent": format!("organizations/{wrong}"), "display_name": "20-media" }
            }));
            let err = tool.execute(&cfg, &ctx).await.unwrap_err();
            match err {
                ToolError::Configuration(msg) => {
                    assert!(msg.contains("org guard REFUSED"), "names the guard: {msg}");
                    assert!(msg.contains(wrong), "names the wrong org {wrong}: {msg}");
                    assert!(msg.contains("561323743912"), "names the pinned org: {msg}");
                }
                other => panic!("expected Configuration refusal, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn guard_correct_org_id_passes_offline() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.folders.ensure",
            "dry_run": true,
            "reconcile": "enforce",
            "guard": { "require_org": "organizations/561323743912" },
            "input": { "parent": "organizations/561323743912", "display_name": "20-media" }
        }));
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["dry_run"], serde_json::json!(true));
        assert_eq!(data["would_call"]["method"], serde_json::json!("POST"));
    }

    #[tokio::test]
    async fn guard_wrong_billing_account_refused_offline() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudbilling.projects.link",
            "dry_run": true,
            "reconcile": "enforce",
            "guard": { "require_org": "organizations/561323743912",
                       "require_billing_account": "0153F3-73E360-BD0B38" },
            "input": { "project_id": "shastaratech-youtube-prod",
                       "billing_account": "billingAccounts/AAAAAA-BBBBBB-CCCCCC" }
        }));
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        match err {
            ToolError::Configuration(msg) => {
                assert!(
                    msg.contains("billing guard REFUSED"),
                    "names the guard: {msg}"
                );
                assert!(
                    msg.contains("AAAAAA-BBBBBB-CCCCCC"),
                    "names the wrong account: {msg}"
                );
            }
            other => panic!("expected Configuration refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn guard_absent_is_noop() {
        // No guard block → unchanged behavior (any org planned).
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.folders.ensure",
            "dry_run": true,
            "reconcile": "enforce",
            "input": { "parent": "organizations/103794563683", "display_name": "x" }
        }));
        assert!(tool.execute(&cfg, &ctx).await.is_ok());
    }

    #[tokio::test]
    async fn organizations_get_builds_read_only_url() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.organizations.get",
            "dry_run": true,
            "input": { "organization": "organizations/561323743912" }
        }));
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["would_call"]["method"], serde_json::json!("GET"));
        assert_eq!(
            data["would_call"]["url"],
            serde_json::json!(
                "https://cloudresourcemanager.googleapis.com/v3/organizations/561323743912"
            )
        );
    }

    #[tokio::test]
    async fn guard_live_display_name_mismatch_refused() {
        // Belt-and-braces: even if the pinned numeric ORG_ID passes the offline
        // check, a live GET whose displayName doesn't match is refused before any
        // mutating call.  Routed to a mock via the endpoint override.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/organizations/561323743912"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "organizations/561323743912", "displayName": "some-other-org"
            })))
            .mount(&server)
            .await;

        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let mut cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.folders.ensure",
            "dry_run": false,
            "reconcile": "enforce",
            "endpoint": server.uri(),
            "guard": { "require_org": "organizations/561323743912",
                       "require_org_display_name": "shastaratech-org" },
            "input": { "parent": "organizations/561323743912", "display_name": "20-media" }
        }));
        cfg.auth = Some(gcp_auth());
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        match err {
            ToolError::Configuration(msg) => {
                assert!(
                    msg.contains("org guard REFUSED (live)"),
                    "live refusal: {msg}"
                );
                assert!(
                    msg.contains("some-other-org"),
                    "names actual displayName: {msg}"
                );
            }
            other => panic!("expected live guard refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn guard_live_display_name_match_proceeds() {
        // Matching displayName → the live guard passes and the folders.ensure
        // converge proceeds against the mock.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/organizations/561323743912"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "organizations/561323743912", "displayName": "shastaratech-org"
            })))
            .mount(&server)
            .await;
        // folders list (converge GET-first): report the folder already present so
        // the ensure is a no-op and no create is attempted.
        Mock::given(method("GET"))
            .and(path("/v3/folders"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "folders": [ { "name": "folders/111", "displayName": "20-media", "state": "ACTIVE" } ]
            })))
            .mount(&server)
            .await;

        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let mut cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.folders.ensure",
            "dry_run": false,
            "reconcile": "enforce",
            "endpoint": server.uri(),
            "guard": { "require_org": "organizations/561323743912",
                       "require_org_display_name": "shastaratech-org" },
            "input": { "parent": "organizations/561323743912", "display_name": "20-media" }
        }));
        cfg.auth = Some(gcp_auth());
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(
            data["changed"],
            serde_json::json!(false),
            "already-present → no-op"
        );
    }

    #[test]
    fn enforce_guard_offline_unit() {
        let g = GuardSpec {
            require_org: Some("organizations/561323743912".into()),
            require_org_display_name: None,
            require_billing_account: Some("0153F3-73E360-BD0B38".into()),
        };
        // matching org via bare org_id form
        assert!(enforce_guard_offline(
            Some(&g),
            "google.cloudresourcemanager.folders.ensure",
            &serde_json::json!({ "org_id": "561323743912", "display_name": "x" })
        )
        .is_ok());
        // wrong org via resource form
        assert!(enforce_guard_offline(
            Some(&g),
            "google.cloudresourcemanager.organizations.iam.ensure_binding",
            &serde_json::json!({ "resource": "organizations/999", "role": "r", "member": "m" })
        )
        .is_err());
        // no guard → ok
        assert!(enforce_guard_offline(None, "x", &serde_json::json!({})).is_ok());
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
            "reconcile": "enforce",
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
        assert!(
            matches!(err, ToolError::Configuration(ref m) if m.contains("not yet implemented"))
        );
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
            matches!(err, ToolError::Configuration(ref m) if m.contains("unknown google provider action"))
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
            // Round 4: converge is now the explicit `enforce` policy (the default
            // `report` changes nothing).  These tests exercise the enforce path.
            "reconcile": "enforce",
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

    // ================= Round 3 =================

    // ---- endpoint-override knob: apply routes to a mock, no live cloud ----

    #[test]
    fn endpoint_override_deserializes_base_and_per_service() {
        let base: ProviderSpec = serde_json::from_value(serde_json::json!({
            "provider": "google", "action": "x", "endpoint": "http://127.0.0.1:8089",
        }))
        .unwrap();
        let ep = ApiEndpoints::resolve(base.endpoint.as_ref());
        assert_eq!(ep.crm, "http://127.0.0.1:8089/v3");
        assert_eq!(ep.billing, "http://127.0.0.1:8089/v1");
        assert_eq!(ep.serviceusage, "http://127.0.0.1:8089/v1");

        let per: ProviderSpec = serde_json::from_value(serde_json::json!({
            "provider": "google", "action": "x",
            "endpoint": { "crm": "http://mock/crm" },
        }))
        .unwrap();
        let ep = ApiEndpoints::resolve(per.endpoint.as_ref());
        assert_eq!(ep.crm, "http://mock/crm");
        // Unset services fall back to the real Google host.
        assert_eq!(ep.billing, BILLING_V1);
    }

    #[tokio::test]
    async fn endpoint_override_routes_apply_to_mock_server() {
        // Prove the *config-level* endpoint override reroutes a real
        // `ProviderTool::new()` (defaulted to Google hosts) onto a mock — the
        // path a `noetl exec --runtime local` offline validation exercises.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/projects/p/services/youtube.googleapis.com"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "projects/p/services/youtube.googleapis.com", "state": "ENABLED"
            })))
            .mount(&server)
            .await;

        // Note: tool built with the REAL default endpoints; only the spec's
        // `endpoint` override points it at the mock.
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let mut cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.serviceusage.services.enable",
            "dry_run": false,
            "reconcile": "enforce",
            "endpoint": server.uri(),
            "input": { "project_id": "p", "service_name": "youtube.googleapis.com" }
        }));
        cfg.auth = Some(gcp_auth());
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(
            data["changed"],
            serde_json::json!(false),
            "already-ENABLED service converges to a no-op — proving the override routed to the mock"
        );
    }

    // ---- ownership fact emission (Fork 1) ----

    #[tokio::test]
    async fn ensure_dry_run_emits_ownership_fact() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.projects.ensure",
            "dry_run": true,
            "reconcile": "enforce",
            "stack": "shastaratech-prod",
            "input": { "project_id": "st-prod", "parent": "folders/20" }
        }));
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        let fact = &data["provider_fact"];
        assert_eq!(
            fact["urn"],
            serde_json::json!("google:cloudresourcemanager:project:st-prod")
        );
        assert_eq!(fact["verb"], serde_json::json!("ensure"));
        assert_eq!(fact["stack"], serde_json::json!("shastaratech-prod"));
        assert_eq!(fact["outcome"], serde_json::json!("planned"));
        assert_eq!(fact["resource_type"], serde_json::json!("project"));
    }

    #[tokio::test]
    async fn read_action_emits_no_ownership_fact() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.folders.list",
            "dry_run": true,
            "input": { "parent": "organizations/1" }
        }));
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert!(
            data.get("provider_fact").is_none(),
            "a pure read asserts no ownership"
        );
    }

    // ---- destroy: no-wildcard, auth-required-to-plan ----

    #[tokio::test]
    async fn destroy_rejects_wildcard_target() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let mut cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.projects.delete",
            "dry_run": true,
            "input": { "project_id": "st-*" }
        }));
        cfg.auth = Some(gcp_auth());
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        assert!(
            matches!(err, ToolError::Configuration(ref m) if m.contains("wildcard")),
            "wildcard destroy target must be refused: {err:?}"
        );
    }

    #[tokio::test]
    async fn destroy_plan_requires_auth() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        // dry_run:true destroy with NO auth → refused (plan must resolve live).
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.projects.delete",
            "dry_run": true,
            "input": { "project_id": "st-prod" }
        }));
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        assert!(
            matches!(err, ToolError::Configuration(ref m)
                if m.contains("requires an explicit `auth:`") && m.contains("stale")),
            "destroy plan without auth must be refused (no live resolution possible): {err:?}"
        );
    }

    // ---- destroy happy path: resolve → digest → confirm → delete (LRO) ----

    fn destroy_cfg(action: &str, input: serde_json::Value, extra: serde_json::Value) -> ToolConfig {
        let mut body = serde_json::json!({
            "provider": "google",
            "action": action,
            "input": input,
            "poll": { "max_attempts": 3, "interval_ms": 5, "max_wait_secs": 2 },
        });
        // Merge dry_run / confirm / endpoint overrides.
        for (k, v) in extra.as_object().unwrap() {
            body[k.as_str()] = v.clone();
        }
        let mut cfg = spec_config(body);
        cfg.auth = Some(gcp_auth());
        cfg
    }

    #[tokio::test]
    async fn destroy_folder_dryrun_then_confirmed_apply_deletes() {
        // dry-run and apply hit the SAME endpoint (as in production), so the
        // digest is stable across the review→apply gap when live state is
        // unchanged.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/folders"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "folders": [
                    { "name": "folders/99", "displayName": "20-media", "state": "ACTIVE", "etag": "ETAG-1" }
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/v3/folders/99"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "operations/folder-del", "done": false
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v3/operations/folder-del"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "operations/folder-del", "done": true,
                "response": { "name": "folders/99", "state": "DELETE_REQUESTED" }
            })))
            .mount(&server)
            .await;

        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();

        // 1) dry-run → explicit plan + digest.
        let plan_cfg = destroy_cfg(
            "google.cloudresourcemanager.folders.delete",
            serde_json::json!({ "parent": "organizations/1", "display_name": "20-media" }),
            serde_json::json!({ "dry_run": true }),
        );
        let plan = tool.execute(&plan_cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(plan["destroy"], serde_json::json!(true));
        assert_eq!(plan["present"], serde_json::json!(true));
        assert_eq!(
            plan["plan"]["resolved_name"],
            serde_json::json!("folders/99")
        );
        assert_eq!(
            plan["plan"]["request"]["method"],
            serde_json::json!("DELETE")
        );
        let digest = plan["plan_digest"].as_str().unwrap().to_string();

        // 2) apply with the reviewed digest → DELETE + LRO poll to done.
        let apply_cfg = destroy_cfg(
            "google.cloudresourcemanager.folders.delete",
            serde_json::json!({ "parent": "organizations/1", "display_name": "20-media" }),
            serde_json::json!({ "dry_run": false, "confirm": digest }),
        );
        let out = tool.execute(&apply_cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(out["changed"], serde_json::json!(true));
        assert_eq!(
            out["provider_fact"]["outcome"],
            serde_json::json!("deleted")
        );
    }

    // ---- CORE SAFETY: blind apply (no confirm) is refused ----

    #[tokio::test]
    async fn destroy_blind_apply_without_confirm_is_refused() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/st-prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "projects/st-prod", "projectId": "st-prod", "state": "ACTIVE", "etag": "E1"
            })))
            .mount(&server)
            .await;
        // No DELETE mock — if a deletion were attempted it would 404.
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = destroy_cfg(
            "google.cloudresourcemanager.projects.delete",
            serde_json::json!({ "project_id": "st-prod" }),
            serde_json::json!({ "dry_run": false }), // no confirm
        );
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        assert!(
            matches!(err, ToolError::Configuration(ref m)
                if m.contains("no `confirm`") && m.contains("blind applies are")),
            "a destroy apply with no confirm must be refused: {err:?}"
        );
    }

    // ---- CORE SAFETY: stale / mismatched confirm digest is refused ----

    #[tokio::test]
    async fn destroy_stale_confirm_digest_is_refused() {
        // The human reviewed a plan when the project etag was E1; by apply time
        // the cloud drifted (etag now E2) → the live digest differs → refuse.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/st-prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "projects/st-prod", "projectId": "st-prod", "state": "ACTIVE", "etag": "E2"
            })))
            .mount(&server)
            .await;
        // No DELETE mock — proving no deletion is attempted.
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();

        // A digest computed against the *old* (E1) state — what the human echoed.
        let stale_resolved = ResolvedDestroy {
            urn: "google:cloudresourcemanager:project:st-prod".to_string(),
            resolved_name: "projects/st-prod".to_string(),
            etag: Some("E1".to_string()),
            state: Some("ACTIVE".to_string()),
            present: true,
            request: PlannedRequest::delete(format!("{}/projects/st-prod", tool.endpoints.crm)),
        };
        let stale_digest = destroy_plan_digest(
            "google.cloudresourcemanager.projects.delete",
            &stale_resolved,
        );

        let cfg = destroy_cfg(
            "google.cloudresourcemanager.projects.delete",
            serde_json::json!({ "project_id": "st-prod" }),
            serde_json::json!({ "dry_run": false, "confirm": stale_digest }),
        );
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        match err {
            ToolError::ExecutionFailed(msg) => {
                assert!(msg.contains("REFUSED"), "names the refusal: {msg}");
                assert!(
                    msg.contains("changed since the plan was reviewed"),
                    "explains staleness: {msg}"
                );
                assert!(
                    msg.contains("No deletion was performed"),
                    "confirms nothing was deleted: {msg}"
                );
            }
            other => panic!("expected ExecutionFailed refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn destroy_digest_changes_when_live_state_drifts() {
        // Same target, two different live etags → two different digests.  This is
        // the property that makes the stale-confirm refusal work.
        let mk = |etag: &str| ResolvedDestroy {
            urn: "google:cloudresourcemanager:project:p".to_string(),
            resolved_name: "projects/p".to_string(),
            etag: Some(etag.to_string()),
            state: Some("ACTIVE".to_string()),
            present: true,
            request: PlannedRequest::delete("https://x/v3/projects/p".to_string()),
        };
        let a = destroy_plan_digest("google.cloudresourcemanager.projects.delete", &mk("E1"));
        let b = destroy_plan_digest("google.cloudresourcemanager.projects.delete", &mk("E2"));
        assert_ne!(a, b, "a drifted etag must produce a different plan digest");
        // Presence flip also changes the digest.
        let mut absent = mk("E1");
        absent.present = false;
        let c = destroy_plan_digest("google.cloudresourcemanager.projects.delete", &absent);
        assert_ne!(a, c, "presence change must produce a different plan digest");
    }

    #[tokio::test]
    async fn destroy_ambiguous_folder_match_is_refused() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/folders"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "folders": [
                    { "name": "folders/1", "displayName": "dup", "state": "ACTIVE" },
                    { "name": "folders/2", "displayName": "dup", "state": "ACTIVE" },
                ]
            })))
            .mount(&server)
            .await;
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = destroy_cfg(
            "google.cloudresourcemanager.folders.delete",
            serde_json::json!({ "parent": "organizations/1", "display_name": "dup" }),
            serde_json::json!({ "dry_run": true }),
        );
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        assert!(
            matches!(err, ToolError::Configuration(ref m) if m.contains("ambiguous")),
            "a display-name matching >1 folder is not an explicit target: {err:?}"
        );
    }

    // ---- IAM remove_binding pure logic ----

    #[test]
    fn remove_binding_drops_member_and_empty_binding() {
        let policy = serde_json::json!({
            "etag": "BwXyz",
            "bindings": [
                { "role": "roles/owner", "members": ["user:a@x.com", "user:b@x.com"] },
                { "role": "roles/viewer", "members": ["user:c@x.com"] },
            ]
        });
        // Drop one of two members — binding survives.
        let r1 = remove_binding(&policy, "roles/owner", "user:b@x.com");
        assert!(binding_present(&r1, "roles/owner", "user:a@x.com"));
        assert!(!binding_present(&r1, "roles/owner", "user:b@x.com"));
        assert_eq!(r1["etag"], serde_json::json!("BwXyz"));
        // Drop the sole member — the whole binding is removed.
        let r2 = remove_binding(&policy, "roles/viewer", "user:c@x.com");
        let has_viewer = r2["bindings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|b| b["role"] == serde_json::json!("roles/viewer"));
        assert!(!has_viewer, "an emptied binding is dropped entirely");
    }

    // ---- Fork 1: ownership fold / drift / orphan (pure) ----

    use super::state::{compute_drift, detect_orphans, fold_facts, Drift, ProviderFact};

    fn fact(urn: &str, verb: &str, outcome: &str, desired: serde_json::Value) -> ProviderFact {
        ProviderFact {
            urn: urn.to_string(),
            provider: "google".to_string(),
            service: "cloudresourcemanager".to_string(),
            resource_type: "project".to_string(),
            verb: verb.to_string(),
            stack: "prod".to_string(),
            desired,
            execution_id: 1,
            outcome: outcome.to_string(),
        }
    }

    #[test]
    fn fold_facts_asserts_and_tombstones_ownership() {
        let facts = vec![
            // planned (intent) — does NOT assert live ownership.
            fact(
                "urn:a",
                "ensure",
                "planned",
                serde_json::json!({"parent": "folders/1"}),
            ),
            // applied ensure — asserts ownership.
            fact(
                "urn:a",
                "ensure",
                "changed",
                serde_json::json!({"parent": "folders/1"}),
            ),
            fact(
                "urn:b",
                "ensure",
                "noop",
                serde_json::json!({"parent": "folders/2"}),
            ),
            // applied delete — tombstones urn:b.
            fact("urn:b", "delete", "deleted", serde_json::Value::Null),
        ];
        let model = fold_facts(&facts);
        assert!(model.owned.contains_key("urn:a"), "urn:a is owned");
        assert!(!model.owned.contains_key("urn:b"), "urn:b was deleted");
        assert!(model.tombstoned.contains_key("urn:b"));

        // A planned-only URN never enters the live model.
        let planned_only = vec![fact("urn:c", "ensure", "planned", serde_json::json!({}))];
        assert!(fold_facts(&planned_only).owned.is_empty());
    }

    #[test]
    fn compute_drift_detects_missing_modified_insync_untracked() {
        let facts = vec![fact(
            "urn:a",
            "ensure",
            "changed",
            serde_json::json!({ "parent": "folders/1", "displayName": "prod" }),
        )];
        let model = fold_facts(&facts);

        // In sync: actual matches desired on all desired keys.
        let actual =
            serde_json::json!({ "parent": "folders/1", "displayName": "prod", "state": "ACTIVE" });
        assert_eq!(compute_drift(&model, "urn:a", Some(&actual)), Drift::InSync);

        // Missing: owned but live GET found nothing.
        assert_eq!(compute_drift(&model, "urn:a", None), Drift::Missing);

        // Modified: a desired field diverged.
        let drifted = serde_json::json!({ "parent": "folders/999", "displayName": "prod" });
        assert_eq!(
            compute_drift(&model, "urn:a", Some(&drifted)),
            Drift::Modified {
                fields: vec!["parent".to_string()]
            }
        );

        // Untracked: a live resource with no ownership record.
        let orphan_live = serde_json::json!({ "parent": "folders/1" });
        assert_eq!(
            compute_drift(&model, "urn:unknown", Some(&orphan_live)),
            Drift::Untracked
        );
    }

    #[test]
    fn detect_orphans_finds_owned_but_undeclared() {
        let facts = vec![
            fact("urn:a", "ensure", "changed", serde_json::json!({})),
            fact("urn:b", "ensure", "changed", serde_json::json!({})),
            fact("urn:c", "ensure", "changed", serde_json::json!({})),
        ];
        let model = fold_facts(&facts);
        // Current stack declares only a and c → b is orphaned.
        let declared = vec!["urn:a".to_string(), "urn:c".to_string()];
        let orphans = detect_orphans(&model, &declared);
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].urn, "urn:b");
    }

    // ================= Round 4: reconcile policy — report / enforce / adopt ===

    /// Config with `provider`/`action`/`input`/fast-poll defaults + `auth`, then
    /// merge `extra` (reconcile / dry_run / confirm / known_desired / …).
    fn reconcile_cfg(
        action: &str,
        input: serde_json::Value,
        extra: serde_json::Value,
    ) -> ToolConfig {
        let mut body = serde_json::json!({
            "provider": "google",
            "action": action,
            "input": input,
            "poll": { "max_attempts": 3, "interval_ms": 5, "max_wait_secs": 2 },
        });
        for (k, v) in extra.as_object().unwrap() {
            body[k.as_str()] = v.clone();
        }
        let mut cfg = spec_config(body);
        cfg.auth = Some(gcp_auth());
        cfg
    }

    // ---- report (default): no cloud mutation, no state write ----

    #[tokio::test]
    async fn report_default_makes_no_cloud_mutation_and_no_state_write() {
        // Only a GET mock is mounted.  No POST/PUT/DELETE mock exists, so any
        // write attempt would 404 and fail the run — proving report reads only.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/st-prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "projects/st-prod", "projectId": "st-prod",
                "parent": "folders/2", "displayName": "prod", "state": "ACTIVE", "etag": "E1"
            })))
            .mount(&server)
            .await;

        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        // No `reconcile` key → defaults to `report`.  Known-desired says parent
        // folders/1, live says folders/2 → modified.
        let cfg = reconcile_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "st-prod", "parent": "folders/1" }),
            serde_json::json!({
                "known_desired": { "project_id": "st-prod", "parent": "folders/1", "display_name": "prod" }
            }),
        );
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();

        assert_eq!(data["reconcile"], serde_json::json!("report"));
        assert_eq!(data["changed"], serde_json::json!(false));
        // Drift detected + reported...
        assert_eq!(data["drift"]["drift"], serde_json::json!("modified"));
        assert_eq!(
            data["drift"]["fields"],
            serde_json::json!(["parent"]),
            "the diverged desired field is named"
        );
        assert_eq!(
            data["diff"]["parent"]["desired"],
            serde_json::json!("folders/1")
        );
        assert_eq!(
            data["diff"]["parent"]["actual"],
            serde_json::json!("folders/2")
        );
        // ...but NO ownership fact is written — a report changes no state.
        assert!(
            data.get("provider_fact").is_none(),
            "report must NOT write an ownership fact: {data}"
        );
    }

    #[tokio::test]
    async fn report_without_auth_is_undetermined_and_safe() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        // Default report, NO auth on the ToolConfig, no network reachable.
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.projects.ensure",
            "input": { "project_id": "p", "parent": "folders/1" },
        }));
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["reconcile"], serde_json::json!("report"));
        assert_eq!(data["drift"], serde_json::json!("undetermined"));
        assert_eq!(data["changed"], serde_json::json!(false));
        assert!(data.get("provider_fact").is_none());
    }

    // ---- enforce: still converges like today ----

    #[tokio::test]
    async fn enforce_still_converges_like_today() {
        // GET service DISABLED → enable POST (LRO done) → changed:true + fact.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/projects/p/services/youtube.googleapis.com"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "projects/p/services/youtube.googleapis.com", "state": "DISABLED"
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/projects/p/services/youtube.googleapis.com:enable"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "operations/enable-1", "done": true,
                "response": { "name": "projects/p/services/youtube.googleapis.com", "state": "ENABLED" }
            })))
            .mount(&server)
            .await;

        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = reconcile_cfg(
            "google.serviceusage.services.enable",
            serde_json::json!({ "project_id": "p", "service_name": "youtube.googleapis.com" }),
            serde_json::json!({ "reconcile": "enforce", "dry_run": false }),
        );
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["changed"], serde_json::json!(true), "enforce writes");
        assert_eq!(
            data["provider_fact"]["outcome"],
            serde_json::json!("changed"),
            "enforce records a converge fact — same as round-2"
        );
    }

    // ---- adopt dry-run: field-by-field diff + digest ----

    fn drifted_project_mock() -> serde_json::Value {
        serde_json::json!({
            "name": "projects/st-prod", "projectId": "st-prod",
            "parent": "folders/2", "displayName": "renamed-in-console",
            "state": "ACTIVE", "etag": "E1"
        })
    }

    #[tokio::test]
    async fn adopt_dryrun_emits_diff_and_digest() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/st-prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(drifted_project_mock()))
            .mount(&server)
            .await;
        // NO write mock — adopt never mutates.
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = reconcile_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "st-prod", "parent": "folders/1" }),
            serde_json::json!({
                "reconcile": "adopt",
                "dry_run": true,
                "known_desired": { "project_id": "st-prod", "parent": "folders/1", "display_name": "prod" }
            }),
        );
        let data = tool.execute(&cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(data["adopt"], serde_json::json!(true));
        assert_eq!(data["dry_run"], serde_json::json!(true));
        assert_eq!(data["changed"], serde_json::json!(false));
        assert!(data["plan_digest"].as_str().unwrap().len() == 64);
        assert_eq!(data["drift"]["drift"], serde_json::json!("modified"));
        // Both diverged fields surface in the diff.
        assert_eq!(
            data["diff"]["parent"]["actual"],
            serde_json::json!("folders/2")
        );
        assert_eq!(
            data["diff"]["display_name"]["actual"],
            serde_json::json!("renamed-in-console")
        );
        // Dry-run fact is planned intent — the fold ignores it.
        assert_eq!(data["provider_fact"]["verb"], serde_json::json!("adopt"));
        assert_eq!(
            data["provider_fact"]["outcome"],
            serde_json::json!("planned")
        );
    }

    // ---- CORE SAFETY: adopt blind apply (no confirm) is refused ----

    #[tokio::test]
    async fn adopt_blind_apply_without_confirm_is_refused() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/st-prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(drifted_project_mock()))
            .mount(&server)
            .await;
        // No write mock — proving nothing is written.
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = reconcile_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "st-prod", "parent": "folders/1" }),
            serde_json::json!({ "reconcile": "adopt", "dry_run": false }), // no confirm
        );
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        assert!(
            matches!(err, ToolError::Configuration(ref m)
                if m.contains("no `confirm`") && m.contains("blind adoptions")),
            "an adopt apply with no confirm must be refused: {err:?}"
        );
    }

    // ---- CORE SAFETY: adopt stale confirm digest is refused ----

    #[tokio::test]
    async fn adopt_stale_confirm_digest_is_refused() {
        // The human reviewed a diff when the project etag was E1; by apply time
        // the cloud drifted (etag now E2) → the live digest differs → refuse,
        // and NO state is written.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/st-prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "projects/st-prod", "projectId": "st-prod",
                "parent": "folders/2", "displayName": "renamed", "state": "ACTIVE", "etag": "E2"
            })))
            .mount(&server)
            .await;
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();

        // A digest computed against the *old* (E1) actual — what the human echoed.
        let stale_resolved = ResolvedActual {
            urn: "google:cloudresourcemanager:project:st-prod".to_string(),
            present: true,
            actual: serde_json::json!({
                "project_id": "st-prod", "parent": "folders/2", "display_name": "renamed"
            }),
            etag: Some("E1".to_string()),
            state: Some("ACTIVE".to_string()),
        };
        let stale_digest = adopt_plan_digest(
            "google.cloudresourcemanager.projects.ensure",
            &stale_resolved,
        );

        let cfg = reconcile_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "st-prod", "parent": "folders/1" }),
            serde_json::json!({ "reconcile": "adopt", "dry_run": false, "confirm": stale_digest }),
        );
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        match err {
            ToolError::ExecutionFailed(msg) => {
                assert!(msg.contains("REFUSED"), "names the refusal: {msg}");
                assert!(
                    msg.contains("changed again since the diff was reviewed"),
                    "explains staleness: {msg}"
                );
                assert!(
                    msg.contains("No state was written"),
                    "confirms nothing was written: {msg}"
                );
            }
            other => panic!("expected ExecutionFailed refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn adopt_digest_changes_when_actual_drifts() {
        let mk = |etag: &str, parent: &str| ResolvedActual {
            urn: "google:cloudresourcemanager:project:p".to_string(),
            present: true,
            actual: serde_json::json!({ "project_id": "p", "parent": parent }),
            etag: Some(etag.to_string()),
            state: Some("ACTIVE".to_string()),
        };
        let action = "google.cloudresourcemanager.projects.ensure";
        let a = adopt_plan_digest(action, &mk("E1", "folders/1"));
        // A drifted etag changes the digest.
        assert_ne!(a, adopt_plan_digest(action, &mk("E2", "folders/1")));
        // A drifted field changes the digest.
        assert_ne!(a, adopt_plan_digest(action, &mk("E1", "folders/9")));
        // A presence flip changes the digest.
        let mut absent = mk("E1", "folders/1");
        absent.present = false;
        assert_ne!(a, adopt_plan_digest(action, &absent));
    }

    // ---- adopt of an ALREADY-OWNED resource updates last-known-desired ----

    #[tokio::test]
    async fn adopt_already_owned_updates_last_known_desired() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/st-prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(drifted_project_mock()))
            .mount(&server)
            .await;
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();

        // 1) dry-run → digest (live actual parent folders/2).
        let plan_cfg = reconcile_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "st-prod", "parent": "folders/1" }),
            serde_json::json!({
                "reconcile": "adopt", "dry_run": true, "stack": "prod",
                "known_desired": { "project_id": "st-prod", "parent": "folders/1", "display_name": "prod" }
            }),
        );
        let plan = tool.execute(&plan_cfg, &ctx).await.unwrap().data.unwrap();
        let digest = plan["plan_digest"].as_str().unwrap().to_string();

        // 2) apply with the reviewed digest → accepts actual as desired.
        let apply_cfg = reconcile_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "st-prod", "parent": "folders/1" }),
            serde_json::json!({
                "reconcile": "adopt", "dry_run": false, "confirm": digest, "stack": "prod",
                "known_desired": { "project_id": "st-prod", "parent": "folders/1", "display_name": "prod" }
            }),
        );
        let out = tool.execute(&apply_cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(out["adopted"], serde_json::json!(true));
        assert_eq!(
            out["changed"],
            serde_json::json!(false),
            "adopt makes no cloud change"
        );
        assert_eq!(
            out["import"],
            serde_json::json!(false),
            "this URN was already owned"
        );
        assert_eq!(
            out["provider_fact"]["outcome"],
            serde_json::json!("adopted")
        );
        // The new desired is the live actual (parent folders/2, renamed display).
        assert_eq!(
            out["provider_fact"]["desired"]["parent"],
            serde_json::json!("folders/2")
        );

        // 3) fold [prior ensure(folders/1), this adopt(folders/2)] → owned desired UPDATED.
        let prior = fact(
            "google:cloudresourcemanager:project:st-prod",
            "ensure",
            "changed",
            serde_json::json!({ "project_id": "st-prod", "parent": "folders/1", "display_name": "prod" }),
        );
        let adopt_fact: ProviderFact =
            serde_json::from_value(out["provider_fact"].clone()).unwrap();
        let model = fold_facts(&[prior, adopt_fact]);
        let owned = model
            .owned
            .get("google:cloudresourcemanager:project:st-prod")
            .expect("still owned after adopt");
        assert_eq!(
            owned.last_desired["parent"],
            serde_json::json!("folders/2"),
            "adopt refreshed last-known-desired to the accepted actual"
        );
    }

    // ---- adopt of an UNTRACKED (console-created) resource → Owned (import) ----

    #[tokio::test]
    async fn adopt_untracked_transitions_to_owned() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/console-made"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "name": "projects/console-made", "projectId": "console-made",
                "parent": "folders/7", "displayName": "made-in-console", "state": "ACTIVE", "etag": "Z1"
            })))
            .mount(&server)
            .await;
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();

        // No `known_desired` → the resource is untracked (created outside NoETL).
        let plan_cfg = reconcile_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "console-made", "parent": "folders/7" }),
            serde_json::json!({ "reconcile": "adopt", "dry_run": true, "stack": "prod" }),
        );
        let plan = tool.execute(&plan_cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(
            plan["import"],
            serde_json::json!(true),
            "no owning fact → import"
        );
        assert_eq!(plan["drift"]["drift"], serde_json::json!("untracked"));
        let digest = plan["plan_digest"].as_str().unwrap().to_string();

        let apply_cfg = reconcile_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "console-made", "parent": "folders/7" }),
            serde_json::json!({ "reconcile": "adopt", "dry_run": false, "confirm": digest, "stack": "prod" }),
        );
        let out = tool.execute(&apply_cfg, &ctx).await.unwrap().data.unwrap();
        assert_eq!(out["import"], serde_json::json!(true));
        assert_eq!(
            out["provider_fact"]["outcome"],
            serde_json::json!("adopted")
        );

        // Fold the single adopt fact → the previously-untracked URN is now Owned.
        let adopt_fact: ProviderFact =
            serde_json::from_value(out["provider_fact"].clone()).unwrap();
        let model = fold_facts(&[adopt_fact]);
        assert!(
            model
                .owned
                .contains_key("google:cloudresourcemanager:project:console-made"),
            "adopt imported the untracked resource into the ownership model"
        );
    }

    #[tokio::test]
    async fn adopt_apply_refused_when_resource_absent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v3/projects/ghost"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": { "code": 404, "message": "not found" }
            })))
            .mount(&server)
            .await;
        let tool = tool_for(&server);
        let ctx = ExecutionContext::default();
        let cfg = reconcile_cfg(
            "google.cloudresourcemanager.projects.ensure",
            serde_json::json!({ "project_id": "ghost", "parent": "folders/1" }),
            serde_json::json!({ "reconcile": "adopt", "dry_run": true }),
        );
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        assert!(
            matches!(err, ToolError::ExecutionFailed(ref m)
                if m.contains("nothing to adopt") && m.contains("No state was written")),
            "adopt of an absent resource must be refused: {err:?}"
        );
    }

    #[tokio::test]
    async fn adopt_requires_auth_to_plan() {
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.projects.ensure",
            "reconcile": "adopt",
            "dry_run": true,
            "input": { "project_id": "p", "parent": "folders/1" },
        }));
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        assert!(
            matches!(err, ToolError::Configuration(ref m)
                if m.contains("requires an explicit `auth:`") && m.contains("stale")),
            "adopt plan without auth must be refused: {err:?}"
        );
    }

    // ---- fold: adopt asserts ownership (import + update) ----

    #[test]
    fn fold_facts_adopt_imports_and_updates_ownership() {
        // Import: a bare adopt fact for a never-seen URN → owned.
        let import = fact(
            "urn:x",
            "adopt",
            "adopted",
            serde_json::json!({ "parent": "folders/7" }),
        );
        let m1 = fold_facts(&[import]);
        assert_eq!(
            m1.owned.get("urn:x").unwrap().last_desired["parent"],
            serde_json::json!("folders/7"),
            "adopt imports an untracked URN"
        );

        // Update: ensure(folders/1) then adopt(folders/2) → last_desired updated.
        let ensure = fact(
            "urn:y",
            "ensure",
            "changed",
            serde_json::json!({ "parent": "folders/1" }),
        );
        let adopt = fact(
            "urn:y",
            "adopt",
            "adopted",
            serde_json::json!({ "parent": "folders/2" }),
        );
        let m2 = fold_facts(&[ensure, adopt]);
        assert_eq!(
            m2.owned.get("urn:y").unwrap().last_desired["parent"],
            serde_json::json!("folders/2"),
            "adopt refreshes last-known-desired for an owned URN"
        );
    }

    // ---- raw-eventlog-tier adapter: extract nested provider_fact, fold ----

    #[test]
    fn extract_facts_from_eventlog_pulls_nested_fact_and_folds() {
        use super::state::{extract_facts_from_eventlog, fold_eventlog};
        // Three raw eventlog-tier record shapes; each carries a provider_fact
        // inside a step result payload at a different nesting depth.
        let records = vec![
            serde_json::json!({
                "event_id": 1,
                "result": { "data": { "provider_fact": {
                    "urn": "urn:a", "provider": "google", "service": "cloudresourcemanager",
                    "resource_type": "project", "verb": "ensure", "stack": "prod",
                    "desired": { "parent": "folders/1" }, "execution_id": 10, "outcome": "changed"
                } } }
            }),
            serde_json::json!({
                "event_id": 2,
                "data": { "provider_fact": {
                    "urn": "urn:a", "provider": "google", "service": "cloudresourcemanager",
                    "resource_type": "project", "verb": "adopt", "stack": "prod",
                    "desired": { "parent": "folders/2" }, "execution_id": 11, "outcome": "adopted"
                } }
            }),
            // A record with no provider_fact is ignored.
            serde_json::json!({ "event_id": 3, "result": { "data": { "note": "unrelated" } } }),
        ];
        let facts = extract_facts_from_eventlog(&records);
        assert_eq!(
            facts.len(),
            2,
            "two provider_facts extracted, unrelated skipped"
        );

        let model = fold_eventlog(&records);
        assert_eq!(
            model.owned.get("urn:a").unwrap().last_desired["parent"],
            serde_json::json!("folders/2"),
            "the adopt fact (newest) sets the owned desired"
        );
    }

    // ---- Round 5: wire-shape tier adapter + stack scoping ----

    #[test]
    fn extract_facts_from_tier_records_unwraps_payload_and_deep_nesting() {
        use super::state::{extract_facts_from_tier_records, fold_tier_records};
        // The REAL `call.done` envelope nests the fact four levels deep
        // (`context.result.context.data.provider_fact`) — deeper than the fixed
        // `fact_in_record` paths — AND the tier serves the whole body as a JSON
        // *string* in `payload`.  Both must be handled or the fact is invisible.
        let body = serde_json::json!({
            "execution_id": "555",
            "event_type": "call.done",
            "context": { "command_id": "c1", "call_index": 0, "result": {
                "status": "success",
                "context": { "status": "success", "data": {
                    "provider": "google", "resource": { "state": "ACTIVE" },
                    "provider_fact": {
                        "urn": "urn:deep", "provider": "google",
                        "service": "cloudresourcemanager", "resource_type": "folder",
                        "verb": "ensure", "stack": "prod",
                        "desired": { "parent": "folders/1", "display_name": "media" },
                        "execution_id": 555, "outcome": "changed"
                    }
                } }
            } }
        });
        let records = vec![
            serde_json::json!({
                "global_sequence": 1, "execution_id": "555", "transaction_id": "t1",
                "byte_len": 512, "payload": serde_json::to_string(&body).unwrap()
            }),
            // A record whose payload has no provider_fact is skipped.
            serde_json::json!({
                "global_sequence": 2, "execution_id": "555", "transaction_id": "t2",
                "byte_len": 20, "payload": "{\"event_type\":\"execution.start\"}"
            }),
        ];
        let facts = extract_facts_from_tier_records(&records);
        assert_eq!(
            facts.len(),
            1,
            "the deeply-nested fact is found; the other skipped"
        );
        assert_eq!(facts[0].urn, "urn:deep");

        let model = fold_tier_records(&records);
        assert!(model.owned.contains_key("urn:deep"));
        assert_eq!(model.owned["urn:deep"].stack, "prod");
    }

    #[test]
    fn for_stack_and_scoped_orphans_respect_stack() {
        use super::state::{detect_orphans, detect_orphans_scoped, fold_facts, ProviderFact};
        let f = |urn: &str, stack: &str| ProviderFact {
            urn: urn.to_string(),
            provider: "google".to_string(),
            service: "cloudresourcemanager".to_string(),
            resource_type: "project".to_string(),
            verb: "ensure".to_string(),
            stack: stack.to_string(),
            desired: serde_json::json!({}),
            execution_id: 1,
            outcome: "changed".to_string(),
        };
        // prod owns a,b; staging owns c.
        let model = fold_facts(&[
            f("urn:a", "prod"),
            f("urn:b", "prod"),
            f("urn:c", "staging"),
        ]);
        assert_eq!(
            model.stacks(),
            vec!["prod".to_string(), "staging".to_string()]
        );
        assert_eq!(model.for_stack("prod").owned.len(), 2);
        assert_eq!(model.for_stack("staging").owned.len(), 1);

        // prod's run declares only a → b is prod's orphan; c (staging) is NOT.
        let declared = vec!["urn:a".to_string()];
        let scoped = detect_orphans_scoped(&model, "prod", &declared);
        assert_eq!(scoped.len(), 1, "only prod's undeclared resource");
        assert_eq!(scoped[0].urn, "urn:b");

        // Un-scoped detect over the mixed model would wrongly flag staging's c.
        let unscoped = detect_orphans(&model, &declared);
        assert_eq!(
            unscoped.len(),
            2,
            "unscoped conflates stacks — the bug this fixes"
        );
    }

    #[test]
    fn detect_stack_conflicts_flags_contention_not_transfer() {
        use super::state::{detect_stack_conflicts, ProviderFact};
        let f = |urn: &str, stack: &str, verb: &str, outcome: &str| ProviderFact {
            urn: urn.to_string(),
            provider: "google".to_string(),
            service: "cloudresourcemanager".to_string(),
            resource_type: "project".to_string(),
            verb: verb.to_string(),
            stack: stack.to_string(),
            desired: serde_json::json!({}),
            execution_id: 1,
            outcome: outcome.to_string(),
        };
        // Contention: prod owns urn:x, then staging asserts it with no delete.
        // Transfer: prod owns urn:y, deletes it, staging adopts — NOT a conflict.
        let facts = vec![
            f("urn:x", "prod", "ensure", "changed"),
            f("urn:x", "staging", "ensure", "changed"),
            f("urn:y", "prod", "ensure", "changed"),
            f("urn:y", "prod", "delete", "deleted"),
            f("urn:y", "staging", "adopt", "adopted"),
        ];
        let conflicts = detect_stack_conflicts(&facts);
        assert_eq!(conflicts.len(), 1, "only the un-transferred contention");
        assert_eq!(conflicts[0].urn, "urn:x");
        assert_eq!(
            conflicts[0].stacks,
            vec!["prod".to_string(), "staging".to_string()]
        );
    }
}
