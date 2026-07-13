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
//! ```
//!
//! ## Round-1 scope (noetl/ai-meta cloud-provider-tools handoff)
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
//! - **Idempotency.** `ensure_*` / `enable` verbs are designed read-then-write.
//!   Single-request actions (reads, `services.enable`, `projects.link`,
//!   `*.iam.get_policy`) execute in apply mode; the multi-step `ensure` /
//!   `ensure_binding` actions are fully plan-able (dry-run) now and their
//!   apply path is stubbed with a clear error (round-1 boundary).
//! - **No credential logging.** The bearer token lives only on the outbound
//!   `reqwest` builder — never in `ToolResult`, the `would_call` echo, spans,
//!   or errors.  A field allowlist redaction ([`redact_sensitive`]) scrubs any
//!   sensitive-looking key from echoed input / request bodies as defence in
//!   depth.
//!
//! ## Cross-cloud shape
//!
//! [`ProviderFamily`] keeps `aws` / `azure` as explicit, not-yet-implemented
//! arms behind the same `kind: provider` surface — a later backend attaches
//! without changing the YAML contract.

use async_trait::async_trait;
use reqwest::Method;
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

/// Round-1 action plan: the primary request plus whether it mutates state and
/// whether apply-mode execution is supported yet.
struct ActionPlan {
    /// The primary request (for a read, the whole op; for a mutation, the write).
    request: PlannedRequest,
    /// True if the action changes cloud state.
    mutates: bool,
    /// True if apply-mode execution is implemented in round 1.  The multi-step
    /// `ensure` / `ensure_binding` actions are `false` (plan-able only).
    apply_supported: bool,
}

/// Cloud provider operation tool.
pub struct ProviderTool {
    client: reqwest::Client,
    auth_resolver: AuthResolver,
    template_engine: TemplateEngine,
}

impl ProviderTool {
    /// Create a new provider tool.
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .unwrap_or_default();
        Self {
            client,
            auth_resolver: AuthResolver::new(),
            template_engine: TemplateEngine::new(),
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

    /// Build the round-1 action plan for a canonical Google action.
    fn plan_google(action: &str, input: &serde_json::Value) -> Result<ActionPlan, ToolError> {
        let get = |k: &str| input_str(input, k);

        match action {
            "google.cloudresourcemanager.folders.list" => {
                let parent = require(&get("parent"), "parent", action)?;
                Ok(ActionPlan {
                    request: PlannedRequest::get(format!(
                        "{CRM_V3}/folders?parent={}",
                        urlencode(&parent)
                    )),
                    mutates: false,
                    apply_supported: true,
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
                    // Primary (write) request; apply path (list-then-create) stubbed.
                    request: PlannedRequest::post(
                        format!("{CRM_V3}/folders"),
                        Some(serde_json::json!({ "parent": parent, "displayName": display_name })),
                    ),
                    mutates: true,
                    apply_supported: false,
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
                        format!("{CRM_V3}/{}:getIamPolicy", org),
                        Some(serde_json::json!({})),
                    ),
                    mutates: false,
                    apply_supported: true,
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
                        format!("{CRM_V3}/{}:setIamPolicy", org),
                        Some(serde_json::json!({
                            "policy": { "bindings": [ { "role": role, "members": [member] } ] }
                        })),
                    ),
                    mutates: true,
                    apply_supported: false,
                })
            }
            "google.cloudresourcemanager.projects.describe" => {
                let project = require(
                    &get("project_id").or_else(|| get("projectId")),
                    "project_id",
                    action,
                )?;
                Ok(ActionPlan {
                    request: PlannedRequest::get(format!("{CRM_V3}/projects/{project}")),
                    mutates: false,
                    apply_supported: true,
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
                        format!("{CRM_V3}/projects"),
                        Some(serde_json::json!({
                            "projectId": project,
                            "displayName": display_name,
                            "parent": parent,
                        })),
                    ),
                    mutates: true,
                    apply_supported: false,
                })
            }
            "google.cloudbilling.projects.link" => {
                let project = require(
                    &get("project_id").or_else(|| get("projectId")),
                    "project_id",
                    action,
                )?;
                let billing = require(
                    &get("billing_account")
                        .or_else(|| get("billingAccountName"))
                        .or_else(|| get("billing_account_name")),
                    "billing_account",
                    action,
                )?;
                Ok(ActionPlan {
                    request: PlannedRequest::put(
                        format!("{BILLING_V1}/projects/{project}/billingInfo"),
                        Some(serde_json::json!({ "billingAccountName": billing })),
                    ),
                    mutates: true,
                    apply_supported: true,
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
                        format!("{BILLING_V1}/{}:getIamPolicy", ba),
                        Some(serde_json::json!({})),
                    ),
                    mutates: false,
                    apply_supported: true,
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
                        format!("{BILLING_V1}/{}:setIamPolicy", ba),
                        Some(serde_json::json!({
                            "policy": { "bindings": [ { "role": role, "members": [member] } ] }
                        })),
                    ),
                    mutates: true,
                    apply_supported: false,
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
                        "{SERVICEUSAGE_V1}/projects/{project}/services?filter=state:ENABLED"
                    )),
                    mutates: false,
                    apply_supported: true,
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
                    // Inherently idempotent: returns done if already enabled.
                    request: PlannedRequest::post(
                        format!("{SERVICEUSAGE_V1}/projects/{project}/services/{service}:enable"),
                        Some(serde_json::json!({})),
                    ),
                    mutates: true,
                    apply_supported: true,
                })
            }
            other => Err(ToolError::Configuration(format!(
                "unknown google provider action: {other:?}"
            ))),
        }
    }

    /// Execute the planned request against the live API (apply mode only).
    async fn execute_request(
        &self,
        plan: &PlannedRequest,
        creds: &crate::auth::AuthCredentials,
    ) -> Result<serde_json::Value, ToolError> {
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
        let plan = Self::plan_google(&action, &spec.input)?;
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

        if !plan.apply_supported {
            return Err(ToolError::Configuration(format!(
                "provider action {action:?} apply-mode execution is not yet implemented \
                 in the round-1 REST MVP (multi-step ensure); use dry_run=true to preview \
                 the planned request"
            )));
        }

        let creds = self.auth_resolver.resolve(auth_config, ctx).await?;
        let response = self.execute_request(&plan.request, &creds).await?;

        let data = serde_json::json!({
            "provider": "google",
            "action": action,
            "dry_run": false,
            // A successful mutating call changed state; a read did not.  A
            // finer per-resource idempotency signal is a documented follow-up.
            "changed": plan.mutates,
            "backend": backend,
            "resource": response,
        });
        Ok(ToolResult::success(data).with_duration(start.elapsed().as_millis() as u64))
    }
}

// ---- helpers ----

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

    #[tokio::test]
    async fn apply_multistep_ensure_is_stubbed_but_auth_checked_first() {
        // With auth present, a multi-step ensure still returns a clear
        // not-implemented error rather than attempting a partial mutation.
        let tool = ProviderTool::new();
        let ctx = ExecutionContext::default();
        let mut cfg = spec_config(serde_json::json!({
            "provider": "google",
            "action": "google.cloudresourcemanager.folders.ensure",
            "dry_run": false,
            "input": { "parent": "organizations/1", "display_name": "20-media" }
        }));
        cfg.auth = Some(crate::registry::AuthConfig {
            auth_type: crate::registry::AuthType::GcpAdc,
            credential: Some("gcp_org_admin".to_string()),
            token: None,
            username: None,
            password: None,
            header: None,
            scopes: None,
        });
        let err = tool.execute(&cfg, &ctx).await.unwrap_err();
        assert!(matches!(err, ToolError::Configuration(m) if m.contains("not yet implemented")));
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
}
