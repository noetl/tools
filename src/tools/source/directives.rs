//! Header / attribute directive engine.
//!
//! Phase 2 of the subscription/listener RFC
//! ([noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90), RFC §7).
//!
//! ### What this is
//!
//! Every message source carries a metadata channel alongside the payload —
//! Pub/Sub **attributes**, Kafka/NATS **headers**, HTTP **headers** for
//! webhook/push ingress.  Phase 1 already normalizes all of them into one
//! uniform [`PolledMessage::headers`](super::PolledMessage::headers) map
//! (lowercased keys, RFC §7.1) via [`normalize_headers`](super::normalize_headers).
//!
//! This module is the **dispatch-layer** half of RFC §7: it turns selected,
//! **allowlisted** headers into *instructions* that influence how the
//! continuous runtime (RFC Mode B) dispatches a message — redirect to a
//! different target playbook, route to a different worker pool / command
//! segment, supply an idempotency key, hint the content type, and carry a
//! W3C distributed-trace context into the execution.
//!
//! ### Untrusted by default (RFC §7.5)
//!
//! Nothing here trusts an arbitrary inbound header.  A header acts as a
//! directive **only** if its key appears in the configured
//! [`DirectiveSpec::directives`] allowlist, and even then a value allowlist
//! (`allowed:` / `map:`) constrains what target it may select.  A header not
//! in the allowlist is data — it stays in `message.headers` and can never
//! drive routing.  (Auth-gated directive trust for *push* ingress is Phase 3;
//! this engine is the same in every runtime, and the gateway simply runs it
//! only after verification succeeds.)
//!
//! ### Output
//!
//! [`DirectiveSpec::resolve`] returns a [`DispatchPlan`] — the effective
//! `dispatch.playbook` + `execution_pool` overrides, idempotency key, content
//! hints, the extracted [`TraceContext`], and an `applied` audit list for the
//! `subscription.message.directives_applied` event (RFC §7.6).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::ToolError;

// ---------------------------------------------------------------------------
// Controls — what a directive header may bind to (RFC §7.2 table)
// ---------------------------------------------------------------------------

/// The dispatch concern a directive header controls.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Control {
    /// Redirect — run a different target playbook than the subscription
    /// default.  Constrained by an `allowed:` list (no arbitrary playbooks).
    #[serde(rename = "dispatch.playbook")]
    DispatchPlaybook,
    /// Route the run to a different worker pool / command segment.
    /// Constrained by an `allowed:` list.
    #[serde(rename = "dispatch.execution_pool")]
    DispatchExecutionPool,
    /// Map a priority class to a pool/segment via `map:` (value → pool).
    Priority,
    /// Feed the dedup window + the spool item key.  Free value (a key, not a
    /// target).
    IdempotencyKey,
    /// Tell the dispatched playbook how to parse the body.  Free hint.
    ContentType,
    /// Schema hint for the body.  Free hint.
    SchemaHint,
}

impl Control {
    /// Stable wire string used in the applied-directive audit list.
    pub fn as_str(&self) -> &'static str {
        match self {
            Control::DispatchPlaybook => "dispatch.playbook",
            Control::DispatchExecutionPool => "dispatch.execution_pool",
            Control::Priority => "priority",
            Control::IdempotencyKey => "idempotency_key",
            Control::ContentType => "content_type",
            Control::SchemaHint => "schema_hint",
        }
    }
}

// ---------------------------------------------------------------------------
// Spec — the configurable allowlist (parsed from the `headers:` block)
// ---------------------------------------------------------------------------

/// One allowlisted directive rule: a header key, the concern it controls, and
/// an optional value constraint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectiveRule {
    /// Header key (lowercased on parse to match the normalized headers map).
    pub header: String,
    /// The dispatch concern this header controls.
    pub controls: Control,
    /// Value allowlist for routing controls — only these values may be
    /// selected (`dispatch.playbook` / `dispatch.execution_pool`).  A value
    /// not on the list is ignored (the directive does not apply).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed: Option<Vec<String>>,
    /// Value → target map for `priority` (e.g. `{ high: priority, normal: shared }`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub map: Option<BTreeMap<String, String>>,
}

/// How a header should be propagated as a distributed-trace context (RFC §7.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TracePropagation {
    /// Do not extract trace context.
    #[default]
    None,
    /// Honor W3C `traceparent` / `tracestate` / `baggage`.
    W3c,
}

/// Trace-propagation configuration (the `headers.trace` block).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TraceConfig {
    /// Propagation mode.  Default `none`.
    #[serde(default)]
    pub propagate: TracePropagation,
    /// Baggage keys allowed to cross the boundary.  Empty → no baggage.
    #[serde(default)]
    pub baggage_allowlist: Vec<String>,
}

impl TraceConfig {
    fn is_enabled(&self) -> bool {
        matches!(self.propagate, TracePropagation::W3c)
    }
}

/// The parsed `headers:` directive block (RFC §7.2).
///
/// Default is fully off — no normalization influence, no directives, no
/// trace — so a subscription without a `headers:` block behaves exactly as
/// Phase 1.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DirectiveSpec {
    /// Build `message.headers` from the source channel.  Phase 1 always
    /// normalizes at the source client, so this is informational; the
    /// directive engine operates on the already-normalized map regardless.
    #[serde(default)]
    pub normalize: bool,
    /// The allowlist — only these header keys act as instructions.
    #[serde(default)]
    pub directives: Vec<DirectiveRule>,
    /// Distributed-trace propagation config.
    #[serde(default)]
    pub trace: TraceConfig,
    /// What to do with non-allowlisted headers.  Always `data` in Phase 2
    /// (they stay in `message.headers`); kept for forward-compat with a
    /// future `drop`.
    #[serde(default = "default_passthrough")]
    pub passthrough: String,
}

fn default_passthrough() -> String {
    "data".to_string()
}

// ---------------------------------------------------------------------------
// Output — the resolved dispatch plan
// ---------------------------------------------------------------------------

/// W3C trace context extracted from a message's headers (RFC §7.4).
///
/// `execution_id` stays the primary NoETL trace key; this is the *external*
/// join so cross-system traces stitch together.  It rides in the execution's
/// event `meta.trace` and is never a metric label (Observability P4).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TraceContext {
    /// W3C `traceparent` (`00-<trace-id>-<span-id>-<flags>`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceparent: Option<String>,
    /// W3C `tracestate` (vendor list).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracestate: Option<String>,
    /// Allowlisted baggage key→value pairs.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub baggage: BTreeMap<String, String>,
}

impl TraceContext {
    /// True when no trace context was found (nothing to propagate).
    pub fn is_empty(&self) -> bool {
        self.traceparent.is_none() && self.tracestate.is_none() && self.baggage.is_empty()
    }
}

/// One directive that actually applied, for the audit event (RFC §7.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppliedDirective {
    /// The header key that drove the directive.
    pub header: String,
    /// The concern it controlled.
    pub controls: String,
    /// The effective value selected (after `allowed:` / `map:`).
    pub effective_value: String,
}

/// The resolved effect of a message's directives — what the runtime applies
/// before `POST /api/execute`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DispatchPlan {
    /// Effective target playbook override (`dispatch.playbook` redirect).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub playbook_override: Option<String>,
    /// Effective worker-pool / command-segment override
    /// (`dispatch.execution_pool` or a `priority` map).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_pool_override: Option<String>,
    /// Idempotency key for the dedup window + spool item key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Content-type parse hint for the dispatched playbook.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Schema hint for the body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_hint: Option<String>,
    /// Extracted W3C trace context.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace: Option<TraceContext>,
    /// Audit list of the directives that applied (RFC §7.6).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub applied: Vec<AppliedDirective>,
}

impl DispatchPlan {
    /// True when no directive applied and no trace was extracted (the common
    /// case for a subscription with no `headers:` block).
    pub fn is_noop(&self) -> bool {
        self.playbook_override.is_none()
            && self.execution_pool_override.is_none()
            && self.idempotency_key.is_none()
            && self.content_type.is_none()
            && self.schema_hint.is_none()
            && self.trace.is_none()
            && self.applied.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

impl DirectiveSpec {
    /// Parse a `headers:` block (a JSON object) into a [`DirectiveSpec`].
    ///
    /// Validates that routing controls carry the value constraint they
    /// require: `dispatch.playbook` / `dispatch.execution_pool` need an
    /// `allowed:` list, `priority` needs a `map:`.  Without it an allowlisted
    /// header could select an arbitrary target, defeating §7.5.
    pub fn parse(value: &serde_json::Value) -> Result<DirectiveSpec, ToolError> {
        let mut spec: DirectiveSpec = serde_json::from_value(value.clone()).map_err(|e| {
            ToolError::Configuration(format!("Invalid subscription 'headers' block: {e}"))
        })?;

        for rule in spec.directives.iter_mut() {
            // Normalize the header key to match the lowercased headers map.
            rule.header = rule.header.to_ascii_lowercase();

            match rule.controls {
                Control::DispatchPlaybook | Control::DispatchExecutionPool => {
                    let ok = rule.allowed.as_ref().map(|a| !a.is_empty()).unwrap_or(false);
                    if !ok {
                        return Err(ToolError::Configuration(format!(
                            "directive header '{}' controls '{}' but declares no non-empty \
                             'allowed:' value list — a routing directive must constrain its \
                             targets (RFC §7.5)",
                            rule.header,
                            rule.controls.as_str()
                        )));
                    }
                }
                Control::Priority => {
                    let ok = rule.map.as_ref().map(|m| !m.is_empty()).unwrap_or(false);
                    if !ok {
                        return Err(ToolError::Configuration(format!(
                            "directive header '{}' controls 'priority' but declares no non-empty \
                             'map:' (value → pool) — a priority directive must map to allowed \
                             pools (RFC §7.5)",
                            rule.header
                        )));
                    }
                }
                Control::IdempotencyKey | Control::ContentType | Control::SchemaHint => {}
            }
        }

        Ok(spec)
    }

    /// Resolve this spec against a message's normalized headers map, producing
    /// the [`DispatchPlan`] the runtime applies before dispatch.
    ///
    /// Only allowlisted keys are honored; routing controls are further
    /// constrained by their `allowed:` / `map:` value lists.  Multi-value
    /// headers (Kafka allows duplicate keys; the array shape from
    /// [`normalize_headers`](super::normalize_headers)) are **last-wins** for
    /// directives (RFC §10 OQ7).
    pub fn resolve(&self, headers: &serde_json::Map<String, serde_json::Value>) -> DispatchPlan {
        let mut plan = DispatchPlan::default();

        for rule in &self.directives {
            let Some(raw) = headers.get(&rule.header) else {
                continue;
            };
            let Some(value) = last_value(raw) else {
                continue;
            };

            match rule.controls {
                Control::DispatchPlaybook => {
                    if value_allowed(rule.allowed.as_ref(), &value) {
                        plan.playbook_override = Some(value.clone());
                        plan.applied.push(applied(rule, &value));
                    }
                }
                Control::DispatchExecutionPool => {
                    if value_allowed(rule.allowed.as_ref(), &value) {
                        plan.execution_pool_override = Some(value.clone());
                        plan.applied.push(applied(rule, &value));
                    }
                }
                Control::Priority => {
                    if let Some(map) = rule.map.as_ref() {
                        if let Some(pool) = map.get(&value) {
                            // An explicit dispatch.execution_pool directive wins
                            // over a priority mapping (RFC §10 OQ7 precedence).
                            if plan.execution_pool_override.is_none() {
                                plan.execution_pool_override = Some(pool.clone());
                            }
                            plan.applied.push(AppliedDirective {
                                header: rule.header.clone(),
                                controls: rule.controls.as_str().to_string(),
                                effective_value: pool.clone(),
                            });
                        }
                    }
                }
                Control::IdempotencyKey => {
                    plan.idempotency_key = Some(value.clone());
                    plan.applied.push(applied(rule, &value));
                }
                Control::ContentType => {
                    plan.content_type = Some(value.clone());
                    plan.applied.push(applied(rule, &value));
                }
                Control::SchemaHint => {
                    plan.schema_hint = Some(value.clone());
                    plan.applied.push(applied(rule, &value));
                }
            }
        }

        // Precedence fix-up: a `dispatch.execution_pool` directive must win
        // over a `priority` map even when priority was declared first.  Re-run
        // the explicit-pool rules last.
        for rule in &self.directives {
            if rule.controls == Control::DispatchExecutionPool {
                if let Some(raw) = headers.get(&rule.header) {
                    if let Some(value) = last_value(raw) {
                        if value_allowed(rule.allowed.as_ref(), &value) {
                            plan.execution_pool_override = Some(value);
                        }
                    }
                }
            }
        }

        if self.trace.is_enabled() {
            let trace = extract_w3c_trace(headers, &self.trace.baggage_allowlist);
            if !trace.is_empty() {
                plan.trace = Some(trace);
            }
        }

        plan
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Pull the effective string value from a normalized header value.  A
/// single-value header is a `String`; a multi-value header is an `Array` —
/// last-wins for directives (RFC §10 OQ7).
fn last_value(raw: &serde_json::Value) -> Option<String> {
    match raw {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Array(arr) => arr
            .iter()
            .rev()
            .find_map(|v| v.as_str().map(str::to_string)),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// True when `value` is permitted by the (optional) value allowlist.  A
/// routing control always carries one (enforced at parse); a free control
/// passes any value.
fn value_allowed(allowed: Option<&Vec<String>>, value: &str) -> bool {
    match allowed {
        Some(list) => list.iter().any(|a| a == value),
        None => true,
    }
}

fn applied(rule: &DirectiveRule, value: &str) -> AppliedDirective {
    AppliedDirective {
        header: rule.header.clone(),
        controls: rule.controls.as_str().to_string(),
        effective_value: value.to_string(),
    }
}

/// Extract a W3C trace context from the normalized headers map.
///
/// Reads `traceparent` + `tracestate`, and parses the W3C `baggage` header
/// (`k1=v1,k2=v2`) keeping only allowlisted keys.  Validation of
/// `traceparent` is loose — a malformed value is still carried (it is an
/// external join, not a NoETL-authoritative id), but an obviously non-W3C
/// shape is dropped so it never pollutes the join.
pub fn extract_w3c_trace(
    headers: &serde_json::Map<String, serde_json::Value>,
    baggage_allowlist: &[String],
) -> TraceContext {
    let mut tc = TraceContext::default();

    if let Some(tp) = headers.get("traceparent").and_then(last_value_ref) {
        if is_plausible_traceparent(&tp) {
            tc.traceparent = Some(tp);
        }
    }
    if let Some(ts) = headers.get("tracestate").and_then(last_value_ref) {
        tc.tracestate = Some(ts);
    }
    if !baggage_allowlist.is_empty() {
        if let Some(raw) = headers.get("baggage").and_then(last_value_ref) {
            for item in raw.split(',') {
                let item = item.trim();
                if let Some((k, v)) = item.split_once('=') {
                    let key = k.trim();
                    // A baggage member may carry `;`-delimited properties; keep
                    // only the value.
                    let val = v.split(';').next().unwrap_or("").trim();
                    if baggage_allowlist.iter().any(|a| a == key) {
                        tc.baggage.insert(key.to_string(), val.to_string());
                    }
                }
            }
        }
    }

    tc
}

fn last_value_ref(raw: &serde_json::Value) -> Option<String> {
    last_value(raw)
}

/// Loose W3C `traceparent` shape check: 4 hyphen-delimited hex fields
/// (`version-traceid-spanid-flags`), trace-id 32 hex, span-id 16 hex.  We do
/// not reject an all-zero id here (that is the caller's concern) — only an
/// obviously non-W3C string.
fn is_plausible_traceparent(s: &str) -> bool {
    let parts: Vec<&str> = s.split('-').collect();
    parts.len() == 4
        && parts[0].len() == 2
        && parts[1].len() == 32
        && parts[2].len() == 16
        && parts[3].len() == 2
        && parts.iter().all(|p| p.bytes().all(|b| b.is_ascii_hexdigit()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn headers(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn empty_spec_is_noop() {
        let spec = DirectiveSpec::default();
        let plan = spec.resolve(&headers(json!({ "x-anything": "value" })));
        assert!(plan.is_noop());
    }

    #[test]
    fn parse_requires_allowed_for_routing() {
        let err = DirectiveSpec::parse(&json!({
            "directives": [{ "header": "x-route", "controls": "dispatch.playbook" }]
        }))
        .unwrap_err();
        assert!(format!("{err}").contains("allowed"));

        let err = DirectiveSpec::parse(&json!({
            "directives": [{ "header": "x-pool", "controls": "dispatch.execution_pool", "allowed": [] }]
        }))
        .unwrap_err();
        assert!(format!("{err}").contains("allowed"));

        let err = DirectiveSpec::parse(&json!({
            "directives": [{ "header": "x-prio", "controls": "priority" }]
        }))
        .unwrap_err();
        assert!(format!("{err}").contains("map"));
    }

    #[test]
    fn parse_lowercases_header_keys() {
        let spec = DirectiveSpec::parse(&json!({
            "directives": [{ "header": "X-Idempotency-Key", "controls": "idempotency_key" }]
        }))
        .unwrap();
        assert_eq!(spec.directives[0].header, "x-idempotency-key");
    }

    #[test]
    fn redirect_playbook_respects_allowlist() {
        let spec = DirectiveSpec::parse(&json!({
            "directives": [{
                "header": "x-noetl-route",
                "controls": "dispatch.playbook",
                "allowed": ["domain/handle_billing", "domain/handle_fraud"]
            }]
        }))
        .unwrap();

        // Allowlisted value applies.
        let plan = spec.resolve(&headers(json!({ "x-noetl-route": "domain/handle_fraud" })));
        assert_eq!(plan.playbook_override.as_deref(), Some("domain/handle_fraud"));
        assert_eq!(plan.applied.len(), 1);
        assert_eq!(plan.applied[0].controls, "dispatch.playbook");

        // Non-allowlisted value is ignored — never routes to an arbitrary playbook.
        let plan = spec.resolve(&headers(json!({ "x-noetl-route": "domain/evil" })));
        assert!(plan.playbook_override.is_none());
        assert!(plan.applied.is_empty());
    }

    #[test]
    fn execution_pool_override_and_priority_precedence() {
        let spec = DirectiveSpec::parse(&json!({
            "directives": [
                { "header": "x-priority", "controls": "priority", "map": { "high": "priority", "normal": "shared" } },
                { "header": "x-noetl-pool", "controls": "dispatch.execution_pool", "allowed": ["iot", "priority", "shared"] }
            ]
        }))
        .unwrap();

        // Priority maps to a pool.
        let plan = spec.resolve(&headers(json!({ "x-priority": "high" })));
        assert_eq!(plan.execution_pool_override.as_deref(), Some("priority"));

        // Explicit pool wins over priority even when both present.
        let plan = spec.resolve(&headers(json!({ "x-priority": "high", "x-noetl-pool": "iot" })));
        assert_eq!(plan.execution_pool_override.as_deref(), Some("iot"));

        // Unmapped priority class does nothing.
        let plan = spec.resolve(&headers(json!({ "x-priority": "bogus" })));
        assert!(plan.execution_pool_override.is_none());
    }

    #[test]
    fn idempotency_content_schema_are_free_values() {
        let spec = DirectiveSpec::parse(&json!({
            "directives": [
                { "header": "x-idempotency-key", "controls": "idempotency_key" },
                { "header": "content-type", "controls": "content_type" },
                { "header": "x-schema", "controls": "schema_hint" }
            ]
        }))
        .unwrap();
        let plan = spec.resolve(&headers(json!({
            "x-idempotency-key": "abc-123",
            "content-type": "application/json",
            "x-schema": "order.v2"
        })));
        assert_eq!(plan.idempotency_key.as_deref(), Some("abc-123"));
        assert_eq!(plan.content_type.as_deref(), Some("application/json"));
        assert_eq!(plan.schema_hint.as_deref(), Some("order.v2"));
        assert_eq!(plan.applied.len(), 3);
    }

    #[test]
    fn non_allowlisted_headers_are_data_only() {
        let spec = DirectiveSpec::parse(&json!({
            "directives": [{ "header": "x-noetl-pool", "controls": "dispatch.execution_pool", "allowed": ["iot"] }]
        }))
        .unwrap();
        // A header NOT in the allowlist never drives anything.
        let plan = spec.resolve(&headers(json!({ "x-evil-route": "domain/evil", "x-random": "data" })));
        assert!(plan.is_noop());
    }

    #[test]
    fn multi_value_header_is_last_wins() {
        let spec = DirectiveSpec::parse(&json!({
            "directives": [{ "header": "x-noetl-pool", "controls": "dispatch.execution_pool", "allowed": ["iot", "priority"] }]
        }))
        .unwrap();
        let plan = spec.resolve(&headers(json!({ "x-noetl-pool": ["iot", "priority"] })));
        assert_eq!(plan.execution_pool_override.as_deref(), Some("priority"));
    }

    #[test]
    fn w3c_trace_extracted_when_enabled() {
        let spec = DirectiveSpec::parse(&json!({
            "trace": { "propagate": "w3c", "baggage_allowlist": ["tenant", "request_id"] }
        }))
        .unwrap();
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let plan = spec.resolve(&headers(json!({
            "traceparent": tp,
            "tracestate": "vendor=abc",
            "baggage": "tenant=acme,request_id=r-9, secret=nope"
        })));
        let trace = plan.trace.unwrap();
        assert_eq!(trace.traceparent.as_deref(), Some(tp));
        assert_eq!(trace.tracestate.as_deref(), Some("vendor=abc"));
        assert_eq!(trace.baggage.get("tenant").map(String::as_str), Some("acme"));
        assert_eq!(trace.baggage.get("request_id").map(String::as_str), Some("r-9"));
        // Non-allowlisted baggage dropped.
        assert!(!trace.baggage.contains_key("secret"));
    }

    #[test]
    fn trace_disabled_by_default() {
        let spec = DirectiveSpec::default();
        let plan = spec.resolve(&headers(json!({
            "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        })));
        assert!(plan.trace.is_none());
    }

    #[test]
    fn malformed_traceparent_dropped() {
        let tc = extract_w3c_trace(&headers(json!({ "traceparent": "not-a-trace" })), &[]);
        assert!(tc.traceparent.is_none());
        assert!(tc.is_empty());
    }

    #[test]
    fn redirect_example_from_rfc_7_3() {
        // The RFC §7.3 worked example: x-noetl-route + x-noetl-pool, both allowlisted.
        let spec = DirectiveSpec::parse(&json!({
            "directives": [
                { "header": "x-noetl-route", "controls": "dispatch.playbook",
                  "allowed": ["domain/handle_billing", "domain/handle_fraud", "domain/handle_event"] },
                { "header": "x-noetl-pool", "controls": "dispatch.execution_pool",
                  "allowed": ["priority", "shared"] }
            ],
            "trace": { "propagate": "w3c" }
        }))
        .unwrap();
        let plan = spec.resolve(&headers(json!({
            "x-noetl-route": "domain/handle_fraud",
            "x-noetl-pool": "priority"
        })));
        assert_eq!(plan.playbook_override.as_deref(), Some("domain/handle_fraud"));
        assert_eq!(plan.execution_pool_override.as_deref(), Some("priority"));
        assert_eq!(plan.applied.len(), 2);
    }
}
