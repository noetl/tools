//! Source-client abstraction for the `subscription` tool.
//!
//! Phase 1 of the subscription/listener RFC
//! ([noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90)).
//!
//! ### Why this exists
//!
//! The `nats` tool's `js_consume` operation is a *bounded* pull-consumer
//! fetch: ask a durable consumer for up to `batch` messages, wait at most
//! `timeout_ms`, ack per policy, return whatever arrived
//! (`crate::tools::nats`).  It deliberately does NOT expose long-lived
//! subscriptions — those would hold a worker slot indefinitely, which
//! violates the NoETL execution model (`agents/rules/execution-model.md`).
//!
//! The `subscription` tool generalises that one-source bounded drain
//! across message backends.  [`SourceClient`] is the seam: one trait, one
//! bounded [`SourceClient::poll`] call per source, returning a normalized
//! [`PollOutcome`].  Each backend (NATS / Pub/Sub / Kafka) implements it.
//!
//! ### Reuse beyond Phase 1
//!
//! The trait is the architectural deliverable, not just the tool.  A
//! later **continuous subscription runtime** (RFC Mode B) calls
//! [`SourceClient::poll`] in a loop instead of on a scheduled cadence; a
//! **gateway push ingress** (Mode C) reuses [`normalize_headers`] /
//! [`decode_payload`] / [`PolledMessage`] to shape an inbound webhook into
//! the same envelope.  Keeping fetch-and-ack bounded inside `poll` means
//! the worker-slot contract holds in every mode: the bounded drain never
//! blocks past `timeout_ms`.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::ToolError;

#[cfg(feature = "kafka")]
pub mod kafka;
pub mod nats;
#[cfg(feature = "pubsub")]
pub mod pubsub;

// The header/attribute directive engine now lives in the standalone, lean
// `noetl-directives` crate (noetl/ai-meta#92) so the internet-facing
// noetl-gateway can consume the same security-sensitive allowlist without
// pulling noetl-tools' heavy graph.  Re-exported here so existing
// `noetl_tools::tools::source::{DirectiveSpec, DispatchPlan, …}` call sites
// (the worker subscription runtime) are unchanged.
pub use noetl_directives::{
    extract_w3c_trace, normalize_http_headers, AppliedDirective, Control, DirectiveError,
    DirectiveRule, DirectiveSpec, DispatchPlan, TraceConfig, TraceContext, TracePropagation,
};

// ---------------------------------------------------------------------------
// Bounded-drain limits (shared across every source backend)
// ---------------------------------------------------------------------------

/// Default messages to fetch in one bounded drain.
pub const POLL_BATCH_DEFAULT: u32 = 100;
/// Hard cap on `batch` — a bounded drain never fetches more in one slot.
pub const POLL_BATCH_MAX: u32 = 1000;
/// Default wait for a bounded drain.
pub const POLL_TIMEOUT_DEFAULT_MS: u64 = 1_000;
/// Hard cap on `timeout_ms` — honors the execution-model "don't hold a
/// worker slot waiting" rule.  No backend waits longer than this.
pub const POLL_TIMEOUT_MAX_MS: u64 = 5_000;

/// Clamp a requested batch size into `[1, POLL_BATCH_MAX]`.
pub fn clamp_batch(requested: Option<u32>) -> u32 {
    requested
        .unwrap_or(POLL_BATCH_DEFAULT)
        .clamp(1, POLL_BATCH_MAX)
}

/// Clamp a requested timeout into `[0, POLL_TIMEOUT_MAX_MS]`.
pub fn clamp_timeout_ms(requested: Option<u64>) -> u64 {
    requested
        .unwrap_or(POLL_TIMEOUT_DEFAULT_MS)
        .min(POLL_TIMEOUT_MAX_MS)
}

// ---------------------------------------------------------------------------
// Ack policy
// ---------------------------------------------------------------------------

/// How a bounded drain treats the messages it fetched.
///
/// The behavioral axes are *when* the drain acks and *whether* it surfaces
/// durable ack handles for a later out-of-band disposition:
///
/// - `auto` / `on_success` — ack each message inside the drain (the
///   bounded-drain default; what every Phase-1/2 subscription user gets).
/// - `manual` — leave the messages pending so the source redelivers them on
///   the next drain (the legacy `js_consume` `ack: false` semantics). The
///   backend surfaces whatever ack ids it can in [`PollOutcome::ack_ids`].
/// - `defer` — **ack-after-processing**: do NOT ack in the drain, and surface
///   a *durable* ack handle per message ([`PolledMessage::ack_id`]) that a
///   later [`SourceClient::ack`] call disposes (ack / nack / term) once
///   downstream processing has succeeded. The message stays in-flight until
///   the handle is acked or the source's ack-wait expires (then it
///   redelivers). This is the durability boundary the CQRS materializer needs
///   ([noetl/ai-meta#103](https://github.com/noetl/ai-meta/issues/103)) so a
///   transient downstream failure between drain and write redelivers the
///   batch instead of losing it.
/// - `none` — never ack and surface no ids (a pure peek).
///
/// `defer` is opt-in: existing callers default to `on_success` and are
/// unaffected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// Ack each fetched message before returning (the bounded-drain default).
    #[default]
    OnSuccess,
    /// Alias for [`AckMode::OnSuccess`] — ack within the drain.
    Auto,
    /// Do not ack; messages stay pending and the source redelivers them.
    /// Their ack ids ride back in [`PollOutcome::ack_ids`] for a caller
    /// that wants to ack out of band.
    Manual,
    /// Ack-after-processing: do not ack in the drain, but surface a *durable*
    /// ack handle per message so a later [`SourceClient::ack`] disposes it
    /// once downstream processing succeeds. Un-acked handles redeliver after
    /// the source's ack-wait. See the type docs for the durability rationale.
    Defer,
    /// Never ack and do not surface ack ids (a pure peek).
    None,
}

impl AckMode {
    /// True when the drain should ack every fetched message before
    /// returning.
    pub fn should_ack(&self) -> bool {
        matches!(self, AckMode::OnSuccess | AckMode::Auto)
    }

    /// True when un-acked ack ids should ride back in the outcome — for a
    /// caller that will dispose them out of band ([`AckMode::Manual`]) or
    /// after downstream success ([`AckMode::Defer`]).
    pub fn surfaces_ack_ids(&self) -> bool {
        matches!(self, AckMode::Manual | AckMode::Defer)
    }

    /// True when the backend must capture a *durable* ack handle (one that
    /// survives the drain and can be disposed by a later, possibly
    /// out-of-process, [`SourceClient::ack`] call).
    pub fn defers_ack(&self) -> bool {
        matches!(self, AckMode::Defer)
    }

    /// Stable wire string for the result payload.
    pub fn as_str(&self) -> &'static str {
        match self {
            AckMode::OnSuccess => "on_success",
            AckMode::Auto => "auto",
            AckMode::Manual => "manual",
            AckMode::Defer => "defer",
            AckMode::None => "none",
        }
    }

    /// Parse the playbook-facing `ack` field, which accepts either a
    /// boolean (legacy `js_consume` shape: `true` → ack, `false` →
    /// manual) or one of the mode strings.
    pub fn parse(value: Option<&serde_json::Value>) -> Result<AckMode, ToolError> {
        match value {
            None => Ok(AckMode::default()),
            Some(serde_json::Value::Bool(true)) => Ok(AckMode::OnSuccess),
            Some(serde_json::Value::Bool(false)) => Ok(AckMode::Manual),
            Some(serde_json::Value::String(s)) => match s.to_ascii_lowercase().as_str() {
                "on_success" => Ok(AckMode::OnSuccess),
                "auto" | "true" => Ok(AckMode::Auto),
                "manual" | "false" => Ok(AckMode::Manual),
                "defer" | "deferred" => Ok(AckMode::Defer),
                "none" | "peek" => Ok(AckMode::None),
                other => Err(ToolError::Configuration(format!(
                    "Invalid subscription 'ack' value '{}'. Valid: on_success, auto, manual, \
                     defer, none",
                    other
                ))),
            },
            Some(other) => Err(ToolError::Configuration(format!(
                "Invalid subscription 'ack' value {}. Expected a bool or one of: \
                 on_success, auto, manual, defer, none",
                other
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Out-of-band ack disposition (deferred-ack capability)
// ---------------------------------------------------------------------------

/// How a previously-fetched (deferred) message should be disposed by a later
/// [`SourceClient::ack`] call.
///
/// The three dispositions map onto the JetStream ack protocol (and the
/// equivalent Pub/Sub `acknowledge` / `modifyAckDeadline` surface):
///
/// - [`Ack`](AckDisposition::Ack) — positive ack; the source advances its
///   cursor past the message and never redelivers it.
/// - [`Nack`](AckDisposition::Nack) — negative ack; the source redelivers the
///   message (optionally after `delay_ms`). Use after a *transient* downstream
///   failure to retry sooner than ack-wait would.
/// - [`Term`](AckDisposition::Term) — terminate delivery; the source stops
///   redelivering (dead-letter). Use for a *poison* message that will never
///   succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckDisposition {
    /// Positive ack — done, never redeliver.
    Ack,
    /// Negative ack — redeliver, optionally after a delay (ms).
    Nack { delay_ms: Option<u64> },
    /// Terminate — stop redelivering (dead-letter).
    Term,
}

impl AckDisposition {
    /// Stable wire string for the result payload / `operation` field.
    pub fn as_str(&self) -> &'static str {
        match self {
            AckDisposition::Ack => "ack",
            AckDisposition::Nack { .. } => "nack",
            AckDisposition::Term => "term",
        }
    }

    /// Parse a disposition from the subscription tool's `operation` value
    /// (`ack` | `nack` | `term`), with an optional redelivery delay (ms) that
    /// only applies to `nack`.
    pub fn parse(operation: &str, delay_ms: Option<u64>) -> Result<AckDisposition, ToolError> {
        match operation.to_ascii_lowercase().as_str() {
            "ack" => Ok(AckDisposition::Ack),
            "nack" | "nak" => Ok(AckDisposition::Nack { delay_ms }),
            "term" => Ok(AckDisposition::Term),
            other => Err(ToolError::Configuration(format!(
                "Invalid subscription ack disposition '{}'. Valid: ack, nack, term",
                other
            ))),
        }
    }
}

/// Result of an out-of-band [`SourceClient::ack`] disposition.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AckReport {
    /// Number of ack handles disposed successfully.
    pub disposed: usize,
    /// Per-handle errors (handle id → message) for the handles that failed.
    /// Empty on full success.
    pub errors: Vec<String>,
}

impl AckReport {
    /// True when every handle was disposed without error.
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Poll options + outcome
// ---------------------------------------------------------------------------

/// Bounded-drain parameters, already clamped to the worker-slot caps.
#[derive(Debug, Clone, Copy)]
pub struct PollOptions {
    /// Maximum messages to fetch (`[1, POLL_BATCH_MAX]`).
    pub batch: u32,
    /// Maximum wait (`[0, POLL_TIMEOUT_MAX_MS]`).
    pub timeout_ms: u64,
    /// Ack behaviour for the fetched batch.
    pub ack: AckMode,
}

impl PollOptions {
    /// Build clamped options from the raw playbook fields.
    pub fn new(batch: Option<u32>, timeout_ms: Option<u64>, ack: AckMode) -> Self {
        Self {
            batch: clamp_batch(batch),
            timeout_ms: clamp_timeout_ms(timeout_ms),
            ack,
        }
    }
}

/// One message after source-specific normalization.
///
/// Every backend produces this same shape so the dispatched playbook (and
/// later the directive engine, RFC §7) sees a uniform envelope regardless
/// of source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolledMessage {
    /// Source message id — JetStream stream sequence, Pub/Sub `messageId`,
    /// or Kafka `partition:offset`.  Stable for idempotency keying.
    pub id: String,
    /// Decoded payload — parsed JSON when the body is valid JSON, otherwise
    /// the UTF-8 string.
    pub data: serde_json::Value,
    /// Normalized metadata channel: lowercased keys, single string value or
    /// an array for multi-value headers (RFC §7.1).  Empty when the source
    /// carried none.
    pub headers: serde_json::Map<String, serde_json::Value>,
    /// Raw per-source metadata preserved verbatim for round-trips
    /// (Pub/Sub attributes, Kafka/NATS headers).
    pub attributes: serde_json::Value,
    /// Source-specific positional metadata (subject, partition, offset,
    /// delivery count, pending count, …).
    pub metadata: serde_json::Value,
    /// Ack id for out-of-band ack under [`AckMode::Manual`]; `None` when the
    /// drain already acked or the backend has no id-addressable ack.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ack_id: Option<String>,
}

/// Result of one bounded drain.
#[derive(Debug, Clone)]
pub struct PollOutcome {
    /// Normalized messages, in receive order.
    pub messages: Vec<PolledMessage>,
    /// Whether the drain acked the batch.
    pub acked: bool,
    /// Un-acked ack ids surfaced under [`AckMode::Manual`]; empty otherwise.
    pub ack_ids: Vec<String>,
}

impl PollOutcome {
    /// Number of messages drained.
    pub fn count(&self) -> usize {
        self.messages.len()
    }
}

// ---------------------------------------------------------------------------
// The trait
// ---------------------------------------------------------------------------

/// A bounded-drain message source.
///
/// One [`poll`](SourceClient::poll) call performs the whole atomic drain —
/// connect, fetch up to `batch` (or until empty / until `timeout_ms`), ack
/// per [`PollOptions::ack`], and return a normalized [`PollOutcome`].  It
/// never blocks past the clamped timeout, so it satisfies the worker-slot
/// contract in every runtime that drives it.
#[async_trait]
pub trait SourceClient: Send + Sync {
    /// Stable source kind name (`"nats"`, `"pubsub"`, `"kafka"`).  Used for
    /// the observability `source` label.
    fn source_name(&self) -> &'static str;

    /// Perform one bounded drain.
    async fn poll(&self, opts: &PollOptions) -> Result<PollOutcome, ToolError>;

    /// Dispose a set of *durable* ack handles previously surfaced under
    /// [`AckMode::Defer`] (or [`AckMode::Manual`] where the backend supports
    /// out-of-band ack).
    ///
    /// This is the back half of the deferred-ack capability: `poll` with
    /// `defer` returns handles without acking; the caller does its downstream
    /// processing; then it calls `ack` to advance the cursor
    /// ([`AckDisposition::Ack`]), retry ([`AckDisposition::Nack`]), or
    /// dead-letter ([`AckDisposition::Term`]). Handles are connection- and
    /// process-independent strings so a different worker run can dispose what
    /// another fetched, within the source's ack-wait window.
    ///
    /// Default impl errors — a backend opts in by overriding this. `ack_ids`
    /// being empty is a no-op success.
    async fn ack(
        &self,
        ack_ids: &[String],
        _disposition: AckDisposition,
    ) -> Result<AckReport, ToolError> {
        if ack_ids.is_empty() {
            return Ok(AckReport::default());
        }
        Err(ToolError::Configuration(format!(
            "subscription[{}]: source does not support out-of-band ack (deferred-ack)",
            self.source_name()
        )))
    }
}

// ---------------------------------------------------------------------------
// Shared normalization helpers (reused by every backend)
// ---------------------------------------------------------------------------

/// Decode a raw payload into a JSON value: parse as JSON when it is valid
/// JSON, otherwise fall back to the UTF-8 string (lossy).  This is the same
/// rule `js_consume` applied per message.
pub fn decode_payload(bytes: &[u8]) -> serde_json::Value {
    let text = String::from_utf8_lossy(bytes);
    serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text.into_owned()))
}

/// Normalize a metadata channel into the uniform `headers` map (RFC §7.1):
/// lowercased keys, single-value collapsed to a string, multi-value kept as
/// an array.  `entries` is `(key, values)` for each header.
pub fn normalize_headers<I, K, V>(entries: I) -> serde_json::Map<String, serde_json::Value>
where
    I: IntoIterator<Item = (K, Vec<V>)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let mut out = serde_json::Map::new();
    for (key, values) in entries {
        let mut vals: Vec<serde_json::Value> = values
            .into_iter()
            .map(|v| serde_json::Value::String(v.into()))
            .collect();
        let value = if vals.len() == 1 {
            vals.pop().unwrap()
        } else {
            serde_json::Value::Array(vals)
        };
        out.insert(key.as_ref().to_ascii_lowercase(), value);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_batch_bounds() {
        assert_eq!(clamp_batch(None), POLL_BATCH_DEFAULT);
        assert_eq!(clamp_batch(Some(0)), 1);
        assert_eq!(clamp_batch(Some(50)), 50);
        assert_eq!(clamp_batch(Some(99_999)), POLL_BATCH_MAX);
    }

    #[test]
    fn clamp_timeout_bounds() {
        assert_eq!(clamp_timeout_ms(None), POLL_TIMEOUT_DEFAULT_MS);
        assert_eq!(clamp_timeout_ms(Some(500)), 500);
        assert_eq!(clamp_timeout_ms(Some(60_000)), POLL_TIMEOUT_MAX_MS);
    }

    #[test]
    fn defaults_within_caps() {
        const { assert!(POLL_BATCH_DEFAULT <= POLL_BATCH_MAX) };
        const { assert!(POLL_TIMEOUT_DEFAULT_MS <= POLL_TIMEOUT_MAX_MS) };
    }

    #[test]
    fn ack_mode_parse_bool() {
        assert_eq!(
            AckMode::parse(Some(&serde_json::json!(true))).unwrap(),
            AckMode::OnSuccess
        );
        assert_eq!(
            AckMode::parse(Some(&serde_json::json!(false))).unwrap(),
            AckMode::Manual
        );
    }

    #[test]
    fn ack_mode_parse_strings() {
        assert_eq!(
            AckMode::parse(Some(&serde_json::json!("on_success"))).unwrap(),
            AckMode::OnSuccess
        );
        assert_eq!(
            AckMode::parse(Some(&serde_json::json!("auto"))).unwrap(),
            AckMode::Auto
        );
        assert_eq!(
            AckMode::parse(Some(&serde_json::json!("manual"))).unwrap(),
            AckMode::Manual
        );
        assert_eq!(
            AckMode::parse(Some(&serde_json::json!("defer"))).unwrap(),
            AckMode::Defer
        );
        assert_eq!(
            AckMode::parse(Some(&serde_json::json!("deferred"))).unwrap(),
            AckMode::Defer
        );
        assert_eq!(
            AckMode::parse(Some(&serde_json::json!("none"))).unwrap(),
            AckMode::None
        );
    }

    #[test]
    fn ack_mode_parse_default_and_invalid() {
        assert_eq!(AckMode::parse(None).unwrap(), AckMode::OnSuccess);
        assert!(AckMode::parse(Some(&serde_json::json!("bogus"))).is_err());
        assert!(AckMode::parse(Some(&serde_json::json!(42))).is_err());
    }

    #[test]
    fn ack_mode_should_ack() {
        assert!(AckMode::OnSuccess.should_ack());
        assert!(AckMode::Auto.should_ack());
        assert!(!AckMode::Manual.should_ack());
        assert!(!AckMode::Defer.should_ack());
        assert!(!AckMode::None.should_ack());
        assert!(AckMode::Manual.surfaces_ack_ids());
        assert!(AckMode::Defer.surfaces_ack_ids());
        assert!(!AckMode::None.surfaces_ack_ids());
        // Only `defer` requires a durable, dispose-later handle.
        assert!(AckMode::Defer.defers_ack());
        assert!(!AckMode::Manual.defers_ack());
        assert!(!AckMode::OnSuccess.defers_ack());
    }

    #[test]
    fn ack_disposition_parse_and_str() {
        assert_eq!(
            AckDisposition::parse("ack", None).unwrap(),
            AckDisposition::Ack
        );
        assert_eq!(
            AckDisposition::parse("nack", Some(500)).unwrap(),
            AckDisposition::Nack {
                delay_ms: Some(500)
            }
        );
        assert_eq!(
            AckDisposition::parse("nak", None).unwrap(),
            AckDisposition::Nack { delay_ms: None }
        );
        assert_eq!(
            AckDisposition::parse("term", None).unwrap(),
            AckDisposition::Term
        );
        assert!(AckDisposition::parse("bogus", None).is_err());
        assert_eq!(AckDisposition::Ack.as_str(), "ack");
        assert_eq!(
            AckDisposition::Nack { delay_ms: None }.as_str(),
            "nack"
        );
        assert_eq!(AckDisposition::Term.as_str(), "term");
    }

    #[test]
    fn ack_report_clean() {
        let r = AckReport::default();
        assert!(r.is_clean());
        assert_eq!(r.disposed, 0);
        let r = AckReport {
            disposed: 2,
            errors: vec!["boom".into()],
        };
        assert!(!r.is_clean());
    }

    #[test]
    fn decode_payload_json_and_string() {
        assert_eq!(decode_payload(b"{\"a\":1}"), serde_json::json!({"a": 1}));
        assert_eq!(
            decode_payload(b"plain text"),
            serde_json::Value::String("plain text".to_string())
        );
        // Invalid UTF-8 falls back lossily, not a panic.
        let v = decode_payload(&[0xff, 0xfe]);
        assert!(v.is_string());
    }

    #[test]
    fn normalize_headers_single_and_multi() {
        let entries = vec![
            ("X-Single".to_string(), vec!["one".to_string()]),
            (
                "X-Multi".to_string(),
                vec!["a".to_string(), "b".to_string()],
            ),
        ];
        let map = normalize_headers(entries);
        assert_eq!(map.get("x-single").unwrap(), &serde_json::json!("one"));
        assert_eq!(map.get("x-multi").unwrap(), &serde_json::json!(["a", "b"]));
    }

    #[test]
    fn poll_options_clamps() {
        let opts = PollOptions::new(Some(99_999), Some(60_000), AckMode::Manual);
        assert_eq!(opts.batch, POLL_BATCH_MAX);
        assert_eq!(opts.timeout_ms, POLL_TIMEOUT_MAX_MS);
        assert_eq!(opts.ack, AckMode::Manual);
    }
}
