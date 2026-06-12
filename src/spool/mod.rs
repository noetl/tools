//! Store-and-forward spool + circuit breaker for the subscription/listener
//! runtime — Phase 4 of the subscription/listener RFC
//! ([noetl/ai-meta#90](https://github.com/noetl/ai-meta/issues/90), RFC §8).
//!
//! ### What this closes
//!
//! The default backpressure model is *stop-acking → source redelivers*:
//! cheapest and most durable, because the source (Pub/Sub / Kafka /
//! JetStream) is the durable buffer.  It has two gaps the spool closes:
//!
//! 1. **Bounded source retention.** A multi-hour downstream outage can
//!    exceed Pub/Sub's redelivery window or a short-retention Kafka topic.
//! 2. **Push/webhooks don't redeliver.** A generic webhook that gets a
//!    non-2xx may give up. Stop-acking has nothing to stop.
//!
//! The spool is a configurable store-and-forward layer: when a downstream
//! dependency is unavailable, accumulate incoming messages in a durable
//! fallback buffer ([`backend::SpoolBackend`]) and replay them in order on
//! recovery ([`engine::SpoolEngine`]).  Unavailability is detected per
//! downstream dependency by a [`circuit::CircuitBreaker`].
//!
//! ### Module map
//!
//! - [`circuit`] — pure circuit-breaker state machine (trip / half-open /
//!   close), per-downstream scope, serializable state for KV persistence.
//! - [`item`] — the [`item::SpoolItem`] envelope + SHA-256 integrity +
//!   `noetl://spool/...` ref + ordered object keys.
//! - [`backend`] — the [`backend::SpoolBackend`] trait + `local_disk` and
//!   `nats_object` backends (reusing the NATS Object Store the `nats` tool
//!   already speaks).
//! - [`engine`] — the [`engine::SpoolEngine`] that ties a backend to
//!   ordering / idempotency / dead-letter / retention + GC.
//!
//! The pieces are deliberately I/O-free where they carry the logic (the
//! breaker, the item envelope, the ordering/idempotency rules) so the
//! correctness-critical paths are unit-testable without a cluster.  The
//! worker subscription runtime ([`noetl-worker`]) drives the I/O: probe →
//! breaker → spool-or-dispatch → ack, then drain on recovery.

pub mod backend;
pub mod circuit;
pub mod engine;
pub mod item;
pub mod probe;

pub use backend::{LocalDiskBackend, NatsObjectBackend, SpoolBackend, SpoolMeta};
#[cfg(feature = "gcs")]
pub use backend::GcsBackend;
pub use circuit::{
    CircuitBreaker, CircuitConfig, CircuitDecision, CircuitPhase, CircuitRegistry, CircuitState,
    DownstreamSpec, ProbeKind,
};
pub use engine::{Admission, DeadLetter, DrainReport, GcReport, SpoolEngine, SpooledRef};
pub use item::{sha256_hex, spool_ref, SpoolItem};
pub use probe::probe_downstream;

use serde::{Deserialize, Serialize};

use crate::error::ToolError;

// ---------------------------------------------------------------------------
// spool.mode — the durability tradeoff (RFC §8.2)
// ---------------------------------------------------------------------------

/// How the subscription treats incoming messages when a downstream it
/// depends on is unavailable (the circuit is open).
///
/// | mode | behaviour | durability owner |
/// |---|---|---|
/// | [`off`](SpoolMode::Off) | stop-acking; let the source redeliver | the source |
/// | [`buffer_and_ack`](SpoolMode::BufferAndAck) | write to the spool backend, then ack | the spool backend |
/// | [`hybrid`](SpoolMode::Hybrid) | stop-ack short blips, escalate to buffer-and-ack on a sustained outage | source, then spool |
///
/// Defaults per the RFC: `hybrid` for pull, `buffer_and_ack` for
/// push/webhook (push can't redeliver, so stop-acking has nothing to stop).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SpoolMode {
    /// Stop-acking: don't ack, let the source redeliver later. Free; leans
    /// on the source as the durable buffer. The default — explicit opt-in
    /// required for the write-bearing modes.
    #[default]
    Off,
    /// Write the message to the spool backend, then ack the source.
    /// Survives arbitrarily long outages and non-redelivering sources at
    /// the cost of one backend write per message.
    BufferAndAck,
    /// Stop-ack first (absorb short blips on the source), escalate to
    /// buffer-and-ack once the outage outlasts
    /// [`SpoolSpec::hybrid_escalate_after_ms`].
    Hybrid,
}

impl SpoolMode {
    /// Stable wire string for events + metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            SpoolMode::Off => "off",
            SpoolMode::BufferAndAck => "buffer_and_ack",
            SpoolMode::Hybrid => "hybrid",
        }
    }
}

// ---------------------------------------------------------------------------
// spool.ordering — replay order vs throughput (RFC §8.3, OQ4)
// ---------------------------------------------------------------------------

/// Replay order on drain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OrderingMode {
    /// Strict receive order across the whole spool; serialises the drain.
    Global,
    /// Per-`ordering_key` lanes (per device_id / Kafka partition / Pub/Sub
    /// ordering key). The recommended default for IoT — preserves order
    /// within a key while letting independent keys drain concurrently.
    #[default]
    PerKey,
    /// Any order; maximum throughput. Use when order-independent.
    None,
}

impl OrderingMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderingMode::Global => "global",
            OrderingMode::PerKey => "per_key",
            OrderingMode::None => "none",
        }
    }
}

// ---------------------------------------------------------------------------
// retention.on_full (RFC §8.3)
// ---------------------------------------------------------------------------

/// What to do when the spool hits `max_bytes` / `max_age` and another
/// message would otherwise be written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnFull {
    /// Fall back to source-redelivery (stop-acking the new message). The
    /// safe default — never drops data, defers to the source.
    #[default]
    StopAcking,
    /// Move the oldest item(s) to the dead-letter lane to make room.
    DropToDlq,
    /// Keep writing but emit an alert event; operator-managed ceiling.
    AlertOnly,
}

impl OnFull {
    pub fn as_str(&self) -> &'static str {
        match self {
            OnFull::StopAcking => "stop_acking",
            OnFull::DropToDlq => "drop_to_dlq",
            OnFull::AlertOnly => "alert_only",
        }
    }
}

// ---------------------------------------------------------------------------
// drain.on_recovery (RFC §8.3)
// ---------------------------------------------------------------------------

/// How a recovered subscription interleaves the spool backlog with live
/// traffic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DrainOnRecovery {
    /// Drain the backlog fully before resuming live — preserves global
    /// order. The safe default.
    #[default]
    OrderedThenLive,
    /// Resume live + drain concurrently — lower latency, safe only with
    /// `per_key` / `none` ordering.
    Interleave,
}

impl DrainOnRecovery {
    pub fn as_str(&self) -> &'static str {
        match self {
            DrainOnRecovery::OrderedThenLive => "ordered_then_live",
            DrainOnRecovery::Interleave => "interleave",
        }
    }
}

/// Which durable backend stores spooled payload bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SpoolBackendKind {
    /// NATS Object Store bucket — reuses the ops already in the `nats`
    /// tool; the in-cluster default (no external creds needed).
    #[default]
    NatsObject,
    /// A local directory — CLI / dev mode.
    LocalDisk,
    /// Google Cloud Storage via the tenant bucket credential (external
    /// system, keychain-auth). Implemented behind the `gcs` feature.
    Gcs,
    /// AWS S3 via the tenant bucket credential. Implemented behind the
    /// `s3` feature.
    S3,
}

impl SpoolBackendKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            SpoolBackendKind::NatsObject => "nats_object",
            SpoolBackendKind::LocalDisk => "local_disk",
            SpoolBackendKind::Gcs => "gcs",
            SpoolBackendKind::S3 => "s3",
        }
    }
}

// ---------------------------------------------------------------------------
// retention + drain config
// ---------------------------------------------------------------------------

/// Spool retention ceiling — the cost bound (RFC §8.3, OQ3/OQ5).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Drop / DLQ items older than this many hours. `None` = no age bound.
    pub max_age_hours: Option<u64>,
    /// Hard ceiling on total spooled bytes. `None` = no byte bound. This is
    /// the documented cost ceiling — `noetl_subscription_spool_bytes`
    /// tracks the live value against it.
    pub max_bytes: Option<u64>,
    /// What to do when a write would breach the ceiling.
    pub on_full: OnFull,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            max_age_hours: None,
            // 1 GiB default ceiling so an unbounded firehose can't fill a
            // disk / bucket silently — the RFC's "no unbounded buffer".
            max_bytes: Some(1024 * 1024 * 1024),
            on_full: OnFull::default(),
        }
    }
}

/// Drain / replay policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DrainConfig {
    /// Replay rate ceiling (messages/sec) so the drain doesn't re-hammer a
    /// just-recovered downstream. `None` = unthrottled.
    pub rate_per_sec: Option<u32>,
    /// An item that fails this many replay attempts is a poison message →
    /// dead-lettered (RFC §8.3, OQ6).
    pub max_replay_attempts: u32,
    /// Backlog-vs-live interleave policy on recovery.
    pub on_recovery: DrainOnRecovery,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            rate_per_sec: None,
            max_replay_attempts: 5,
            on_recovery: DrainOnRecovery::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// The top-level spool spec (the playbook-facing `spool:` block)
// ---------------------------------------------------------------------------

/// The `spool:` block on a `kind: Subscription`. Absent → [`SpoolSpec::off`]
/// (no spool; pure stop-acking).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpoolSpec {
    /// Durability mode (RFC §8.2).
    pub mode: SpoolMode,
    /// Backend that stores spooled payload bytes.
    pub backend: SpoolBackendKind,
    /// NATS Object Store bucket name / GCS|S3 bucket. Required for every
    /// backend except `local_disk`.
    pub bucket: Option<String>,
    /// Directory for the `local_disk` backend.
    pub path: Option<String>,
    /// Keychain alias for the bucket credential (gcs / s3 only — external
    /// system, per `data-access-boundary.md`).
    pub credential: Option<String>,
    /// Circuit-breaker config (per-downstream scope).
    pub circuit: CircuitConfig,
    /// Replay order on drain.
    pub ordering: OrderingMode,
    /// Header / attribute name carrying the per-key ordering key (used when
    /// `ordering: per_key`). Defaults to the source ordering key.
    pub ordering_key: Option<String>,
    /// Retention ceiling + on-full policy.
    pub retention: RetentionConfig,
    /// Drain / replay policy.
    pub drain: DrainConfig,
    /// For [`SpoolMode::Hybrid`]: escalate from stop-acking to buffering
    /// once the circuit has been open this many ms continuously. Default
    /// 30s — long enough to absorb a redeploy blip, short enough to start
    /// buffering well before a typical source retention window.
    pub hybrid_escalate_after_ms: u64,
}

impl SpoolSpec {
    /// The no-op spec: spooling disabled, pure stop-acking. Used when a
    /// subscription declares no `spool:` block.
    pub fn off() -> Self {
        Self {
            mode: SpoolMode::Off,
            backend: SpoolBackendKind::default(),
            bucket: None,
            path: None,
            credential: None,
            circuit: CircuitConfig::default(),
            ordering: OrderingMode::default(),
            ordering_key: None,
            retention: RetentionConfig::default(),
            drain: DrainConfig::default(),
            hybrid_escalate_after_ms: 30_000,
        }
    }

    /// True when this spec actually buffers (mode is not `off`).
    pub fn buffers(&self) -> bool {
        !matches!(self.mode, SpoolMode::Off)
    }

    /// Parse the `spool:` block from the subscription spec JSON.
    ///
    /// Tolerant of the YAML→JSON shape the worker produces. Unknown keys
    /// are ignored (forward-compat); enum values validated against the
    /// allowed set. Returns [`SpoolSpec::off`] for a missing / null block.
    pub fn parse(value: Option<&serde_json::Value>) -> Result<SpoolSpec, ToolError> {
        let obj = match value {
            None | Some(serde_json::Value::Null) => return Ok(SpoolSpec::off()),
            Some(serde_json::Value::Object(o)) => o,
            Some(other) => {
                return Err(ToolError::Configuration(format!(
                    "subscription 'spool' must be a mapping, got {other}"
                )))
            }
        };

        let mut spec = SpoolSpec::off();

        if let Some(m) = obj.get("mode") {
            spec.mode = parse_enum_str(m, "spool.mode", &[
                ("off", SpoolMode::Off),
                ("buffer_and_ack", SpoolMode::BufferAndAck),
                ("hybrid", SpoolMode::Hybrid),
            ])?;
        }
        if let Some(b) = obj.get("backend") {
            spec.backend = parse_enum_str(b, "spool.backend", &[
                ("nats_object", SpoolBackendKind::NatsObject),
                ("local_disk", SpoolBackendKind::LocalDisk),
                ("gcs", SpoolBackendKind::Gcs),
                ("s3", SpoolBackendKind::S3),
            ])?;
        }
        spec.bucket = obj.get("bucket").and_then(|v| v.as_str()).map(str::to_string);
        spec.path = obj.get("path").and_then(|v| v.as_str()).map(str::to_string);
        spec.credential = obj
            .get("credential")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        if let Some(c) = obj.get("circuit") {
            spec.circuit = CircuitConfig::parse(c)?;
        }
        if let Some(o) = obj.get("ordering") {
            spec.ordering = parse_enum_str(o, "spool.ordering", &[
                ("global", OrderingMode::Global),
                ("per_key", OrderingMode::PerKey),
                ("none", OrderingMode::None),
            ])?;
        }
        spec.ordering_key = obj
            .get("ordering_key")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        if let Some(r) = obj.get("retention").and_then(|v| v.as_object()) {
            spec.retention.max_age_hours = r.get("max_age_hours").and_then(|v| v.as_u64());
            if r.contains_key("max_bytes") {
                spec.retention.max_bytes = r.get("max_bytes").and_then(|v| v.as_u64());
            }
            if let Some(f) = r.get("on_full") {
                spec.retention.on_full = parse_enum_str(f, "spool.retention.on_full", &[
                    ("stop_acking", OnFull::StopAcking),
                    ("drop_to_dlq", OnFull::DropToDlq),
                    ("alert_only", OnFull::AlertOnly),
                ])?;
            }
        }
        if let Some(d) = obj.get("drain").and_then(|v| v.as_object()) {
            spec.drain.rate_per_sec = d
                .get("rate_per_sec")
                .and_then(|v| v.as_u64())
                .map(|v| v as u32);
            if let Some(m) = d.get("max_replay_attempts").and_then(|v| v.as_u64()) {
                spec.drain.max_replay_attempts = m as u32;
            }
            if let Some(r) = d.get("on_recovery") {
                spec.drain.on_recovery = parse_enum_str(r, "spool.drain.on_recovery", &[
                    ("ordered_then_live", DrainOnRecovery::OrderedThenLive),
                    ("interleave", DrainOnRecovery::Interleave),
                ])?;
            }
        }
        if let Some(h) = obj.get("hybrid_escalate_after_ms").and_then(|v| v.as_u64()) {
            spec.hybrid_escalate_after_ms = h;
        }

        spec.validate()?;
        Ok(spec)
    }

    /// Validate cross-field invariants after parse.
    pub fn validate(&self) -> Result<(), ToolError> {
        if !self.buffers() {
            return Ok(()); // off — no backend needed.
        }
        match self.backend {
            SpoolBackendKind::NatsObject | SpoolBackendKind::Gcs | SpoolBackendKind::S3 => {
                if self.bucket.as_deref().unwrap_or("").is_empty() {
                    return Err(ToolError::Configuration(format!(
                        "spool.backend '{}' requires a non-empty 'bucket'",
                        self.backend.as_str()
                    )));
                }
            }
            SpoolBackendKind::LocalDisk => {
                if self.path.as_deref().unwrap_or("").is_empty() {
                    return Err(ToolError::Configuration(
                        "spool.backend 'local_disk' requires a non-empty 'path'".to_string(),
                    ));
                }
            }
        }
        if matches!(self.backend, SpoolBackendKind::Gcs | SpoolBackendKind::S3)
            && self.credential.as_deref().unwrap_or("").is_empty()
        {
            return Err(ToolError::Configuration(format!(
                "spool.backend '{}' requires a keychain 'credential' alias for the bucket",
                self.backend.as_str()
            )));
        }
        // interleave is only order-safe with per_key / none.
        if matches!(self.drain.on_recovery, DrainOnRecovery::Interleave)
            && matches!(self.ordering, OrderingMode::Global)
        {
            return Err(ToolError::Configuration(
                "spool.drain.on_recovery 'interleave' is unsafe with ordering 'global'; \
                 use 'ordered_then_live' or change ordering to 'per_key'/'none'"
                    .to_string(),
            ));
        }
        self.circuit.validate()?;
        Ok(())
    }
}

/// Parse a string field against an allowlist of `(wire, value)` pairs.
pub(crate) fn parse_enum_str<T: Copy>(
    value: &serde_json::Value,
    field: &str,
    allowed: &[(&str, T)],
) -> Result<T, ToolError> {
    let s = value.as_str().ok_or_else(|| {
        ToolError::Configuration(format!("{field} must be a string"))
    })?;
    let lower = s.to_ascii_lowercase();
    for (wire, val) in allowed {
        if *wire == lower {
            return Ok(*val);
        }
    }
    let valid: Vec<&str> = allowed.iter().map(|(w, _)| *w).collect();
    Err(ToolError::Configuration(format!(
        "{field} '{s}' invalid; valid: {}",
        valid.join(", ")
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_missing_block_is_off() {
        assert_eq!(SpoolSpec::parse(None).unwrap(), SpoolSpec::off());
        assert_eq!(
            SpoolSpec::parse(Some(&serde_json::Value::Null)).unwrap(),
            SpoolSpec::off()
        );
        assert!(!SpoolSpec::off().buffers());
    }

    #[test]
    fn parse_buffer_and_ack_nats_object() {
        let v = serde_json::json!({
            "mode": "buffer_and_ack",
            "backend": "nats_object",
            "bucket": "noetl_spool",
            "ordering": "per_key",
            "ordering_key": "device_id",
            "circuit": { "trip_after": 3, "probe_after_ms": 5000,
                "downstream": [{"name": "warehouse", "type": "http", "target": "http://wh/health"}] },
            "retention": { "max_bytes": 1048576, "on_full": "drop_to_dlq" },
            "drain": { "max_replay_attempts": 2, "on_recovery": "ordered_then_live" }
        });
        let s = SpoolSpec::parse(Some(&v)).unwrap();
        assert_eq!(s.mode, SpoolMode::BufferAndAck);
        assert_eq!(s.backend, SpoolBackendKind::NatsObject);
        assert_eq!(s.bucket.as_deref(), Some("noetl_spool"));
        assert_eq!(s.ordering, OrderingMode::PerKey);
        assert_eq!(s.ordering_key.as_deref(), Some("device_id"));
        assert_eq!(s.circuit.trip_after, 3);
        assert_eq!(s.circuit.probe_after_ms, 5000);
        assert_eq!(s.circuit.downstream.len(), 1);
        assert_eq!(s.retention.max_bytes, Some(1048576));
        assert_eq!(s.retention.on_full, OnFull::DropToDlq);
        assert_eq!(s.drain.max_replay_attempts, 2);
        assert!(s.buffers());
    }

    #[test]
    fn parse_rejects_missing_bucket_for_nats_object() {
        let v = serde_json::json!({ "mode": "buffer_and_ack", "backend": "nats_object" });
        assert!(SpoolSpec::parse(Some(&v)).is_err());
    }

    #[test]
    fn parse_rejects_local_disk_without_path() {
        let v = serde_json::json!({ "mode": "buffer_and_ack", "backend": "local_disk" });
        assert!(SpoolSpec::parse(Some(&v)).is_err());
    }

    #[test]
    fn parse_rejects_gcs_without_credential() {
        let v = serde_json::json!({
            "mode": "buffer_and_ack", "backend": "gcs", "bucket": "b"
        });
        assert!(SpoolSpec::parse(Some(&v)).is_err());
    }

    #[test]
    fn parse_rejects_interleave_with_global_ordering() {
        let v = serde_json::json!({
            "mode": "buffer_and_ack", "backend": "local_disk", "path": "/tmp/x",
            "ordering": "global", "drain": { "on_recovery": "interleave" }
        });
        assert!(SpoolSpec::parse(Some(&v)).is_err());
    }

    #[test]
    fn parse_rejects_bad_enum() {
        let v = serde_json::json!({ "mode": "bogus" });
        assert!(SpoolSpec::parse(Some(&v)).is_err());
    }

    #[test]
    fn off_mode_skips_backend_validation() {
        // mode off with no backend config is valid (pure stop-acking).
        let v = serde_json::json!({ "mode": "off" });
        assert!(SpoolSpec::parse(Some(&v)).unwrap().validate().is_ok());
    }

    #[test]
    fn default_retention_has_byte_ceiling() {
        // The cost ceiling (OQ3) is on by default — no silent unbounded buffer.
        assert_eq!(RetentionConfig::default().max_bytes, Some(1024 * 1024 * 1024));
    }
}
