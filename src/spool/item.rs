//! The spooled-item envelope (RFC §8.3 / §8.4).
//!
//! Each spooled message is one durable object. The **payload bytes** go to
//! the backend (NATS Object Store / local disk / GCS / S3); the
//! **metadata + a `noetl://spool/...` ref + SHA-256** go to the event log
//! (`subscription.message.spooled`). That is the same payload-ref split the
//! Result Store already uses (`repos/server/src/services/result_store.rs`).
//!
//! The backend object key is `recv_seq`-prefixed and zero-padded so a plain
//! lexical `list()` returns items in **receive order** — that is what makes
//! `ordering: global` a cheap ordered scan rather than an in-memory sort of
//! the whole backlog.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::tools::source::PolledMessage;

/// Width of the zero-padded `recv_seq` in the object key. u64::MAX is 20
/// decimal digits, so 20 keeps lexical order == numeric order for the whole
/// range.
const RECV_SEQ_WIDTH: usize = 20;

/// One spooled message + the metadata needed to replay it in order with
/// idempotency, retry accounting, and dead-letter routing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpoolItem {
    /// Catalog path of the owning subscription.
    pub subscription: String,
    /// Source kind (`nats` / `pubsub` / `kafka` / `webhook`).
    pub source: String,
    /// Source message id (JetStream seq, Pub/Sub messageId, Kafka
    /// partition:offset). Stable for idempotency keying.
    pub message_id: String,
    /// Explicit idempotency key from the header directive, when present
    /// (RFC §7.2). Wins over `message_id` for dedup (OQ8).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// The key the dedup window + drain idempotency check uses:
    /// `idempotency_key` when present, else `message_id`.
    pub dedup_key: String,
    /// Monotone receive sequence — the global drain order. Assigned by the
    /// runtime as messages arrive.
    pub recv_seq: u64,
    /// Per-key ordering lane (device id, partition, ordering key) for
    /// `ordering: per_key`. `None` → the global lane.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordering_key: Option<String>,
    /// Which declared downstream this message was routed to when spooled —
    /// the breaker that was open (RFC §8.1 per-downstream scope).
    pub downstream: String,
    /// SHA-256 (hex) of the canonical payload bytes — integrity across the
    /// spool round-trip; logged so a reviewer can verify replay fidelity.
    pub sha256: String,
    /// Replay attempts so far; an item exceeding `drain.max_replay_attempts`
    /// is dead-lettered (poison message, OQ6).
    pub attempts: u32,
    /// Epoch-millis when spooled — drives `retention.max_age_hours`.
    pub spooled_at_ms: u64,
    /// Why it was spooled (`circuit_open` / `on_full_*` / `hybrid_escalate`).
    pub reason: String,
    /// The full normalized message envelope, replayed verbatim on drain.
    pub message: PolledMessage,
}

impl SpoolItem {
    /// Build an item from a polled message + the resolved spool context.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subscription: impl Into<String>,
        source: impl Into<String>,
        message: PolledMessage,
        idempotency_key: Option<String>,
        recv_seq: u64,
        ordering_key: Option<String>,
        downstream: impl Into<String>,
        reason: impl Into<String>,
        spooled_at_ms: u64,
    ) -> Self {
        let message_id = message.id.clone();
        let dedup_key = idempotency_key.clone().unwrap_or_else(|| message_id.clone());
        let sha256 = sha256_hex(&canonical_payload_bytes(&message));
        Self {
            subscription: subscription.into(),
            source: source.into(),
            message_id,
            idempotency_key,
            dedup_key,
            recv_seq,
            ordering_key,
            downstream: downstream.into(),
            sha256,
            attempts: 0,
            spooled_at_ms,
            reason: reason.into(),
            message,
        }
    }

    /// The backend object key — `recv_seq`-prefixed (zero-padded) so a
    /// lexical list yields receive order. The message id (sanitized) is
    /// appended for human-debuggability + uniqueness.
    pub fn object_key(&self) -> String {
        format!(
            "{:0width$}-{}",
            self.recv_seq,
            sanitize_key(&self.message_id),
            width = RECV_SEQ_WIDTH
        )
    }

    /// The `noetl://spool/...` ref recorded in the event log alongside the
    /// metadata (the payload-ref split, RFC §8.4).
    pub fn spool_ref(&self) -> String {
        spool_ref(&self.subscription, self.recv_seq, &self.message_id)
    }

    /// The lane this item drains in for the given ordering mode.
    pub fn lane(&self, ordering: super::OrderingMode) -> Option<String> {
        match ordering {
            super::OrderingMode::PerKey => self.ordering_key.clone(),
            super::OrderingMode::Global | super::OrderingMode::None => None,
        }
    }

    /// Re-serialize for backend storage.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// Deserialize from backend bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<SpoolItem, crate::error::ToolError> {
        serde_json::from_slice(bytes).map_err(|e| {
            crate::error::ToolError::Json(format!("spool item decode failed: {e}"))
        })
    }
}

/// The canonical `noetl://spool/<subscription>/<recv_seq>/<message_id>` ref.
pub fn spool_ref(subscription: &str, recv_seq: u64, message_id: &str) -> String {
    format!("noetl://spool/{subscription}/{recv_seq}/{message_id}")
}

/// Parse the `recv_seq` back out of a backend object key (the zero-padded
/// prefix before the first `-`). Used on runtime startup to recover the
/// receive-sequence high-water mark from a surviving spool so new items
/// continue the monotone sequence rather than colliding with the backlog
/// (noetl/ai-meta#93). Returns `None` for a key that isn't in the
/// [`SpoolItem::object_key`] shape.
pub fn recv_seq_from_object_key(key: &str) -> Option<u64> {
    if key.len() < RECV_SEQ_WIDTH {
        return None;
    }
    key[..RECV_SEQ_WIDTH].parse::<u64>().ok()
}

/// Lower-case hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Canonical payload bytes for hashing: the decoded `data` serialized
/// deterministically. Falls back to the raw id when data can't serialize
/// (never expected — `data` is always a valid serde value).
fn canonical_payload_bytes(message: &PolledMessage) -> Vec<u8> {
    serde_json::to_vec(&message.data).unwrap_or_else(|_| message.id.as_bytes().to_vec())
}

/// Object-store-safe key fragment: keep alnum / `-` / `_` / `.`; replace the
/// rest with `_`. Bounds length so a pathological Kafka key can't blow the
/// object name limit.
fn sanitize_key(s: &str) -> String {
    let mut out: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if out.len() > 128 {
        out.truncate(128);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(id: &str, data: serde_json::Value) -> PolledMessage {
        PolledMessage {
            id: id.to_string(),
            data,
            headers: serde_json::Map::new(),
            attributes: serde_json::Value::Null,
            metadata: serde_json::Value::Null,
            ack_id: None,
        }
    }

    #[test]
    fn dedup_key_prefers_idempotency_then_message_id() {
        let i1 = SpoolItem::new("s", "nats", msg("m1", serde_json::json!({})), Some("idem-9".into()), 1, None, "default", "circuit_open", 0);
        assert_eq!(i1.dedup_key, "idem-9");
        let i2 = SpoolItem::new("s", "nats", msg("m2", serde_json::json!({})), None, 2, None, "default", "circuit_open", 0);
        assert_eq!(i2.dedup_key, "m2");
    }

    #[test]
    fn object_keys_sort_in_receive_order() {
        let a = SpoolItem::new("s", "nats", msg("z", serde_json::json!(1)), None, 9, None, "d", "circuit_open", 0);
        let b = SpoolItem::new("s", "nats", msg("a", serde_json::json!(2)), None, 10, None, "d", "circuit_open", 0);
        let c = SpoolItem::new("s", "nats", msg("a", serde_json::json!(3)), None, 100, None, "d", "circuit_open", 0);
        // Lexical sort of the keys must equal numeric recv_seq order despite
        // the message ids sorting the other way.
        let mut keys = vec![b.object_key(), c.object_key(), a.object_key()];
        keys.sort();
        assert_eq!(keys, vec![a.object_key(), b.object_key(), c.object_key()]);
    }

    #[test]
    fn sha256_is_stable_and_payload_sensitive() {
        let a = SpoolItem::new("s", "nats", msg("m", serde_json::json!({"v": 1})), None, 1, None, "d", "circuit_open", 0);
        let b = SpoolItem::new("s", "nats", msg("m", serde_json::json!({"v": 1})), None, 2, None, "d", "circuit_open", 0);
        let c = SpoolItem::new("s", "nats", msg("m", serde_json::json!({"v": 2})), None, 3, None, "d", "circuit_open", 0);
        assert_eq!(a.sha256, b.sha256); // same payload → same hash
        assert_ne!(a.sha256, c.sha256); // different payload → different hash
        assert_eq!(a.sha256.len(), 64);
    }

    #[test]
    fn spool_ref_format() {
        let i = SpoolItem::new("subscriptions/iot", "nats", msg("42", serde_json::json!(1)), None, 7, None, "d", "circuit_open", 0);
        assert_eq!(i.spool_ref(), "noetl://spool/subscriptions/iot/7/42");
    }

    #[test]
    fn bytes_round_trip() {
        let i = SpoolItem::new("s", "kafka", msg("p:1", serde_json::json!({"x": [1,2,3]})), Some("k".into()), 5, Some("dev-7".into()), "wh", "circuit_open", 123);
        let bytes = i.to_bytes();
        let back = SpoolItem::from_bytes(&bytes).unwrap();
        assert_eq!(i, back);
    }

    #[test]
    fn lane_depends_on_ordering() {
        let i = SpoolItem::new("s", "nats", msg("m", serde_json::json!(1)), None, 1, Some("dev-7".into()), "d", "circuit_open", 0);
        assert_eq!(i.lane(super::super::OrderingMode::PerKey).as_deref(), Some("dev-7"));
        assert_eq!(i.lane(super::super::OrderingMode::Global), None);
        assert_eq!(i.lane(super::super::OrderingMode::None), None);
    }

    #[test]
    fn sanitize_key_strips_unsafe_chars() {
        let i = SpoolItem::new("s", "nats", msg("a/b c:d", serde_json::json!(1)), None, 1, None, "d", "circuit_open", 0);
        assert!(i.object_key().ends_with("a_b_c_d"));
    }

    #[test]
    fn recv_seq_round_trips_through_object_key() {
        for seq in [1u64, 7, 42, 1000, u64::MAX] {
            let i = SpoolItem::new("s", "nats", msg("m", serde_json::json!(1)), None, seq, None, "d", "circuit_open", 0);
            assert_eq!(recv_seq_from_object_key(&i.object_key()), Some(seq));
        }
        // not an object key
        assert_eq!(recv_seq_from_object_key("garbage"), None);
        assert_eq!(recv_seq_from_object_key(""), None);
    }
}
