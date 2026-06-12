//! The spool engine (RFC §8.3) — ties a [`SpoolBackend`] to the ordering,
//! idempotency, dead-letter, and retention rules.
//!
//! The engine is storage + policy; it owns **no** event emission and **no**
//! dispatch transport. The worker subscription runtime drives it:
//!
//! - on the dispatch path, when a downstream's circuit is open, the runtime
//!   calls [`SpoolEngine::admit`] (retention ceiling check) then
//!   [`SpoolEngine::spool`], and emits `subscription.message.spooled`;
//! - on recovery, the runtime calls [`SpoolEngine::drain`] with a closure
//!   that POSTs `/api/execute` for each replayed item and emits
//!   `subscription.message.replayed`; the engine returns the dead-letter
//!   list so the runtime emits `subscription.message.dead_lettered`.
//!
//! Keeping event emission in the worker preserves `execution_id`
//! correlation and the single `POST /api/events` path; keeping ordering /
//! idempotency / retention here makes the data-loss-safety logic
//! unit-testable against an in-memory backend without a cluster.

use std::collections::HashSet;

use crate::error::ToolError;

use super::backend::SpoolBackend;
use super::item::SpoolItem;
use super::{DrainOnRecovery, OnFull, OrderingMode, SpoolSpec};

/// What [`SpoolEngine::admit`] decided about accepting one more message.
#[derive(Debug, Clone, PartialEq)]
pub enum Admission {
    /// Within the ceiling — spool it.
    Accept,
    /// `on_full: stop_acking` and the ceiling is hit — don't spool; the
    /// runtime falls back to not-acking so the source redelivers.
    RejectStopAck,
    /// `on_full: drop_to_dlq` — accept after dead-lettering the oldest
    /// items (their keys returned so the runtime can emit events).
    AcceptAfterEvict(Vec<DeadLetter>),
    /// `on_full: alert_only` — accept but the ceiling is breached; the
    /// runtime emits an alert event.
    AcceptWithAlert { spool_bytes: u64 },
}

/// A dead-lettered item — a poison message (too many replay attempts) or an
/// evicted / expired one. Returned to the runtime so it emits
/// `subscription.message.dead_lettered`.
#[derive(Debug, Clone, PartialEq)]
pub struct DeadLetter {
    pub message_id: String,
    pub dedup_key: String,
    pub recv_seq: u64,
    pub spool_ref: String,
    pub attempts: u32,
    pub reason: String,
}

impl DeadLetter {
    fn from_item(item: &SpoolItem, reason: impl Into<String>) -> Self {
        Self {
            message_id: item.message_id.clone(),
            dedup_key: item.dedup_key.clone(),
            recv_seq: item.recv_seq,
            spool_ref: item.spool_ref(),
            attempts: item.attempts,
            reason: reason.into(),
        }
    }
}

/// Outcome of one [`SpoolEngine::drain`] pass.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DrainReport {
    /// Items successfully replayed (downstream accepted) + GC'd.
    pub replayed: u64,
    /// Items skipped because their dedup key was already seen (idempotency).
    pub deduped: u64,
    /// Items dead-lettered this pass (poison messages).
    pub dead_lettered: Vec<DeadLetter>,
    /// Items still in the spool after this pass (drain stopped early under
    /// ordered draining because the downstream failed again).
    pub remaining: u64,
    /// True when the spool fully drained (no remaining, no early stop).
    pub fully_drained: bool,
}

/// Outcome of an age-based retention sweep.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GcReport {
    /// Items removed because they exceeded `max_age_hours`.
    pub expired: Vec<DeadLetter>,
    /// Bytes remaining after the sweep.
    pub spool_bytes: u64,
}

/// The drain transport: replay one item, returning `Ok` when the downstream
/// accepted it, `Err` when it's still failing.
pub type DrainResult = Result<(), ToolError>;

/// Ties a backend to the spool spec's ordering / idempotency / retention /
/// dead-letter rules.
pub struct SpoolEngine {
    spec: SpoolSpec,
    backend: Box<dyn SpoolBackend>,
    dlq: Box<dyn SpoolBackend>,
    /// Dedup keys already dispatched (live or replayed) — the idempotency
    /// window for this runtime's lifetime. Durable cross-restart dedup is
    /// the opt-in server-side window (RFC OQ2 / Phase 7).
    seen: HashSet<String>,
}

impl SpoolEngine {
    /// Build an engine over `backend` (live spool) + `dlq` (dead letters).
    pub fn new(spec: SpoolSpec, backend: Box<dyn SpoolBackend>, dlq: Box<dyn SpoolBackend>) -> Self {
        Self {
            spec,
            backend,
            dlq,
            seen: HashSet::new(),
        }
    }

    pub fn spec(&self) -> &SpoolSpec {
        &self.spec
    }

    /// Record that a dedup key was dispatched live (so a later spooled
    /// duplicate of the same logical message is deduped on drain).
    pub fn mark_dispatched(&mut self, dedup_key: &str) {
        self.seen.insert(dedup_key.to_string());
    }

    /// Current spooled byte total (the gauge value).
    pub async fn spool_bytes(&self) -> Result<u64, ToolError> {
        self.backend.total_bytes().await
    }

    /// Number of items currently spooled.
    pub async fn len(&self) -> Result<usize, ToolError> {
        self.backend.len().await
    }

    pub async fn is_empty(&self) -> Result<bool, ToolError> {
        self.backend.is_empty().await
    }

    /// Decide whether one more item (`incoming_bytes`) fits under the
    /// retention ceiling, applying `retention.on_full`.
    pub async fn admit(&self, now_ms: u64, incoming_bytes: u64) -> Result<Admission, ToolError> {
        let max_bytes = match self.spec.retention.max_bytes {
            None => return Ok(Admission::Accept), // no ceiling
            Some(m) => m,
        };
        let current = self.backend.total_bytes().await?;
        if current + incoming_bytes <= max_bytes {
            return Ok(Admission::Accept);
        }
        match self.spec.retention.on_full {
            OnFull::StopAcking => Ok(Admission::RejectStopAck),
            OnFull::AlertOnly => Ok(Admission::AcceptWithAlert { spool_bytes: current }),
            OnFull::DropToDlq => {
                // Evict oldest until there's room (or nothing left to evict).
                let metas = self.backend.list().await?;
                let mut evicted = Vec::new();
                let mut freed = 0u64;
                for meta in metas {
                    if current.saturating_sub(freed) + incoming_bytes <= max_bytes {
                        break;
                    }
                    if let Ok(item) = self.backend.get(&meta.key).await {
                        self.dlq.put(&item).await.ok();
                        evicted.push(DeadLetter::from_item(&item, "on_full_evicted"));
                    }
                    self.backend.delete(&meta.key).await.ok();
                    freed += meta.size;
                }
                let _ = now_ms;
                Ok(Admission::AcceptAfterEvict(evicted))
            }
        }
    }

    /// Persist one spooled item. Returns the `noetl://spool/...` ref + sha256
    /// for the `subscription.message.spooled` event the runtime emits.
    pub async fn spool(&mut self, item: &SpoolItem) -> Result<SpooledRef, ToolError> {
        self.backend.put(item).await?;
        Ok(SpooledRef {
            spool_ref: item.spool_ref(),
            sha256: item.sha256.clone(),
            recv_seq: item.recv_seq,
        })
    }

    /// Age-based retention sweep — dead-letter items older than
    /// `max_age_hours`. Returns the expired items + remaining bytes.
    pub async fn gc_expired(&mut self, now_ms: u64) -> Result<GcReport, ToolError> {
        let max_age_ms = match self.spec.retention.max_age_hours {
            None => {
                return Ok(GcReport {
                    expired: Vec::new(),
                    spool_bytes: self.backend.total_bytes().await?,
                })
            }
            Some(h) => h.saturating_mul(3_600_000),
        };
        let mut expired = Vec::new();
        for meta in self.backend.list().await? {
            if let Ok(item) = self.backend.get(&meta.key).await {
                if now_ms.saturating_sub(item.spooled_at_ms) >= max_age_ms {
                    self.dlq.put(&item).await.ok();
                    self.backend.delete(&meta.key).await.ok();
                    expired.push(DeadLetter::from_item(&item, "retention_expired"));
                }
            }
        }
        Ok(GcReport {
            expired,
            spool_bytes: self.backend.total_bytes().await?,
        })
    }

    /// Replay the spool on recovery, honoring ordering + idempotency +
    /// dead-letter. `dispatch` POSTs the replay for one item; `Ok` means the
    /// downstream accepted it (it is then GC'd), `Err` means still failing.
    ///
    /// Drain semantics per ordering:
    /// - `global` — strict `recv_seq` order; a transient failure stops the
    ///   pass (the circuit re-trips); a poison item is dead-lettered and the
    ///   drain continues (an auditable gap, OQ6).
    /// - `per_key` — independent lanes; a stuck lane stops only itself.
    /// - `none` — any order; transient failures skip, poison dead-letters.
    pub async fn drain<F, Fut>(&mut self, mut dispatch: F) -> Result<DrainReport, ToolError>
    where
        F: FnMut(SpoolItem) -> Fut,
        Fut: std::future::Future<Output = DrainResult>,
    {
        let metas = self.backend.list().await?; // receive order
        let mut items = Vec::with_capacity(metas.len());
        for m in metas {
            if let Ok(it) = self.backend.get(&m.key).await {
                items.push((m.key, it));
            }
        }

        let mut report = DrainReport::default();
        let max_attempts = self.spec.drain.max_replay_attempts.max(1);

        // Lane key per ordering mode. global / none share a single lane;
        // per_key splits by ordering_key.
        let lane_of = |it: &SpoolItem| -> String {
            match self.spec.ordering {
                OrderingMode::PerKey => it
                    .lane(OrderingMode::PerKey)
                    .unwrap_or_else(|| "__global__".to_string()),
                OrderingMode::Global | OrderingMode::None => "__global__".to_string(),
            }
        };
        // A lane that hit a transient failure is "stalled" — under ordered
        // semantics later items in the same lane must wait.
        let mut stalled_lanes: HashSet<String> = HashSet::new();
        let ordered = !matches!(self.spec.ordering, OrderingMode::None);

        for (key, mut item) in items {
            let lane = lane_of(&item);
            if ordered && stalled_lanes.contains(&lane) {
                report.remaining += 1;
                continue; // preserve order within the stalled lane
            }

            // Idempotency: already dispatched this logical message → GC + skip.
            if self.seen.contains(&item.dedup_key) {
                self.backend.delete(&key).await.ok();
                report.deduped += 1;
                continue;
            }

            match dispatch(item.clone()).await {
                Ok(()) => {
                    self.seen.insert(item.dedup_key.clone());
                    self.backend.delete(&key).await.ok(); // GC drained item
                    report.replayed += 1;
                }
                Err(_) => {
                    item.attempts = item.attempts.saturating_add(1);
                    if item.attempts >= max_attempts {
                        // Poison → dead-letter; under global ordering this is
                        // an auditable gap (OQ6); the drain continues.
                        self.dlq.put(&item).await.ok();
                        self.backend.delete(&key).await.ok();
                        report.dead_lettered.push(DeadLetter::from_item(&item, "max_replay_attempts"));
                    } else {
                        // Persist the incremented attempt count + stall the lane.
                        self.backend.put(&item).await.ok();
                        report.remaining += 1;
                        if ordered {
                            stalled_lanes.insert(lane);
                        }
                    }
                }
            }
        }

        report.fully_drained = report.remaining == 0;
        Ok(report)
    }

    /// Whether the drain should run backlog-first before live (RFC §8.3
    /// `drain.on_recovery`).
    pub fn drain_before_live(&self) -> bool {
        matches!(self.spec.drain.on_recovery, DrainOnRecovery::OrderedThenLive)
    }
}

/// The ref + integrity hash of a just-spooled item, for the spooled event.
#[derive(Debug, Clone, PartialEq)]
pub struct SpooledRef {
    pub spool_ref: String,
    pub sha256: String,
    pub recv_seq: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spool::backend::{SpoolBackend, SpoolMeta};
    use crate::tools::source::PolledMessage;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // In-memory backend for deterministic engine tests (no cluster, no fs).
    #[derive(Default, Clone)]
    struct MemBackend {
        items: Arc<Mutex<std::collections::BTreeMap<String, SpoolItem>>>,
    }

    #[async_trait::async_trait]
    impl SpoolBackend for MemBackend {
        fn kind(&self) -> &'static str {
            "mem"
        }
        async fn put(&self, item: &SpoolItem) -> Result<(), ToolError> {
            self.items.lock().await.insert(item.object_key(), item.clone());
            Ok(())
        }
        async fn list(&self) -> Result<Vec<SpoolMeta>, ToolError> {
            Ok(self
                .items
                .lock()
                .await
                .iter()
                .map(|(k, v)| SpoolMeta { key: k.clone(), size: v.to_bytes().len() as u64 })
                .collect())
        }
        async fn get(&self, key: &str) -> Result<SpoolItem, ToolError> {
            self.items
                .lock()
                .await
                .get(key)
                .cloned()
                .ok_or_else(|| ToolError::ExecutionFailed(format!("no {key}")))
        }
        async fn delete(&self, key: &str) -> Result<(), ToolError> {
            self.items.lock().await.remove(key);
            Ok(())
        }
    }

    fn msg(id: &str, data: serde_json::Value, key: Option<&str>) -> PolledMessage {
        let mut headers = serde_json::Map::new();
        if let Some(k) = key {
            headers.insert("ordering_key".to_string(), serde_json::json!(k));
        }
        PolledMessage {
            id: id.to_string(),
            data,
            headers,
            attributes: serde_json::Value::Null,
            metadata: serde_json::Value::Null,
            ack_id: None,
        }
    }

    fn item(seq: u64, id: &str, key: Option<&str>) -> SpoolItem {
        SpoolItem::new(
            "subscriptions/t",
            "nats",
            msg(id, serde_json::json!({"seq": seq}), key),
            None,
            seq,
            key.map(str::to_string),
            "default",
            "circuit_open",
            seq,
        )
    }

    fn engine(spec: SpoolSpec) -> SpoolEngine {
        SpoolEngine::new(spec, Box::new(MemBackend::default()), Box::new(MemBackend::default()))
    }

    fn spec_with(ordering: OrderingMode, max_attempts: u32) -> SpoolSpec {
        let mut s = SpoolSpec::off();
        s.mode = super::super::SpoolMode::BufferAndAck;
        s.ordering = ordering;
        s.drain.max_replay_attempts = max_attempts;
        s.retention.max_bytes = None; // no ceiling for these tests
        s
    }

    #[tokio::test]
    async fn drain_replays_in_global_order_then_gcs() {
        let mut eng = engine(spec_with(OrderingMode::Global, 3));
        for (seq, id) in [(3, "c"), (1, "a"), (2, "b")] {
            eng.spool(&item(seq, id, None)).await.unwrap();
        }
        let order = Arc::new(Mutex::new(Vec::<u64>::new()));
        let o2 = order.clone();
        let report = eng
            .drain(move |it| {
                let o = o2.clone();
                async move {
                    o.lock().await.push(it.recv_seq);
                    Ok(())
                }
            })
            .await
            .unwrap();
        assert_eq!(report.replayed, 3);
        assert!(report.fully_drained);
        assert_eq!(*order.lock().await, vec![1, 2, 3]); // strict receive order
        assert!(eng.is_empty().await.unwrap()); // GC'd after drain
    }

    #[tokio::test]
    async fn drain_idempotency_skips_already_dispatched() {
        let mut eng = engine(spec_with(OrderingMode::None, 3));
        // Same logical message spooled under two recv_seqs (redelivered).
        let mut a = item(1, "dup", None);
        a.dedup_key = "dup".into();
        let mut b = item(2, "dup", None);
        b.dedup_key = "dup".into();
        eng.spool(&a).await.unwrap();
        eng.spool(&b).await.unwrap();
        let count = Arc::new(Mutex::new(0u32));
        let c2 = count.clone();
        let report = eng
            .drain(move |_it| {
                let c = c2.clone();
                async move {
                    *c.lock().await += 1;
                    Ok(())
                }
            })
            .await
            .unwrap();
        assert_eq!(*count.lock().await, 1); // dispatched exactly once
        assert_eq!(report.replayed, 1);
        assert_eq!(report.deduped, 1);
    }

    #[tokio::test]
    async fn poison_message_dead_letters_after_max_attempts() {
        let mut eng = engine(spec_with(OrderingMode::None, 2));
        eng.spool(&item(1, "poison", None)).await.unwrap();
        // Always fails → after 2 attempts becomes a dead letter.
        // Two drain passes (one attempt each).
        let _ = eng.drain(|_it| async { Err(ToolError::ExecutionFailed("down".into())) }).await.unwrap();
        let r2 = eng.drain(|_it| async { Err(ToolError::ExecutionFailed("down".into())) }).await.unwrap();
        assert_eq!(r2.dead_lettered.len(), 1);
        assert_eq!(r2.dead_lettered[0].reason, "max_replay_attempts");
        assert!(eng.is_empty().await.unwrap()); // removed from live spool
    }

    #[tokio::test]
    async fn global_order_stops_on_transient_failure_preserving_order() {
        let mut eng = engine(spec_with(OrderingMode::Global, 5));
        for (seq, id) in [(1, "a"), (2, "b"), (3, "c")] {
            eng.spool(&item(seq, id, None)).await.unwrap();
        }
        // Fail on recv_seq 2 → drain must stop, NOT skip ahead to 3.
        let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
        let s2 = seen.clone();
        let report = eng
            .drain(move |it| {
                let s = s2.clone();
                async move {
                    s.lock().await.push(it.recv_seq);
                    if it.recv_seq == 2 {
                        Err(ToolError::ExecutionFailed("still down".into()))
                    } else {
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();
        // saw 1 (ok), 2 (fail → stall lane); never attempted 3
        assert_eq!(*seen.lock().await, vec![1, 2]);
        assert_eq!(report.replayed, 1);
        assert!(!report.fully_drained);
        assert_eq!(report.remaining, 2); // 2 and 3 still spooled
    }

    #[tokio::test]
    async fn per_key_lanes_are_independent() {
        let mut eng = engine(spec_with(OrderingMode::PerKey, 5));
        // lane A: seq 1,3 ; lane B: seq 2 (B is down, A is fine)
        eng.spool(&item(1, "a1", Some("A"))).await.unwrap();
        eng.spool(&item(2, "b1", Some("B"))).await.unwrap();
        eng.spool(&item(3, "a2", Some("A"))).await.unwrap();
        let report = eng
            .drain(|it| async move {
                if it.ordering_key.as_deref() == Some("B") {
                    Err(ToolError::ExecutionFailed("B down".into()))
                } else {
                    Ok(())
                }
            })
            .await
            .unwrap();
        // Both A items replay despite B stalling its own lane.
        assert_eq!(report.replayed, 2);
        assert_eq!(report.remaining, 1); // the B item
    }

    #[tokio::test]
    async fn admit_stop_acking_when_ceiling_hit() {
        let mut s = spec_with(OrderingMode::None, 3);
        s.retention.max_bytes = Some(10); // tiny ceiling
        s.retention.on_full = OnFull::StopAcking;
        let mut eng = engine(s);
        eng.spool(&item(1, "a", None)).await.unwrap(); // already over 10 bytes
        let admission = eng.admit(0, 100).await.unwrap();
        assert_eq!(admission, Admission::RejectStopAck);
    }

    #[tokio::test]
    async fn admit_drop_to_dlq_evicts_oldest() {
        let mut s = spec_with(OrderingMode::None, 3);
        s.retention.on_full = OnFull::DropToDlq;
        let mut eng = engine(s.clone());
        // Fill, then set a ceiling that forces eviction.
        eng.spool(&item(1, "a", None)).await.unwrap();
        eng.spool(&item(2, "b", None)).await.unwrap();
        let bytes = eng.spool_bytes().await.unwrap();
        // Rebuild engine with a ceiling just under current so admit evicts.
        let mut s2 = s;
        s2.retention.max_bytes = Some(bytes / 2);
        eng.spec = s2;
        let admission = eng.admit(0, 1).await.unwrap();
        match admission {
            Admission::AcceptAfterEvict(evicted) => assert!(!evicted.is_empty()),
            other => panic!("expected eviction, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn gc_expired_removes_old_items() {
        let mut s = spec_with(OrderingMode::None, 3);
        s.retention.max_age_hours = Some(1);
        let mut eng = engine(s);
        // spooled_at_ms = 0 (seq used as ts in helper)
        eng.spool(&item(1, "old", None)).await.unwrap();
        // now = 2 hours later
        let report = eng.gc_expired(2 * 3_600_000).await.unwrap();
        assert_eq!(report.expired.len(), 1);
        assert_eq!(report.expired[0].reason, "retention_expired");
        assert!(eng.is_empty().await.unwrap());
    }

    #[tokio::test]
    async fn mark_dispatched_dedupes_subsequent_spool_replay() {
        let mut eng = engine(spec_with(OrderingMode::None, 3));
        // Live dispatch marks the key, then the same message gets spooled and
        // drained — must be deduped, not double-dispatched.
        eng.mark_dispatched("m1");
        let mut it = item(1, "m1", None);
        it.dedup_key = "m1".into();
        eng.spool(&it).await.unwrap();
        let report = eng.drain(|_it| async { Ok(()) }).await.unwrap();
        assert_eq!(report.replayed, 0);
        assert_eq!(report.deduped, 1);
    }
}
