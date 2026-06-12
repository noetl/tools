//! Per-downstream circuit breaker (RFC §8.1, resolves OQ2).
//!
//! The breaker is a **pure** state machine: it owns no I/O. The worker
//! subscription runtime probes the declared downstream (HTTP / TCP / NATS),
//! feeds the probe + dispatch outcomes in via [`CircuitBreaker::record_success`]
//! / [`CircuitBreaker::record_failure`] / [`CircuitBreaker::on_probe`], and
//! asks [`CircuitBreaker::decide`] whether to dispatch, spool, or probe. That
//! split keeps the correctness-critical transition logic unit-testable
//! without a cluster and lets the state be serialized to NATS KV so it
//! survives a runtime restart mid-outage.
//!
//! ### Scope — per downstream dependency, not per subscription (OQ2)
//!
//! "Downstream" = whatever a dispatched run depends on to make progress: a
//! target store / DB it writes to, or a topic it produces to. Keying the
//! breaker per *declared downstream* means one dead dependency only spools
//! the traffic that actually touches it — a subscription fanning to two
//! downstreams keeps flowing to the healthy one. [`CircuitRegistry`] holds
//! one [`CircuitBreaker`] per [`DownstreamSpec::name`].
//!
//! ### The state machine
//!
//! ```text
//!            record_failure × trip_after
//!   Closed ───────────────────────────────▶ Open
//!     ▲                                       │ probe_after_ms elapsed
//!     │ on_probe(ok)                          ▼
//!     └──────────────── HalfOpen ◀────── decide() → Probe
//!                          │ on_probe(!ok)
//!                          └──────────────▶ Open  (reset opened_at)
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::ToolError;

// ---------------------------------------------------------------------------
// Downstream descriptor + probe kind
// ---------------------------------------------------------------------------

/// How the runtime probes a downstream's health.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ProbeKind {
    /// HTTP GET/HEAD the `target` URL; 2xx/3xx/4xx-that-answers = up,
    /// connection refused / timeout / 5xx = down.
    #[default]
    Http,
    /// TCP connect to `host:port`; connect success = up.
    Tcp,
    /// NATS request/flush against the `target` subject / URL.
    Nats,
    /// No active probe — the breaker is fed only by passive dispatch
    /// outcomes (connection refused / 5xx from the dispatched run).
    Passive,
}

impl ProbeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProbeKind::Http => "http",
            ProbeKind::Tcp => "tcp",
            ProbeKind::Nats => "nats",
            ProbeKind::Passive => "passive",
        }
    }
}

/// A declared downstream dependency the circuit breaker keys on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownstreamSpec {
    /// Stable name — the breaker registry key, and the value a directive /
    /// dispatch resolves to so a message maps to the right breaker.
    pub name: String,
    /// How to probe it.
    pub probe: ProbeKind,
    /// Probe target — URL (http), `host:port` (tcp), subject/url (nats).
    /// Optional for [`ProbeKind::Passive`].
    pub target: Option<String>,
}

impl DownstreamSpec {
    /// The implicit single downstream used when a subscription declares no
    /// `circuit.downstream` list — keyed `"default"`, passive-fed.
    pub fn default_named() -> Self {
        Self {
            name: "default".to_string(),
            probe: ProbeKind::Passive,
            target: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Circuit config
// ---------------------------------------------------------------------------

/// Circuit-breaker config (the `spool.circuit` block).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CircuitConfig {
    /// Consecutive failures that trip the circuit open.
    pub trip_after: u32,
    /// How long to stay open before the half-open probe.
    pub probe_after_ms: u64,
    /// Declared downstream dependencies. Empty → a single implicit
    /// `"default"` downstream (the single-downstream case, RFC OQ2 lean).
    pub downstream: Vec<DownstreamSpec>,
    /// How often the runtime actively probes an open downstream's health,
    /// independent of `probe_after_ms` (which gates the half-open dispatch
    /// probe). Bounds how fast recovery is noticed.
    pub probe_interval_ms: u64,
}

impl Default for CircuitConfig {
    fn default() -> Self {
        Self {
            trip_after: 5,
            probe_after_ms: 30_000,
            downstream: Vec::new(),
            probe_interval_ms: 5_000,
        }
    }
}

impl CircuitConfig {
    /// The effective downstream list — the declared one, or a single
    /// implicit `"default"` entry.
    pub fn downstreams(&self) -> Vec<DownstreamSpec> {
        if self.downstream.is_empty() {
            vec![DownstreamSpec::default_named()]
        } else {
            self.downstream.clone()
        }
    }

    pub fn validate(&self) -> Result<(), ToolError> {
        if self.trip_after == 0 {
            return Err(ToolError::Configuration(
                "spool.circuit.trip_after must be >= 1".to_string(),
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for d in &self.downstream {
            if d.name.is_empty() {
                return Err(ToolError::Configuration(
                    "spool.circuit.downstream[].name must be non-empty".to_string(),
                ));
            }
            if !seen.insert(d.name.clone()) {
                return Err(ToolError::Configuration(format!(
                    "spool.circuit.downstream name '{}' is duplicated",
                    d.name
                )));
            }
            if !matches!(d.probe, ProbeKind::Passive)
                && d.target.as_deref().unwrap_or("").is_empty()
            {
                return Err(ToolError::Configuration(format!(
                    "spool.circuit.downstream '{}' probe '{}' requires a 'target'",
                    d.name,
                    d.probe.as_str()
                )));
            }
        }
        Ok(())
    }

    /// Parse the `spool.circuit` block.
    pub fn parse(value: &serde_json::Value) -> Result<CircuitConfig, ToolError> {
        let obj = value.as_object().ok_or_else(|| {
            ToolError::Configuration("spool.circuit must be a mapping".to_string())
        })?;
        let mut cfg = CircuitConfig::default();
        if let Some(t) = obj.get("trip_after").and_then(|v| v.as_u64()) {
            cfg.trip_after = t as u32;
        }
        if let Some(p) = obj.get("probe_after_ms").and_then(|v| v.as_u64()) {
            cfg.probe_after_ms = p;
        }
        if let Some(p) = obj.get("probe_interval_ms").and_then(|v| v.as_u64()) {
            cfg.probe_interval_ms = p;
        }
        if let Some(list) = obj.get("downstream").and_then(|v| v.as_array()) {
            for d in list {
                let name = d
                    .get("name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        ToolError::Configuration(
                            "spool.circuit.downstream[].name is required".to_string(),
                        )
                    })?
                    .to_string();
                let probe = match d.get("type").or_else(|| d.get("probe")) {
                    Some(p) => crate::spool::parse_enum_str(p, "spool.circuit.downstream[].type", &[
                        ("http", ProbeKind::Http),
                        ("tcp", ProbeKind::Tcp),
                        ("nats", ProbeKind::Nats),
                        ("passive", ProbeKind::Passive),
                    ])?,
                    None => ProbeKind::Http,
                };
                let target = d.get("target").and_then(|v| v.as_str()).map(str::to_string);
                cfg.downstream.push(DownstreamSpec { name, probe, target });
            }
        }
        cfg.validate()?;
        Ok(cfg)
    }
}

// ---------------------------------------------------------------------------
// Circuit state (serializable for KV persistence)
// ---------------------------------------------------------------------------

/// The three breaker phases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CircuitPhase {
    /// Healthy — dispatch normally.
    #[default]
    Closed,
    /// Tripped — spool incoming, wait for the probe window.
    Open,
    /// Probing — one attempt is allowed to test recovery.
    HalfOpen,
}

impl CircuitPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            CircuitPhase::Closed => "closed",
            CircuitPhase::Open => "open",
            CircuitPhase::HalfOpen => "half_open",
        }
    }
}

/// Persisted breaker state — written to NATS KV so it survives a runtime
/// restart mid-outage (RFC §8.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CircuitState {
    pub phase: CircuitPhase,
    pub consecutive_failures: u32,
    /// Epoch-millis when the circuit last opened (`None` while closed).
    pub opened_at_ms: Option<u64>,
    /// Epoch-millis of the last phase transition.
    pub last_transition_ms: u64,
    /// Monotone counter of how many times this downstream has tripped —
    /// surfaced for the `circuit.opened` event + ops.
    pub trips: u64,
}

/// What the runtime should do with the next message for this downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitDecision {
    /// Closed — dispatch normally.
    Dispatch,
    /// Open — buffer / stop-ack (don't touch the dead downstream).
    Spool,
    /// The probe window elapsed — let exactly one attempt through to test
    /// recovery. The runtime calls [`CircuitBreaker::on_probe`] with the
    /// result.
    Probe,
}

// ---------------------------------------------------------------------------
// The breaker
// ---------------------------------------------------------------------------

/// A single downstream's circuit breaker. Pure logic; clone-cheap state.
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    trip_after: u32,
    probe_after_ms: u64,
    state: CircuitState,
}

impl CircuitBreaker {
    pub fn new(trip_after: u32, probe_after_ms: u64) -> Self {
        Self {
            trip_after: trip_after.max(1),
            probe_after_ms,
            state: CircuitState::default(),
        }
    }

    /// Rehydrate a breaker from persisted state (KV).
    pub fn from_state(trip_after: u32, probe_after_ms: u64, state: CircuitState) -> Self {
        Self {
            trip_after: trip_after.max(1),
            probe_after_ms,
            state,
        }
    }

    pub fn state(&self) -> &CircuitState {
        &self.state
    }

    pub fn phase(&self) -> CircuitPhase {
        self.state.phase
    }

    /// Decide what to do with the next message at `now_ms`. Pure read —
    /// except it advances Open→HalfOpen when the probe window elapses so a
    /// single [`CircuitDecision::Probe`] is handed out (subsequent calls
    /// before the probe resolves return [`CircuitDecision::Spool`]).
    pub fn decide(&mut self, now_ms: u64) -> CircuitDecision {
        match self.state.phase {
            CircuitPhase::Closed => CircuitDecision::Dispatch,
            CircuitPhase::HalfOpen => CircuitDecision::Spool,
            CircuitPhase::Open => {
                let opened = self.state.opened_at_ms.unwrap_or(now_ms);
                if now_ms.saturating_sub(opened) >= self.probe_after_ms {
                    self.state.phase = CircuitPhase::HalfOpen;
                    self.state.last_transition_ms = now_ms;
                    CircuitDecision::Probe
                } else {
                    CircuitDecision::Spool
                }
            }
        }
    }

    /// Record a successful dispatch / probe to the downstream. Closes the
    /// circuit and resets the failure count. Returns `true` if this caused
    /// a transition to Closed (so the caller can emit `circuit.closed` +
    /// start a drain).
    pub fn record_success(&mut self, now_ms: u64) -> bool {
        let was_open = !matches!(self.state.phase, CircuitPhase::Closed);
        self.state.consecutive_failures = 0;
        if was_open {
            self.state.phase = CircuitPhase::Closed;
            self.state.opened_at_ms = None;
            self.state.last_transition_ms = now_ms;
        }
        was_open
    }

    /// Record a downstream-unavailable failure. Trips the circuit open once
    /// `trip_after` consecutive failures accumulate. Returns `true` if this
    /// caused a fresh transition to Open (so the caller emits
    /// `circuit.opened`).
    pub fn record_failure(&mut self, now_ms: u64) -> bool {
        self.state.consecutive_failures = self.state.consecutive_failures.saturating_add(1);
        if matches!(self.state.phase, CircuitPhase::Closed)
            && self.state.consecutive_failures >= self.trip_after
        {
            self.open(now_ms);
            true
        } else {
            false
        }
    }

    /// Apply a half-open probe result. `ok` → Closed (returns `true` so the
    /// caller drains); `!ok` → back to Open with a fresh probe window.
    pub fn on_probe(&mut self, ok: bool, now_ms: u64) -> bool {
        if ok {
            self.record_success(now_ms)
        } else {
            self.open(now_ms);
            false
        }
    }

    fn open(&mut self, now_ms: u64) {
        let fresh = !matches!(self.state.phase, CircuitPhase::Open);
        self.state.phase = CircuitPhase::Open;
        self.state.opened_at_ms = Some(now_ms);
        self.state.last_transition_ms = now_ms;
        if fresh {
            self.state.trips = self.state.trips.saturating_add(1);
        }
    }

    /// Milliseconds the circuit has been continuously open at `now_ms`, or
    /// `0` when closed. Used by [`crate::spool::SpoolMode::Hybrid`] to
    /// decide when to escalate from stop-ack to buffering.
    pub fn open_for_ms(&self, now_ms: u64) -> u64 {
        match (self.state.phase, self.state.opened_at_ms) {
            (CircuitPhase::Closed, _) | (_, None) => 0,
            (_, Some(opened)) => now_ms.saturating_sub(opened),
        }
    }
}

// ---------------------------------------------------------------------------
// Registry — one breaker per downstream (the OQ2 scoping)
// ---------------------------------------------------------------------------

/// Holds one [`CircuitBreaker`] per declared downstream. A message resolves
/// to a downstream name (default `"default"`); the registry routes it to
/// that downstream's breaker so one dead dependency only spools its own
/// traffic.
#[derive(Debug, Clone)]
pub struct CircuitRegistry {
    trip_after: u32,
    probe_after_ms: u64,
    breakers: BTreeMap<String, CircuitBreaker>,
    downstreams: BTreeMap<String, DownstreamSpec>,
}

impl CircuitRegistry {
    pub fn new(config: &CircuitConfig) -> Self {
        let mut breakers = BTreeMap::new();
        let mut downstreams = BTreeMap::new();
        for d in config.downstreams() {
            breakers.insert(
                d.name.clone(),
                CircuitBreaker::new(config.trip_after, config.probe_after_ms),
            );
            downstreams.insert(d.name.clone(), d);
        }
        Self {
            trip_after: config.trip_after,
            probe_after_ms: config.probe_after_ms,
            breakers,
            downstreams,
        }
    }

    /// The declared downstreams (for the runtime to drive probes).
    pub fn downstreams(&self) -> impl Iterator<Item = &DownstreamSpec> {
        self.downstreams.values()
    }

    /// Resolve which downstream a message touches. The directive/dispatch
    /// `resolved` target (pool or playbook) is matched against declared
    /// downstream names; falls back to `"default"` when present, else the
    /// first declared downstream (single-downstream case).
    pub fn route<'a>(&'a self, resolved: Option<&str>) -> &'a str {
        if let Some(r) = resolved {
            if self.breakers.contains_key(r) {
                return self.breakers.get_key_value(r).unwrap().0;
            }
        }
        if self.breakers.contains_key("default") {
            return self.breakers.get_key_value("default").unwrap().0;
        }
        // First declared downstream.
        self.breakers
            .keys()
            .next()
            .map(String::as_str)
            .unwrap_or("default")
    }

    /// Mutable access to a downstream's breaker (creating it lazily so a
    /// directive-resolved downstream not in the static list still gets a
    /// breaker — fail-safe).
    pub fn breaker_mut(&mut self, name: &str) -> &mut CircuitBreaker {
        let (trip, probe) = (self.trip_after, self.probe_after_ms);
        self.breakers
            .entry(name.to_string())
            .or_insert_with(|| CircuitBreaker::new(trip, probe))
    }

    pub fn breaker(&self, name: &str) -> Option<&CircuitBreaker> {
        self.breakers.get(name)
    }

    /// Snapshot every breaker's state for KV persistence.
    pub fn snapshot(&self) -> BTreeMap<String, CircuitState> {
        self.breakers
            .iter()
            .map(|(k, v)| (k.clone(), v.state().clone()))
            .collect()
    }

    /// Rehydrate breaker states from a KV snapshot.
    pub fn restore(&mut self, snapshot: &BTreeMap<String, CircuitState>) {
        for (name, state) in snapshot {
            let b = CircuitBreaker::from_state(self.trip_after, self.probe_after_ms, state.clone());
            self.breakers.insert(name.clone(), b);
        }
    }

    /// True if any breaker is not Closed (used to gate the drain pass).
    pub fn any_open(&self) -> bool {
        self.breakers
            .values()
            .any(|b| !matches!(b.phase(), CircuitPhase::Closed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trips_after_n_consecutive_failures() {
        let mut b = CircuitBreaker::new(3, 1000);
        assert_eq!(b.decide(0), CircuitDecision::Dispatch);
        assert!(!b.record_failure(0));
        assert!(!b.record_failure(0));
        assert!(b.record_failure(0)); // 3rd → trips
        assert_eq!(b.phase(), CircuitPhase::Open);
        assert_eq!(b.decide(0), CircuitDecision::Spool);
        assert_eq!(b.state().trips, 1);
    }

    #[test]
    fn a_success_resets_the_failure_count() {
        let mut b = CircuitBreaker::new(3, 1000);
        b.record_failure(0);
        b.record_failure(0);
        b.record_success(0); // reset
        assert!(!b.record_failure(0)); // count starts over
        assert_eq!(b.phase(), CircuitPhase::Closed);
    }

    #[test]
    fn half_open_probe_then_close_on_success() {
        let mut b = CircuitBreaker::new(1, 1000);
        b.record_failure(0); // trip at t=0
        assert_eq!(b.decide(500), CircuitDecision::Spool); // window not elapsed
        assert_eq!(b.decide(1000), CircuitDecision::Probe); // window elapsed → half-open
        assert_eq!(b.phase(), CircuitPhase::HalfOpen);
        assert_eq!(b.decide(1000), CircuitDecision::Spool); // only one probe handed out
        assert!(b.on_probe(true, 1100)); // recovers → closed + drain signal
        assert_eq!(b.phase(), CircuitPhase::Closed);
        assert_eq!(b.decide(1100), CircuitDecision::Dispatch);
    }

    #[test]
    fn half_open_probe_failure_reopens() {
        let mut b = CircuitBreaker::new(1, 1000);
        b.record_failure(0);
        assert_eq!(b.decide(1000), CircuitDecision::Probe);
        assert!(!b.on_probe(false, 1000)); // still down → reopen
        assert_eq!(b.phase(), CircuitPhase::Open);
        // fresh window: spool until 1000ms later
        assert_eq!(b.decide(1500), CircuitDecision::Spool);
        assert_eq!(b.decide(2000), CircuitDecision::Probe);
    }

    #[test]
    fn record_failure_returns_true_only_on_fresh_trip() {
        let mut b = CircuitBreaker::new(2, 1000);
        assert!(!b.record_failure(0));
        assert!(b.record_failure(0)); // trips now
        assert!(!b.record_failure(0)); // already open, not a fresh trip
        assert_eq!(b.state().trips, 1);
    }

    #[test]
    fn open_for_ms_tracks_outage_duration() {
        let mut b = CircuitBreaker::new(1, 100_000);
        assert_eq!(b.open_for_ms(0), 0); // closed
        b.record_failure(1000);
        assert_eq!(b.open_for_ms(1000), 0);
        assert_eq!(b.open_for_ms(31_000), 30_000); // 30s open
    }

    #[test]
    fn state_round_trips_through_serde() {
        let mut b = CircuitBreaker::new(2, 5000);
        b.record_failure(10);
        b.record_failure(10); // open
        let json = serde_json::to_string(b.state()).unwrap();
        let restored: CircuitState = serde_json::from_str(&json).unwrap();
        let b2 = CircuitBreaker::from_state(2, 5000, restored);
        assert_eq!(b2.phase(), CircuitPhase::Open);
        assert_eq!(b2.state().trips, 1);
    }

    #[test]
    fn registry_scopes_per_downstream() {
        // Two downstreams; killing one must not spool the other (OQ2).
        let cfg = CircuitConfig {
            trip_after: 1,
            probe_after_ms: 1000,
            downstream: vec![
                DownstreamSpec { name: "warehouse".into(), probe: ProbeKind::Passive, target: None },
                DownstreamSpec { name: "analytics".into(), probe: ProbeKind::Passive, target: None },
            ],
            probe_interval_ms: 1000,
        };
        let mut reg = CircuitRegistry::new(&cfg);
        // warehouse goes down
        reg.breaker_mut("warehouse").record_failure(0);
        assert_eq!(reg.breaker("warehouse").unwrap().phase(), CircuitPhase::Open);
        // analytics is untouched — still dispatches
        assert_eq!(
            reg.breaker_mut("analytics").decide(0),
            CircuitDecision::Dispatch
        );
        assert!(reg.any_open());
    }

    #[test]
    fn registry_routes_resolved_target_else_default() {
        let cfg = CircuitConfig::default(); // single implicit "default"
        let reg = CircuitRegistry::new(&cfg);
        assert_eq!(reg.route(None), "default");
        assert_eq!(reg.route(Some("nonexistent")), "default");
    }

    #[test]
    fn registry_snapshot_restore_round_trip() {
        let cfg = CircuitConfig { trip_after: 1, probe_after_ms: 1000, downstream: vec![
            DownstreamSpec { name: "wh".into(), probe: ProbeKind::Passive, target: None },
        ], probe_interval_ms: 1000 };
        let mut reg = CircuitRegistry::new(&cfg);
        reg.breaker_mut("wh").record_failure(0); // open
        let snap = reg.snapshot();
        let mut reg2 = CircuitRegistry::new(&cfg);
        reg2.restore(&snap);
        assert_eq!(reg2.breaker("wh").unwrap().phase(), CircuitPhase::Open);
    }

    #[test]
    fn config_parse_and_validate() {
        let v = serde_json::json!({
            "trip_after": 4, "probe_after_ms": 8000,
            "downstream": [{"name": "db", "type": "tcp", "target": "db:5432"}]
        });
        let cfg = CircuitConfig::parse(&v).unwrap();
        assert_eq!(cfg.trip_after, 4);
        assert_eq!(cfg.downstream[0].probe, ProbeKind::Tcp);

        // trip_after 0 rejected
        assert!(CircuitConfig::parse(&serde_json::json!({"trip_after": 0})).is_err());
        // probe without target rejected
        assert!(CircuitConfig::parse(&serde_json::json!({
            "downstream": [{"name": "x", "type": "http"}]
        })).is_err());
        // duplicate names rejected
        assert!(CircuitConfig::parse(&serde_json::json!({
            "downstream": [{"name": "x", "type": "passive"}, {"name": "x", "type": "passive"}]
        })).is_err());
    }
}
