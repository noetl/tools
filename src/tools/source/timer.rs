//! `timer` subscription source — a clock as a message source.
//!
//! It emits one message every `every` (e.g. `60s`, `5m`, `1h`, `500ms`, or a
//! bare number of seconds). With it, `noetl subscribe` becomes a self-contained
//! edge daemon: a long-lived process that runs a playbook on a schedule, with
//! the same event log, header directives and store-and-forward spool as a
//! NATS / Pub/Sub / Kafka subscription. No cron, launchd or systemd timer.
//!
//! ```yaml
//! apiVersion: noetl.io/v2
//! kind: Subscription
//! spec:
//!   source: timer
//!   every: 60s
//!   immediate: true          # first tick at start (default true)
//!   payload: { site: ranch } # merged into every tick message
//!   dispatch: { playbook: ./cycle.yaml }
//! ```
//!
//! Semantics:
//! - **One tick per poll.** A tick is never batched, so a run that overruns the
//!   period produces exactly one catch-up tick, never a burst.
//! - **Fixed delay from the fire time.** The next tick is due `every` after the
//!   previous one fired. Because the subscribe loop dispatches a message before
//!   polling again, runs never overlap.
//! - **No ack.** Ticks are not redelivered: a tick that fails is not retried;
//!   the next tick is the retry.
//! - Message `data` = `payload` (object) plus `tick` (1-based counter),
//!   `scheduled_at` and `fired_at` (RFC 3339 UTC). Its `id` is
//!   `timer-<start epoch ms>-<tick>`, unique per process lifetime.

use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::error::ToolError;

use super::{PollOptions, PollOutcome, PolledMessage, SourceClient};

/// Smallest accepted period. A timer is a scheduler, not a busy loop.
pub const TIMER_MIN_EVERY: Duration = Duration::from_millis(100);

struct TimerState {
    next_due: Instant,
    next_due_wall: chrono::DateTime<chrono::Utc>,
    tick: u64,
}

pub struct TimerSource {
    every: Duration,
    payload: serde_json::Map<String, serde_json::Value>,
    started_ms: i64,
    state: Mutex<TimerState>,
}

impl TimerSource {
    pub fn new(
        every: Duration,
        immediate: bool,
        payload: Option<serde_json::Value>,
    ) -> Result<Self, ToolError> {
        if every < TIMER_MIN_EVERY {
            return Err(ToolError::Configuration(format!(
                "subscription[timer]: 'every' must be at least {} ms",
                TIMER_MIN_EVERY.as_millis()
            )));
        }
        let payload = match payload {
            None | Some(serde_json::Value::Null) => serde_json::Map::new(),
            Some(serde_json::Value::Object(m)) => m,
            Some(_) => {
                return Err(ToolError::Configuration(
                    "subscription[timer]: 'payload' must be a mapping".into(),
                ))
            }
        };
        let now = chrono::Utc::now();
        let first = if immediate { Duration::ZERO } else { every };
        Ok(Self {
            every,
            payload,
            started_ms: now.timestamp_millis(),
            state: Mutex::new(TimerState {
                next_due: Instant::now() + first,
                next_due_wall: now + chrono::Duration::from_std(first).unwrap_or_default(),
                tick: 0,
            }),
        })
    }

    fn message(&self, tick: u64, scheduled: chrono::DateTime<chrono::Utc>) -> PolledMessage {
        let mut data = self.payload.clone();
        data.insert("tick".into(), serde_json::json!(tick));
        data.insert(
            "scheduled_at".into(),
            serde_json::json!(scheduled.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
        );
        data.insert(
            "fired_at".into(),
            serde_json::json!(
                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            ),
        );
        PolledMessage {
            id: format!("timer-{}-{}", self.started_ms, tick),
            data: serde_json::Value::Object(data),
            headers: serde_json::Map::new(),
            attributes: serde_json::json!({}),
            metadata: serde_json::json!({ "every_ms": self.every.as_millis() as u64 }),
            ack_id: None,
        }
    }
}

/// Parse `every`: `"500ms"`, `"30s"`, `"5m"`, `"1h"`, a bare number string, or
/// a JSON number (seconds).
pub fn parse_every(value: Option<&serde_json::Value>) -> Result<Duration, ToolError> {
    let bad = |v: &str| {
        ToolError::Configuration(format!(
            "subscription[timer]: invalid 'every' {v:?} (use e.g. 500ms, 30s, 5m, 1h, or seconds)"
        ))
    };
    match value {
        None => Err(ToolError::Configuration(
            "subscription[timer] requires 'every' (e.g. 60s)".into(),
        )),
        Some(serde_json::Value::Number(n)) => n
            .as_f64()
            .filter(|s| s.is_finite() && *s > 0.0)
            .map(Duration::from_secs_f64)
            .ok_or_else(|| bad(&n.to_string())),
        Some(serde_json::Value::String(s)) => {
            let t = s.trim().to_ascii_lowercase();
            let (num, unit) = match t.find(|c: char| !(c.is_ascii_digit() || c == '.')) {
                Some(i) => (&t[..i], t[i..].trim()),
                None => (t.as_str(), "s"),
            };
            let n: f64 = num.parse().map_err(|_| bad(s))?;
            if !n.is_finite() || n <= 0.0 {
                return Err(bad(s));
            }
            let secs = match unit {
                "ms" => n / 1000.0,
                "s" | "sec" | "secs" => n,
                "m" | "min" | "mins" => n * 60.0,
                "h" | "hr" | "hrs" => n * 3600.0,
                _ => return Err(bad(s)),
            };
            Ok(Duration::from_secs_f64(secs))
        }
        Some(other) => Err(bad(&other.to_string())),
    }
}

#[async_trait]
impl SourceClient for TimerSource {
    fn source_name(&self) -> &'static str {
        "timer"
    }

    /// Wait up to `opts.timeout_ms` for the next tick. Returns one message if
    /// it fired in that window, an empty outcome otherwise.
    async fn poll(&self, opts: &PollOptions) -> Result<PollOutcome, ToolError> {
        let mut st = self.state.lock().await;
        let deadline = Instant::now() + Duration::from_millis(opts.timeout_ms);
        if st.next_due > Instant::now() {
            tokio::time::sleep_until(st.next_due.min(deadline)).await;
        }
        if Instant::now() < st.next_due {
            return Ok(PollOutcome {
                messages: vec![],
                acked: true,
                ack_ids: vec![],
            });
        }
        st.tick += 1;
        let msg = self.message(st.tick, st.next_due_wall);
        st.next_due = Instant::now() + self.every;
        st.next_due_wall =
            chrono::Utc::now() + chrono::Duration::from_std(self.every).unwrap_or_default();
        Ok(PollOutcome {
            messages: vec![msg],
            acked: true,
            ack_ids: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::source::AckMode;

    fn opts(timeout_ms: u64) -> PollOptions {
        PollOptions {
            batch: 100,
            timeout_ms,
            ack: AckMode::OnSuccess,
        }
    }

    #[test]
    fn parses_every() {
        let p = |v: serde_json::Value| parse_every(Some(&v)).unwrap();
        assert_eq!(p(serde_json::json!("500ms")), Duration::from_millis(500));
        assert_eq!(p(serde_json::json!("30s")), Duration::from_secs(30));
        assert_eq!(p(serde_json::json!("5m")), Duration::from_secs(300));
        assert_eq!(p(serde_json::json!("1h")), Duration::from_secs(3600));
        assert_eq!(p(serde_json::json!("90")), Duration::from_secs(90));
        assert_eq!(p(serde_json::json!(2)), Duration::from_secs(2));
        for bad in [
            serde_json::json!("0s"),
            serde_json::json!("-1m"),
            serde_json::json!("5d"),
            serde_json::json!("soon"),
            serde_json::json!(true),
        ] {
            assert!(parse_every(Some(&bad)).is_err(), "{bad} must be rejected");
        }
        assert!(parse_every(None).is_err());
    }

    #[test]
    fn rejects_tiny_period_and_non_mapping_payload() {
        assert!(TimerSource::new(Duration::from_millis(10), true, None).is_err());
        assert!(
            TimerSource::new(Duration::from_secs(1), true, Some(serde_json::json!([1]))).is_err()
        );
    }

    #[tokio::test]
    async fn immediate_tick_carries_payload_then_waits_a_period() {
        let t = TimerSource::new(
            Duration::from_millis(300),
            true,
            Some(serde_json::json!({ "site": "ranch" })),
        )
        .unwrap();
        let first = t.poll(&opts(50)).await.unwrap();
        assert_eq!(first.count(), 1, "immediate: the first poll fires");
        let m = &first.messages[0];
        assert_eq!(m.data["tick"], 1);
        assert_eq!(m.data["site"], "ranch");
        assert!(m.id.starts_with("timer-") && m.id.ends_with("-1"));
        assert!(m.ack_id.is_none());

        let early = t.poll(&opts(50)).await.unwrap();
        assert_eq!(early.count(), 0, "not due within the poll window → empty");

        let due = t.poll(&opts(1000)).await.unwrap();
        assert_eq!(
            due.count(),
            1,
            "fires once the period elapses inside the window"
        );
        assert_eq!(due.messages[0].data["tick"], 2);
    }

    #[tokio::test]
    async fn not_immediate_waits_first_period() {
        let t = TimerSource::new(Duration::from_millis(200), false, None).unwrap();
        assert_eq!(t.poll(&opts(50)).await.unwrap().count(), 0);
        assert_eq!(t.poll(&opts(1000)).await.unwrap().count(), 1);
    }

    #[tokio::test]
    async fn overrun_gives_one_catch_up_tick_not_a_burst() {
        let t = TimerSource::new(Duration::from_millis(100), true, None).unwrap();
        assert_eq!(t.poll(&opts(10)).await.unwrap().count(), 1);
        tokio::time::sleep(Duration::from_millis(450)).await; // a "run" overran 4 periods
        let catch_up = t.poll(&opts(10)).await.unwrap();
        assert_eq!(catch_up.count(), 1);
        assert_eq!(catch_up.messages[0].data["tick"], 2);
        assert_eq!(
            t.poll(&opts(10)).await.unwrap().count(),
            0,
            "no backlog of missed ticks"
        );
    }
}
