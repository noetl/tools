//! Apache Kafka source backend for the `subscription` tool.
//!
//! Feature-gated (`kafka`).  Implements a bounded consumer-group poll over
//! the pure-Rust [`kafka`](https://docs.rs/kafka) crate (no `librdkafka` C
//! dependency).  The crate's consumer is synchronous, so the whole bounded
//! drain — build consumer, one fetch round, optional offset commit — runs
//! inside [`tokio::task::spawn_blocking`] and never blocks the async
//! runtime past the worker-slot cap (`fetch_max_wait` is set to
//! `timeout_ms`).
//!
//! ### Phase 1 limitations (documented, not hidden)
//!
//! - **Record headers are not surfaced.** The `kafka` crate's message type
//!   exposes key + value + offset but not Kafka 0.11 record headers, so
//!   `headers` / `attributes` come back empty for this backend.  A richer
//!   client (rdkafka) would be needed; deferred past Phase 1 — the
//!   header-directive engine itself lands in Phase 2.
//! - **`batch` is a soft cap.** One fetch round may return a full message
//!   set; the drain returns the whole round (never acking a message it
//!   didn't return) rather than splitting a set.
//! - **Plaintext brokers.** TLS/SASL are out of scope for Phase 1; the
//!   default features that pull OpenSSL are disabled.

use async_trait::async_trait;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::time::Duration;

use crate::error::ToolError;

use super::{decode_payload, PollOptions, PollOutcome, PolledMessage, SourceClient};

/// A bounded Kafka consumer-group poll.
pub struct KafkaSource {
    brokers: Vec<String>,
    topic: String,
    group: String,
}

impl KafkaSource {
    /// Build a Kafka source from brokers + topic + consumer group.
    pub(crate) fn new(brokers: Vec<String>, topic: String, group: String) -> Self {
        Self {
            brokers,
            topic,
            group,
        }
    }
}

#[async_trait]
impl SourceClient for KafkaSource {
    fn source_name(&self) -> &'static str {
        "kafka"
    }

    async fn poll(&self, opts: &PollOptions) -> Result<PollOutcome, ToolError> {
        let brokers = self.brokers.clone();
        let topic = self.topic.clone();
        let group = self.group.clone();
        let batch = opts.batch as usize;
        let timeout_ms = opts.timeout_ms;
        let do_ack = opts.ack.should_ack();

        // The kafka crate is synchronous; run the whole bounded drain on a
        // blocking thread so the async runtime isn't held.
        let result = tokio::task::spawn_blocking(move || {
            drain_kafka(&brokers, &topic, &group, batch, timeout_ms, do_ack)
        })
        .await
        .map_err(|e| {
            ToolError::ExecutionFailed(format!("subscription[kafka] join error: {}", e))
        })??;

        Ok(result)
    }
}

/// The synchronous bounded Kafka drain (runs inside `spawn_blocking`).
fn drain_kafka(
    brokers: &[String],
    topic: &str,
    group: &str,
    batch: usize,
    timeout_ms: u64,
    do_ack: bool,
) -> Result<PollOutcome, ToolError> {
    let mut consumer = Consumer::from_hosts(brokers.to_vec())
        .with_topic(topic.to_string())
        .with_group(group.to_string())
        // A fresh group with no committed offset reads from the start, so a
        // bounded drain over a new group doesn't silently skip the backlog.
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(Some(GroupOffsetStorage::Kafka))
        .with_fetch_max_wait_time(Duration::from_millis(timeout_ms))
        .create()
        .map_err(|e| {
            ToolError::ExecutionFailed(format!(
                "subscription[kafka] consumer create failed for topic '{}': {}",
                topic, e
            ))
        })?;

    let message_sets = consumer.poll().map_err(|e| {
        ToolError::ExecutionFailed(format!("subscription[kafka] poll failed: {}", e))
    })?;

    let mut out: Vec<PolledMessage> = Vec::new();
    for ms in message_sets.iter() {
        let partition = ms.partition();
        for m in ms.messages() {
            let key = if m.key.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(String::from_utf8_lossy(m.key).into_owned())
            };
            out.push(PolledMessage {
                id: format!("{}:{}:{}", topic, partition, m.offset),
                data: decode_payload(m.value),
                // The kafka crate doesn't surface record headers (see module
                // docs) — empty for this backend in Phase 1.
                headers: serde_json::Map::new(),
                attributes: serde_json::Value::Object(serde_json::Map::new()),
                metadata: serde_json::json!({
                    "topic": topic,
                    "partition": partition,
                    "offset": m.offset,
                    "key": key,
                }),
                ack_id: None,
            });
        }
        // Mark the whole set consumed so the committed offset advances past
        // exactly the messages we returned.
        consumer.consume_messageset(ms).map_err(|e| {
            ToolError::ExecutionFailed(format!("subscription[kafka] consume_messageset: {}", e))
        })?;
        // Honor the soft batch cap: stop fetching more sets once reached.
        if out.len() >= batch {
            break;
        }
    }

    let acked = if do_ack && !out.is_empty() {
        consumer.commit_consumed().map_err(|e| {
            ToolError::ExecutionFailed(format!("subscription[kafka] commit_consumed: {}", e))
        })?;
        true
    } else {
        false
    };

    Ok(PollOutcome {
        messages: out,
        acked,
        // Kafka acks are offset commits, not id-addressable; manual mode
        // simply doesn't commit, so no ack ids ride back.
        ack_ids: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_source_fields() {
        let src = KafkaSource::new(
            vec!["localhost:9092".to_string()],
            "edge.clicks".to_string(),
            "noetl-edge".to_string(),
        );
        assert_eq!(src.source_name(), "kafka");
        assert_eq!(src.topic, "edge.clicks");
        assert_eq!(src.group, "noetl-edge");
        assert_eq!(src.brokers, vec!["localhost:9092".to_string()]);
    }
}
