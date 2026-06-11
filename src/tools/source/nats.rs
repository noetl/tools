//! NATS JetStream source backend for the `subscription` tool.
//!
//! Generalises the `nats` tool's bounded `js_consume` fetch
//! ([`crate::tools::nats`]) into the shared [`SourceClient`] abstraction.
//! The bounded fetch + normalize + ack loop lives in
//! [`drain_pull_consumer`]; both this backend and the `nats` tool's
//! `js_consume` operation drive it, so there is exactly one NATS bounded
//! drain in the crate.

use async_nats::jetstream::{self, consumer::PullConsumer};
use async_trait::async_trait;
use futures::StreamExt;

use crate::error::ToolError;
use crate::tools::nats::NatsConnParams;

use super::{
    decode_payload, normalize_headers, PollOptions, PollOutcome, PolledMessage, SourceClient,
};

/// A bounded NATS JetStream pull-consumer drain.
///
/// The named durable `consumer` must already exist on `stream` — this
/// backend never creates or alters consumer configurations (same contract
/// as `js_consume`).
pub struct NatsSource {
    conn: NatsConnParams,
    stream: String,
    consumer: String,
}

impl NatsSource {
    /// Build a NATS source from resolved connection params + the
    /// stream/consumer pair.
    pub(crate) fn new(conn: NatsConnParams, stream: String, consumer: String) -> Self {
        Self {
            conn,
            stream,
            consumer,
        }
    }
}

#[async_trait]
impl SourceClient for NatsSource {
    fn source_name(&self) -> &'static str {
        "nats"
    }

    async fn poll(&self, opts: &PollOptions) -> Result<PollOutcome, ToolError> {
        let nc = self.conn.connect().await?;
        let js = jetstream::new(nc);

        let stream = js.get_stream(&self.stream).await.map_err(|e| {
            ToolError::ExecutionFailed(format!(
                "subscription[nats]: stream '{}' not found: {}",
                self.stream, e
            ))
        })?;

        let consumer: PullConsumer = stream.get_consumer(&self.consumer).await.map_err(|e| {
            ToolError::ExecutionFailed(format!(
                "subscription[nats]: consumer '{}' on stream '{}' not found: {}",
                self.consumer, self.stream, e
            ))
        })?;

        drain_pull_consumer(&consumer, opts).await
    }
}

/// The shared bounded NATS drain.
///
/// `fetch()` returns as soon as `max_messages` is reached OR `expires`
/// elapses — whichever comes first — so the call never blocks past the
/// clamped timeout.  Each message is normalized into a [`PolledMessage`];
/// acks happen inline when [`PollOptions::ack`] requests them.
///
/// NATS acks are addressed by the in-flight message handle, not by a
/// durable id, so [`AckMode::Manual`] simply leaves messages pending for
/// redelivery and surfaces no `ack_id` (there is nothing to ack out of
/// band later).
pub(crate) async fn drain_pull_consumer(
    consumer: &PullConsumer,
    opts: &PollOptions,
) -> Result<PollOutcome, ToolError> {
    let mut messages = consumer
        .fetch()
        .max_messages(opts.batch as usize)
        .expires(std::time::Duration::from_millis(opts.timeout_ms))
        .messages()
        .await
        .map_err(|e| {
            ToolError::ExecutionFailed(format!("subscription[nats] fetch failed: {}", e))
        })?;

    let do_ack = opts.ack.should_ack();
    let mut out: Vec<PolledMessage> = Vec::with_capacity(opts.batch as usize);

    while let Some(message_result) = messages.next().await {
        let message = message_result.map_err(|e| {
            ToolError::ExecutionFailed(format!("subscription[nats] message error: {}", e))
        })?;

        let info = message.info().map_err(|e| {
            ToolError::ExecutionFailed(format!("subscription[nats] message info failed: {}", e))
        })?;

        // Normalize headers (lowercased) + preserve the raw multi-value map.
        let mut header_entries: Vec<(String, Vec<String>)> = Vec::new();
        let mut raw = serde_json::Map::new();
        if let Some(hdrs) = &message.headers {
            for (k, v) in hdrs.iter() {
                let values: Vec<String> = v.iter().map(|val| val.to_string()).collect();
                let raw_value = if values.len() == 1 {
                    serde_json::Value::String(values[0].clone())
                } else {
                    serde_json::Value::Array(
                        values
                            .iter()
                            .cloned()
                            .map(serde_json::Value::String)
                            .collect(),
                    )
                };
                raw.insert(k.to_string(), raw_value);
                header_entries.push((k.to_string(), values));
            }
        }

        out.push(PolledMessage {
            id: info.stream_sequence.to_string(),
            data: decode_payload(&message.payload),
            headers: normalize_headers(header_entries),
            attributes: serde_json::Value::Object(raw),
            metadata: serde_json::json!({
                "subject": message.subject.to_string(),
                "stream_seq": info.stream_sequence,
                "consumer_seq": info.consumer_sequence,
                "delivered": info.delivered,
                "pending": info.pending,
            }),
            ack_id: None,
        });

        if do_ack {
            message.ack().await.map_err(|e| {
                ToolError::ExecutionFailed(format!("subscription[nats] ack failed: {}", e))
            })?;
        }
    }

    // NATS has no id-addressable out-of-band ack; manual mode just leaves
    // messages pending, so ack_ids stays empty regardless.
    Ok(PollOutcome {
        messages: out,
        acked: do_ack,
        ack_ids: Vec::new(),
    })
}
