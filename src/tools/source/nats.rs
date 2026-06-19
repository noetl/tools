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
    decode_payload, normalize_headers, AckDisposition, AckReport, PollOptions, PollOutcome,
    PolledMessage, SourceClient,
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

    /// Dispose deferred ack handles out of band.
    ///
    /// NATS JetStream acks are published to the per-message `$JS.ACK.*` reply
    /// subject — a server-side subject that any connection can publish to, so
    /// the handle captured under [`AckMode::Defer`] (the reply-subject string)
    /// disposes the message from a fresh connection, even a different process,
    /// as long as the consumer's ack-wait hasn't expired and redelivered it.
    ///
    /// The disposition maps to the JetStream ack protocol bytes:
    /// `Ack` → empty body (`AckAck`), `Nack` → `-NAK` (+ optional
    /// `{"delay": <ns>}`), `Term` → `+TERM`. We `flush()` before returning so
    /// the ack is on the wire — durability requires confirming the dispose
    /// reached the server, not just buffering it.
    async fn ack(
        &self,
        ack_ids: &[String],
        disposition: AckDisposition,
    ) -> Result<AckReport, ToolError> {
        if ack_ids.is_empty() {
            return Ok(AckReport::default());
        }

        let nc = self.conn.connect().await?;
        let payload = ack_payload(disposition);

        let mut report = AckReport::default();
        for subject in ack_ids {
            match nc.publish(subject.clone(), payload.clone().into()).await {
                Ok(()) => report.disposed += 1,
                Err(e) => report
                    .errors
                    .push(format!("ack publish to '{}' failed: {}", subject, e)),
            }
        }

        // Confirm the ack bytes left the client before we report success — an
        // un-flushed ack that never reaches the server would silently let the
        // message redeliver, defeating ack-after-processing.
        nc.flush().await.map_err(|e| {
            ToolError::ExecutionFailed(format!("subscription[nats] ack flush failed: {}", e))
        })?;

        Ok(report)
    }
}

/// JetStream ack-protocol body for a disposition.
fn ack_payload(disposition: AckDisposition) -> Vec<u8> {
    match disposition {
        // An empty body to the ack subject is a positive ack (AckAck).
        AckDisposition::Ack => Vec::new(),
        AckDisposition::Nack { delay_ms: None } => b"-NAK".to_vec(),
        AckDisposition::Nack {
            delay_ms: Some(ms),
        } => format!("-NAK {{\"delay\": {}}}", (ms as u128) * 1_000_000)
            .into_bytes(),
        AckDisposition::Term => b"+TERM".to_vec(),
    }
}

/// The shared bounded NATS drain.
///
/// `fetch()` returns as soon as `max_messages` is reached OR `expires`
/// elapses — whichever comes first — so the call never blocks past the
/// clamped timeout.  Each message is normalized into a [`PolledMessage`];
/// acks happen inline when [`PollOptions::ack`] requests them.
///
/// Ack behaviour by mode:
/// - `on_success` / `auto` — ack each message inline before returning.
/// - `manual` / `none` — leave messages pending; surface no `ack_id` (the
///   legacy `js_consume ack: false` shape — they redeliver on the next drain).
/// - `defer` — do NOT ack inline; capture each message's `$JS.ACK.*` reply
///   subject as a durable `ack_id` so a later [`SourceClient::ack`] disposes
///   it after downstream processing succeeds. Un-disposed handles redeliver
///   after the consumer's ack-wait.
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
    let defer = opts.ack.defers_ack();
    let mut out: Vec<PolledMessage> = Vec::with_capacity(opts.batch as usize);
    let mut ack_ids: Vec<String> = Vec::new();

    while let Some(message_result) = messages.next().await {
        let message = message_result.map_err(|e| {
            ToolError::ExecutionFailed(format!("subscription[nats] message error: {}", e))
        })?;

        let info = message.info().map_err(|e| {
            ToolError::ExecutionFailed(format!("subscription[nats] message info failed: {}", e))
        })?;

        // Under `defer`, the durable ack handle is the message's `$JS.ACK.*`
        // reply subject — a server-side subject any connection can publish to,
        // so the handle survives the drain and disposes out of band.
        let ack_id = if defer {
            match message.reply.as_ref() {
                Some(reply) => {
                    let subj = reply.to_string();
                    ack_ids.push(subj.clone());
                    Some(subj)
                }
                None => {
                    // No reply subject means the consumer is not ack-explicit
                    // (or the message isn't ackable) — deferred-ack can't be
                    // honoured, so fail loudly rather than silently dropping
                    // durability.
                    return Err(ToolError::ExecutionFailed(format!(
                        "subscription[nats] deferred-ack requested but message at stream_seq {} \
                         carries no ack reply subject (consumer must be ack-explicit)",
                        info.stream_sequence
                    )));
                }
            }
        } else {
            None
        };

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
            ack_id,
        });

        if do_ack {
            message.ack().await.map_err(|e| {
                ToolError::ExecutionFailed(format!("subscription[nats] ack failed: {}", e))
            })?;
        }
    }

    // `acked` is true only when the drain itself acked (on_success/auto).
    // Under `defer` the messages are in-flight pending an out-of-band ack, so
    // `acked` stays false and the durable handles ride back in `ack_ids`.
    Ok(PollOutcome {
        messages: out,
        acked: do_ack,
        ack_ids,
    })
}
