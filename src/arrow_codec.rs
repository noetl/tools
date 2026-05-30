//! Apache Arrow IPC codec — the entry point for the columnar data plane.
//!
//! This module wires the `arrow-rs` crate family ([arrow.apache.org/rust/arrow](https://arrow.apache.org/rust/arrow/index.html))
//! into `noetl-tools` so subsequent tools and the future `noetl-arrow-cache`
//! crate (R-2.1 follow-up per Appendix H of the global hybrid cloud
//! blueprint) can speak the same Feather V2 / IPC bytes that the Python
//! side's `ArrowIpcSharedMemoryCache` produces and consumes.
//!
//! R-2.1 PR-1 (this module) ships ONLY the deps and a round-trip
//! benchmark hook.  The shared-memory cache implementation lives in
//! `noetl-arrow-cache` (forthcoming).  The Arrow Flight gRPC endpoint
//! lives in `noetl-worker` (R-2.2).
//!
//! ## Compatibility contract
//!
//! - Bytes produced by [`encode_record_batch`] are valid Feather V2 / IPC
//!   stream bytes that the Python side reads via `pyarrow.ipc` without
//!   modification.
//! - Bytes produced by Python's `pyarrow.ipc.RecordBatchStreamWriter`
//!   (the writer used by `ArrowIpcSharedMemoryCache.put_arrow_ipc`) are
//!   readable by [`decode_record_batches`].

use std::io::Cursor;

use anyhow::{Context, Result};
use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;

/// Encode a single `RecordBatch` as Feather V2 / IPC stream bytes.
///
/// Reuses the schema from the input batch.  For batches with the same
/// schema, an Arrow IPC consumer can stream multiple batches; this
/// helper is the single-batch convenience.
pub fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buffer: Vec<u8> = Vec::with_capacity(8 * 1024);
    {
        let mut writer = StreamWriter::try_new(&mut buffer, batch.schema_ref())
            .context("create Arrow IPC stream writer")?;
        writer
            .write(batch)
            .context("write record batch to Arrow IPC stream")?;
        writer
            .finish()
            .context("finish Arrow IPC stream writer")?;
    }
    Ok(buffer)
}

/// Decode Feather V2 / IPC stream bytes back into a list of
/// `RecordBatch` instances.  The list will usually have length 1
/// (single-batch put), but multi-batch streams round-trip cleanly too.
pub fn decode_record_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None)
        .context("open Arrow IPC stream reader")?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.context("read record batch from Arrow IPC stream")?);
    }
    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Int64, false),
            Field::new("event_type", DataType::Utf8, false),
        ]));
        let event_ids = Int64Array::from(vec![1, 2, 3]);
        let event_types =
            StringArray::from(vec!["batch.accepted", "batch.processing", "batch.completed"]);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(event_ids), Arc::new(event_types)],
        )
        .expect("build record batch")
    }

    #[test]
    fn round_trip_single_batch_preserves_rows_and_schema() {
        let original = sample_batch();
        let encoded = encode_record_batch(&original).expect("encode");
        let decoded = decode_record_batches(&encoded).expect("decode");

        assert_eq!(decoded.len(), 1, "single-batch stream");
        let decoded_batch = &decoded[0];
        assert_eq!(decoded_batch.num_rows(), 3);
        assert_eq!(decoded_batch.num_columns(), 2);
        assert_eq!(
            decoded_batch.schema_ref().as_ref(),
            original.schema_ref().as_ref(),
        );
    }

    #[test]
    fn encoded_bytes_have_arrow_ipc_magic() {
        // Feather V2 / IPC stream bytes are framed with the
        // continuation marker 0xFFFFFFFF followed by a flatbuffer
        // message.  Confirming the first 4 bytes match the
        // continuation tag is a cheap smoke test that we're actually
        // emitting Arrow IPC, not raw row data.
        let batch = sample_batch();
        let encoded = encode_record_batch(&batch).expect("encode");
        assert!(encoded.len() > 8, "non-trivial output");
        assert_eq!(
            &encoded[0..4],
            &[0xFF, 0xFF, 0xFF, 0xFF],
            "Arrow IPC continuation marker",
        );
    }

    #[test]
    fn decode_rejects_garbage_bytes() {
        let result = decode_record_batches(b"not an arrow ipc stream");
        assert!(result.is_err(), "garbage must not decode");
    }
}
