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
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
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
        writer.finish().context("finish Arrow IPC stream writer")?;
    }
    Ok(buffer)
}

/// Decode Feather V2 / IPC stream bytes back into a list of
/// `RecordBatch` instances.  The list will usually have length 1
/// (single-batch put), but multi-batch streams round-trip cleanly too.
pub fn decode_record_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let cursor = Cursor::new(bytes);
    let reader = StreamReader::try_new(cursor, None).context("open Arrow IPC stream reader")?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.context("read record batch from Arrow IPC stream")?);
    }
    Ok(batches)
}

/// Result of [`try_encode_tabular_json`].  Carries the encoded bytes,
/// the row count (for the producer to put on `noetl_arrow_cache`'s
/// `IpcHint.row_count` field), and the canonical media type that
/// consumers use to discriminate Arrow IPC streams from JSON bytes
/// in `result.reference`.
///
/// The `schema_digest` is currently a fixed `"arrow"` literal — the
/// schema is recoverable from the IPC stream's own first message, so
/// consumers don't strictly need a separate digest field.  Kept on
/// the struct for parity with `noetl-arrow-cache::IpcHint.schema_digest`
/// and so a future version can compute a real digest (e.g. a hash of
/// the column-name + DataType list) without breaking the helper's
/// signature.
#[derive(Debug, Clone)]
pub struct TabularEncoding {
    /// Feather V2 / IPC stream bytes.  Suitable for direct staging
    /// in the `noetl-arrow-cache` shared-memory cache or for the
    /// durable result-store PUT.
    pub bytes: Vec<u8>,
    /// Number of rows the encoded batch carries.
    pub row_count: usize,
    /// Schema-stability hint for the consumer side.  Currently the
    /// fixed literal `"arrow"`; future revisions may compute a
    /// content-based digest here.
    pub schema_digest: String,
    /// Canonical media type for the encoded bytes.  Matches the
    /// `noetl_arrow_cache::IpcHint.media_type` default and the
    /// Python side's `ARROW_STREAM_MEDIA_TYPE`.
    pub media_type: &'static str,
}

/// Canonical media type for Arrow IPC stream payloads.  Mirrors
/// Python's `noetl.core.storage.arrow_ipc.ARROW_STREAM_MEDIA_TYPE`
/// so cross-stack consumers can switch on this constant.
pub const ARROW_STREAM_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";

/// Try to encode a tabular JSON value as Feather V2 / IPC stream
/// bytes.  Returns `None` for values that don't match the canonical
/// tabular shape so callers can fall back to the JSON-bytes path.
///
/// ## Shape detection
///
/// The helper accepts two JSON shapes:
///
/// 1. **Wrapped**: `{ "columns": ["a", "b"], "rows": [...], ... }`
///    (the shape DuckDB / Postgres / Snowflake tool outputs use).
/// 2. **Nested under `data`**: `{ "data": { "columns": [...], "rows": [...] } }`
///    (the shape that lands when the value is the full `ToolResult`
///    JSON, since `ToolResult::success(...)` wraps the tool's payload
///    under the `data` field).
///
/// Rows must be either:
///
/// - **Object rows**: each row is an object whose keys are column
///   names — `{"a": 1, "b": "hello"}`.
/// - **Array rows**: each row is an array whose positions match
///   `columns` — `[1, "hello"]`.
///
/// ## Type inference
///
/// One Arrow type is inferred per column from the first non-null
/// value found across all rows:
///
/// - All integers (any null mix in) → `Int64`.
/// - All numbers (integer + float) → `Float64`.
/// - All booleans → `Boolean`.
/// - Anything else (mixed types, strings, objects, arrays) → `Utf8`
///   with non-string cells stringified via `to_string()`.
///
/// `null` cells round-trip as Arrow nulls (each builder appends
/// `None`).
///
/// ## When to return `None`
///
/// - Value isn't a JSON object.
/// - Object lacks a `rows` field at the top level or nested under
///   `data`.
/// - `rows` field isn't a JSON array.
/// - `rows` array is empty (no schema can be inferred; caller falls
///   back to the JSON-bytes path which encodes the empty `{rows:[]}`
///   structure verbatim).
/// - `columns` is missing AND the first row isn't an object (no way
///   to derive column names).
/// - Arrow encoding itself fails (highly unusual; usually
///   `RecordBatch::try_new` schema/array mismatch).
///
/// On `None`, the caller stages the value as JSON bytes with
/// `media_type = "application/json"`; on `Some(...)`, the caller
/// uses the returned bytes with [`ARROW_STREAM_MEDIA_TYPE`].
pub fn try_encode_tabular_json(value: &serde_json::Value) -> Option<TabularEncoding> {
    // Locate the tabular payload — either at the top level or
    // nested under `data` (the latter is the shape that lands when
    // the value is a full `ToolResult`).
    let payload = if value.get("rows").is_some() {
        value
    } else if let Some(nested) = value.get("data") {
        if nested.get("rows").is_some() {
            nested
        } else {
            return None;
        }
    } else {
        return None;
    };

    let rows = payload.get("rows").and_then(|r| r.as_array())?;
    if rows.is_empty() {
        return None;
    }

    // Determine column names — prefer the explicit `columns` field,
    // fall back to keys of the first object row.
    let column_names: Vec<String> =
        if let Some(cols) = payload.get("columns").and_then(|c| c.as_array()) {
            cols.iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect()
        } else if let Some(first_obj) = rows.first().and_then(|r| r.as_object()) {
            first_obj.keys().cloned().collect()
        } else {
            return None;
        };

    if column_names.is_empty() {
        return None;
    }

    // Per-column type inference + value extraction.
    let mut columns_inferred: Vec<(String, ColumnType, Vec<Option<serde_json::Value>>)> =
        column_names
            .into_iter()
            .map(|name| (name, ColumnType::Unset, Vec::with_capacity(rows.len())))
            .collect();

    for row in rows {
        for (idx, (name, ctype, values)) in columns_inferred.iter_mut().enumerate() {
            let cell = if let Some(obj) = row.as_object() {
                obj.get(name).cloned()
            } else if let Some(arr) = row.as_array() {
                arr.get(idx).cloned()
            } else {
                return None;
            };
            let cell = cell.unwrap_or(serde_json::Value::Null);
            if !cell.is_null() {
                ctype.observe(&cell);
            }
            values.push(if cell.is_null() { None } else { Some(cell) });
        }
    }

    // Build Arrow arrays + fields.
    let mut fields: Vec<Field> = Vec::with_capacity(columns_inferred.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns_inferred.len());

    for (name, ctype, values) in columns_inferred {
        let data_type = ctype.to_arrow();
        let array: ArrayRef = match data_type {
            DataType::Int64 => Arc::new(Int64Array::from_iter(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(|cell| cell.as_i64())),
            )),
            DataType::Float64 => Arc::new(Float64Array::from_iter(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(|cell| cell.as_f64())),
            )),
            DataType::Boolean => Arc::new(BooleanArray::from_iter(
                values
                    .iter()
                    .map(|v| v.as_ref().and_then(|cell| cell.as_bool())),
            )),
            DataType::Utf8 => Arc::new(StringArray::from_iter(values.iter().map(|v| {
                v.as_ref().map(|cell| match cell {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
            }))),
            _ => unreachable!("ColumnType::to_arrow yields only Int64/Float64/Boolean/Utf8"),
        };
        fields.push(Field::new(name.clone(), data_type, true));
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    let row_count = arrays.first().map(|a| a.len()).unwrap_or(0);
    let batch = RecordBatch::try_new(schema, arrays).ok()?;
    let bytes = encode_record_batch(&batch).ok()?;

    Some(TabularEncoding {
        bytes,
        row_count,
        schema_digest: "arrow".to_string(),
        media_type: ARROW_STREAM_MEDIA_TYPE,
    })
}

/// Per-column type observed across rows.  Promoted in the order
/// `Unset → Int64 → Float64`; any non-matching observation collapses
/// to `Utf8` and stays there.  `Boolean` is exclusive — a column is
/// `Boolean` only if every observed cell was a JSON boolean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnType {
    Unset,
    Int64,
    Float64,
    Boolean,
    Utf8,
}

impl ColumnType {
    fn observe(&mut self, value: &serde_json::Value) {
        let next = match value {
            serde_json::Value::Bool(_) => match self {
                ColumnType::Unset | ColumnType::Boolean => ColumnType::Boolean,
                _ => ColumnType::Utf8,
            },
            serde_json::Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    match self {
                        ColumnType::Unset | ColumnType::Int64 => ColumnType::Int64,
                        ColumnType::Float64 => ColumnType::Float64,
                        _ => ColumnType::Utf8,
                    }
                } else {
                    // f64
                    match self {
                        ColumnType::Unset | ColumnType::Int64 | ColumnType::Float64 => {
                            ColumnType::Float64
                        }
                        _ => ColumnType::Utf8,
                    }
                }
            }
            serde_json::Value::String(_)
            | serde_json::Value::Array(_)
            | serde_json::Value::Object(_) => ColumnType::Utf8,
            serde_json::Value::Null => *self,
        };
        *self = next;
    }

    fn to_arrow(self) -> DataType {
        match self {
            ColumnType::Int64 => DataType::Int64,
            ColumnType::Float64 => DataType::Float64,
            ColumnType::Boolean => DataType::Boolean,
            // Unset (all-null column) falls back to Utf8 — Arrow
            // requires a concrete type; Utf8 with all-null cells is
            // the safest round-trip with downstream Python consumers
            // that expect strings.
            ColumnType::Unset | ColumnType::Utf8 => DataType::Utf8,
        }
    }
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
        let event_types = StringArray::from(vec![
            "batch.accepted",
            "batch.processing",
            "batch.completed",
        ]);
        RecordBatch::try_new(schema, vec![Arc::new(event_ids), Arc::new(event_types)])
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

    /// Happy path: DuckDB-shape rowset (`{columns, rows}` with array
    /// rows) encodes cleanly and the decoded batch carries the right
    /// row + column counts + dtypes.
    #[test]
    fn tabular_array_rows_round_trip_via_arrow_ipc() {
        let payload = serde_json::json!({
            "columns": ["id", "name", "score", "active"],
            "rows": [
                [1, "alice", 0.95, true],
                [2, "bob", 0.72, false],
                [3, "carol", 0.88, true],
            ],
            "row_count": 3,
        });
        let encoded = try_encode_tabular_json(&payload).expect("must encode");
        assert_eq!(encoded.row_count, 3);
        assert_eq!(encoded.media_type, ARROW_STREAM_MEDIA_TYPE);
        assert_eq!(encoded.schema_digest, "arrow");

        let batches = decode_record_batches(&encoded.bytes).expect("decode");
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 4);

        // Verify the column types per the inference rules: id=Int64,
        // name=Utf8, score=Float64, active=Boolean.
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
        assert_eq!(batch.schema().field(2).data_type(), &DataType::Float64);
        assert_eq!(batch.schema().field(3).data_type(), &DataType::Boolean);
    }

    /// Object-row shape (the DuckDB `as_objects: true` variant).
    /// Column order derives from the first row's key iteration.
    #[test]
    fn tabular_object_rows_round_trip_via_arrow_ipc() {
        let payload = serde_json::json!({
            "rows": [
                {"id": 1, "label": "x"},
                {"id": 2, "label": "y"},
            ],
            "row_count": 2,
        });
        let encoded = try_encode_tabular_json(&payload).expect("must encode");
        assert_eq!(encoded.row_count, 2);

        let batches = decode_record_batches(&encoded.bytes).expect("decode");
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);
    }

    /// `ToolResult` wraps tool output under `data`.  The helper must
    /// also recognise that nested shape so the worker can call it
    /// directly on `result.context = serde_json::to_value(&ToolResult)`.
    #[test]
    fn tabular_nested_under_data_round_trip_via_arrow_ipc() {
        let payload = serde_json::json!({
            "status": "Success",
            "data": {
                "columns": ["a"],
                "rows": [[1], [2], [3]],
                "row_count": 3,
            },
            "duration_ms": 12,
        });
        let encoded = try_encode_tabular_json(&payload).expect("must encode nested-under-data");
        assert_eq!(encoded.row_count, 3);
    }

    /// Mixed-type column (some ints, some strings) falls back to
    /// `Utf8` and stringifies non-string cells.  Cross-stack
    /// consumers can still read the bytes.
    #[test]
    fn tabular_mixed_type_column_collapses_to_utf8() {
        let payload = serde_json::json!({
            "columns": ["mixed"],
            "rows": [
                [1],
                ["two"],
                [4.2],
            ],
        });
        let encoded = try_encode_tabular_json(&payload).expect("must encode");
        let batches = decode_record_batches(&encoded.bytes).expect("decode");
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(batches[0].num_rows(), 3);
    }

    /// Null cells round-trip as Arrow nulls.
    #[test]
    fn tabular_null_cells_round_trip_as_arrow_nulls() {
        let payload = serde_json::json!({
            "columns": ["id"],
            "rows": [
                [1],
                [null],
                [3],
            ],
        });
        let encoded = try_encode_tabular_json(&payload).expect("must encode");
        let batches = decode_record_batches(&encoded.bytes).expect("decode");
        let batch = &batches[0];
        let col = batch.column(0);
        assert_eq!(col.len(), 3);
        // Middle cell is null.
        assert!(col.is_null(1));
    }

    /// Empty rows array → None.  The caller falls back to the JSON-
    /// bytes path which encodes the empty `{rows:[]}` structure
    /// verbatim.
    #[test]
    fn tabular_empty_rows_returns_none() {
        let payload = serde_json::json!({
            "columns": ["id"],
            "rows": [],
        });
        assert!(try_encode_tabular_json(&payload).is_none());
    }

    /// Missing `rows` field at every level → None.  The caller
    /// falls back to the JSON-bytes path.
    #[test]
    fn tabular_missing_rows_returns_none() {
        let payload = serde_json::json!({
            "stdout": "hello",
            "exit_code": 0,
        });
        assert!(try_encode_tabular_json(&payload).is_none());
    }

    /// Non-object input → None.
    #[test]
    fn tabular_non_object_input_returns_none() {
        assert!(try_encode_tabular_json(&serde_json::json!([1, 2, 3])).is_none());
        assert!(try_encode_tabular_json(&serde_json::json!("hello")).is_none());
        assert!(try_encode_tabular_json(&serde_json::json!(42)).is_none());
        assert!(try_encode_tabular_json(&serde_json::Value::Null).is_none());
    }

    /// All-null column infers `Utf8` (the fallback for `Unset` after
    /// the row pass).  Round-trips cleanly with all-null cells.
    #[test]
    fn tabular_all_null_column_infers_utf8() {
        let payload = serde_json::json!({
            "columns": ["nullable"],
            "rows": [
                [null],
                [null],
            ],
        });
        let encoded = try_encode_tabular_json(&payload).expect("must encode");
        let batches = decode_record_batches(&encoded.bytes).expect("decode");
        let batch = &batches[0];
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert!(batch.column(0).is_null(0));
        assert!(batch.column(0).is_null(1));
    }
}
