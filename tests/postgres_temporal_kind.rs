//! Kind-cluster integration check for the postgres tool's row→JSON
//! temporal/identity conversions (noetl/ai-meta#95).
//!
//! Gated on `NOETL_PG_KIND_DSN` so it never runs in CI / unit runs
//! without a live database and never hardcodes a credential.  Point it
//! at the local kind postgres and run:
//!
//! ```sh
//! NOETL_PG_KIND_DSN='postgresql://noetl:noetl@localhost:54321/noetl' \
//!   cargo test --test postgres_temporal_kind -- --nocapture
//! ```
//!
//! The test proves the regression end-to-end on the same kind table:
//!   * `before` — a local replica of the PRE-FIX arm set (the arms the
//!     tool shipped through v3.14.1: i64/i32/f64/bool/String/json/
//!     `DateTime<Utc>`) returns `null` for `timestamp`, `date`, `time`,
//!     `uuid`, `numeric`, and `bytea`.
//!   * `after` — the real `PostgresTool::execute_query` returns the
//!     actual values for every one of those columns.

use noetl_tools::tools::PostgresTool;

/// Replica of the pre-#95 `pg_value_to_json` arm set, used to
/// reproduce the "before" null on the same kind row.  Intentionally
/// stops at the `DateTime<Utc>` (timestamptz) arm.
fn old_pg_value_to_json(row: &tokio_postgres::Row, idx: usize) -> serde_json::Value {
    if let Ok(v) = row.try_get::<_, Option<i64>>(idx) {
        return v.map(|n| serde_json::json!(n)).unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<i32>>(idx) {
        return v.map(|n| serde_json::json!(n)).unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<f64>>(idx) {
        return v.map(|n| serde_json::json!(n)).unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<bool>>(idx) {
        return v.map(|b| serde_json::json!(b)).unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
        return v.map(|s| serde_json::json!(s)).unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<serde_json::Value>>(idx) {
        return v.unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
        return v
            .map(|dt| serde_json::json!(dt.to_rfc3339()))
            .unwrap_or(serde_json::Value::Null);
    }
    serde_json::Value::Null
}

const DDL: &str = "\
    DROP TABLE IF EXISTS noetl_pg95_probe;\n\
    CREATE TABLE noetl_pg95_probe (\n\
        ts_tz   TIMESTAMPTZ NOT NULL,\n\
        ts      TIMESTAMP   NOT NULL,\n\
        d       DATE        NOT NULL,\n\
        t       TIME        NOT NULL,\n\
        u       UUID        NOT NULL,\n\
        n       NUMERIC     NOT NULL,\n\
        b       BYTEA       NOT NULL\n\
    );\n\
    INSERT INTO noetl_pg95_probe VALUES (\n\
        '2026-06-14T12:30:45Z',\n\
        '2026-06-14T12:30:45',\n\
        '2026-06-14',\n\
        '23:59:01',\n\
        '00000000-0000-0000-0000-000000000abc',\n\
        1234.5678,\n\
        decode('0102ff', 'hex')\n\
    );";

const SELECT: &str =
    "SELECT ts_tz, ts, d, t, u, n, b FROM noetl_pg95_probe";

#[tokio::test]
async fn temporal_columns_roundtrip_against_kind_postgres() {
    let dsn = match std::env::var("NOETL_PG_KIND_DSN") {
        Ok(v) => v,
        Err(_) => {
            eprintln!("skipping: NOETL_PG_KIND_DSN not set");
            return;
        }
    };

    // Seed the probe table on the kind postgres.
    let (client, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to kind postgres");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client.batch_execute(DDL).await.expect("seed probe table");

    // --- BEFORE: pre-fix arm set returns null for the affected columns.
    let rows = client.query(SELECT, &[]).await.expect("select probe row");
    let row = &rows[0];
    let before: Vec<serde_json::Value> =
        (0..7).map(|i| old_pg_value_to_json(row, i)).collect();
    eprintln!("BEFORE (pre-#95 arms): {before:?}");
    // ts_tz worked already (DateTime<Utc> arm); the rest fell to null.
    assert!(before[0].is_string(), "timestamptz worked before the fix");
    for (i, col) in ["timestamp", "date", "time", "uuid", "numeric", "bytea"]
        .iter()
        .enumerate()
    {
        assert_eq!(
            before[i + 1],
            serde_json::Value::Null,
            "pre-fix arm set should null `{col}`"
        );
    }

    // --- AFTER: the real tool returns the actual values.
    let result = PostgresTool::new()
        .execute_query(SELECT, &[], &dsn, None, true)
        .await
        .expect("execute_query");
    let data = result.data.expect("result data");
    let obj = &data["rows"][0];
    eprintln!("AFTER (PostgresTool): {obj}");

    assert_eq!(obj["ts_tz"], serde_json::json!("2026-06-14T12:30:45+00:00"));
    assert_eq!(obj["ts"], serde_json::json!("2026-06-14T12:30:45"));
    assert_eq!(obj["d"], serde_json::json!("2026-06-14"));
    assert_eq!(obj["t"], serde_json::json!("23:59:01"));
    assert_eq!(
        obj["u"],
        serde_json::json!("00000000-0000-0000-0000-000000000abc")
    );
    assert_eq!(obj["n"], serde_json::json!("1234.5678"));
    // 0x0102ff → base64 "AQL/".
    assert_eq!(obj["b"], serde_json::json!("AQL/"));

    // Every affected column is now non-null.
    for col in ["ts", "d", "t", "u", "n", "b"] {
        assert!(!obj[col].is_null(), "`{col}` must not be null after the fix");
    }

    client
        .batch_execute("DROP TABLE IF EXISTS noetl_pg95_probe;")
        .await
        .ok();
}
