//! Real PostgreSQL regression: an i32/i64 decoder cannot deserialize INT2.
//! Run with NOETL_TEST_POSTGRES_URL pointing at an isolated database:
//! cargo test --test postgres_smallint -- --ignored
//! This test issues SELECT only and creates no database objects.
use noetl_tools::tools::PostgresTool;
use serde_json::json;

#[tokio::test]
#[ignore = "requires an explicitly configured isolated PostgreSQL database"]
async fn smallint_values_and_null_survive_both_result_shapes() {
    let url = std::env::var("NOETL_TEST_POSTGRES_URL")
        .expect("set NOETL_TEST_POSTGRES_URL to an isolated PostgreSQL database");
    let query = "SELECT (-32768)::smallint AS low, 32767::smallint AS high, \
                 0::smallint AS zero, NULL::smallint AS absent, \
                 42::integer AS int4, 43::bigint AS int8";
    let tool = PostgresTool::new();
    let objects = tool
        .execute_query(query, &[], &url, None, true)
        .await
        .unwrap();
    assert_eq!(
        objects.data.unwrap()["rows"],
        json!([{
            "low": -32768, "high": 32767, "zero": 0, "absent": null,
            "int4": 42, "int8": 43
        }])
    );
    let arrays = tool
        .execute_query(query, &[], &url, None, false)
        .await
        .unwrap();
    assert_eq!(
        arrays.data.unwrap()["rows"],
        json!([[-32768, 32767, 0, null, 42, 43]])
    );
}
