//! Integration test for the `nats_object` spool backend (RFC #90 Phase 4)
//! against a real NATS JetStream Object Store.
//!
//! Gated on `NOETL_TEST_NATS_URL` so it is a no-op in CI without a broker.
//! Run against the local kind NATS (port-forwarded to localhost:4222):
//!
//! ```bash
//! NOETL_TEST_NATS_URL=nats://localhost:4222 \
//!   NOETL_TEST_NATS_USER=noetl NOETL_TEST_NATS_PASSWORD=noetl \
//!   cargo test --test spool_nats_object -- --nocapture
//! ```
//!
//! Proves end-to-end against real NATS: a spool item written via
//! [`NatsObjectBackend::put`] round-trips byte-identically through
//! `get`, `list` returns items in **receive order** (the property that makes
//! `ordering: global` a cheap ordered scan), `total_bytes` reflects the
//! stored payload, and `delete` GCs an item — the exact operations the
//! drain + retention paths depend on.

use noetl_tools::spool::backend::{NatsObjectBackend, SpoolBackend};
use noetl_tools::spool::item::SpoolItem;
use noetl_tools::tools::source::PolledMessage;

fn nats_url() -> Option<String> {
    std::env::var("NOETL_TEST_NATS_URL").ok()
}

async fn connect_js() -> async_nats::jetstream::Context {
    let url = nats_url().expect("NOETL_TEST_NATS_URL set (checked by caller)");
    let mut opts = async_nats::ConnectOptions::new();
    if let (Ok(u), Ok(p)) = (
        std::env::var("NOETL_TEST_NATS_USER"),
        std::env::var("NOETL_TEST_NATS_PASSWORD"),
    ) {
        opts = opts.user_and_password(u, p);
    }
    let client = opts.connect(&url).await.expect("nats connect");
    async_nats::jetstream::new(client)
}

fn item(seq: u64, id: &str, data: serde_json::Value) -> SpoolItem {
    let msg = PolledMessage {
        id: id.to_string(),
        data,
        headers: serde_json::Map::new(),
        attributes: serde_json::Value::Null,
        metadata: serde_json::Value::Null,
        ack_id: None,
    };
    SpoolItem::new(
        "subscriptions/itest",
        "nats",
        msg,
        None,
        seq,
        None,
        "default",
        "circuit_open",
        seq,
    )
}

#[tokio::test]
async fn nats_object_backend_roundtrip_and_order() {
    if nats_url().is_none() {
        eprintln!("skipping: NOETL_TEST_NATS_URL not set");
        return;
    }
    let js = connect_js().await;
    // Unique bucket per run so concurrent runs don't collide. NATS object
    // store bucket names allow [A-Za-z0-9_-]; derive from the pid + nanos.
    let suffix = format!("{}", std::process::id());
    let bucket = format!("noetl_spool_itest_{suffix}");

    let backend = NatsObjectBackend::open(&js, &bucket)
        .await
        .expect("open backend");

    // empty to start
    assert_eq!(backend.len().await.unwrap(), 0);

    // Put out of order; list must come back in receive (recv_seq) order.
    backend.put(&item(3, "c", serde_json::json!({"v": 3}))).await.unwrap();
    backend.put(&item(1, "a", serde_json::json!({"v": 1}))).await.unwrap();
    backend.put(&item(2, "b", serde_json::json!({"v": 2}))).await.unwrap();

    let metas = backend.list().await.unwrap();
    assert_eq!(metas.len(), 3, "all three items listed");

    // receive order
    let first = backend.get(&metas[0].key).await.unwrap();
    let last = backend.get(&metas[2].key).await.unwrap();
    assert_eq!(first.recv_seq, 1, "list is in receive order (first = seq 1)");
    assert_eq!(last.recv_seq, 3, "list is in receive order (last = seq 3)");

    // byte-identical round-trip (integrity)
    assert_eq!(first.message.data, serde_json::json!({"v": 1}));
    assert_eq!(first.sha256.len(), 64);

    // total bytes reflects stored payload
    assert!(backend.total_bytes().await.unwrap() > 0);

    // delete GCs one item
    backend.delete(&metas[0].key).await.unwrap();
    assert_eq!(backend.len().await.unwrap(), 2);
    // delete is idempotent
    backend.delete(&metas[0].key).await.unwrap();

    // cleanup the test bucket
    let _ = js.delete_object_store(&bucket).await;
}

#[tokio::test]
async fn nats_object_backend_overwrite_is_idempotent_on_key() {
    if nats_url().is_none() {
        return;
    }
    let js = connect_js().await;
    let bucket = format!("noetl_spool_idem_{}", std::process::id());
    let backend = NatsObjectBackend::open(&js, &bucket).await.unwrap();

    // Same recv_seq + id → same object key → overwrite (at-least-once spool
    // inherits the source's redelivery without double-storing).
    backend.put(&item(7, "same", serde_json::json!(1))).await.unwrap();
    backend.put(&item(7, "same", serde_json::json!(2))).await.unwrap();
    assert_eq!(backend.len().await.unwrap(), 1);

    let _ = js.delete_object_store(&bucket).await;
}
