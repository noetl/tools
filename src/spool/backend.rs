//! Spool backends (RFC §8.3) — the durable buffer that holds payload bytes
//! while a downstream is unavailable.
//!
//! Two backends ship here:
//!
//! - [`NatsObjectBackend`] — wraps the NATS Object Store the `nats` tool
//!   already speaks (`object_put` / `object_get` / `object_list` /
//!   `object_delete`). The in-cluster default: NATS is already deployed, so
//!   no external bucket credential is needed and the live outage proof runs
//!   against it.
//! - [`LocalDiskBackend`] — one JSON file per item under a directory; the
//!   CLI / dev backend (RFC Mode/Phase 6 reuses it).
//!
//! `gcs` / `s3` backends (the tenant object store via keychain-auth) are
//! the same trait with an HTTP/SDK body; they are feature-gated and tracked
//! separately — the trait is the seam so adding them is additive.
//!
//! Every backend stores items keyed by [`super::item::SpoolItem::object_key`]
//! (a zero-padded `recv_seq` prefix), so [`SpoolBackend::list`] returns them
//! in **receive order** — the cheap path for `ordering: global`.

use async_trait::async_trait;

use crate::error::ToolError;

use super::item::SpoolItem;

/// One stored item's metadata, returned by [`SpoolBackend::list`] in
/// receive order (lexical by object key == numeric by `recv_seq`).
#[derive(Debug, Clone, PartialEq)]
pub struct SpoolMeta {
    /// Backend object key ([`SpoolItem::object_key`]).
    pub key: String,
    /// Stored byte size — summed for the retention/cost gauge.
    pub size: u64,
}

/// A durable store-and-forward buffer. Implementations hold whatever
/// connection / handle they need; the trait is pure CRUD over keyed items.
#[async_trait]
pub trait SpoolBackend: Send + Sync {
    /// Backend kind name for events / metrics (`nats_object`, `local_disk`).
    fn kind(&self) -> &'static str;

    /// Persist one item. Idempotent on the object key — re-writing the same
    /// key (a redelivered message spooled twice) overwrites rather than
    /// duplicating, so the spool inherits the source's at-least-once
    /// guarantee without double-storing.
    async fn put(&self, item: &SpoolItem) -> Result<(), ToolError>;

    /// List every stored item's metadata in receive order.
    async fn list(&self) -> Result<Vec<SpoolMeta>, ToolError>;

    /// Fetch + decode one item by key.
    async fn get(&self, key: &str) -> Result<SpoolItem, ToolError>;

    /// Delete one item by key (GC after a successful drain).
    async fn delete(&self, key: &str) -> Result<(), ToolError>;

    /// Total bytes currently stored — the live value for the
    /// `noetl_subscription_spool_bytes` gauge + the `max_bytes` ceiling.
    async fn total_bytes(&self) -> Result<u64, ToolError> {
        Ok(self.list().await?.iter().map(|m| m.size).sum())
    }

    /// Number of items currently stored.
    async fn len(&self) -> Result<usize, ToolError> {
        Ok(self.list().await?.len())
    }

    /// True when the spool is empty.
    async fn is_empty(&self) -> Result<bool, ToolError> {
        Ok(self.len().await? == 0)
    }
}

// ---------------------------------------------------------------------------
// local_disk backend
// ---------------------------------------------------------------------------

/// One JSON file per item under `dir`. The CLI / dev backend.
#[derive(Debug, Clone)]
pub struct LocalDiskBackend {
    dir: std::path::PathBuf,
}

impl LocalDiskBackend {
    /// Open (creating if absent) the spool directory.
    pub async fn open(dir: impl Into<std::path::PathBuf>) -> Result<Self, ToolError> {
        let dir = dir.into();
        tokio::fs::create_dir_all(&dir).await.map_err(|e| {
            ToolError::Io(format!("spool dir '{}' create failed: {e}", dir.display()))
        })?;
        Ok(Self { dir })
    }

    fn path_for(&self, key: &str) -> std::path::PathBuf {
        self.dir.join(format!("{key}.json"))
    }
}

#[async_trait]
impl SpoolBackend for LocalDiskBackend {
    fn kind(&self) -> &'static str {
        "local_disk"
    }

    async fn put(&self, item: &SpoolItem) -> Result<(), ToolError> {
        let path = self.path_for(&item.object_key());
        tokio::fs::write(&path, item.to_bytes()).await.map_err(|e| {
            ToolError::Io(format!("spool write '{}' failed: {e}", path.display()))
        })
    }

    async fn list(&self) -> Result<Vec<SpoolMeta>, ToolError> {
        let mut rd = match tokio::fs::read_dir(&self.dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(ToolError::Io(format!("spool list failed: {e}"))),
        };
        let mut metas = Vec::new();
        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|e| ToolError::Io(format!("spool list entry failed: {e}")))?
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(key) = name.strip_suffix(".json") else {
                continue;
            };
            let size = entry
                .metadata()
                .await
                .map(|m| m.len())
                .unwrap_or(0);
            metas.push(SpoolMeta { key: key.to_string(), size });
        }
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(metas)
    }

    async fn get(&self, key: &str) -> Result<SpoolItem, ToolError> {
        let path = self.path_for(key);
        let bytes = tokio::fs::read(&path).await.map_err(|e| {
            ToolError::Io(format!("spool read '{}' failed: {e}", path.display()))
        })?;
        SpoolItem::from_bytes(&bytes)
    }

    async fn delete(&self, key: &str) -> Result<(), ToolError> {
        let path = self.path_for(key);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()), // already gone — idempotent
            Err(e) => Err(ToolError::Io(format!(
                "spool delete '{}' failed: {e}",
                path.display()
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// nats_object backend
// ---------------------------------------------------------------------------

/// Wraps a NATS Object Store bucket — the in-cluster default. Reuses the
/// same `async-nats` Object Store ops the `nats` tool exposes.
#[derive(Clone)]
pub struct NatsObjectBackend {
    store: async_nats::jetstream::object_store::ObjectStore,
    bucket: String,
}

impl NatsObjectBackend {
    /// Open (creating if absent) the Object Store bucket on `js`.
    pub async fn open(
        js: &async_nats::jetstream::Context,
        bucket: &str,
    ) -> Result<Self, ToolError> {
        // Try to open; create on first use so the runtime is self-bootstrapping
        // (ops doesn't have to pre-provision the bucket).
        let store = match js.get_object_store(bucket).await {
            Ok(s) => s,
            Err(_) => js
                .create_object_store(async_nats::jetstream::object_store::Config {
                    bucket: bucket.to_string(),
                    description: Some("NoETL subscription spool (RFC #90 Phase 4)".to_string()),
                    ..Default::default()
                })
                .await
                .map_err(|e| {
                    ToolError::ExecutionFailed(format!(
                        "spool object store bucket '{bucket}' open/create failed: {e}"
                    ))
                })?,
        };
        Ok(Self {
            store,
            bucket: bucket.to_string(),
        })
    }
}

#[async_trait]
impl SpoolBackend for NatsObjectBackend {
    fn kind(&self) -> &'static str {
        "nats_object"
    }

    async fn put(&self, item: &SpoolItem) -> Result<(), ToolError> {
        let key = item.object_key();
        let meta = async_nats::jetstream::object_store::ObjectMetadata {
            name: key.clone(),
            description: Some(item.spool_ref()),
            ..Default::default()
        };
        let mut reader = std::io::Cursor::new(item.to_bytes());
        self.store.put(meta, &mut reader).await.map_err(|e| {
            ToolError::ExecutionFailed(format!(
                "spool object_put '{key}' to '{}' failed: {e}",
                self.bucket
            ))
        })?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<SpoolMeta>, ToolError> {
        use futures::StreamExt;
        let mut stream = self.store.list().await.map_err(|e| {
            ToolError::ExecutionFailed(format!("spool object_list '{}' failed: {e}", self.bucket))
        })?;
        let mut metas = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(info) if !info.deleted => {
                    metas.push(SpoolMeta {
                        key: info.name,
                        size: info.size as u64,
                    });
                }
                Ok(_) => {} // tombstone
                Err(e) => tracing::warn!(bucket = %self.bucket, "spool list entry error: {e}"),
            }
        }
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(metas)
    }

    async fn get(&self, key: &str) -> Result<SpoolItem, ToolError> {
        use tokio::io::AsyncReadExt;
        let mut object = self.store.get(key).await.map_err(|e| {
            ToolError::ExecutionFailed(format!("spool object_get '{key}' failed: {e}"))
        })?;
        let mut buf = Vec::new();
        object.read_to_end(&mut buf).await.map_err(|e| {
            ToolError::ExecutionFailed(format!("spool object_get '{key}' read failed: {e}"))
        })?;
        SpoolItem::from_bytes(&buf)
    }

    async fn delete(&self, key: &str) -> Result<(), ToolError> {
        self.store.delete(key).await.map_err(|e| {
            ToolError::ExecutionFailed(format!("spool object_delete '{key}' failed: {e}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::source::PolledMessage;

    fn item(seq: u64, id: &str, data: serde_json::Value) -> SpoolItem {
        let msg = PolledMessage {
            id: id.to_string(),
            data,
            headers: serde_json::Map::new(),
            attributes: serde_json::Value::Null,
            metadata: serde_json::Value::Null,
            ack_id: None,
        };
        SpoolItem::new("subscriptions/t", "nats", msg, None, seq, None, "default", "circuit_open", seq)
    }

    #[tokio::test]
    async fn local_disk_put_list_get_delete_roundtrip() {
        let tmp = std::env::temp_dir().join(format!("noetl-spool-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let backend = LocalDiskBackend::open(&tmp).await.unwrap();

        assert!(backend.is_empty().await.unwrap());

        backend.put(&item(2, "b", serde_json::json!({"v": 2}))).await.unwrap();
        backend.put(&item(1, "a", serde_json::json!({"v": 1}))).await.unwrap();
        backend.put(&item(3, "c", serde_json::json!({"v": 3}))).await.unwrap();

        let metas = backend.list().await.unwrap();
        assert_eq!(metas.len(), 3);
        // list must be in receive order despite insert order
        let got = backend.get(&metas[0].key).await.unwrap();
        assert_eq!(got.recv_seq, 1);
        assert_eq!(backend.get(&metas[2].key).await.unwrap().recv_seq, 3);

        assert!(backend.total_bytes().await.unwrap() > 0);

        backend.delete(&metas[0].key).await.unwrap();
        assert_eq!(backend.len().await.unwrap(), 2);
        // delete is idempotent
        backend.delete(&metas[0].key).await.unwrap();

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[tokio::test]
    async fn local_disk_list_missing_dir_is_empty() {
        let tmp = std::env::temp_dir().join(format!("noetl-spool-missing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let backend = LocalDiskBackend { dir: tmp.clone() };
        // dir never created
        assert_eq!(backend.list().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn local_disk_overwrite_is_idempotent_on_key() {
        let tmp = std::env::temp_dir().join(format!("noetl-spool-idem-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let backend = LocalDiskBackend::open(&tmp).await.unwrap();
        // same recv_seq + id → same object key → overwrite, not duplicate
        backend.put(&item(7, "same", serde_json::json!(1))).await.unwrap();
        backend.put(&item(7, "same", serde_json::json!(2))).await.unwrap();
        assert_eq!(backend.len().await.unwrap(), 1);
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
