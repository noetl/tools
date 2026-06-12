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

// ---------------------------------------------------------------------------
// gcs backend
// ---------------------------------------------------------------------------

/// Google Cloud Storage spool backend — the durable buffer for the
/// out-of-cluster Cloud Run runtime (RFC #90 Phase 5) and any in-cluster
/// runtime that prefers an object store over NATS.
///
/// Reuses the crate's existing dependencies: [`crate::auth::GcpAuth`] for
/// Application Default Credentials (Workload Identity on Cloud Run, the
/// gcloud ADC file locally, or `GOOGLE_APPLICATION_CREDENTIALS`) and
/// `reqwest` against the GCS JSON API — **no new dependency, no gRPC**, the
/// same shape the `pubsub` source backend uses.
///
/// One bucket holds many subscriptions' spools, separated by `prefix`
/// (e.g. `subscriptions/orders/spool/` for the live buffer and
/// `subscriptions/orders/dlq/` for the dead-letter sibling). Items are
/// stored under `{prefix}{object_key}` where `object_key` is the
/// zero-padded `recv_seq` (no slashes), so a `prefix`-scoped list returns
/// them in receive order — the cheap path for `ordering: global`.
///
/// On Cloud Run the credential is the runtime service account via Workload
/// Identity — "already-in-place trust" per `execution-model.md` (no key
/// file, no keychain hop). The keychain-alias path for a *tenant-owned*
/// external bucket is a future extension (the config carries the alias; ADC
/// is the platform-bucket default).
#[cfg(feature = "gcs")]
#[derive(Clone)]
pub struct GcsBackend {
    client: reqwest::Client,
    /// `None` when pointed at a no-auth emulator (fake-gcs-server); else the
    /// ADC token provider.
    auth: Option<crate::auth::GcpAuth>,
    bucket: String,
    /// Object-name prefix (ends with `/` unless empty) so one bucket serves
    /// many subscriptions + the live/dlq split.
    prefix: String,
    /// API base URL with no trailing slash (`https://storage.googleapis.com`
    /// for real GCS; an emulator base for tests).
    endpoint: String,
}

#[cfg(feature = "gcs")]
impl GcsBackend {
    /// Open the GCS spool backend against the real API using ADC.
    ///
    /// `prefix` is normalized to end with `/` (unless empty). The bucket is
    /// assumed to already exist (ops provisions it — GCS bucket creation is
    /// a project-admin op, not a runtime op).
    pub async fn open(bucket: &str, prefix: &str) -> Result<Self, ToolError> {
        Ok(Self {
            client: reqwest::Client::new(),
            auth: Some(crate::auth::GcpAuth::new()),
            bucket: bucket.to_string(),
            prefix: Self::norm_prefix(prefix),
            endpoint: "https://storage.googleapis.com".to_string(),
        })
    }

    /// Open against an explicit endpoint (a fake-gcs-server emulator) with an
    /// optional ADC provider — the seam the integration test + dev recipe use.
    pub fn with_endpoint(bucket: &str, prefix: &str, endpoint: &str, use_adc: bool) -> Self {
        Self {
            client: reqwest::Client::new(),
            auth: use_adc.then(crate::auth::GcpAuth::new),
            bucket: bucket.to_string(),
            prefix: Self::norm_prefix(prefix),
            endpoint: endpoint.trim_end_matches('/').to_string(),
        }
    }

    fn norm_prefix(prefix: &str) -> String {
        if prefix.is_empty() || prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        }
    }

    /// Full object name for a bare key.
    fn name_for(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }

    /// Percent-encode an object name for use in a URL path segment — GCS
    /// requires the full name (slashes included) encoded in the `o/{name}`
    /// path. Encodes everything outside the unreserved set.
    fn enc_path(name: &str) -> String {
        let mut out = String::with_capacity(name.len() * 3);
        for b in name.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }

    /// Resolve the `Authorization` header value, if auth is configured.
    async fn auth_header(&self) -> Result<Option<String>, ToolError> {
        match &self.auth {
            Some(gcp) => {
                let token = gcp
                    .get_token(&["https://www.googleapis.com/auth/devstorage.read_write"])
                    .await?;
                Ok(Some(format!("Bearer {token}")))
            }
            None => Ok(None),
        }
    }
}

#[cfg(feature = "gcs")]
#[async_trait]
impl SpoolBackend for GcsBackend {
    fn kind(&self) -> &'static str {
        "gcs"
    }

    async fn put(&self, item: &SpoolItem) -> Result<(), ToolError> {
        let name = self.name_for(&item.object_key());
        let url = format!("{}/upload/storage/v1/b/{}/o", self.endpoint, self.bucket);
        let mut req = self
            .client
            .post(&url)
            .query(&[("uploadType", "media"), ("name", name.as_str())])
            .header("Content-Type", "application/json")
            .body(item.to_bytes());
        if let Some(auth) = self.auth_header().await? {
            req = req.header("Authorization", auth);
        }
        let resp = req.send().await.map_err(|e| {
            ToolError::ExecutionFailed(format!("spool gcs put '{name}' failed: {e}"))
        })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ToolError::ExecutionFailed(format!(
                "spool gcs put '{name}' to '{}' returned {status}: {body}",
                self.bucket
            )));
        }
        Ok(())
    }

    async fn list(&self) -> Result<Vec<SpoolMeta>, ToolError> {
        let url = format!("{}/storage/v1/b/{}/o", self.endpoint, self.bucket);
        let mut metas = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut query: Vec<(&str, String)> = vec![("prefix", self.prefix.clone())];
            if let Some(tok) = &page_token {
                query.push(("pageToken", tok.clone()));
            }
            let mut req = self.client.get(&url).query(&query);
            if let Some(auth) = self.auth_header().await? {
                req = req.header("Authorization", auth);
            }
            let resp = req.send().await.map_err(|e| {
                ToolError::ExecutionFailed(format!("spool gcs list '{}' failed: {e}", self.bucket))
            })?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ToolError::ExecutionFailed(format!(
                    "spool gcs list '{}' returned {status}: {body}",
                    self.bucket
                )));
            }
            let page: GcsListResponse = resp.json().await.map_err(|e| {
                ToolError::Json(format!("spool gcs list decode failed: {e}"))
            })?;
            for obj in page.items {
                // Strip the prefix so SpoolMeta.key is the bare object_key the
                // engine orders/gets/deletes by.
                let Some(key) = obj.name.strip_prefix(&self.prefix) else {
                    continue;
                };
                if key.is_empty() {
                    continue; // the prefix "directory" placeholder, if any
                }
                let size = obj.size.parse::<u64>().unwrap_or(0);
                metas.push(SpoolMeta {
                    key: key.to_string(),
                    size,
                });
            }
            match page.next_page_token {
                Some(tok) if !tok.is_empty() => page_token = Some(tok),
                _ => break,
            }
        }
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(metas)
    }

    async fn get(&self, key: &str) -> Result<SpoolItem, ToolError> {
        let name = self.name_for(key);
        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            self.bucket,
            Self::enc_path(&name)
        );
        let mut req = self.client.get(&url).query(&[("alt", "media")]);
        if let Some(auth) = self.auth_header().await? {
            req = req.header("Authorization", auth);
        }
        let resp = req.send().await.map_err(|e| {
            ToolError::ExecutionFailed(format!("spool gcs get '{name}' failed: {e}"))
        })?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ToolError::ExecutionFailed(format!(
                "spool gcs get '{name}' returned {status}: {body}"
            )));
        }
        let bytes = resp.bytes().await.map_err(|e| {
            ToolError::ExecutionFailed(format!("spool gcs get '{name}' read failed: {e}"))
        })?;
        SpoolItem::from_bytes(&bytes)
    }

    async fn delete(&self, key: &str) -> Result<(), ToolError> {
        let name = self.name_for(key);
        let url = format!(
            "{}/storage/v1/b/{}/o/{}",
            self.endpoint,
            self.bucket,
            Self::enc_path(&name)
        );
        let mut req = self.client.delete(&url);
        if let Some(auth) = self.auth_header().await? {
            req = req.header("Authorization", auth);
        }
        let resp = req.send().await.map_err(|e| {
            ToolError::ExecutionFailed(format!("spool gcs delete '{name}' failed: {e}"))
        })?;
        // 404 == already gone → idempotent, like the other backends.
        if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(ToolError::ExecutionFailed(format!(
                "spool gcs delete '{name}' returned {status}: {body}"
            )))
        }
    }
}

/// GCS JSON-API object-list response (the fields the spool needs).
#[cfg(feature = "gcs")]
#[derive(serde::Deserialize)]
struct GcsListResponse {
    #[serde(default)]
    items: Vec<GcsObject>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[cfg(feature = "gcs")]
#[derive(serde::Deserialize)]
struct GcsObject {
    name: String,
    /// GCS reports object size as a decimal string.
    #[serde(default)]
    size: String,
}

// ---------------------------------------------------------------------------
// s3 backend
// ---------------------------------------------------------------------------

/// AWS S3 (and any S3-compatible store: MinIO, Cloudflare R2, Backblaze B2)
/// spool backend (noetl/ai-meta#94) — the durable buffer for runtimes that
/// use an S3 bucket instead of GCS or NATS Object Store.
///
/// Mirrors [`GcsBackend`]: one bucket holds many subscriptions' spools,
/// separated by `prefix` (`subscriptions/orders/spool/` for the live buffer,
/// `subscriptions/orders/dlq/` for the dead-letter sibling). Items are stored
/// under `{prefix}{object_key}` where `object_key` is the zero-padded
/// `recv_seq`, so a `prefix`-scoped `ListObjectsV2` returns them in receive
/// order — the cheap path for `ordering: global`.
///
/// Authentication is **hand-rolled AWS Signature Version 4** over the crate's
/// existing `reqwest` client + `hmac`/`sha2` — no AWS SDK, no new heavy
/// dependency, the same lean shape the `gcs` backend takes with the GCS JSON
/// API. Credentials (access key / secret / region / endpoint) resolve from
/// the NoETL keychain by alias (`spool.credential`) per
/// `data-access-boundary.md` — an external system, so keychain/Wallet-auth,
/// never a worker env var.
///
/// Path-style addressing (`{endpoint}/{bucket}/{key}`) is used — required by
/// MinIO and still honored by AWS for existing buckets; virtual-host style is
/// a future extension the constructor can grow.
#[cfg(feature = "s3")]
#[derive(Clone)]
pub struct S3Backend {
    client: reqwest::Client,
    bucket: String,
    /// Object-name prefix (ends with `/` unless empty).
    prefix: String,
    /// API endpoint base, no trailing slash. Real S3:
    /// `https://s3.<region>.amazonaws.com`; MinIO: `http://minio.minio:9000`.
    endpoint: String,
    region: String,
    access_key: String,
    secret_key: String,
    /// Temporary-credential session token (STS / IRSA). `None` for static
    /// access-key pairs (MinIO, long-lived IAM users).
    session_token: Option<String>,
}

#[cfg(feature = "s3")]
impl S3Backend {
    /// Build an S3 spool backend from explicit credentials + endpoint. The
    /// bucket is assumed to already exist (provisioning is an admin op, not a
    /// runtime op — same contract as the GCS backend).
    pub fn new(
        bucket: &str,
        prefix: &str,
        endpoint: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        session_token: Option<String>,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            bucket: bucket.to_string(),
            prefix: Self::norm_prefix(prefix),
            endpoint: endpoint.trim_end_matches('/').to_string(),
            region: region.to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            session_token,
        }
    }

    fn norm_prefix(prefix: &str) -> String {
        if prefix.is_empty() || prefix.ends_with('/') {
            prefix.to_string()
        } else {
            format!("{prefix}/")
        }
    }

    fn name_for(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }

    /// Host authority of the endpoint (what the signed `host` header + the
    /// `Host` reqwest sends must agree on).
    fn host(&self) -> String {
        self.endpoint
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .to_string()
    }

    /// Percent-encode a URI path, preserving `/` (S3 canonical URIs are
    /// single-encoded with slashes kept). Our keys are object-store-safe
    /// (`sanitize_key` restricts to alnum/`-`/`_`/`.`), so in practice this is
    /// an identity transform — which also sidesteps the reqwest URL
    /// re-encoding pitfall.
    fn uri_encode_path(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }

    /// Percent-encode a query component (no `/` exemption — SigV4 requires
    /// every reserved char encoded).
    fn uri_encode_query(s: &str) -> String {
        let mut out = String::with_capacity(s.len());
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    out.push(b as char)
                }
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
        out
    }

    /// Sign one request with AWS SigV4 and send it. `object_path` is the
    /// object name within the bucket (already `name_for`'d, unencoded; empty
    /// for a bucket-level `ListObjectsV2`); `query` is the unsorted/unencoded
    /// `(key, value)` list; `body` is the request body (empty for
    /// GET/DELETE/LIST). Payload is signed (not `UNSIGNED-PAYLOAD`).
    async fn signed_request(
        &self,
        method: reqwest::Method,
        object_path: &str,
        query: &[(&str, String)],
        body: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<reqwest::Response, ToolError> {
        let (amzdate, datestamp) = amz_dates();
        let host = self.host();
        let payload_hash = super::item::sha256_hex(&body);

        // Canonical URI (path-style): /{bucket}[/{object_path}].
        let canonical_path = if object_path.is_empty() {
            format!("/{}", self.bucket)
        } else {
            format!("/{}/{}", self.bucket, object_path)
        };
        let canonical_uri = Self::uri_encode_path(&canonical_path);

        // Canonical query string: encode each pair, then sort by encoded key.
        let mut q: Vec<(String, String)> = query
            .iter()
            .map(|(k, v)| (Self::uri_encode_query(k), Self::uri_encode_query(v)))
            .collect();
        q.sort();
        let canonical_query = q
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        // Canonical headers — sign host + the x-amz-* set (sorted, lowercased).
        let mut signed: Vec<(String, String)> = vec![
            ("host".to_string(), host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amzdate.clone()),
        ];
        if let Some(tok) = &self.session_token {
            signed.push(("x-amz-security-token".to_string(), tok.clone()));
        }
        signed.sort();
        let canonical_headers = signed
            .iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect::<String>();
        let signed_headers = signed
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(";");

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method.as_str(),
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        );

        let scope = format!("{datestamp}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amzdate,
            scope,
            super::item::sha256_hex(canonical_request.as_bytes()),
        );

        let key = sigv4_signing_key(&self.secret_key, &datestamp, &self.region, "s3");
        let signature = hex_lower(&hmac_sha256(&key, string_to_sign.as_bytes()));

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key, scope, signed_headers, signature,
        );

        // Build the request URL with the already-encoded canonical query (the
        // `url` crate leaves valid %XX / unreserved / `=` / `&` verbatim, so
        // the signed query and the wire query stay byte-identical).
        let mut url = format!("{}{}", self.endpoint, canonical_uri);
        if !canonical_query.is_empty() {
            url.push('?');
            url.push_str(&canonical_query);
        }

        let mut req = self
            .client
            .request(method, &url)
            .header("x-amz-content-sha256", &payload_hash)
            .header("x-amz-date", &amzdate)
            .header("Authorization", authorization);
        if let Some(tok) = &self.session_token {
            req = req.header("x-amz-security-token", tok);
        }
        if let Some(ct) = content_type {
            req = req.header("Content-Type", ct);
        }
        if !body.is_empty() {
            req = req.body(body);
        }
        req.send()
            .await
            .map_err(|e| ToolError::ExecutionFailed(format!("spool s3 request to '{}' failed: {e}", self.bucket)))
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl SpoolBackend for S3Backend {
    fn kind(&self) -> &'static str {
        "s3"
    }

    async fn put(&self, item: &SpoolItem) -> Result<(), ToolError> {
        let name = self.name_for(&item.object_key());
        let resp = self
            .signed_request(
                reqwest::Method::PUT,
                &name,
                &[],
                item.to_bytes(),
                Some("application/json"),
            )
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ToolError::ExecutionFailed(format!(
                "spool s3 put '{name}' to '{}' returned {status}: {body}",
                self.bucket
            )));
        }
        Ok(())
    }

    async fn list(&self) -> Result<Vec<SpoolMeta>, ToolError> {
        let mut metas = Vec::new();
        let mut cont: Option<String> = None;
        loop {
            let mut query: Vec<(&str, String)> = vec![
                ("list-type", "2".to_string()),
                ("prefix", self.prefix.clone()),
            ];
            if let Some(c) = &cont {
                query.push(("continuation-token", c.clone()));
            }
            let resp = self
                .signed_request(reqwest::Method::GET, "", &query, Vec::new(), None)
                .await?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(ToolError::ExecutionFailed(format!(
                    "spool s3 list '{}' returned {status}: {body}",
                    self.bucket
                )));
            }
            let xml = resp.text().await.map_err(|e| {
                ToolError::ExecutionFailed(format!("spool s3 list '{}' read failed: {e}", self.bucket))
            })?;
            let (objects, next) = parse_list_v2(&xml);
            for (key, size) in objects {
                // Strip the prefix so SpoolMeta.key is the bare object_key the
                // engine orders/gets/deletes by.
                let Some(bare) = key.strip_prefix(&self.prefix) else {
                    continue;
                };
                if bare.is_empty() {
                    continue;
                }
                metas.push(SpoolMeta {
                    key: bare.to_string(),
                    size,
                });
            }
            match next {
                Some(t) if !t.is_empty() => cont = Some(t),
                _ => break,
            }
        }
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(metas)
    }

    async fn get(&self, key: &str) -> Result<SpoolItem, ToolError> {
        let name = self.name_for(key);
        let resp = self
            .signed_request(reqwest::Method::GET, &name, &[], Vec::new(), None)
            .await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ToolError::ExecutionFailed(format!(
                "spool s3 get '{name}' returned {status}: {body}"
            )));
        }
        let bytes = resp.bytes().await.map_err(|e| {
            ToolError::ExecutionFailed(format!("spool s3 get '{name}' read failed: {e}"))
        })?;
        SpoolItem::from_bytes(&bytes)
    }

    async fn delete(&self, key: &str) -> Result<(), ToolError> {
        let name = self.name_for(key);
        let resp = self
            .signed_request(reqwest::Method::DELETE, &name, &[], Vec::new(), None)
            .await?;
        // S3 DELETE is 204 on success and also 204 for a missing key; treat
        // 404 as already-gone too → idempotent, like the other backends.
        if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            Err(ToolError::ExecutionFailed(format!(
                "spool s3 delete '{name}' returned {status}: {body}"
            )))
        }
    }
}

/// Minimal `ListObjectsV2` XML extractor — pulls `(key, size)` per
/// `<Contents>` block and the `<NextContinuationToken>`. Our object keys are
/// object-store-safe (alnum/`-`/`_`/`.` + prefix slashes), so no XML entity
/// decoding is needed. Avoids pulling an XML crate into the lean spool crate.
#[cfg(feature = "s3")]
fn parse_list_v2(xml: &str) -> (Vec<(String, u64)>, Option<String>) {
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Contents>") {
        let after = &rest[start + "<Contents>".len()..];
        let Some(end) = after.find("</Contents>") else {
            break;
        };
        let block = &after[..end];
        if let Some(key) = xml_tag(block, "Key") {
            let size = xml_tag(block, "Size")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            out.push((key, size));
        }
        rest = &after[end + "</Contents>".len()..];
    }
    let next = xml_tag(xml, "NextContinuationToken");
    (out, next)
}

/// Extract the inner text of the first `<tag>…</tag>` in `haystack`.
#[cfg(feature = "s3")]
fn xml_tag(haystack: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = haystack.find(&open)? + open.len();
    let end = haystack[start..].find(&close)? + start;
    Some(haystack[start..end].to_string())
}

/// HMAC-SHA256 of `data` under `key`.
#[cfg(feature = "s3")]
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(key).expect("HMAC accepts a key of any length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Derive the AWS SigV4 signing key (the four-step HMAC chain).
#[cfg(feature = "s3")]
fn sigv4_signing_key(secret: &str, datestamp: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), datestamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// Lower-case hex of `bytes`.
#[cfg(feature = "s3")]
fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Current UTC as the SigV4 `(x-amz-date, datestamp)` pair.
#[cfg(feature = "s3")]
fn amz_dates() -> (String, String) {
    let now = chrono::Utc::now();
    (
        now.format("%Y%m%dT%H%M%SZ").to_string(),
        now.format("%Y%m%d").to_string(),
    )
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

    // ---- gcs backend ----

    #[cfg(feature = "gcs")]
    #[test]
    fn gcs_prefix_is_normalized_to_trailing_slash() {
        assert_eq!(GcsBackend::norm_prefix("subscriptions/orders"), "subscriptions/orders/");
        assert_eq!(GcsBackend::norm_prefix("subscriptions/orders/"), "subscriptions/orders/");
        assert_eq!(GcsBackend::norm_prefix(""), "");
    }

    #[cfg(feature = "gcs")]
    #[test]
    fn gcs_name_for_joins_prefix_and_key() {
        let b = GcsBackend::with_endpoint("bkt", "sub/spool", "http://x", false);
        assert_eq!(b.name_for("00000000000000000001-abc"), "sub/spool/00000000000000000001-abc");
    }

    #[cfg(feature = "gcs")]
    #[test]
    fn gcs_enc_path_encodes_slashes_and_reserved() {
        // slashes in the prefix must be percent-encoded for the o/{name} path
        assert_eq!(GcsBackend::enc_path("a/b-c.d_e~f"), "a%2Fb-c.d_e~f");
        assert_eq!(GcsBackend::enc_path("k=1&v"), "k%3D1%26v");
        // unreserved set passes through untouched
        assert_eq!(GcsBackend::enc_path("AZaz09-._~"), "AZaz09-._~");
    }

    /// Live round-trip against a real GCS bucket. Gated on
    /// `NOETL_GCS_TEST_BUCKET` (set to a bucket the ambient ADC can write).
    /// Run: `NOETL_GCS_TEST_BUCKET=my-bucket cargo test --features gcs gcs_live -- --ignored --nocapture`.
    #[cfg(feature = "gcs")]
    #[tokio::test]
    #[ignore]
    async fn gcs_live_put_list_get_delete_roundtrip() {
        let Ok(bucket) = std::env::var("NOETL_GCS_TEST_BUCKET") else {
            eprintln!("skipping: NOETL_GCS_TEST_BUCKET unset");
            return;
        };
        let prefix = format!("noetl-spool-test/{}", std::process::id());
        let backend = GcsBackend::open(&bucket, &prefix).await.unwrap();

        assert!(backend.is_empty().await.unwrap(), "test prefix must start empty");

        backend.put(&item(2, "b", serde_json::json!({"v": 2}))).await.unwrap();
        backend.put(&item(1, "a", serde_json::json!({"v": 1}))).await.unwrap();
        backend.put(&item(3, "c", serde_json::json!({"v": 3}))).await.unwrap();

        let metas = backend.list().await.unwrap();
        assert_eq!(metas.len(), 3, "all three items listed");
        // list is in receive order despite insert order
        assert_eq!(backend.get(&metas[0].key).await.unwrap().recv_seq, 1);
        assert_eq!(backend.get(&metas[2].key).await.unwrap().recv_seq, 3);
        assert!(backend.total_bytes().await.unwrap() > 0);

        // payload integrity survives the round-trip
        let got = backend.get(&metas[0].key).await.unwrap();
        assert_eq!(got.message_id, "a");

        backend.delete(&metas[0].key).await.unwrap();
        assert_eq!(backend.len().await.unwrap(), 2);
        // delete is idempotent (404 → Ok)
        backend.delete(&metas[0].key).await.unwrap();

        // clean up the remaining items so reruns start clean
        for m in backend.list().await.unwrap() {
            backend.delete(&m.key).await.unwrap();
        }
        assert!(backend.is_empty().await.unwrap());
    }

    // ---- s3 backend ----

    #[cfg(feature = "s3")]
    #[test]
    fn s3_sigv4_signing_key_matches_aws_reference_vector() {
        // The documented AWS SigV4 signing-key derivation example
        // (secret/date/region/service → signing key). Locks the HMAC chain.
        let key = sigv4_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20120215",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex_lower(&key),
            "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d"
        );
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_prefix_normalized_and_name_joined() {
        let b = S3Backend::new(
            "bkt", "sub/spool", "http://minio:9000", "us-east-1", "ak", "sk", None,
        );
        assert_eq!(b.prefix, "sub/spool/");
        assert_eq!(
            b.name_for("00000000000000000001-abc"),
            "sub/spool/00000000000000000001-abc"
        );
        assert_eq!(b.host(), "minio:9000");
        assert_eq!(S3Backend::norm_prefix(""), "");
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_uri_encoders() {
        // path keeps '/', query encodes it; both encode reserved chars.
        assert_eq!(S3Backend::uri_encode_path("a/b-c.d_e~f"), "a/b-c.d_e~f");
        assert_eq!(S3Backend::uri_encode_query("a/b=c"), "a%2Fb%3Dc");
        // a base64 continuation token round-trips through query encoding
        assert_eq!(S3Backend::uri_encode_query("aGVsbG8+/w=="), "aGVsbG8%2B%2Fw%3D%3D");
    }

    #[cfg(feature = "s3")]
    #[test]
    fn s3_parse_list_v2_extracts_keys_sizes_and_token() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bkt</Name>
  <Prefix>sub/spool/</Prefix>
  <IsTruncated>true</IsTruncated>
  <Contents><Key>sub/spool/00000000000000000001-a</Key><Size>120</Size><StorageClass>STANDARD</StorageClass></Contents>
  <Contents><Key>sub/spool/00000000000000000002-b</Key><Size>240</Size></Contents>
  <NextContinuationToken>1ueGcxLPRx1Tr/XYExampleToken</NextContinuationToken>
</ListBucketResult>"#;
        let (objs, next) = parse_list_v2(xml);
        assert_eq!(objs.len(), 2);
        assert_eq!(objs[0], ("sub/spool/00000000000000000001-a".to_string(), 120));
        assert_eq!(objs[1].1, 240);
        assert_eq!(next.as_deref(), Some("1ueGcxLPRx1Tr/XYExampleToken"));

        // last page (no token)
        let (objs2, next2) = parse_list_v2("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>");
        assert!(objs2.is_empty());
        assert!(next2.is_none());
    }

    /// Live round-trip against a real S3 / S3-compatible endpoint (MinIO).
    /// Gated on env so it never runs in CI without a bucket. Run e.g.:
    /// `NOETL_S3_TEST_BUCKET=spool NOETL_S3_ENDPOINT=http://localhost:9000 \
    ///  NOETL_S3_ACCESS_KEY=minioadmin NOETL_S3_SECRET_KEY=minioadmin \
    ///  cargo test --features s3 s3_live -- --ignored --nocapture`.
    #[cfg(feature = "s3")]
    #[tokio::test]
    #[ignore]
    async fn s3_live_put_list_get_delete_roundtrip() {
        let Ok(bucket) = std::env::var("NOETL_S3_TEST_BUCKET") else {
            eprintln!("skipping: NOETL_S3_TEST_BUCKET unset");
            return;
        };
        let endpoint = std::env::var("NOETL_S3_ENDPOINT").unwrap_or_else(|_| "https://s3.us-east-1.amazonaws.com".to_string());
        let region = std::env::var("NOETL_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let access = std::env::var("NOETL_S3_ACCESS_KEY").expect("NOETL_S3_ACCESS_KEY");
        let secret = std::env::var("NOETL_S3_SECRET_KEY").expect("NOETL_S3_SECRET_KEY");
        let prefix = format!("noetl-spool-test/{}", std::process::id());
        let backend = S3Backend::new(&bucket, &prefix, &endpoint, &region, &access, &secret, None);

        assert!(backend.is_empty().await.unwrap(), "test prefix must start empty");

        backend.put(&item(2, "b", serde_json::json!({"v": 2}))).await.unwrap();
        backend.put(&item(1, "a", serde_json::json!({"v": 1}))).await.unwrap();
        backend.put(&item(3, "c", serde_json::json!({"v": 3}))).await.unwrap();

        let metas = backend.list().await.unwrap();
        assert_eq!(metas.len(), 3, "all three items listed");
        // list is in receive order despite insert order
        assert_eq!(backend.get(&metas[0].key).await.unwrap().recv_seq, 1);
        assert_eq!(backend.get(&metas[2].key).await.unwrap().recv_seq, 3);
        assert!(backend.total_bytes().await.unwrap() > 0);

        let got = backend.get(&metas[0].key).await.unwrap();
        assert_eq!(got.message_id, "a");

        backend.delete(&metas[0].key).await.unwrap();
        assert_eq!(backend.len().await.unwrap(), 2);
        backend.delete(&metas[0].key).await.unwrap(); // idempotent

        for m in backend.list().await.unwrap() {
            backend.delete(&m.key).await.unwrap();
        }
        assert!(backend.is_empty().await.unwrap());
    }
}
