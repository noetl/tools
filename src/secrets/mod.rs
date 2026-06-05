//! Secret-provider registry for the `secrets` / `secret_manager` tools.
//!
//! Phase 3 of the Secrets Wallet (noetl/ai-meta#61): resolve a secret
//! reference against an external secret manager at step-execution time, so a
//! playbook references a secret by name instead of carrying the value. Each
//! backend implements [`SecretProvider`]; the `secrets` / `secret_manager`
//! tools dispatch on the config's `provider` field.
//!
//! The first provider is [`GcpSecretManager`] (matches the GCP-first KMS
//! choice for the KEK in noetl-server). AWS Secrets Manager, Azure Key Vault,
//! HashiCorp Vault, and Kubernetes Secrets follow behind the same trait.

mod gcp;

pub use gcp::GcpSecretManager;

use async_trait::async_trait;

use crate::error::ToolError;

/// A resolved secret plus its provenance.
///
/// `value` is the secret material as a UTF-8 string; `version` is the
/// provider's resolved version identifier when the backend reports one
/// (e.g. the concrete version number behind the `latest` alias).
#[derive(Debug, Clone)]
pub struct SecretValue {
    pub value: String,
    pub version: Option<String>,
}

/// A request to fetch one secret from a provider.
///
/// Fields are provider-agnostic; each backend interprets them:
/// - `name` — the secret id / name, or a fully-qualified resource path.
/// - `project` — GCP project / AWS account / Azure vault / Vault mount.
/// - `version` — version / stage; defaults to the provider's "latest".
#[derive(Debug, Clone)]
pub struct SecretRef {
    pub name: String,
    pub project: Option<String>,
    pub version: Option<String>,
}

/// A backend that resolves [`SecretRef`]s to [`SecretValue`]s.
#[async_trait]
pub trait SecretProvider: Send + Sync {
    /// Stable provider id (`gcp`, `aws`, `azure`, `vault`, `k8s`).
    fn provider(&self) -> &'static str;

    /// Fetch one secret. Implementations never log or embed the resolved
    /// value; callers are responsible for keeping it out of result payloads
    /// that cross the response boundary unmasked.
    async fn fetch(&self, secret: &SecretRef) -> Result<SecretValue, ToolError>;
}
