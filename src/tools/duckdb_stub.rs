//! DuckDB tool stub — compiled when the `duckdb-integration` feature is OFF.
//!
//! The real DuckDB implementation ([`super::duckdb`] / `duckdb.rs`) pulls in the
//! `duckdb` crate with `features = ["bundled"]`, which compiles the full DuckDB
//! C++ amalgamation via `libduckdb-sys` — a multi-hour compile that dominates
//! the noetl-worker release-image build.  The worker's core runtime never
//! touches DuckDB, so the dependency is gated behind the non-default
//! `duckdb-integration` cargo feature (noetl/ai-meta#185).
//!
//! This stub preserves `DuckdbTool`'s public surface (`new`, `execute_query`,
//! `Default`, and the [`Tool`] impl) so `ducklake`, `transfer`, the registry,
//! and every downstream consumer keep compiling unchanged.  The difference is
//! purely at dispatch time: any attempt to execute a `duckdb` / `ducklake`
//! step (or a DuckDB↔Postgres `transfer`) returns a clear `Configuration`
//! error telling the operator to rebuild with `--features duckdb-integration`.

use async_trait::async_trait;

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;

/// Message returned whenever a DuckDB code path is reached in a build that did
/// not compile the DuckDB engine.
const DISABLED_MSG: &str = "duckdb tool is not compiled into this build: the \
    DuckDB C++ engine (libduckdb-sys) is gated behind the non-default \
    `duckdb-integration` cargo feature to keep the default worker / kind image \
    build fast. Rebuild noetl-tools (or the consuming binary) with \
    `--features duckdb-integration` to enable DuckDB / DuckLake support. \
    See noetl/ai-meta#185.";

/// DuckDB query execution tool (stub).
///
/// Zero-sized placeholder that stands in for the real
/// [`DuckdbTool`](super::duckdb::DuckdbTool) when the `duckdb-integration`
/// feature is disabled.  It registers under the same `duckdb` kind so the
/// registry surface is identical, but every execution path fails fast with a
/// clear opt-in error instead of silently doing nothing.
pub struct DuckdbTool;

impl DuckdbTool {
    /// Create a new DuckDB tool stub.
    pub fn new() -> Self {
        Self
    }

    /// Execute a query — always fails in the stub build.
    ///
    /// Signature mirrors the real
    /// [`DuckdbTool::execute_query`](super::duckdb::DuckdbTool::execute_query)
    /// so `transfer`'s DuckDB↔Postgres directions compile unchanged; they get
    /// the opt-in error at runtime rather than a compile break.
    pub fn execute_query(
        &self,
        _query: &str,
        _params: &[serde_json::Value],
        _db_path: Option<&str>,
        _as_objects: bool,
    ) -> Result<ToolResult, ToolError> {
        Err(ToolError::Configuration(DISABLED_MSG.to_string()))
    }
}

impl Default for DuckdbTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for DuckdbTool {
    fn name(&self) -> &'static str {
        "duckdb"
    }

    async fn execute(
        &self,
        _config: &ToolConfig,
        _ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        Err(ToolError::Configuration(DISABLED_MSG.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_name_is_duckdb() {
        assert_eq!(DuckdbTool::new().name(), "duckdb");
    }

    #[test]
    fn stub_execute_query_returns_optin_error() {
        let tool = DuckdbTool::new();
        let err = tool
            .execute_query("SELECT 1", &[], None, true)
            .expect_err("stub must not succeed");
        match err {
            ToolError::Configuration(msg) => {
                assert!(msg.contains("duckdb-integration"), "got: {msg}");
            }
            other => panic!("expected Configuration error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn stub_execute_returns_optin_error() {
        let tool = DuckdbTool::new();
        let config = ToolConfig {
            kind: "duckdb".to_string(),
            config: serde_json::json!({ "query": "SELECT 42" }),
            timeout: None,
            retry: None,
            auth: None,
        };
        let ctx = ExecutionContext::default();
        let err = tool
            .execute(&config, &ctx)
            .await
            .expect_err("stub must not succeed");
        assert!(matches!(err, ToolError::Configuration(_)));
    }
}
