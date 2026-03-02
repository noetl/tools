//! DuckLake tool (compatibility alias to DuckDB).

use async_trait::async_trait;

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::tools::duckdb::DuckdbTool;

/// DuckLake compatibility tool.
///
/// Python runtime exposes `ducklake` as a first-class tool kind.
/// In Rust we map it to DuckDB execution semantics.
pub struct DucklakeTool {
    inner: DuckdbTool,
}

impl DucklakeTool {
    /// Create a new ducklake compatibility tool.
    pub fn new() -> Self {
        Self {
            inner: DuckdbTool::new(),
        }
    }
}

impl Default for DucklakeTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for DucklakeTool {
    fn name(&self) -> &'static str {
        "ducklake"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        self.inner.execute(config, ctx).await
    }
}
