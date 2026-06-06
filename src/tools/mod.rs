//! Built-in tool implementations.
//!
//! This module provides implementations for various tools:
//! - `shell` - Execute shell commands
//! - `rhai` - Execute Rhai scripts
//! - `http` - Make HTTP requests
//! - `duckdb` - Query DuckDB databases
//! - `postgres` - Query PostgreSQL databases
//! - `python` - Execute Python scripts
//! - `snowflake` - Execute Snowflake SQL queries
//! - `transfer` - Transfer data between sources
//! - `script` - Execute scripts as Kubernetes jobs
//! - `mcp` - MCP (Model Context Protocol) JSON-RPC bridge

mod duckdb;
mod ducklake;
mod http;
mod mcp;
mod nats;
mod noop;
mod playbook;
mod postgres;
mod python;
mod result_fetch;
mod rhai;
mod script;
mod shell;
mod snowflake;
mod task_sequence;
mod transfer;

pub use self::duckdb::DuckdbTool;
pub use self::ducklake::DucklakeTool;
pub use self::http::HttpTool;
pub use self::mcp::McpTool;
pub use self::nats::NatsTool;
pub use self::noop::NoopTool;
pub use self::playbook::PlaybookTool;
pub use self::postgres::PostgresTool;
pub use self::python::PythonTool;
pub use self::result_fetch::ResultFetchTool;
pub use self::rhai::RhaiTool;
pub use self::script::ScriptTool;
pub use self::shell::ShellTool;
pub use self::snowflake::SnowflakeTool;
pub use self::task_sequence::TaskSequenceTool;
pub use self::transfer::TransferTool;

use crate::registry::ToolRegistry;

/// Create a tool registry with all built-in tools registered.
pub fn create_default_registry() -> ToolRegistry {
    let mut registry = ToolRegistry::new();

    registry.register(ShellTool::new());
    registry.register(RhaiTool::new());
    registry.register(HttpTool::new());
    registry.register(DuckdbTool::new());
    registry.register(DucklakeTool::new());
    registry.register(PostgresTool::new());
    registry.register(PythonTool::new());
    registry.register(SnowflakeTool::new());
    registry.register(TransferTool::new());
    registry.register(ScriptTool::new());
    registry.register(PlaybookTool::new());
    registry.register(NoopTool::new());
    registry.register(TaskSequenceTool::new());
    registry.register(ResultFetchTool::new());
    registry.register(NatsTool::new());
    registry.register(McpTool::new());

    registry
}
