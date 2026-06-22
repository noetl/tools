//! NoETL Tool Library
//!
//! Shared tool implementations for workflow execution.
//!
//! This crate provides:
//! - Tool execution framework with registry pattern
//! - Built-in tools: shell, rhai, http, duckdb/ducklake, postgres, python, playbook, noop
//! - Template engine with Jinja2-compatible syntax
//! - Authentication resolvers (GCP ADC, credentials)
//! - Apache Arrow IPC codec for the columnar data plane (R-2.1, see
//!   Appendix H of the global hybrid cloud blueprint)

pub mod arrow_codec;
pub mod auth;
// The Resource Locator now lives in the lean, dependency-free `noetl-locator`
// crate (noetl/ai-meta#104) so the control-plane server can depend on it WITHOUT
// noetl-tools' heavy graph.  Re-exported here as `noetl_tools::locator` so
// existing consumers (the worker stamp path
// `noetl_tools::locator::ResultCoordinates`) are unchanged.
pub use noetl_locator as locator;
pub mod context;
pub mod error;
pub mod registry;
pub mod result;
pub mod spool;
pub mod template;
pub mod tools;

pub use context::ExecutionContext;
pub use error::ToolError;
pub use registry::{Tool, ToolConfig, ToolRegistry};
pub use result::{ToolResult, ToolStatus};
