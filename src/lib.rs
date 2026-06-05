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
pub mod context;
pub mod error;
pub mod registry;
pub mod result;
pub mod secrets;
pub mod template;
pub mod tools;

pub use context::ExecutionContext;
pub use error::ToolError;
pub use registry::{Tool, ToolConfig, ToolRegistry};
pub use result::{ToolResult, ToolStatus};
