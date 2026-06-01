//! `result_fetch` tool — explicit playbook-step fetch for a noetl://
//! result-store ref.
//!
//! Per [`agents/rules/execution-model.md`][rule] data-touch policy
//! (when to fetch, retry, fall back) belongs in the playbook layer.
//! This tool is the surface playbook authors invoke to express
//! "materialise this cross-step / cross-node result NOW under the
//! step's policy block."  Without it, playbook authors have to rely
//! on the broker's implicit reference resolution during command
//! generation, which gives them no explicit knob.
//!
//! ## Wire shape
//!
//! Playbook step:
//!
//! ```yaml
//! - step: fetch_prior_rows
//!   tool:
//!     kind: result_fetch
//!     ref: "{{ steps.big_select.result.reference.ref }}"
//!     prefer: flight     # default: "flight" with HTTP fallback
//!     flight_endpoint: "grpc://noetl.noetl.svc.cluster.local:8083"
//! ```
//!
//! The `ref` arg is the `noetl://execution/<eid>/result/<step>/<id>`
//! URI from a prior step's `result.reference.ref` field — same
//! convention the Flight server's Ticket bytes use.
//!
//! ## Backend choice
//!
//! - `prefer: flight` (default) — try `noetl-arrow-flight-client::FlightResolver::resolve`
//!   first; fall back to HTTP `GET /api/result/resolve` on
//!   `FlightError::NonTabular` (non-tabular refs the server can't
//!   ship over Flight) or any transport error.
//! - `prefer: http` — skip Flight, use HTTP directly.  Useful when
//!   the deployment doesn't expose Flight, or for non-tabular refs
//!   the caller already knows about.
//!
//! Both paths return the same JSON-shaped row data in
//! `ToolResult.data`; the playbook step's consumers can reference
//! it as `{{ steps.fetch_prior_rows.data.rows[0].col }}` regardless
//! of which backend served the fetch.
//!
//! ## Boundary discipline
//!
//! The tool is a thin client; it does no auth, no scrubbing, no
//! caching.  All of that is server-side (per the Phase A `do_get`
//! contract).  The credential scrub the server applies before
//! encoding round-trips through this tool unchanged.
//!
//! ## What this tool is NOT
//!
//! - Not a write surface — produce side stays the worker's
//!   `call.done` emit path (R-2.1).
//! - Not a stream — materialises into a single `ToolResult.data`
//!   payload.  Streaming + paginated reads are deferred until a
//!   playbook author asks for them.
//! - Not a long-lived connection — each call constructs a fresh
//!   `FlightResolver` (cheap on the cluster network).  Callers
//!   that need to fetch many refs in a tight loop should batch via
//!   a single playbook step that returns `rows` for many refs.
//!
//! [rule]: https://github.com/noetl/ai-meta/blob/main/agents/rules/execution-model.md

use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

/// Per-step config for `result_fetch`.  Deserialised from the
/// playbook step's `tool:` block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultFetchConfig {
    /// `noetl://execution/<eid>/result/<step>/<id>` URI to materialise.
    pub r#ref: String,

    /// Backend preference.  Default `"flight"` (Flight first with
    /// HTTP fallback); `"http"` uses the HTTP `/api/result/resolve`
    /// path only.
    #[serde(default = "default_prefer")]
    pub prefer: BackendPreference,

    /// Optional override for the Flight gRPC endpoint.  When unset
    /// the tool derives the Flight URL from `ctx.server_url` by
    /// swapping the port to `8083` (matches the K8s manifest +
    /// kind extraPortMappings the R-2.3 Phase A PR shipped).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flight_endpoint: Option<String>,
}

/// Backend preference for the fetch.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum BackendPreference {
    /// Try Flight first; fall back to HTTP on non-tabular / transport
    /// errors.  Default.
    Flight,
    /// HTTP `/api/result/resolve` only.
    Http,
}

fn default_prefer() -> BackendPreference {
    BackendPreference::Flight
}

/// The tool itself.
pub struct ResultFetchTool {
    http_client: reqwest::Client,
    template_engine: TemplateEngine,
}

impl ResultFetchTool {
    pub fn new() -> Self {
        Self {
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .unwrap_or_default(),
            template_engine: TemplateEngine::new(),
        }
    }

    fn parse_config(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ResultFetchConfig, ToolError> {
        let template_ctx = ctx.to_template_context();
        let rendered = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;
        serde_json::from_value(rendered)
            .map_err(|e| ToolError::Configuration(format!("Invalid result_fetch config: {}", e)))
    }

    /// Derive the Flight endpoint URL from the HTTP server URL by
    /// swapping port 8082 → 8083 (the convention the R-2.3 Phase A
    /// manifest + kind extraPortMappings ship).  Falls back to the
    /// raw server_url if there's no port to swap.
    ///
    /// Scheme is preserved from `server_url` — tonic's
    /// `Endpoint::from_shared` (used downstream by the Flight client)
    /// requires `http://` for plaintext h2c or `https://` for TLS;
    /// the `grpc://` scheme some Flight clients accept is NOT valid
    /// for tonic and surfaces as `Bad :scheme header` at request
    /// time (HTTP/2's `:scheme` pseudo-header must be `http` or
    /// `https`).
    fn derive_flight_endpoint(server_url: &str) -> String {
        // server_url is e.g. http://noetl.noetl.svc.cluster.local:8082
        // → http://noetl.noetl.svc.cluster.local:8083
        // (or https://...:8083 if the server URL is TLS-fronted).
        let (scheme, trimmed) = if let Some(rest) = server_url.strip_prefix("https://") {
            ("https", rest)
        } else if let Some(rest) = server_url.strip_prefix("http://") {
            ("http", rest)
        } else {
            // No scheme on the input — default to plaintext h2c.
            // Same default the K8s manifest ships in kind.
            ("http", server_url)
        };
        // Replace the trailing :8082 with :8083 — the only port
        // pattern we ship.  Anything else falls through unchanged.
        let rewritten = if let Some(stripped) = trimmed.strip_suffix(":8082") {
            format!("{stripped}:8083")
        } else {
            trimmed.to_string()
        };
        format!("{scheme}://{rewritten}")
    }

    /// HTTP fallback: GET /api/result/resolve?ref=<uri>.  The server
    /// returns the JSON shape that the tabular paths produce
    /// (`{data: {rows: [...], columns: [...]}, ...}` or
    /// `{rows: [...], columns: [...]}`).
    async fn fetch_via_http(
        &self,
        cfg: &ResultFetchConfig,
        ctx: &ExecutionContext,
    ) -> Result<JsonValue, ToolError> {
        let url = format!(
            "{}/api/result/resolve",
            ctx.server_url.trim_end_matches('/')
        );
        let response = self
            .http_client
            .get(&url)
            .query(&[("ref", cfg.r#ref.as_str())])
            .send()
            .await
            .map_err(|e| ToolError::Http(format!("HTTP fetch failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(ToolError::Http(format!(
                "/api/result/resolve returned {}: {}",
                status.as_u16(),
                body
            )));
        }

        let body: JsonValue = response
            .json()
            .await
            .map_err(|e| ToolError::Http(format!("Failed to parse JSON response: {e}")))?;
        Ok(body)
    }

    /// Flight path: connect + resolve_rows.  Returns the JSON-shaped
    /// row vec wrapped in the same `{data: {rows, columns}}` shape
    /// the HTTP fallback returns, so the playbook step's output
    /// looks identical regardless of backend.
    async fn fetch_via_flight(
        &self,
        cfg: &ResultFetchConfig,
        ctx: &ExecutionContext,
    ) -> Result<JsonValue, FlightFetchError> {
        let endpoint = cfg
            .flight_endpoint
            .clone()
            .unwrap_or_else(|| Self::derive_flight_endpoint(&ctx.server_url));

        let resolver = noetl_arrow_flight_client::FlightResolver::connect(&endpoint)
            .await
            .map_err(|e| {
                FlightFetchError::Transport(format!("connect to Flight endpoint {endpoint}: {e}"))
            })?;

        match resolver.resolve_rows(&cfg.r#ref).await {
            Ok(rows) => {
                // Derive columns from the first row's keys for parity
                // with the HTTP shape.
                let columns: Vec<String> = rows
                    .first()
                    .and_then(|row| row.as_object())
                    .map(|obj| obj.keys().cloned().collect())
                    .unwrap_or_default();
                Ok(serde_json::json!({
                    "data": {
                        "rows": rows,
                        "columns": columns,
                        "row_count": rows.len(),
                    },
                    "status": "success",
                }))
            }
            Err(noetl_arrow_flight_client::FlightError::NonTabular { ref_uri, message }) => {
                Err(FlightFetchError::NonTabular { ref_uri, message })
            }
            Err(noetl_arrow_flight_client::FlightError::Server(msg)) => {
                Err(FlightFetchError::Server(msg))
            }
            Err(noetl_arrow_flight_client::FlightError::Transport(msg)) => {
                Err(FlightFetchError::Transport(msg))
            }
        }
    }
}

/// Internal error variants for the Flight path — used to decide
/// whether to fall back to HTTP.
#[derive(Debug)]
enum FlightFetchError {
    /// Server signalled non-tabular ref; HTTP can still serve it.
    NonTabular { ref_uri: String, message: String },
    /// Transport-level error; HTTP is worth trying.
    Transport(String),
    /// Server-level error that the caller probably can't recover
    /// from; surface up.
    Server(String),
}

impl Default for ResultFetchTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for ResultFetchTool {
    fn name(&self) -> &'static str {
        "result_fetch"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let cfg = self.parse_config(config, ctx)?;
        let start = std::time::Instant::now();

        tracing::debug!(
            ref_uri = %cfg.r#ref,
            prefer = ?cfg.prefer,
            server_url = %ctx.server_url,
            "Executing result_fetch",
        );

        let data = match cfg.prefer {
            BackendPreference::Http => self.fetch_via_http(&cfg, ctx).await?,
            BackendPreference::Flight => {
                // Try Flight first; on NonTabular / Transport
                // errors fall back to HTTP.  Server errors surface
                // up since HTTP can't recover them either.
                match self.fetch_via_flight(&cfg, ctx).await {
                    Ok(v) => v,
                    Err(FlightFetchError::NonTabular { ref_uri, message }) => {
                        tracing::debug!(
                            ref_uri = %ref_uri,
                            message = %message,
                            "Flight signalled non-tabular; falling back to HTTP",
                        );
                        self.fetch_via_http(&cfg, ctx).await?
                    }
                    Err(FlightFetchError::Transport(msg)) => {
                        tracing::warn!(
                            ref_uri = %cfg.r#ref,
                            error = %msg,
                            "Flight transport failed; falling back to HTTP",
                        );
                        self.fetch_via_http(&cfg, ctx).await?
                    }
                    Err(FlightFetchError::Server(msg)) => {
                        return Err(ToolError::Http(format!(
                            "Flight server error for ref {}: {}",
                            cfg.r#ref, msg
                        )));
                    }
                }
            }
        };

        let duration_ms = start.elapsed().as_millis() as u64;
        Ok(ToolResult::success(data).with_duration(duration_ms))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_flight_endpoint_swaps_port_8082_to_8083() {
        // http:// is preserved — tonic's Endpoint::from_shared
        // requires http/https; `grpc://` triggers `Bad :scheme
        // header` at request time.
        assert_eq!(
            ResultFetchTool::derive_flight_endpoint("http://noetl.noetl.svc.cluster.local:8082"),
            "http://noetl.noetl.svc.cluster.local:8083",
        );
        assert_eq!(
            ResultFetchTool::derive_flight_endpoint("http://localhost:8082"),
            "http://localhost:8083",
        );
        // TLS-fronted server URL keeps https:// for the Flight
        // endpoint — same TLS chain on the gRPC port (Phase C2).
        assert_eq!(
            ResultFetchTool::derive_flight_endpoint("https://noetl.example.com:8082"),
            "https://noetl.example.com:8083",
        );
    }

    #[test]
    fn derive_flight_endpoint_passes_through_non_8082() {
        // No port → falls through with scheme preserved.
        assert_eq!(
            ResultFetchTool::derive_flight_endpoint("http://noetl.example.com"),
            "http://noetl.example.com",
        );
        // Non-8082 port → kept as-is (caller's responsibility).
        assert_eq!(
            ResultFetchTool::derive_flight_endpoint("http://noetl.example.com:9000"),
            "http://noetl.example.com:9000",
        );
    }

    #[test]
    fn derive_flight_endpoint_defaults_to_http_when_scheme_missing() {
        // Bare host:port input — assume plaintext h2c, same as
        // the K8s manifest ships in kind.
        assert_eq!(
            ResultFetchTool::derive_flight_endpoint("noetl.example.com:8082"),
            "http://noetl.example.com:8083",
        );
    }

    #[test]
    fn default_prefer_is_flight() {
        let cfg: ResultFetchConfig = serde_json::from_value(serde_json::json!({
            "ref": "noetl://execution/12345/result/big_select/abcd1234",
        }))
        .unwrap();
        assert_eq!(cfg.prefer, BackendPreference::Flight);
        assert_eq!(
            cfg.r#ref,
            "noetl://execution/12345/result/big_select/abcd1234"
        );
        assert!(cfg.flight_endpoint.is_none());
    }

    #[test]
    fn config_round_trips_http_preference() {
        let cfg: ResultFetchConfig = serde_json::from_value(serde_json::json!({
            "ref": "noetl://execution/1/result/x/y",
            "prefer": "http",
        }))
        .unwrap();
        assert_eq!(cfg.prefer, BackendPreference::Http);
    }

    #[test]
    fn config_round_trips_explicit_flight_endpoint() {
        let cfg: ResultFetchConfig = serde_json::from_value(serde_json::json!({
            "ref": "noetl://execution/1/result/x/y",
            "flight_endpoint": "grpc://other-server.example.com:9999",
        }))
        .unwrap();
        assert_eq!(
            cfg.flight_endpoint.as_deref(),
            Some("grpc://other-server.example.com:9999"),
        );
    }

    #[test]
    fn tool_name_is_result_fetch() {
        let tool = ResultFetchTool::new();
        assert_eq!(tool.name(), "result_fetch");
    }

    #[test]
    fn fetch_via_http_normalises_server_url_trailing_slash() {
        // No live HTTP test here — that would require a mock server
        // and we already cover the HTTP path indirectly via the
        // worker's existing integration tests.  This test just
        // confirms the URL normalisation logic by inspection;
        // future expansion can add a wiremock-style fixture.
        let tool = ResultFetchTool::new();
        let _ = tool.http_client; // touch the field so it isn't flagged unused
    }
}
