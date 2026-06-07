//! HTTP request tool.

use async_trait::async_trait;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

use crate::auth::{AuthCredentials, AuthResolver};
use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

/// HTTP method.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
#[allow(clippy::upper_case_acronyms)] // HTTP methods are conventionally uppercase
pub enum HttpMethod {
    #[default]
    GET,
    POST,
    PUT,
    PATCH,
    DELETE,
    HEAD,
    OPTIONS,
}

impl From<HttpMethod> for Method {
    fn from(method: HttpMethod) -> Self {
        match method {
            HttpMethod::GET => Method::GET,
            HttpMethod::POST => Method::POST,
            HttpMethod::PUT => Method::PUT,
            HttpMethod::PATCH => Method::PATCH,
            HttpMethod::DELETE => Method::DELETE,
            HttpMethod::HEAD => Method::HEAD,
            HttpMethod::OPTIONS => Method::OPTIONS,
        }
    }
}

/// HTTP tool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// URL to request.
    pub url: String,

    /// HTTP method (default: GET).
    #[serde(default)]
    pub method: HttpMethod,

    /// Request headers.
    #[serde(default, deserialize_with = "deserialize_string_map")]
    pub headers: HashMap<String, String>,

    /// Request body (for POST/PUT/PATCH).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<serde_json::Value>,

    /// JSON body (alternative to body, sets Content-Type).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json: Option<serde_json::Value>,

    /// Form data (sets Content-Type to application/x-www-form-urlencoded).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_opt_string_map"
    )]
    pub form: Option<HashMap<String, String>>,

    /// Query parameters.
    #[serde(default, deserialize_with = "deserialize_string_map")]
    pub params: HashMap<String, String>,

    /// Request timeout in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,

    /// Whether to follow redirects (default: true).
    #[serde(default = "default_follow_redirects")]
    pub follow_redirects: bool,

    /// Expected response type.
    #[serde(default)]
    pub response_type: ResponseType,
}

fn default_follow_redirects() -> bool {
    true
}

/// Coerce a JSON-valued map into a string-valued map. Template rendering
/// commonly produces non-string scalars for header / query-param values —
/// e.g. `offset: "{{ ctx.offset }}"` rendering to an integer, or an
/// undefined reference rendering to null. Without coercion these fail
/// deserialization with `invalid type: integer/null, expected a string`,
/// which blocks the bulk of the pagination / http fixtures. Numbers and
/// bools are stringified; null entries are dropped (an unset param/header
/// is omitted rather than sent as the literal "null").
fn coerce_string_map(raw: HashMap<String, serde_json::Value>) -> HashMap<String, String> {
    let mut out = HashMap::with_capacity(raw.len());
    for (k, v) in raw {
        match v {
            serde_json::Value::Null => {}
            serde_json::Value::String(s) => {
                out.insert(k, s);
            }
            serde_json::Value::Bool(b) => {
                out.insert(k, b.to_string());
            }
            serde_json::Value::Number(n) => {
                out.insert(k, n.to_string());
            }
            other => {
                out.insert(k, other.to_string());
            }
        }
    }
    out
}

fn deserialize_string_map<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = HashMap::<String, serde_json::Value>::deserialize(deserializer)?;
    Ok(coerce_string_map(raw))
}

fn deserialize_opt_string_map<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = Option::<HashMap<String, serde_json::Value>>::deserialize(deserializer)?;
    Ok(raw.map(coerce_string_map))
}

/// Expected response type.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ResponseType {
    /// Parse response as JSON.
    #[default]
    Json,
    /// Return response as text.
    Text,
    /// Return response as base64-encoded binary.
    Binary,
}

/// HTTP request tool.
pub struct HttpTool {
    client: reqwest::Client,
    auth_resolver: AuthResolver,
    template_engine: TemplateEngine,
}

impl HttpTool {
    /// Create a new HTTP tool.
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_default();

        Self {
            client,
            auth_resolver: AuthResolver::new(),
            template_engine: TemplateEngine::new(),
        }
    }

    /// Create an HTTP tool with a custom client.
    pub fn with_client(client: reqwest::Client) -> Self {
        Self {
            client,
            auth_resolver: AuthResolver::new(),
            template_engine: TemplateEngine::new(),
        }
    }

    /// Execute an HTTP request.
    pub async fn request(
        &self,
        config: &HttpConfig,
        auth: Option<AuthCredentials>,
    ) -> Result<ToolResult, ToolError> {
        let start = std::time::Instant::now();

        // Build the request
        let method: Method = config.method.clone().into();
        let mut request = self.client.request(method, &config.url);

        // Set query parameters
        if !config.params.is_empty() {
            request = request.query(&config.params);
        }

        // Set headers
        for (key, value) in &config.headers {
            request = request.header(key.as_str(), value.as_str());
        }

        // Set body
        if let Some(ref json) = config.json {
            request = request.json(json);
        } else if let Some(ref form) = config.form {
            request = request.form(form);
        } else if let Some(ref body) = config.body {
            match body {
                serde_json::Value::String(s) => {
                    request = request.body(s.clone());
                }
                _ => {
                    request = request.json(body);
                }
            }
        }

        // Apply authentication
        if let Some(creds) = auth {
            request = creds.apply_to_request(request);
        }

        // Set timeout
        if let Some(timeout) = config.timeout_seconds {
            request = request.timeout(Duration::from_secs(timeout));
        }

        // Execute request
        let response = request.send().await?;

        let status_code = response.status().as_u16();
        let headers: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // Parse response based on type
        let (data, text_body) = match config.response_type {
            ResponseType::Json => {
                let text = response.text().await.unwrap_or_default();
                let json: serde_json::Value =
                    serde_json::from_str(&text).unwrap_or(serde_json::json!(text));
                (json, Some(text))
            }
            ResponseType::Text => {
                let text = response.text().await.unwrap_or_default();
                (serde_json::json!(text), Some(text))
            }
            ResponseType::Binary => {
                let bytes = response.bytes().await.unwrap_or_default();
                let encoded =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
                (
                    serde_json::json!({
                        "base64": encoded,
                        "size": bytes.len()
                    }),
                    None,
                )
            }
        };

        let duration_ms = start.elapsed().as_millis() as u64;

        // Determine success based on status code
        let is_success = (200..300).contains(&status_code);

        let result = ToolResult {
            status: if is_success {
                crate::result::ToolStatus::Success
            } else {
                crate::result::ToolStatus::Error
            },
            data: Some(serde_json::json!({
                "status_code": status_code,
                "headers": headers,
                "body": data,
            })),
            error: if !is_success {
                Some(format!("HTTP {} response", status_code))
            } else {
                None
            },
            stdout: text_body,
            stderr: None,
            exit_code: Some(if is_success { 0 } else { 1 }),
            duration_ms: Some(duration_ms),
            pending_callback: None,
        };

        Ok(result)
    }

    /// Parse HTTP config from tool config.
    fn parse_config(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<HttpConfig, ToolError> {
        let template_ctx = ctx.to_template_context();
        let rendered_config = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;

        serde_json::from_value(rendered_config)
            .map_err(|e| ToolError::Configuration(format!("Invalid http config: {}", e)))
    }
}

impl Default for HttpTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for HttpTool {
    fn name(&self) -> &'static str {
        "http"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let http_config = self.parse_config(config, ctx)?;

        // Resolve authentication if configured
        let auth = if let Some(ref auth_config) = config.auth {
            Some(self.auth_resolver.resolve(auth_config, ctx).await?)
        } else {
            None
        };

        tracing::debug!(
            url = %http_config.url,
            method = ?http_config.method,
            has_auth = auth.is_some(),
            "Executing HTTP request"
        );

        self.request(&http_config, auth).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_method_conversion() {
        assert_eq!(Method::from(HttpMethod::GET), Method::GET);
        assert_eq!(Method::from(HttpMethod::POST), Method::POST);
        assert_eq!(Method::from(HttpMethod::PUT), Method::PUT);
        assert_eq!(Method::from(HttpMethod::DELETE), Method::DELETE);
    }

    #[test]
    fn test_http_config_deserialization() {
        let json = serde_json::json!({
            "url": "https://api.example.com/data",
            "method": "POST",
            "headers": {"Content-Type": "application/json"},
            "json": {"key": "value"}
        });

        let config: HttpConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.url, "https://api.example.com/data");
        assert!(matches!(config.method, HttpMethod::POST));
        assert!(config.json.is_some());
    }

    #[test]
    fn test_http_config_lenient_params_headers() {
        // Template rendering produces non-string scalars for params /
        // headers (e.g. `offset: "{{ ctx.offset }}"` -> integer, an
        // undefined ref -> null). Coerce numbers/bools to strings and
        // drop null entries instead of failing deserialization.
        // Surfaced by pagination/offset, /cursor, /max_iterations,
        // /pipeline (noetl/ai-meta#54).
        let json = serde_json::json!({
            "url": "http://test-server/api/v1/users",
            "params": {"offset": 10, "limit": 25, "cursor": null, "active": true},
            "headers": {"X-Page": 1, "X-Trace": "abc"}
        });
        let config: HttpConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.params.get("offset"), Some(&"10".to_string()));
        assert_eq!(config.params.get("limit"), Some(&"25".to_string()));
        assert_eq!(config.params.get("active"), Some(&"true".to_string()));
        assert!(!config.params.contains_key("cursor")); // null dropped
        assert_eq!(config.headers.get("X-Page"), Some(&"1".to_string()));
        assert_eq!(config.headers.get("X-Trace"), Some(&"abc".to_string()));
    }

    #[test]
    fn test_http_config_defaults() {
        let json = serde_json::json!({
            "url": "https://example.com"
        });

        let config: HttpConfig = serde_json::from_value(json).unwrap();
        assert!(matches!(config.method, HttpMethod::GET));
        assert!(config.follow_redirects);
        assert!(matches!(config.response_type, ResponseType::Json));
    }

    #[tokio::test]
    async fn test_http_tool_interface() {
        let tool = HttpTool::new();
        assert_eq!(tool.name(), "http");
    }

    #[test]
    fn test_response_type_serialization() {
        let rt = ResponseType::Json;
        let json = serde_json::to_string(&rt).unwrap();
        assert_eq!(json, "\"json\"");

        let rt = ResponseType::Text;
        let json = serde_json::to_string(&rt).unwrap();
        assert_eq!(json, "\"text\"");
    }
}
