//! Data transfer tool for moving data between database systems.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::auth::AuthResolver;
use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

/// Transfer source type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Snowflake,
    Postgres,
    #[serde(alias = "HTTP")]
    Http,
    DuckDb,
}

/// Transfer target type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TargetType {
    Snowflake,
    Postgres,
    DuckDb,
}

/// Transfer mode.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TransferMode {
    /// Append to existing data.
    #[default]
    Append,
    /// Replace all data in target.
    Replace,
    /// Upsert based on primary key.
    Upsert,
}

/// Source configuration for data transfer.
///
/// Credential fields (Snowflake `account`/`user`/`private_key`/`public_key`/…,
/// Postgres `host`/`port`/`user`/`password`/`database`) are NOT declared
/// explicitly — the worker's credential-alias resolver injects them into the
/// `source` object before this deserializes, and `#[serde(flatten)] extra`
/// captures whatever it injected.  Each per-backend transfer arm reads what it
/// needs from `extra` (e.g. by building a `SnowflakeConfig` / connection
/// string).  A leftover `auth:` / `credential:` key (if resolution was skipped)
/// also lands in `extra` and is ignored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    /// Source type (snowflake, postgres, http, duckdb).
    #[serde(alias = "tool", alias = "kind")]
    #[serde(rename = "type")]
    pub source_type: SourceType,

    /// SQL query to fetch data from source.
    #[serde(default)]
    pub query: String,

    /// URL for HTTP sources.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    /// HTTP method for HTTP sources.
    #[serde(default = "default_http_method")]
    pub method: String,

    /// HTTP headers for HTTP sources.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,

    /// JSON path for extracting data from HTTP response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_path: Option<String>,

    /// Connection string (for postgres/duckdb).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,

    /// Resolver-injected credential fields + any other backend-specific keys.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

fn default_http_method() -> String {
    "GET".to_string()
}

/// Render a JSON value as a SQL literal for an inlined `INSERT ... VALUES`
/// (used writing to Snowflake, whose SQL API statements are generated rather
/// than parameterised).  Strings are single-quoted with `'` doubled; objects /
/// arrays are emitted as quoted JSON text (Snowflake parses these into VARIANT
/// on insert).
fn sql_literal(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}

/// Coerce a Snowflake SQL-API cell (always a JSON string, or null) into the
/// text param the typed `$n::text::<udt>` cast expects.  Snowflake timestamps
/// arrive in an internal `<epoch_seconds>.<nanos> <tz_offset_minutes>` shape
/// that Postgres can't parse, so for timestamp columns we reformat to RFC3339
/// first.  Non-string values (from non-Snowflake callers) are stringified.
fn coerce_snowflake_value(udt: Option<&String>, value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Null => serde_json::Value::Null,
        serde_json::Value::String(s) => {
            if udt.map(|u| u.starts_with("timestamp")).unwrap_or(false) {
                if let Some(iso) = snowflake_timestamp_to_rfc3339(&s) {
                    return serde_json::Value::String(iso);
                }
            }
            serde_json::Value::String(s)
        }
        other => serde_json::Value::String(other.to_string()),
    }
}

/// Convert Snowflake's internal timestamp string (`"<epoch_seconds>.<nanos>
/// <tz_offset_minutes>"`, e.g. `"1781494755.203000000 1020"`) to an RFC3339
/// UTC string a Postgres `timestamptz` accepts.  Returns `None` when the input
/// doesn't match that shape (already-ISO values pass through unchanged).
fn snowflake_timestamp_to_rfc3339(s: &str) -> Option<String> {
    let first = s.split_whitespace().next()?;
    let (secs_str, nanos_str) = match first.split_once('.') {
        Some((a, b)) => (a, b),
        None => (first, ""),
    };
    let secs: i64 = secs_str.parse().ok()?;
    let nanos: u32 = if nanos_str.is_empty() {
        0
    } else {
        let take = nanos_str.len().min(9);
        format!("{:0<9}", &nanos_str[..take]).parse().ok()?
    };
    chrono::DateTime::from_timestamp(secs, nanos).map(|dt| dt.to_rfc3339())
}

/// Build a Postgres connection string for a transfer endpoint from the explicit
/// `connection` string, an injected `connection_string`, or the discrete
/// host/port/user/password/database fields the credential resolver injects
/// (`apply_postgres` in the worker).  Mirrors `PostgresTool`'s own
/// connection-string shape.
fn pg_connection_string(
    connection: Option<&String>,
    extra: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, ToolError> {
    if let Some(conn) = connection {
        return Ok(conn.clone());
    }
    let s = |k: &str| extra.get(k).and_then(|v| v.as_str());
    if let Some(conn) = s("connection_string") {
        return Ok(conn.to_string());
    }
    let host = s("host").ok_or_else(|| {
        ToolError::Configuration(
            "Postgres target requires a host or a connection string".to_string(),
        )
    })?;
    let user =
        s("user").ok_or_else(|| ToolError::Configuration("Postgres target requires a user".to_string()))?;
    let database = s("database")
        .or_else(|| s("dbname"))
        .ok_or_else(|| ToolError::Configuration("Postgres target requires a database".to_string()))?;
    // `port` may arrive as a JSON number or a string (keychain values are strings).
    let port = extra
        .get("port")
        .and_then(|v| v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(5432);
    let conn = match s("password") {
        Some(pw) if !pw.is_empty() => {
            format!("postgresql://{}:{}@{}:{}/{}", user, pw, host, port, database)
        }
        _ => format!("postgresql://{}@{}:{}/{}", user, host, port, database),
    };
    Ok(conn)
}

/// Target configuration for data transfer.  See [`SourceConfig`] for how
/// resolver-injected credential fields flow through `extra`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetConfig {
    /// Target type (snowflake, postgres, duckdb).
    #[serde(alias = "tool", alias = "kind")]
    #[serde(rename = "type")]
    pub target_type: TargetType,

    /// Target table name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,

    /// Custom query for INSERT/UPSERT operations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,

    /// Column mapping from source to target.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mapping: Option<HashMap<String, String>>,

    /// Connection string (for postgres/duckdb).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,

    /// Resolver-injected credential fields + any other backend-specific keys.
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// Transfer tool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferConfig {
    /// Source configuration.
    pub source: SourceConfig,

    /// Target configuration.
    pub target: TargetConfig,

    /// Number of rows per batch.
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,

    /// Transfer mode.
    #[serde(default)]
    pub mode: TransferMode,
}

fn default_chunk_size() -> usize {
    1000
}

/// Transfer result data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferResultData {
    /// Transfer direction description.
    pub direction: String,

    /// Source type.
    pub source_type: String,

    /// Target type.
    pub target_type: String,

    /// Transfer mode used.
    pub mode: String,

    /// Number of rows transferred.
    pub rows_transferred: usize,

    /// Number of chunks processed.
    pub chunks_processed: usize,

    /// Target table name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_table: Option<String>,

    /// Columns transferred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
}

/// Data transfer tool.
pub struct TransferTool {
    http_client: reqwest::Client,
    #[allow(dead_code)]
    auth_resolver: AuthResolver,
    template_engine: TemplateEngine,
}

impl TransferTool {
    /// Create a new transfer tool.
    pub fn new() -> Self {
        Self {
            http_client: reqwest::Client::new(),
            auth_resolver: AuthResolver::new(),
            template_engine: TemplateEngine::new(),
        }
    }

    /// Execute data transfer.
    pub async fn execute_transfer(
        &self,
        config: &TransferConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let start = std::time::Instant::now();

        // Validate transfer direction is supported
        self.validate_transfer_direction(&config.source.source_type, &config.target.target_type)?;

        // Execute transfer based on source/target types
        let result_data = match (&config.source.source_type, &config.target.target_type) {
            (SourceType::Postgres, TargetType::Postgres) => {
                self.transfer_postgres_to_postgres(config, ctx).await?
            }
            (SourceType::Http, TargetType::Postgres) => {
                self.transfer_http_to_postgres(config, ctx).await?
            }
            (SourceType::DuckDb, TargetType::Postgres) => {
                self.transfer_duckdb_to_postgres(config, ctx).await?
            }
            (SourceType::Postgres, TargetType::DuckDb) => {
                self.transfer_postgres_to_duckdb(config, ctx).await?
            }
            (SourceType::Snowflake, TargetType::Postgres) => {
                self.transfer_snowflake_to_postgres(config, ctx).await?
            }
            (SourceType::Postgres, TargetType::Snowflake) => {
                self.transfer_postgres_to_snowflake(config, ctx).await?
            }
            _ => {
                return Err(ToolError::Configuration(format!(
                    "Transfer from {:?} to {:?} is not yet implemented",
                    config.source.source_type, config.target.target_type
                )));
            }
        };

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(
            ToolResult::success(serde_json::to_value(&result_data).unwrap())
                .with_duration(duration_ms),
        )
    }

    /// Validate that the transfer direction is supported.
    fn validate_transfer_direction(
        &self,
        source: &SourceType,
        target: &TargetType,
    ) -> Result<(), ToolError> {
        let supported = matches!(
            (source, target),
            (SourceType::Postgres, TargetType::Postgres)
                | (SourceType::Http, TargetType::Postgres)
                | (SourceType::DuckDb, TargetType::Postgres)
                | (SourceType::Postgres, TargetType::DuckDb)
                | (SourceType::Snowflake, TargetType::Postgres)
                | (SourceType::Postgres, TargetType::Snowflake)
        );

        if !supported {
            return Err(ToolError::Configuration(format!(
                "Unsupported transfer direction: {:?} to {:?}",
                source, target
            )));
        }

        Ok(())
    }

    /// Transfer data from PostgreSQL to PostgreSQL.
    async fn transfer_postgres_to_postgres(
        &self,
        config: &TransferConfig,
        _ctx: &ExecutionContext,
    ) -> Result<TransferResultData, ToolError> {
        use crate::tools::postgres::PostgresTool;

        let pg_tool = PostgresTool::new();

        // Get source connection
        let source_conn = config.source.connection.as_ref().ok_or_else(|| {
            ToolError::Configuration("Source connection string required".to_string())
        })?;

        // Get target connection
        let target_conn = config.target.connection.as_ref().ok_or_else(|| {
            ToolError::Configuration("Target connection string required".to_string())
        })?;

        let target_table =
            config.target.table.as_ref().ok_or_else(|| {
                ToolError::Configuration("Target table name required".to_string())
            })?;

        // Fetch data from source
        let source_result = pg_tool
            .execute_query(&config.source.query, &[], source_conn, None, true)
            .await?;

        let source_data = source_result
            .data
            .ok_or_else(|| ToolError::Database("No data returned from source".to_string()))?;

        let rows = source_data["rows"]
            .as_array()
            .ok_or_else(|| ToolError::Database("Invalid source data format".to_string()))?;

        let columns: Vec<String> = source_data["columns"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        if rows.is_empty() {
            return Ok(TransferResultData {
                direction: "postgres_to_postgres".to_string(),
                source_type: "postgres".to_string(),
                target_type: "postgres".to_string(),
                mode: format!("{:?}", config.mode).to_lowercase(),
                rows_transferred: 0,
                chunks_processed: 0,
                target_table: Some(target_table.clone()),
                columns: Some(columns),
            });
        }

        // Handle replace mode - truncate target first
        if matches!(config.mode, TransferMode::Replace) {
            let truncate_query = format!("TRUNCATE TABLE {}", target_table);
            pg_tool
                .execute_query(&truncate_query, &[], target_conn, None, false)
                .await?;
        }

        // Build INSERT query
        let insert_columns = columns.join(", ");
        let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${}", i)).collect();
        let insert_query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            target_table,
            insert_columns,
            placeholders.join(", ")
        );

        // Insert data in chunks
        let mut rows_transferred = 0;
        let mut chunks_processed = 0;

        for chunk in rows.chunks(config.chunk_size) {
            for row in chunk {
                let params: Vec<serde_json::Value> = columns
                    .iter()
                    .map(|col| row.get(col).cloned().unwrap_or(serde_json::Value::Null))
                    .collect();

                pg_tool
                    .execute_query(&insert_query, &params, target_conn, None, false)
                    .await?;
                rows_transferred += 1;
            }
            chunks_processed += 1;
        }

        Ok(TransferResultData {
            direction: "postgres_to_postgres".to_string(),
            source_type: "postgres".to_string(),
            target_type: "postgres".to_string(),
            mode: format!("{:?}", config.mode).to_lowercase(),
            rows_transferred,
            chunks_processed,
            target_table: Some(target_table.clone()),
            columns: Some(columns),
        })
    }

    /// Transfer data from HTTP to PostgreSQL.
    async fn transfer_http_to_postgres(
        &self,
        config: &TransferConfig,
        _ctx: &ExecutionContext,
    ) -> Result<TransferResultData, ToolError> {
        use crate::tools::postgres::PostgresTool;

        let pg_tool = PostgresTool::new();

        // Get HTTP URL
        let url = config.source.url.as_ref().ok_or_else(|| {
            ToolError::Configuration("Source URL required for HTTP transfer".to_string())
        })?;

        // Assemble the target connection from the explicit `connection` string
        // or the credential fields the worker's alias resolver injects into
        // `target.extra` (host/port/user/password/database), mirroring the
        // snowflake->postgres path.  An alias-based `auth:` (e.g. `pg_local`)
        // never sets a literal `connection`, so requiring one here made every
        // aliased http->postgres transfer fail with "Target connection string
        // required".
        let target_conn =
            pg_connection_string(config.target.connection.as_ref(), &config.target.extra)?;

        let target_table =
            config.target.table.as_ref().ok_or_else(|| {
                ToolError::Configuration("Target table name required".to_string())
            })?;

        // Fetch data from HTTP source
        let mut request = match config.source.method.to_uppercase().as_str() {
            "POST" => self.http_client.post(url),
            _ => self.http_client.get(url),
        };

        // Add headers
        if let Some(ref headers) = config.source.headers {
            for (k, v) in headers {
                request = request.header(k, v);
            }
        }

        let response = request
            .send()
            .await
            .map_err(|e| ToolError::Http(format!("HTTP request failed: {}", e)))?;

        let json_data: serde_json::Value = response
            .json()
            .await
            .map_err(|e| ToolError::Http(format!("Failed to parse JSON response: {}", e)))?;

        // Extract data using data_path if provided
        let data = if let Some(ref path) = config.source.data_path {
            extract_json_path(&json_data, path)?
        } else {
            json_data
        };

        // Convert to array
        let rows = match data {
            serde_json::Value::Array(arr) => arr,
            obj @ serde_json::Value::Object(_) => vec![obj],
            _ => {
                return Err(ToolError::Http(
                    "HTTP response data must be an array or object".to_string(),
                ))
            }
        };

        if rows.is_empty() {
            return Ok(TransferResultData {
                direction: "http_to_postgres".to_string(),
                source_type: "http".to_string(),
                target_type: "postgres".to_string(),
                mode: format!("{:?}", config.mode).to_lowercase(),
                rows_transferred: 0,
                chunks_processed: 0,
                target_table: Some(target_table.clone()),
                columns: None,
            });
        }

        // Get columns from mapping or first row
        let columns: Vec<String> = if let Some(ref mapping) = config.target.mapping {
            mapping.keys().cloned().collect()
        } else if let serde_json::Value::Object(obj) = &rows[0] {
            obj.keys().cloned().collect()
        } else {
            return Err(ToolError::Configuration(
                "Cannot determine columns from HTTP data".to_string(),
            ));
        };

        // Handle replace mode
        if matches!(config.mode, TransferMode::Replace) {
            let truncate_query = format!("TRUNCATE TABLE {}", target_table);
            pg_tool
                .execute_query(&truncate_query, &[], &target_conn, None, false)
                .await?;
        }

        // Build INSERT query
        let insert_columns = columns.join(", ");
        let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${}", i)).collect();
        let insert_query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            target_table,
            insert_columns,
            placeholders.join(", ")
        );

        // Insert data in chunks
        let mut rows_transferred = 0;
        let mut chunks_processed = 0;

        let mapping = config.target.mapping.as_ref();

        for chunk in rows.chunks(config.chunk_size) {
            for row in chunk {
                let params: Vec<serde_json::Value> = columns
                    .iter()
                    .map(|col| {
                        let source_field = mapping
                            .and_then(|m| m.get(col))
                            .map(|s| s.as_str())
                            .unwrap_or(col);
                        row.get(source_field)
                            .cloned()
                            .unwrap_or(serde_json::Value::Null)
                    })
                    .collect();

                pg_tool
                    .execute_query(&insert_query, &params, &target_conn, None, false)
                    .await?;
                rows_transferred += 1;
            }
            chunks_processed += 1;
        }

        Ok(TransferResultData {
            direction: "http_to_postgres".to_string(),
            source_type: "http".to_string(),
            target_type: "postgres".to_string(),
            mode: format!("{:?}", config.mode).to_lowercase(),
            rows_transferred,
            chunks_processed,
            target_table: Some(target_table.clone()),
            columns: Some(columns),
        })
    }

    /// Transfer data from Snowflake to PostgreSQL.
    ///
    /// Reads the source via the Snowflake tool (key-pair JWT auth + the SQL
    /// REST API), then writes the rows to Postgres with the shared chunked
    /// INSERT writer.  Credentials for both ends are injected into
    /// `source.extra` / `target.extra` by the worker's alias resolver.
    async fn transfer_snowflake_to_postgres(
        &self,
        config: &TransferConfig,
        _ctx: &ExecutionContext,
    ) -> Result<TransferResultData, ToolError> {
        use crate::tools::postgres::PostgresTool;
        use crate::tools::snowflake::{SnowflakeConfig, SnowflakeTool};

        // Build a SnowflakeConfig from the resolver-injected source fields +
        // the source query as the command.
        let mut sf_map = config.source.extra.clone();
        sf_map.insert(
            "command".to_string(),
            serde_json::Value::String(config.source.query.clone()),
        );
        let sf_config: SnowflakeConfig =
            serde_json::from_value(serde_json::Value::Object(sf_map)).map_err(|e| {
                ToolError::Configuration(format!("invalid Snowflake source config: {e}"))
            })?;

        let (rows, columns) = SnowflakeTool::new().query_rows(&sf_config).await?;

        let target_table = config
            .target
            .table
            .as_ref()
            .ok_or_else(|| ToolError::Configuration("Target table name required".to_string()))?;
        let conn = pg_connection_string(config.target.connection.as_ref(), &config.target.extra)?;
        let pg_tool = PostgresTool::new();

        let mut result = TransferResultData {
            direction: "snowflake_to_postgres".to_string(),
            source_type: "snowflake".to_string(),
            target_type: "postgres".to_string(),
            mode: format!("{:?}", config.mode).to_lowercase(),
            rows_transferred: 0,
            chunks_processed: 0,
            target_table: Some(target_table.clone()),
            columns: Some(columns.clone()),
        };

        // Snowflake's SQL API returns every cell as a STRING.  Look up the
        // target column types so the INSERT can coerce each value with an
        // explicit `$n::text::<udt>` cast (text -> int4 / numeric / jsonb /
        // timestamptz, etc.); Snowflake timestamps need a Rust-side reformat
        // first (their internal `<epoch>.<nanos> <tzmin>` shape doesn't parse
        // as a Postgres timestamptz literal).
        let col_types = self
            .fetch_pg_column_types(&pg_tool, &conn, target_table)
            .await?;

        if matches!(config.mode, TransferMode::Replace) {
            let truncate_query = format!("TRUNCATE TABLE {}", target_table);
            pg_tool
                .execute_query(&truncate_query, &[], &conn, None, false)
                .await?;
        }

        if !rows.is_empty() {
            let placeholders: Vec<String> = columns
                .iter()
                .enumerate()
                .map(|(i, col)| match col_types.get(&col.to_lowercase()) {
                    Some(udt) => format!("${}::text::{}", i + 1, udt),
                    None => format!("${}", i + 1),
                })
                .collect();
            let insert_query = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                target_table,
                columns.join(", "),
                placeholders.join(", ")
            );

            let mapping = config.target.mapping.as_ref();
            for chunk in rows.chunks(config.chunk_size.max(1)) {
                for row in chunk {
                    let params: Vec<serde_json::Value> = columns
                        .iter()
                        .map(|col| {
                            let source_field = mapping
                                .and_then(|m| m.get(col))
                                .map(|s| s.as_str())
                                .unwrap_or(col);
                            let raw =
                                row.get(source_field).cloned().unwrap_or(serde_json::Value::Null);
                            coerce_snowflake_value(col_types.get(&col.to_lowercase()), raw)
                        })
                        .collect();
                    pg_tool
                        .execute_query(&insert_query, &params, &conn, None, false)
                        .await?;
                    result.rows_transferred += 1;
                }
                result.chunks_processed += 1;
            }
        }

        Ok(result)
    }

    /// Look up `column_name -> udt_name` (the Postgres internal type name, e.g.
    /// `int4` / `numeric` / `timestamptz` / `jsonb`) for a target table, so the
    /// Snowflake->Postgres writer can build typed casts.  Returns an empty map
    /// if the lookup yields nothing (the writer then falls back to plain `$n`).
    async fn fetch_pg_column_types(
        &self,
        pg_tool: &crate::tools::postgres::PostgresTool,
        conn: &str,
        target_table: &str,
    ) -> Result<HashMap<String, String>, ToolError> {
        let (schema, table) = match target_table.split_once('.') {
            Some((s, t)) => (
                Some(s.trim().trim_matches('"').to_string()),
                t.trim().trim_matches('"').to_string(),
            ),
            None => (None, target_table.trim().trim_matches('"').to_string()),
        };
        // Table/schema names are config values inlined the same way the INSERT
        // inlines `target_table`; single-quote-escape for safety.
        let esc = |s: &str| s.replace('\'', "''");
        let query = format!(
            "SELECT column_name, udt_name FROM information_schema.columns WHERE table_name = '{}'{}",
            esc(&table),
            schema
                .as_ref()
                .map(|s| format!(" AND table_schema = '{}'", esc(s)))
                .unwrap_or_default(),
        );
        let res = pg_tool.execute_query(&query, &[], conn, None, true).await?;
        let mut map = HashMap::new();
        if let Some(data) = res.data {
            if let Some(rows) = data["rows"].as_array() {
                for r in rows {
                    if let (Some(c), Some(u)) =
                        (r["column_name"].as_str(), r["udt_name"].as_str())
                    {
                        map.insert(c.to_lowercase(), u.to_string());
                    }
                }
            }
        }
        Ok(map)
    }

    /// Transfer data from PostgreSQL to Snowflake.
    ///
    /// Reads the source via the Postgres tool, then writes each row to
    /// Snowflake as an `INSERT ... VALUES (...)` with SQL-escaped literals
    /// (the Snowflake SQL REST API runs one statement per request and the
    /// statements are generated, not parameterised).  Credentials for both
    /// ends are injected into `source.extra` / `target.extra` by the worker.
    async fn transfer_postgres_to_snowflake(
        &self,
        config: &TransferConfig,
        _ctx: &ExecutionContext,
    ) -> Result<TransferResultData, ToolError> {
        use crate::tools::postgres::PostgresTool;
        use crate::tools::snowflake::{SnowflakeConfig, SnowflakeTool};

        let conn =
            pg_connection_string(config.source.connection.as_ref(), &config.source.extra)?;
        let pg_tool = PostgresTool::new();
        let res = pg_tool
            .execute_query(&config.source.query, &[], &conn, None, true)
            .await?;
        let data = res
            .data
            .ok_or_else(|| ToolError::Database("No data from Postgres source".to_string()))?;
        let rows = data["rows"].as_array().cloned().unwrap_or_default();
        let columns: Vec<String> = data["columns"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let target_table = config
            .target
            .table
            .as_ref()
            .ok_or_else(|| ToolError::Configuration("Target table name required".to_string()))?;

        let mut result = TransferResultData {
            direction: "postgres_to_snowflake".to_string(),
            source_type: "postgres".to_string(),
            target_type: "snowflake".to_string(),
            mode: format!("{:?}", config.mode).to_lowercase(),
            rows_transferred: 0,
            chunks_processed: 0,
            target_table: Some(target_table.clone()),
            columns: Some(columns.clone()),
        };

        if rows.is_empty() {
            return Ok(result);
        }

        // Generate one INSERT per row with SQL-escaped literals.
        let mapping = config.target.mapping.as_ref();
        let col_list = columns.join(", ");
        let statements: Vec<String> = rows
            .iter()
            .map(|row| {
                let values: Vec<String> = columns
                    .iter()
                    .map(|col| {
                        let source_field = mapping
                            .and_then(|m| m.get(col))
                            .map(|s| s.as_str())
                            .unwrap_or(col);
                        sql_literal(row.get(source_field).unwrap_or(&serde_json::Value::Null))
                    })
                    .collect();
                format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    target_table,
                    col_list,
                    values.join(", ")
                )
            })
            .collect();
        let count = statements.len();

        // Build a SnowflakeConfig from the resolver-injected target fields +
        // the generated INSERTs, and run them.
        let mut sf_map = config.target.extra.clone();
        sf_map.insert(
            "commands".to_string(),
            serde_json::Value::Array(
                statements.into_iter().map(serde_json::Value::String).collect(),
            ),
        );
        let sf_config: SnowflakeConfig =
            serde_json::from_value(serde_json::Value::Object(sf_map)).map_err(|e| {
                ToolError::Configuration(format!("invalid Snowflake target config: {e}"))
            })?;
        let sf_result = SnowflakeTool::new().execute_commands(&sf_config).await?;
        if let Some(err) = sf_result.error {
            return Err(ToolError::Database(format!(
                "Snowflake write failed: {} ({})",
                err,
                sf_result
                    .data
                    .map(|d| d.to_string())
                    .unwrap_or_default()
            )));
        }

        result.rows_transferred = count;
        result.chunks_processed = 1;
        Ok(result)
    }

    /// Transfer data from DuckDB to PostgreSQL.
    async fn transfer_duckdb_to_postgres(
        &self,
        config: &TransferConfig,
        _ctx: &ExecutionContext,
    ) -> Result<TransferResultData, ToolError> {
        use crate::tools::duckdb::DuckdbTool;
        use crate::tools::postgres::PostgresTool;

        let duckdb_tool = DuckdbTool::new();
        let pg_tool = PostgresTool::new();

        let db_path = config.source.connection.as_deref();
        let target_conn = config.target.connection.as_ref().ok_or_else(|| {
            ToolError::Configuration("Target connection string required".to_string())
        })?;

        let target_table =
            config.target.table.as_ref().ok_or_else(|| {
                ToolError::Configuration("Target table name required".to_string())
            })?;

        // Fetch data from DuckDB
        let source_result = duckdb_tool.execute_query(&config.source.query, &[], db_path, true)?;

        let source_data = source_result
            .data
            .ok_or_else(|| ToolError::Database("No data returned from source".to_string()))?;

        let rows = source_data["rows"]
            .as_array()
            .ok_or_else(|| ToolError::Database("Invalid source data format".to_string()))?;

        let columns: Vec<String> = source_data["columns"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        if rows.is_empty() {
            return Ok(TransferResultData {
                direction: "duckdb_to_postgres".to_string(),
                source_type: "duckdb".to_string(),
                target_type: "postgres".to_string(),
                mode: format!("{:?}", config.mode).to_lowercase(),
                rows_transferred: 0,
                chunks_processed: 0,
                target_table: Some(target_table.clone()),
                columns: Some(columns),
            });
        }

        // Handle replace mode
        if matches!(config.mode, TransferMode::Replace) {
            let truncate_query = format!("TRUNCATE TABLE {}", target_table);
            pg_tool
                .execute_query(&truncate_query, &[], target_conn, None, false)
                .await?;
        }

        // Build INSERT query
        let insert_columns = columns.join(", ");
        let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${}", i)).collect();
        let insert_query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            target_table,
            insert_columns,
            placeholders.join(", ")
        );

        // Insert data in chunks
        let mut rows_transferred = 0;
        let mut chunks_processed = 0;

        for chunk in rows.chunks(config.chunk_size) {
            for row in chunk {
                let params: Vec<serde_json::Value> = columns
                    .iter()
                    .map(|col| row.get(col).cloned().unwrap_or(serde_json::Value::Null))
                    .collect();

                pg_tool
                    .execute_query(&insert_query, &params, target_conn, None, false)
                    .await?;
                rows_transferred += 1;
            }
            chunks_processed += 1;
        }

        Ok(TransferResultData {
            direction: "duckdb_to_postgres".to_string(),
            source_type: "duckdb".to_string(),
            target_type: "postgres".to_string(),
            mode: format!("{:?}", config.mode).to_lowercase(),
            rows_transferred,
            chunks_processed,
            target_table: Some(target_table.clone()),
            columns: Some(columns),
        })
    }

    /// Transfer data from PostgreSQL to DuckDB.
    async fn transfer_postgres_to_duckdb(
        &self,
        config: &TransferConfig,
        _ctx: &ExecutionContext,
    ) -> Result<TransferResultData, ToolError> {
        use crate::tools::duckdb::DuckdbTool;
        use crate::tools::postgres::PostgresTool;

        let pg_tool = PostgresTool::new();
        let duckdb_tool = DuckdbTool::new();

        let source_conn = config.source.connection.as_ref().ok_or_else(|| {
            ToolError::Configuration("Source connection string required".to_string())
        })?;

        let db_path = config.target.connection.as_deref();
        let target_table =
            config.target.table.as_ref().ok_or_else(|| {
                ToolError::Configuration("Target table name required".to_string())
            })?;

        // Fetch data from PostgreSQL
        let source_result = pg_tool
            .execute_query(&config.source.query, &[], source_conn, None, true)
            .await?;

        let source_data = source_result
            .data
            .ok_or_else(|| ToolError::Database("No data returned from source".to_string()))?;

        let rows = source_data["rows"]
            .as_array()
            .ok_or_else(|| ToolError::Database("Invalid source data format".to_string()))?;

        let columns: Vec<String> = source_data["columns"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        if rows.is_empty() {
            return Ok(TransferResultData {
                direction: "postgres_to_duckdb".to_string(),
                source_type: "postgres".to_string(),
                target_type: "duckdb".to_string(),
                mode: format!("{:?}", config.mode).to_lowercase(),
                rows_transferred: 0,
                chunks_processed: 0,
                target_table: Some(target_table.clone()),
                columns: Some(columns),
            });
        }

        // Handle replace mode - drop and recreate table
        if matches!(config.mode, TransferMode::Replace) {
            let drop_query = format!("DROP TABLE IF EXISTS {}", target_table);
            let _ = duckdb_tool.execute_query(&drop_query, &[], db_path, true);
        }

        // Build INSERT query with placeholders
        let insert_columns = columns.join(", ");
        let placeholders: Vec<String> = (0..columns.len()).map(|_| "?".to_string()).collect();
        let insert_query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            target_table,
            insert_columns,
            placeholders.join(", ")
        );

        // Insert data in chunks
        let mut rows_transferred = 0;
        let mut chunks_processed = 0;

        for chunk in rows.chunks(config.chunk_size) {
            for row in chunk {
                let params: Vec<serde_json::Value> = columns
                    .iter()
                    .map(|col| row.get(col).cloned().unwrap_or(serde_json::Value::Null))
                    .collect();

                duckdb_tool.execute_query(&insert_query, &params, db_path, true)?;
                rows_transferred += 1;
            }
            chunks_processed += 1;
        }

        Ok(TransferResultData {
            direction: "postgres_to_duckdb".to_string(),
            source_type: "postgres".to_string(),
            target_type: "duckdb".to_string(),
            mode: format!("{:?}", config.mode).to_lowercase(),
            rows_transferred,
            chunks_processed,
            target_table: Some(target_table.clone()),
            columns: Some(columns),
        })
    }

    /// Parse transfer config from tool config.
    fn parse_config(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<TransferConfig, ToolError> {
        let template_ctx = ctx.to_template_context();
        let rendered_config = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;

        serde_json::from_value(rendered_config)
            .map_err(|e| ToolError::Configuration(format!("Invalid transfer config: {}", e)))
    }
}

/// Extract nested value from JSON using dot notation path.
fn extract_json_path(json: &serde_json::Value, path: &str) -> Result<serde_json::Value, ToolError> {
    let mut current = json;

    for segment in path.split('.') {
        current = current.get(segment).ok_or_else(|| {
            ToolError::Http(format!("Path segment '{}' not found in JSON", segment))
        })?;
    }

    Ok(current.clone())
}

impl Default for TransferTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for TransferTool {
    fn name(&self) -> &'static str {
        "transfer"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let transfer_config = self.parse_config(config, ctx)?;

        tracing::debug!(
            source = ?transfer_config.source.source_type,
            target = ?transfer_config.target.target_type,
            mode = ?transfer_config.mode,
            chunk_size = transfer_config.chunk_size,
            "Executing data transfer"
        );

        self.execute_transfer(&transfer_config, ctx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snowflake_timestamp_converts_to_rfc3339() {
        // The exact shape Snowflake's SQL API returns for TIMESTAMP_TZ(9).
        let iso = snowflake_timestamp_to_rfc3339("1781494755.203000000 1020").unwrap();
        assert!(iso.starts_with("2026-"), "{iso}");
        assert!(iso.contains("T"));
        // Fewer fractional digits still parse (right-padded to 9).
        assert!(snowflake_timestamp_to_rfc3339("1781494755.5").is_some());
        // No fractional / no offset.
        assert!(snowflake_timestamp_to_rfc3339("1781494755").is_some());
        // Non-Snowflake input falls through (caller keeps the original string).
        assert!(snowflake_timestamp_to_rfc3339("2026-06-15T00:00:00Z").is_none());
    }

    #[test]
    fn coerce_snowflake_value_casts_by_type() {
        let ts = "timestamptz".to_string();
        // timestamp string is reformatted
        let v = coerce_snowflake_value(Some(&ts), serde_json::json!("1781494755.203000000 1020"));
        assert!(v.as_str().unwrap().starts_with("2026-"));
        // numeric/text strings pass through unchanged
        let num = "numeric".to_string();
        assert_eq!(
            coerce_snowflake_value(Some(&num), serde_json::json!("100.50")),
            serde_json::json!("100.50")
        );
        // null stays null
        assert!(coerce_snowflake_value(Some(&num), serde_json::Value::Null).is_null());
    }

    #[test]
    fn test_transfer_config_deserialization() {
        let json = serde_json::json!({
            "source": {
                "type": "postgres",
                "query": "SELECT * FROM users",
                "connection": "postgres://localhost/source"
            },
            "target": {
                "type": "postgres",
                "table": "users_copy",
                "connection": "postgres://localhost/target"
            },
            "chunk_size": 500,
            "mode": "append"
        });

        let config: TransferConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.source.source_type, SourceType::Postgres);
        assert_eq!(config.target.target_type, TargetType::Postgres);
        assert_eq!(config.chunk_size, 500);
    }

    #[test]
    fn test_transfer_config_defaults() {
        let json = serde_json::json!({
            "source": {
                "type": "http",
                "url": "https://api.example.com/data",
                "query": ""
            },
            "target": {
                "type": "postgres",
                "table": "imported_data",
                "connection": "postgres://localhost/db"
            }
        });

        let config: TransferConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.chunk_size, 1000);
        assert!(matches!(config.mode, TransferMode::Append));
    }

    #[test]
    fn test_extract_json_path() {
        let json = serde_json::json!({
            "data": {
                "results": {
                    "items": [1, 2, 3]
                }
            }
        });

        let result = extract_json_path(&json, "data.results.items").unwrap();
        assert_eq!(result, serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn test_extract_json_path_not_found() {
        let json = serde_json::json!({"data": {"items": []}});
        let result = extract_json_path(&json, "data.results");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_transfer_tool_interface() {
        let tool = TransferTool::new();
        assert_eq!(tool.name(), "transfer");
    }

    #[test]
    fn pg_connection_string_prefers_literal_connection() {
        let mut extra = serde_json::Map::new();
        extra.insert("host".to_string(), serde_json::json!("db"));
        let conn = "postgresql://u:p@h:5432/d".to_string();
        assert_eq!(
            pg_connection_string(Some(&conn), &extra).unwrap(),
            "postgresql://u:p@h:5432/d"
        );
    }

    #[test]
    fn pg_connection_string_assembles_from_alias_fields() {
        // The credential resolver injects discrete host/port/user/password/
        // database fields into `target.extra` when the fixture uses an
        // alias-based `auth:` (e.g. `pg_local`) with no literal `connection`.
        let mut extra = serde_json::Map::new();
        extra.insert("host".to_string(), serde_json::json!("localhost"));
        // `port` may arrive as a JSON string (keychain values are strings).
        extra.insert("port".to_string(), serde_json::json!("54321"));
        extra.insert("user".to_string(), serde_json::json!("noetl"));
        extra.insert("password".to_string(), serde_json::json!("secret"));
        extra.insert("database".to_string(), serde_json::json!("demo"));

        let conn = pg_connection_string(None, &extra).unwrap();
        assert_eq!(conn, "postgresql://noetl:secret@localhost:54321/demo");
    }

    #[test]
    fn http_to_postgres_target_parses_alias_injected_fields() {
        // An http->postgres transfer with an alias-based `auth:` deserializes
        // with no literal `target.connection`; the resolver-injected fields
        // flow through `target.extra` and pg_connection_string assembles the
        // DSN.  Regression guard for "Target connection string required".
        let json = serde_json::json!({
            "source": {
                "type": "http",
                "url": "https://api.example.com/data",
                "method": "GET"
            },
            "target": {
                "type": "postgres",
                "table": "public.http_dst",
                "mapping": {"post_id": "id"},
                "host": "localhost",
                "port": 5432,
                "user": "noetl",
                "password": "pw",
                "database": "demo"
            }
        });

        let config: TransferConfig = serde_json::from_value(json).unwrap();
        assert!(config.target.connection.is_none());
        let conn =
            pg_connection_string(config.target.connection.as_ref(), &config.target.extra).unwrap();
        assert_eq!(conn, "postgresql://noetl:pw@localhost:5432/demo");
    }

    #[test]
    fn test_transfer_result_serialization() {
        let result = TransferResultData {
            direction: "postgres_to_postgres".to_string(),
            source_type: "postgres".to_string(),
            target_type: "postgres".to_string(),
            mode: "append".to_string(),
            rows_transferred: 100,
            chunks_processed: 10,
            target_table: Some("users".to_string()),
            columns: Some(vec!["id".to_string(), "name".to_string()]),
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("postgres_to_postgres"));
        assert!(json.contains("100"));
    }
}
