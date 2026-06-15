//! PostgreSQL query execution tool.

use async_trait::async_trait;
use deadpool_postgres::{Config, Pool, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;

use crate::context::ExecutionContext;
use crate::error::ToolError;
use crate::registry::{Tool, ToolConfig};
use crate::result::ToolResult;
use crate::template::TemplateEngine;

/// PostgreSQL tool configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    /// SQL to execute. Canonical v10 playbooks use `command:`; `query:`
    /// is accepted as an alias for the same field so both shapes parse.
    #[serde(alias = "command")]
    pub query: String,

    /// Query parameters.
    #[serde(default)]
    pub params: Vec<serde_json::Value>,

    /// Connection string (e.g., "postgresql://user:pass@host/db").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_string: Option<String>,

    /// Host (alternative to connection_string).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,

    /// Port (default: 5432).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

    /// Database name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,

    /// Username.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,

    /// Password (or credential name).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,

    /// Schema to set search_path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,

    /// Whether to return results as JSON objects (default: true).
    #[serde(default = "default_as_objects")]
    pub as_objects: bool,
}

fn default_as_objects() -> bool {
    true
}

/// Match a PostgreSQL dollar-quote tag starting at `chars[i]` (which must be
/// `$`). Returns the full opening/closing tag (e.g. `$$` or `$func$`) when the
/// `$` begins a valid dollar-quote delimiter — a `$`, optional identifier
/// (letters / digits / `_`, not starting with a digit), then a closing `$`.
/// Returns `None` for a bare `$` or a positional parameter like `$1`.
fn match_dollar_tag(chars: &[char], i: usize) -> Option<String> {
    if chars.get(i) != Some(&'$') {
        return None;
    }
    let mut j = i + 1;
    // A tag identifier may not start with a digit (so `$1` is a param, not a tag).
    if let Some(&first) = chars.get(j) {
        if first.is_ascii_digit() {
            return None;
        }
    }
    while let Some(&c) = chars.get(j) {
        if c.is_alphanumeric() || c == '_' {
            j += 1;
        } else {
            break;
        }
    }
    if chars.get(j) == Some(&'$') {
        Some(chars[i..=j].iter().collect())
    } else {
        None
    }
}

/// Split a SQL string into individual statements on top-level semicolons,
/// ignoring semicolons inside single-quoted string literals (and the `''`
/// escape sequence) and inside dollar-quoted blocks (`$$ … $$` / `$tag$ … $tag$`,
/// e.g. a plpgsql function body or a `DO` block). Trailing empty fragments are
/// dropped. Used to support canonical v10 multi-statement `command:` blocks on
/// tools whose normal execution path only accepts a single prepared statement.
fn split_sql_statements(sql: &str) -> Vec<String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut dollar_tag: Option<String> = None;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        // Inside a dollar-quoted block: only the matching close tag ends it;
        // semicolons and single quotes are inert.
        if let Some(tag) = &dollar_tag {
            if c == '$' {
                if let Some(close) = match_dollar_tag(&chars, i) {
                    if &close == tag {
                        current.push_str(&close);
                        i += close.chars().count();
                        dollar_tag = None;
                        continue;
                    }
                }
            }
            current.push(c);
            i += 1;
            continue;
        }
        match c {
            '\'' => {
                // `''` inside a string literal is an escaped quote, not a close.
                if in_single && chars.get(i + 1) == Some(&'\'') {
                    current.push('\'');
                    current.push('\'');
                    i += 2;
                    continue;
                }
                in_single = !in_single;
                current.push(c);
                i += 1;
            }
            '$' if !in_single => {
                if let Some(open) = match_dollar_tag(&chars, i) {
                    current.push_str(&open);
                    i += open.chars().count();
                    dollar_tag = Some(open);
                } else {
                    current.push(c);
                    i += 1;
                }
            }
            // `-- line comment` (outside a string / dollar block): copy the
            // comment verbatim through end-of-line WITHOUT interpreting its
            // characters.  Without this, an apostrophe in a comment (e.g.
            // `-- reset this facility's rows`) flips `in_single` and every
            // later `;` is swallowed as if inside a string literal, merging
            // statements into one — which then fails the extended protocol
            // with "cannot insert multiple commands into a prepared statement".
            '-' if !in_single && chars.get(i + 1) == Some(&'-') => {
                while i < chars.len() && chars[i] != '\n' {
                    current.push(chars[i]);
                    i += 1;
                }
            }
            ';' if !in_single => {
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                }
                current.clear();
                i += 1;
            }
            _ => {
                current.push(c);
                i += 1;
            }
        }
    }
    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }
    statements
}

/// PostgreSQL query execution tool.
pub struct PostgresTool {
    /// Connection pools keyed by connection string.
    pools: Arc<RwLock<HashMap<String, Pool>>>,
    template_engine: TemplateEngine,
}

impl PostgresTool {
    /// Create a new PostgreSQL tool.
    pub fn new() -> Self {
        Self {
            pools: Arc::new(RwLock::new(HashMap::new())),
            template_engine: TemplateEngine::new(),
        }
    }

    /// Get or create a connection pool for the given connection string.
    async fn get_pool(&self, connection_string: &str) -> Result<Pool, ToolError> {
        // Check if pool exists
        {
            let pools = self.pools.read().await;
            if let Some(pool) = pools.get(connection_string) {
                return Ok(pool.clone());
            }
        }

        // Create new pool
        let mut config = Config::new();
        config.url = Some(connection_string.to_string());

        let pool = config
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| ToolError::Database(format!("Failed to create pool: {}", e)))?;

        // Store pool
        {
            let mut pools = self.pools.write().await;
            pools.insert(connection_string.to_string(), pool.clone());
        }

        Ok(pool)
    }

    /// Build connection string from config.
    fn build_connection_string(
        &self,
        config: &PostgresConfig,
        ctx: &ExecutionContext,
    ) -> Result<String, ToolError> {
        if let Some(ref conn_str) = config.connection_string {
            return Ok(conn_str.clone());
        }

        let host = config.host.as_deref().unwrap_or("localhost");
        let port = config.port.unwrap_or(5432);
        let database = config.database.as_deref().unwrap_or("postgres");
        let user = config.user.as_deref().unwrap_or("postgres");

        // Try to get password from secrets or config
        let password = if let Some(ref pw) = config.password {
            // Check if it's a credential reference
            ctx.get_secret(pw)
                .map(|s| s.to_string())
                .unwrap_or_else(|| pw.clone())
        } else {
            String::new()
        };

        let conn_str = if password.is_empty() {
            format!("postgresql://{}@{}:{}/{}", user, host, port, database)
        } else {
            format!(
                "postgresql://{}:{}@{}:{}/{}",
                user, password, host, port, database
            )
        };

        Ok(conn_str)
    }

    /// Execute a query and return results.
    pub async fn execute_query(
        &self,
        query: &str,
        params: &[serde_json::Value],
        connection_string: &str,
        schema: Option<&str>,
        as_objects: bool,
    ) -> Result<ToolResult, ToolError> {
        let start = std::time::Instant::now();

        let pool = self.get_pool(connection_string).await?;
        let client = pool
            .get()
            .await
            .map_err(|e| ToolError::Database(format!("Failed to get connection: {e}")))?;

        // Set search_path if schema specified
        if let Some(schema) = schema {
            client
                .execute(&format!("SET search_path TO {}", schema), &[])
                .await
                .map_err(|e| ToolError::Database(format!("Failed to set schema: {}", e)))?;
        }

        // Convert params
        let pg_params: Vec<Box<dyn ToSql + Sync + Send>> =
            params.iter().map(|v| json_to_pg_param(v)).collect();

        let param_refs: Vec<&(dyn ToSql + Sync)> = pg_params
            .iter()
            .map(|p| p.as_ref() as &(dyn ToSql + Sync))
            .collect();

        // Multi-statement support (canonical v10: `CREATE …; INSERT …; SELECT …`
        // in a single `command:`). The extended protocol used by query()/execute()
        // rejects multiple statements ("cannot insert multiple commands into a
        // prepared statement"), so run every statement except the final one via
        // the simple protocol (batch_execute) and let the existing single-statement
        // path handle the last one (which may be a SELECT that returns rows).
        // Bound parameters can't ride the simple protocol, so this only fires when
        // there are none — multi-statement fixtures inline their values.
        let statements = if params.is_empty() {
            split_sql_statements(query)
        } else {
            vec![query.to_string()]
        };
        let effective_query: String = if statements.len() > 1 {
            let (last, leading) = statements.split_last().unwrap();
            let leading_sql = format!("{};", leading.join(";\n"));
            client
                .batch_execute(&leading_sql)
                .await
                .map_err(|e| ToolError::Database(format_pg_error("Batch execute failed", &e)))?;
            last.clone()
        } else {
            query.to_string()
        };
        let query = effective_query.as_str();

        // Check if it's a SELECT query
        let is_select = query.trim().to_uppercase().starts_with("SELECT")
            || query.trim().to_uppercase().starts_with("WITH");

        let result = if is_select {
            // Execute query with results
            let rows = client
                .query(query, &param_refs)
                .await
                .map_err(|e| ToolError::Database(format_pg_error("Query failed", &e)))?;

            if rows.is_empty() {
                serde_json::json!({
                    "columns": [],
                    "rows": [],
                    "row_count": 0
                })
            } else {
                // Get column names
                let columns: Vec<String> = rows[0]
                    .columns()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect();

                // Convert rows to JSON
                let json_rows: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|row| {
                        if as_objects {
                            let mut obj = serde_json::Map::new();
                            for (i, col) in row.columns().iter().enumerate() {
                                let value = pg_value_to_json(row, i);
                                obj.insert(col.name().to_string(), value);
                            }
                            serde_json::Value::Object(obj)
                        } else {
                            let values: Vec<serde_json::Value> = (0..row.columns().len())
                                .map(|i| pg_value_to_json(row, i))
                                .collect();
                            serde_json::Value::Array(values)
                        }
                    })
                    .collect();

                serde_json::json!({
                    "columns": columns,
                    "rows": json_rows,
                    "row_count": json_rows.len()
                })
            }
        } else {
            // Execute without results
            let affected = client
                .execute(query, &param_refs)
                .await
                .map_err(|e| ToolError::Database(format_pg_error("Execute failed", &e)))?;

            serde_json::json!({
                "affected_rows": affected
            })
        };

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(ToolResult::success(result).with_duration(duration_ms))
    }

    /// Parse PostgreSQL config from tool config.
    fn parse_config(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<PostgresConfig, ToolError> {
        let template_ctx = ctx.to_template_context();
        let rendered_config = self
            .template_engine
            .render_value(&config.config, &template_ctx)?;

        serde_json::from_value(rendered_config)
            .map_err(|e| ToolError::Configuration(format!("Invalid postgres config: {}", e)))
    }
}

impl Default for PostgresTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for PostgresTool {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn execute(
        &self,
        config: &ToolConfig,
        ctx: &ExecutionContext,
    ) -> Result<ToolResult, ToolError> {
        let pg_config = self.parse_config(config, ctx)?;
        let connection_string = self.build_connection_string(&pg_config, ctx)?;

        tracing::debug!(
            query = %pg_config.query,
            params_count = pg_config.params.len(),
            schema = ?pg_config.schema,
            "Executing PostgreSQL query"
        );

        self.execute_query(
            &pg_config.query,
            &pg_config.params,
            &connection_string,
            pg_config.schema.as_deref(),
            pg_config.as_objects,
        )
        .await
    }
}

/// Format a `tokio_postgres::Error` so the real server-side message
/// reaches the caller.
///
/// `tokio_postgres::Error`'s `Display` is terse — for a server-side
/// failure it renders just `"db error"`, hiding the actual SQLSTATE +
/// message (e.g. `syntax error at or near ")"`).  The detail lives in
/// the attached `DbError`.  Surface `severity: message (SQLSTATE code)`
/// plus `DETAIL` / `HINT` when present so operators see the real cause
/// in the event log instead of an opaque "Execute failed: db error".
fn format_pg_error(context: &str, e: &tokio_postgres::Error) -> String {
    if let Some(db) = e.as_db_error() {
        let mut msg = format!(
            "{}: {}: {} (SQLSTATE {})",
            context,
            db.severity(),
            db.message(),
            db.code().code()
        );
        if let Some(detail) = db.detail() {
            msg.push_str(&format!(" | DETAIL: {detail}"));
        }
        if let Some(hint) = db.hint() {
            msg.push_str(&format!(" | HINT: {hint}"));
        }
        msg
    } else {
        // Connection / protocol / type errors carry no DbError — the
        // Display + source chain is the most informative we have.
        let mut msg = format!("{context}: {e}");
        let mut src = std::error::Error::source(e);
        while let Some(inner) = src {
            msg.push_str(&format!(": {inner}"));
            src = inner.source();
        }
        msg
    }
}

/// Convert JSON value to PostgreSQL parameter.
fn json_to_pg_param(value: &serde_json::Value) -> Box<dyn ToSql + Sync + Send> {
    match value {
        serde_json::Value::Null => Box::new(Option::<String>::None),
        serde_json::Value::Bool(b) => Box::new(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Box::new(i)
            } else if let Some(f) = n.as_f64() {
                Box::new(f)
            } else {
                Box::new(n.to_string())
            }
        }
        serde_json::Value::String(s) => Box::new(s.clone()),
        _ => Box::new(value.to_string()),
    }
}

/// Convert PostgreSQL row value to JSON.
fn pg_value_to_json(row: &tokio_postgres::Row, idx: usize) -> serde_json::Value {
    // Try different types
    if let Ok(v) = row.try_get::<_, Option<i64>>(idx) {
        return v
            .map(|n| serde_json::json!(n))
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<i32>>(idx) {
        return v
            .map(|n| serde_json::json!(n))
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<f64>>(idx) {
        return v
            .map(|n| serde_json::json!(n))
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<bool>>(idx) {
        return v
            .map(|b| serde_json::json!(b))
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<String>>(idx) {
        return v
            .map(|s| serde_json::json!(s))
            .unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<serde_json::Value>>(idx) {
        return v.unwrap_or(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx) {
        return v
            .map(|dt| serde_json::json!(dt.to_rfc3339()))
            .unwrap_or(serde_json::Value::Null);
    }

    serde_json::Value::Null
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_config_deserialization() {
        let json = serde_json::json!({
            "query": "SELECT * FROM users WHERE id = $1",
            "params": [42],
            "connection_string": "postgresql://user:pass@localhost/db"
        });

        let config: PostgresConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.query, "SELECT * FROM users WHERE id = $1");
        assert_eq!(config.params.len(), 1);
        assert!(config.connection_string.is_some());
    }

    #[test]
    fn test_postgres_config_command_alias() {
        // Canonical v10 postgres steps use `command:` instead of `query:`.
        // The serde alias must accept it and map it to the same field.
        let json = serde_json::json!({
            "command": "DROP TABLE IF EXISTS t; CREATE TABLE t (id INT);"
        });
        let config: PostgresConfig = serde_json::from_value(json).unwrap();
        assert_eq!(
            config.query,
            "DROP TABLE IF EXISTS t; CREATE TABLE t (id INT);"
        );
    }

    #[test]
    fn test_split_sql_statements() {
        assert_eq!(split_sql_statements("SELECT 1").len(), 1);
        assert_eq!(split_sql_statements("SELECT 1;").len(), 1);
        let s = split_sql_statements(
            "DROP TABLE IF EXISTS t; CREATE TABLE t (id INT); INSERT INTO t VALUES (1);",
        );
        assert_eq!(s.len(), 3);
        assert!(s[0].starts_with("DROP"));
        // Semicolons inside single-quoted literals stay put.
        let s = split_sql_statements("INSERT INTO t VALUES ('a;b'); SELECT 1");
        assert_eq!(s.len(), 2);
        assert!(s[0].contains("'a;b'"));
        // Semicolons inside a dollar-quoted block (plpgsql body) are NOT splits.
        let s = split_sql_statements(
            "CREATE FUNCTION f() RETURNS void AS $$ BEGIN PERFORM 1; PERFORM 2; END; $$ LANGUAGE plpgsql; SELECT f();",
        );
        assert_eq!(s.len(), 2);
        assert!(s[0].contains("$$ BEGIN PERFORM 1; PERFORM 2; END; $$"));
        assert!(s[1].starts_with("SELECT f()"));
        // Tagged dollar-quote ($tag$ … $tag$) with inner semicolons.
        let s = split_sql_statements("DO $do$ BEGIN; END $do$; SELECT 1");
        assert_eq!(s.len(), 2);
        // A positional parameter `$1` is not a dollar-quote opener.
        let s = split_sql_statements("UPDATE t SET a = $1 WHERE id = 2; SELECT 1");
        assert_eq!(s.len(), 2);
        // A `--` line comment with an apostrophe must NOT flip string state —
        // otherwise the trailing `;` are swallowed and statements merge (the
        // PFT setup_facility_work failure).  noetl/ai-meta#100.
        let s = split_sql_statements(
            "INSERT INTO t VALUES (1);\n-- reset this facility's rows\nDELETE FROM t;\nSELECT count(*) FROM t;",
        );
        assert_eq!(s.len(), 3, "{s:?}");
        assert!(s[0].starts_with("INSERT"));
        assert!(s[1].contains("DELETE FROM t"));
        assert!(s[2].starts_with("-- reset") || s[2].contains("SELECT count"));
    }

    #[test]
    fn test_postgres_config_with_components() {
        let json = serde_json::json!({
            "query": "SELECT 1",
            "host": "db.example.com",
            "port": 5433,
            "database": "mydb",
            "user": "admin",
            "schema": "public"
        });

        let config: PostgresConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.host, Some("db.example.com".to_string()));
        assert_eq!(config.port, Some(5433));
        assert_eq!(config.database, Some("mydb".to_string()));
    }

    #[test]
    fn test_postgres_config_defaults() {
        let json = serde_json::json!({
            "query": "SELECT 1"
        });

        let config: PostgresConfig = serde_json::from_value(json).unwrap();
        assert!(config.params.is_empty());
        assert!(config.connection_string.is_none());
        assert!(config.as_objects);
    }

    #[test]
    fn test_build_connection_string() {
        let tool = PostgresTool::new();
        let ctx = ExecutionContext::default();

        let config = PostgresConfig {
            query: "SELECT 1".to_string(),
            params: vec![],
            connection_string: None,
            host: Some("localhost".to_string()),
            port: Some(5432),
            database: Some("testdb".to_string()),
            user: Some("testuser".to_string()),
            password: Some("testpass".to_string()),
            schema: None,
            as_objects: true,
        };

        let conn_str = tool.build_connection_string(&config, &ctx).unwrap();
        assert!(conn_str.contains("localhost"));
        assert!(conn_str.contains("testdb"));
        assert!(conn_str.contains("testuser"));
    }

    #[tokio::test]
    async fn test_postgres_tool_interface() {
        let tool = PostgresTool::new();
        assert_eq!(tool.name(), "postgres");
    }
}
