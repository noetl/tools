//! Template engine implementation using minijinja.
//!
//! ## `.result` accessor proxy (cross-runtime template compatibility)
//!
//! NoETL playbooks address prior-step outputs with the convention
//! `{{ step_name.result.<field> }}`.  The Python renderer
//! (`noetl/core/dsl/render.py`) implements `.result` as a fall-through
//! proxy: if the step dict does not have an explicit `result` key,
//! `step.result.X` is rewritten to `step.X`.
//!
//! Without that proxy on the Rust side, playbooks written against the
//! Python worker break when the same step lands on the Rust worker —
//! the engine reports `undefined value` because minijinja's default
//! map lookup has no fall-through.
//!
//! `StepResultProxy` wraps `serde_json::Value::Object` entries with a
//! custom [`Object`] implementation that intercepts `.result` lookups
//! and aliases them back to the underlying map.  Nested map values are
//! wrapped recursively so the proxy applies at every depth.

use minijinja::value::{Enumerator, Object, Value};
use minijinja::{Environment, UndefinedBehavior};
use std::collections::HashMap;
use std::sync::Arc;

use crate::context::ExecutionContext;
use crate::error::ToolError;

/// Template engine with Jinja2-compatible syntax.
pub struct TemplateEngine {
    env: Environment<'static>,
}

impl TemplateEngine {
    /// Create a new template engine with custom filters.
    pub fn new() -> Self {
        let mut env = Environment::new();

        // Match the server's permissive undefined-variable behavior.
        // The server uses `Chainable` so `{{ iter.item }}` returns
        // undefined (rather than throwing) when `iter` is absent or
        // when an intermediate attribute is undefined.  Without this
        // the tools-side default (`Lenient`) throws "undefined value"
        // on attribute access of undefined variables — a mismatch
        // that breaks pipeline template rendering.
        env.set_undefined_behavior(UndefinedBehavior::Chainable);

        // Register custom filters
        env.add_filter("int", filter_int);
        env.add_filter("float", filter_float);
        env.add_filter("default", filter_default);
        env.add_filter("d", filter_default); // alias
        env.add_filter("tojson", filter_tojson);
        env.add_filter("fromjson", filter_fromjson);
        env.add_filter("length", filter_length);
        env.add_filter("len", filter_length); // alias
        env.add_filter("upper", filter_upper);
        env.add_filter("lower", filter_lower);
        env.add_filter("trim", filter_trim);
        env.add_filter("replace", filter_replace);
        env.add_filter("split", filter_split);
        env.add_filter("join", filter_join);
        env.add_filter("first", filter_first);
        env.add_filter("last", filter_last);
        env.add_filter("b64encode", filter_b64encode);
        env.add_filter("b64decode", filter_b64decode);

        Self { env }
    }

    /// Render a template string with the given context.
    pub fn render(
        &self,
        template: &str,
        context: &HashMap<String, serde_json::Value>,
    ) -> Result<String, ToolError> {
        // Convert context to minijinja Value (deep-clones the JSON +
        // wraps every map in a `StepResultProxy`).  This is the
        // dominant per-render cost when the context carries large
        // accumulated payloads, so callers that render many values
        // against the same context should prefer [`build_context`] +
        // [`render_with`] to pay it once (noetl/ai-meta#127).
        let ctx = context_to_value(context);
        self.render_with(template, &ctx)
    }

    /// Build the proxied minijinja [`Value`] for a context once, so it
    /// can be reused across many [`render_with`] / [`render_value_with`]
    /// calls without re-cloning + re-proxying the whole context each
    /// time.  Cloning the returned `Value` is cheap — minijinja's
    /// `Value` is `Arc`-backed for compound types, so a clone is a
    /// refcount bump, not a deep copy (noetl/ai-meta#127).
    pub fn build_context(context: &HashMap<String, serde_json::Value>) -> Value {
        context_to_value(context)
    }

    /// Build a proxied minijinja [`Value`] directly from borrowed
    /// `variables` plus `overlay` entries that take precedence over a
    /// variable of the same name — mirroring the metadata-wins
    /// semantics of [`ExecutionContext::to_template_context`].
    ///
    /// This lets a hot loop avoid the intermediate
    /// `to_template_context()` `HashMap` deep-clone: the variables are
    /// cloned straight into minijinja `Value`s (the one unavoidable
    /// clone), and small per-iteration overlays (`output`, `_attempt`,
    /// execution metadata) are layered on without rebuilding the base
    /// map (noetl/ai-meta#127).
    pub fn build_context_with_overlay<'a, I>(
        variables: &HashMap<String, serde_json::Value>,
        overlay: I,
    ) -> Value
    where
        I: IntoIterator<Item = (&'a str, serde_json::Value)>,
    {
        let mut map: std::collections::BTreeMap<String, Value> = variables
            .iter()
            .map(|(k, v)| (k.clone(), json_to_proxied_value(v.clone())))
            .collect();
        for (k, v) in overlay {
            map.insert(k.to_string(), json_to_proxied_value(v));
        }
        Value::from(map)
    }

    /// Render a template string against a pre-built proxied context
    /// [`Value`] (see [`build_context`]).  Cloning the `Value` per
    /// render is cheap (`Arc` bump).
    pub fn render_with(&self, template: &str, ctx: &Value) -> Result<String, ToolError> {
        let tmpl = self.env.template_from_str(template)?;
        tmpl.render(ctx.clone())
            .map_err(|e| ToolError::Template(e.to_string()))
    }

    /// Render a template with an ExecutionContext.
    pub fn render_with_context(
        &self,
        template: &str,
        ctx: &ExecutionContext,
    ) -> Result<String, ToolError> {
        self.render(template, &ctx.to_template_context())
    }

    /// Check if a string contains template syntax.
    pub fn is_template(s: &str) -> bool {
        s.contains("{{") || s.contains("{%")
    }

    /// Render a value that might be a template.
    ///
    /// If the value is a string containing template syntax, render it.
    /// Otherwise, return the JSON representation.
    pub fn render_value(
        &self,
        value: &serde_json::Value,
        context: &HashMap<String, serde_json::Value>,
    ) -> Result<serde_json::Value, ToolError> {
        // Build the proxied minijinja context ONCE and reuse it across
        // every nested templated field, instead of rebuilding it inside
        // each `render()` call.  For a config object with K templated
        // fields this turns K full context deep-clones into one
        // (noetl/ai-meta#127).
        let ctx = context_to_value(context);
        self.render_value_with(value, &ctx)
    }

    /// Render a value tree against a pre-built proxied context
    /// [`Value`] (see [`build_context`] / [`build_context_with_overlay`]).
    ///
    /// Behaviourally identical to [`render_value`]; the only difference
    /// is that the caller supplies the already-converted context so the
    /// proxied `Value` is built once and shared across the whole tree
    /// — the lever behind noetl/ai-meta#127's per-sub-task CPU cut.
    pub fn render_value_with(
        &self,
        value: &serde_json::Value,
        ctx: &Value,
    ) -> Result<serde_json::Value, ToolError> {
        match value {
            serde_json::Value::String(s) if Self::is_template(s) => {
                let rendered = self.render_with(s, ctx)?;
                // Try to parse as JSON first.
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&rendered) {
                    return Ok(val);
                }
                // If the template is a single `{{ expr }}` whose
                // rendered output isn't valid JSON (minijinja renders
                // dicts/lists using Python-style repr, not JSON —
                // e.g. `True` instead of `true`), retry with
                // `|tojson` appended to produce a JSON-parseable
                // string.  This keeps `set:` blocks in pipeline
                // policy rules working: `{{ output.data }}` renders
                // an object that subsequent tools can traverse.
                let trimmed = s.trim();
                if trimmed.starts_with("{{")
                    && trimmed.ends_with("}}")
                    && trimmed.matches("{{").count() == 1
                {
                    let inner = &trimmed[2..trimmed.len() - 2];
                    // Only apply if there's no existing filter that
                    // could conflict (e.g. user already wrote |tojson).
                    if !inner.contains("|tojson") {
                        let json_tmpl = format!("{{{{ {} | tojson }}}}", inner.trim());
                        if let Ok(json_rendered) = self.render_with(&json_tmpl, ctx) {
                            if let Ok(val) =
                                serde_json::from_str::<serde_json::Value>(&json_rendered)
                            {
                                return Ok(val);
                            }
                        }
                    }
                }
                // Final fallback: return as a plain string value.
                Ok(serde_json::json!(rendered))
            }
            serde_json::Value::Object(obj) => {
                let mut result = serde_json::Map::new();
                for (k, v) in obj {
                    result.insert(k.clone(), self.render_value_with(v, ctx)?);
                }
                Ok(serde_json::Value::Object(result))
            }
            serde_json::Value::Array(arr) => {
                let result: Result<Vec<_>, _> = arr
                    .iter()
                    .map(|v| self.render_value_with(v, ctx))
                    .collect();
                Ok(serde_json::Value::Array(result?))
            }
            _ => Ok(value.clone()),
        }
    }
}

impl Default for TemplateEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a HashMap context to minijinja Value.
///
/// Top-level map entries are wrapped with [`StepResultProxy`] so the
/// `.result` accessor (cross-runtime compatibility with the Python
/// renderer's `StepResultProxy`) works at any depth.
fn context_to_value(context: &HashMap<String, serde_json::Value>) -> Value {
    let map: std::collections::BTreeMap<String, Value> = context
        .iter()
        .map(|(k, v)| (k.clone(), json_to_proxied_value(v.clone())))
        .collect();
    Value::from(map)
}

/// Convert a `serde_json::Value` to a minijinja [`Value`], wrapping
/// map-shaped values with [`StepResultProxy`] so `.result` lookups
/// fall through to the underlying map.
fn json_to_proxied_value(v: serde_json::Value) -> Value {
    match v {
        serde_json::Value::Object(_) => Value::from_object(StepResultProxy(v)),
        _ => Value::from_serialize(&v),
    }
}

/// Wraps a `serde_json::Value::Object` to provide cross-runtime
/// `.result` accessor semantics.
///
/// `step.result.X` resolves to `step.X` when the underlying map does
/// not have an explicit `result` key.  This matches the Python
/// renderer's [`StepResultProxy`](https://github.com/noetl/noetl/blob/main/noetl/core/dsl/render.py)
/// fall-through, so playbooks that work against the Python worker
/// also work against the Rust worker.
#[derive(Debug)]
struct StepResultProxy(serde_json::Value);

impl Object for StepResultProxy {
    fn get_value(self: &Arc<Self>, key: &Value) -> Option<Value> {
        let key_str = key.as_str()?;
        let serde_json::Value::Object(map) = &self.0 else {
            return None;
        };

        // Explicit key wins (matches Python: real `result` key takes
        // precedence over the fall-through alias).
        if let Some(found) = map.get(key_str) {
            return Some(json_to_proxied_value(found.clone()));
        }

        // Fall-through: `.result` aliases to self.  We wrap a fresh
        // proxy around the same underlying value so further chained
        // accesses (`step.result.foo`, `step.result.result.foo`) also
        // resolve.
        if key_str == "result" {
            return Some(Value::from_object(StepResultProxy(self.0.clone())));
        }

        None
    }

    fn enumerate(self: &Arc<Self>) -> Enumerator {
        let serde_json::Value::Object(map) = &self.0 else {
            return Enumerator::Empty;
        };
        let keys: Vec<Value> = map.keys().map(|k| Value::from(k.as_str())).collect();
        Enumerator::Iter(Box::new(keys.into_iter()))
    }
}

// Custom filters

fn filter_int(value: Value) -> Result<Value, minijinja::Error> {
    // Try to convert to i64 via string parsing
    let s = value.to_string();
    // First try parsing as integer directly
    if let Ok(n) = s.parse::<i64>() {
        return Ok(Value::from(n));
    }
    // Then try parsing as float and truncating
    if let Ok(f) = s.parse::<f64>() {
        return Ok(Value::from(f as i64));
    }
    Ok(Value::from(0i64))
}

fn filter_float(value: Value) -> Result<Value, minijinja::Error> {
    // Try to convert to f64 via string parsing
    let s = value.to_string();
    if let Ok(f) = s.parse::<f64>() {
        return Ok(Value::from(f));
    }
    // Try parsing as integer and converting
    if let Ok(n) = s.parse::<i64>() {
        return Ok(Value::from(n as f64));
    }
    Ok(Value::from(0.0f64))
}

fn filter_default(value: Value, default: Option<Value>) -> Value {
    if value.is_undefined() || value.is_none() {
        default.unwrap_or_else(|| Value::from(""))
    } else {
        value
    }
}

fn filter_tojson(value: Value) -> Result<String, minijinja::Error> {
    Ok(serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string()))
}

fn filter_fromjson(value: Value) -> Result<Value, minijinja::Error> {
    let s = value.to_string();
    let json: serde_json::Value = serde_json::from_str(&s).unwrap_or(serde_json::Value::Null);
    Ok(Value::from_serialize(&json))
}

fn filter_length(value: Value) -> Result<Value, minijinja::Error> {
    match value.kind() {
        minijinja::value::ValueKind::String => Ok(Value::from(value.to_string().len())),
        minijinja::value::ValueKind::Seq => Ok(Value::from(value.len().unwrap_or(0))),
        minijinja::value::ValueKind::Map => Ok(Value::from(value.len().unwrap_or(0))),
        _ => Ok(Value::from(0)),
    }
}

fn filter_upper(value: Value) -> String {
    value.to_string().to_uppercase()
}

fn filter_lower(value: Value) -> String {
    value.to_string().to_lowercase()
}

fn filter_trim(value: Value) -> String {
    value.to_string().trim().to_string()
}

fn filter_replace(value: Value, old: String, new: String) -> String {
    value.to_string().replace(&old, &new)
}

fn filter_split(value: Value, sep: String) -> Vec<String> {
    value
        .to_string()
        .split(&sep)
        .map(|s| s.to_string())
        .collect()
}

fn filter_join(value: Value, sep: Option<String>) -> Result<String, minijinja::Error> {
    let sep = sep.unwrap_or_default();
    if let Some(len) = value.len() {
        let items: Vec<String> = (0..len)
            .filter_map(|i| value.get_item(&Value::from(i)).ok())
            .map(|v| v.to_string())
            .collect();
        Ok(items.join(&sep))
    } else {
        Ok(value.to_string())
    }
}

fn filter_first(value: Value) -> Result<Value, minijinja::Error> {
    if let Some(len) = value.len() {
        if len > 0 {
            return value.get_item(&Value::from(0));
        }
    }
    Ok(Value::UNDEFINED)
}

fn filter_last(value: Value) -> Result<Value, minijinja::Error> {
    if let Some(len) = value.len() {
        if len > 0 {
            return value.get_item(&Value::from(len - 1));
        }
    }
    Ok(Value::UNDEFINED)
}

fn filter_b64encode(value: Value) -> String {
    use base64::{engine::general_purpose::STANDARD, Engine};
    STANDARD.encode(value.to_string().as_bytes())
}

fn filter_b64decode(value: Value) -> Result<String, minijinja::Error> {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let decoded = STANDARD.decode(value.to_string().as_bytes()).map_err(|e| {
        minijinja::Error::new(minijinja::ErrorKind::InvalidOperation, e.to_string())
    })?;
    String::from_utf8(decoded)
        .map_err(|e| minijinja::Error::new(minijinja::ErrorKind::InvalidOperation, e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_template() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("name".to_string(), serde_json::json!("World"));

        let result = engine.render("Hello, {{ name }}!", &ctx).unwrap();
        assert_eq!(result, "Hello, World!");
    }

    #[test]
    fn test_filter_int() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("val".to_string(), serde_json::json!("42"));

        let result = engine.render("{{ val | int }}", &ctx).unwrap();
        assert_eq!(result, "42");
    }

    #[test]
    fn test_filter_float() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("val".to_string(), serde_json::json!("3.14"));

        let result = engine.render("{{ val | float }}", &ctx).unwrap();
        assert_eq!(result, "3.14");
    }

    #[test]
    fn test_filter_default() {
        let engine = TemplateEngine::new();
        let ctx = HashMap::new();

        let result = engine
            .render("{{ missing | default('fallback') }}", &ctx)
            .unwrap();
        assert_eq!(result, "fallback");
    }

    #[test]
    fn test_filter_length() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("items".to_string(), serde_json::json!(["a", "b", "c"]));

        let result = engine.render("{{ items | length }}", &ctx).unwrap();
        assert_eq!(result, "3");
    }

    #[test]
    fn test_filter_upper_lower() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("text".to_string(), serde_json::json!("Hello"));

        let result = engine.render("{{ text | upper }}", &ctx).unwrap();
        assert_eq!(result, "HELLO");

        let result = engine.render("{{ text | lower }}", &ctx).unwrap();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_filter_trim() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("text".to_string(), serde_json::json!("  hello  "));

        let result = engine.render("{{ text | trim }}", &ctx).unwrap();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_filter_replace() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("text".to_string(), serde_json::json!("hello world"));

        let result = engine
            .render("{{ text | replace('world', 'rust') }}", &ctx)
            .unwrap();
        assert_eq!(result, "hello rust");
    }

    #[test]
    fn test_filter_split_join() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("text".to_string(), serde_json::json!("a,b,c"));

        let result = engine
            .render("{{ text | split(',') | join('-') }}", &ctx)
            .unwrap();
        assert_eq!(result, "a-b-c");
    }

    #[test]
    fn test_filter_first_last() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert(
            "items".to_string(),
            serde_json::json!(["first", "middle", "last"]),
        );

        let result = engine.render("{{ items | first }}", &ctx).unwrap();
        assert_eq!(result, "first");

        let result = engine.render("{{ items | last }}", &ctx).unwrap();
        assert_eq!(result, "last");
    }

    #[test]
    fn test_filter_b64() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("text".to_string(), serde_json::json!("hello"));

        let result = engine.render("{{ text | b64encode }}", &ctx).unwrap();
        assert_eq!(result, "aGVsbG8=");

        ctx.insert("encoded".to_string(), serde_json::json!("aGVsbG8="));
        let result = engine.render("{{ encoded | b64decode }}", &ctx).unwrap();
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_filter_tojson() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("data".to_string(), serde_json::json!({"key": "value"}));

        let result = engine.render("{{ data | tojson }}", &ctx).unwrap();
        assert!(result.contains("\"key\"") && result.contains("\"value\""));
    }

    #[test]
    fn test_render_value_with_matches_render_value() {
        // noetl/ai-meta#127: render_value_with (shared pre-built
        // context) must produce byte-identical output to render_value
        // (per-call context build) across the value shapes that show up
        // in a task_sequence config — nested objects, arrays, the
        // tojson-retry path for object-valued single expressions, the
        // `.result` proxy fall-through, and plain non-template values.
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("name".to_string(), serde_json::json!("World"));
        ctx.insert("n".to_string(), serde_json::json!(7));
        ctx.insert(
            "producer".to_string(),
            serde_json::json!({"reference": {"ref": "noetl://x"}, "rows": [1, 2, 3]}),
        );
        ctx.insert(
            "output".to_string(),
            serde_json::json!({"data": {"counter": 42, "label": "hi"}}),
        );

        let values = vec![
            serde_json::json!("Hello, {{ name }}!"),
            serde_json::json!("{{ output.data }}"), // object → tojson retry
            serde_json::json!("{{ producer.result.reference.ref }}"), // proxy
            serde_json::json!({
                "url": "https://api/{{ name }}/{{ n }}",
                "nested": {"count": "{{ output.data.counter }}"},
                "list": ["{{ n }}", "static", "{{ producer.rows[0] }}"],
                "plain": 99,
            }),
        ];

        let shared = TemplateEngine::build_context(&ctx);
        for v in &values {
            let per_call = engine.render_value(v, &ctx).unwrap();
            let shared_out = engine.render_value_with(v, &shared).unwrap();
            assert_eq!(
                per_call, shared_out,
                "render_value_with must match render_value for {v}"
            );
        }
    }

    #[test]
    fn test_build_context_with_overlay_metadata_wins() {
        // The overlay (execution metadata / output / _attempt) takes
        // precedence over a variable of the same name — preserving the
        // metadata-wins semantics of to_template_context that the hot
        // loop relies on (noetl/ai-meta#127).
        let engine = TemplateEngine::new();
        let mut variables = HashMap::new();
        variables.insert("step".to_string(), serde_json::json!("from_variable"));
        variables.insert("keep".to_string(), serde_json::json!("kept"));

        let ctx = TemplateEngine::build_context_with_overlay(
            &variables,
            vec![("step", serde_json::json!("from_overlay"))],
        );
        assert_eq!(
            engine.render_with("{{ step }}", &ctx).unwrap(),
            "from_overlay",
            "overlay must win over a same-named variable"
        );
        assert_eq!(
            engine.render_with("{{ keep }}", &ctx).unwrap(),
            "kept",
            "non-colliding variables survive"
        );
    }

    #[test]
    fn test_is_template() {
        assert!(TemplateEngine::is_template("Hello {{ name }}"));
        assert!(TemplateEngine::is_template(
            "{% for x in items %}{{ x }}{% endfor %}"
        ));
        assert!(!TemplateEngine::is_template("plain text"));
    }

    #[test]
    fn test_render_value() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("name".to_string(), serde_json::json!("World"));

        // String template
        let value = serde_json::json!("Hello, {{ name }}!");
        let result = engine.render_value(&value, &ctx).unwrap();
        assert_eq!(result, serde_json::json!("Hello, World!"));

        // Object with template
        let value = serde_json::json!({
            "greeting": "Hello, {{ name }}!",
            "plain": "no template"
        });
        let result = engine.render_value(&value, &ctx).unwrap();
        assert_eq!(result["greeting"], serde_json::json!("Hello, World!"));
        assert_eq!(result["plain"], serde_json::json!("no template"));

        // Non-template value
        let value = serde_json::json!(42);
        let result = engine.render_value(&value, &ctx).unwrap();
        assert_eq!(result, serde_json::json!(42));
    }

    #[test]
    fn test_execution_context_rendering() {
        let engine = TemplateEngine::new();
        let mut ctx = ExecutionContext::new(12345, "step1", "http://localhost");
        ctx.set_variable("input", serde_json::json!("test"));

        let result = engine
            .render_with_context("Execution {{ execution_id }}: {{ input }}", &ctx)
            .unwrap();
        assert_eq!(result, "Execution 12345: test");
    }

    #[test]
    fn test_nested_object() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert(
            "user".to_string(),
            serde_json::json!({"name": "Alice", "age": 30}),
        );

        let result = engine
            .render("{{ user.name }} is {{ user.age }}", &ctx)
            .unwrap();
        assert_eq!(result, "Alice is 30");
    }

    #[test]
    fn test_loop() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("items".to_string(), serde_json::json!(["a", "b", "c"]));

        let result = engine
            .render("{% for item in items %}{{ item }}{% endfor %}", &ctx)
            .unwrap();
        assert_eq!(result, "abc");
    }

    // ---------------------------------------------------------------
    // `.result` accessor proxy — cross-runtime compatibility with the
    // Python renderer's `StepResultProxy`.  Playbooks address prior-
    // step outputs with `{{ step.result.<field> }}` regardless of
    // whether the next step lands on the Python worker (whose
    // `render_template` proxies `.result` to the step dict itself) or
    // the Rust worker (this engine).  These tests pin the Rust-side
    // proxy semantics.
    // ---------------------------------------------------------------

    #[test]
    fn test_result_proxy_aliases_to_step_dict() {
        // `{{ producer.result.reference.ref }}` resolves to
        // `producer.reference.ref` when no explicit `result` key.
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert(
            "producer".to_string(),
            serde_json::json!({
                "reference": {"ref": "noetl://execution/1/result/producer/abc"}
            }),
        );
        let out = engine
            .render("{{ producer.result.reference.ref }}", &ctx)
            .unwrap();
        assert_eq!(out, "noetl://execution/1/result/producer/abc");
    }

    #[test]
    fn test_result_proxy_chained_at_depth() {
        // Nested maps are proxied too.  `producer.data.result.rows[0]`
        // works because the inner `data` map is wrapped recursively.
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert(
            "producer".to_string(),
            serde_json::json!({
                "data": {"row_count": 6000, "first": "user_000001"}
            }),
        );
        let out = engine
            .render("{{ producer.result.data.result.first }}", &ctx)
            .unwrap();
        assert_eq!(out, "user_000001");
    }

    #[test]
    fn test_result_proxy_explicit_key_wins() {
        // Python convention: if the dict has a real `result` key, it
        // takes precedence over the fall-through alias.
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert(
            "fetch_http".to_string(),
            serde_json::json!({
                "result": "explicit",
                "url": "u",
                "elapsed": 1.23
            }),
        );
        // `.result` returns the literal "explicit" string, not the
        // dict alias.
        let out = engine.render("{{ fetch_http.result }}", &ctx).unwrap();
        assert_eq!(out, "explicit");
    }

    #[test]
    fn test_result_proxy_direct_access_unchanged() {
        // Plain dict access keeps working — `.result` is additive,
        // not replacing.
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert(
            "producer".to_string(),
            serde_json::json!({
                "reference": {"ref": "noetl://..."}
            }),
        );
        let out = engine.render("{{ producer.reference.ref }}", &ctx).unwrap();
        assert_eq!(out, "noetl://...");
    }

    #[test]
    fn test_result_proxy_missing_key_undefined() {
        // Looking up a missing key (other than `result`) returns
        // undefined, same as the bare map.
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert(
            "producer".to_string(),
            serde_json::json!({"reference": {"ref": "x"}}),
        );
        // Use `default` filter so we can observe the undefined.
        let out = engine
            .render("{{ producer.nonexistent | default('missing') }}", &ctx)
            .unwrap();
        assert_eq!(out, "missing");
    }

    #[test]
    fn test_conditional() {
        let engine = TemplateEngine::new();
        let mut ctx = HashMap::new();
        ctx.insert("active".to_string(), serde_json::json!(true));

        let result = engine
            .render("{% if active %}yes{% else %}no{% endif %}", &ctx)
            .unwrap();
        assert_eq!(result, "yes");

        ctx.insert("active".to_string(), serde_json::json!(false));
        let result = engine
            .render("{% if active %}yes{% else %}no{% endif %}", &ctx)
            .unwrap();
        assert_eq!(result, "no");
    }
}
