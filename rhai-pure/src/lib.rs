//! The pure subset of NoETL's `rhai` tool.
//!
//! A `kind: rhai` step runs the same way in two places:
//!
//! - on a worker, through noetl-tools' `RhaiTool`, which registers
//!   [`register_pure`] and then the host-only functions in
//!   [`HOST_ONLY_FUNCTIONS`];
//! - inside a compiled playbook (noetl/server `plugins/playbook-wasm`), which
//!   registers [`register_pure`] only.
//!
//! Both build the step's scope with [`scope`] and convert the result with
//! [`dynamic_to_json`], so a script that calls only [`PURE_FUNCTIONS`] gives the
//! same JSON in both places.
//!
//! [`scan`] is the build-time purity check for a script: it refuses any use of
//! a host-only function, and any way to reach one indirectly (`eval`, `Fn`,
//! `call`, `curry`, `import`).

#[cfg(feature = "engine")]
use std::collections::HashMap;

#[cfg(feature = "engine")]
pub use rhai;
#[cfg(feature = "engine")]
use rhai::{Dynamic, Engine, Scope};

/// The host functions a compiled playbook provides. Deterministic: no
/// clock, network, filesystem or randomness. The logging family runs the
/// `log` hook passed to [`register_pure`] (a worker traces; a compiled
/// playbook drops the message).
pub const PURE_FUNCTIONS: &[&str] = &[
    "parse_json",
    "to_json",
    "contains",
    "contains_any",
    "sha256_hex",
    "log",
    "print",
    "debug",
    "info",
    "warn",
    "error",
];

/// Functions only a worker has: I/O, the clock, sleeping, randomness, and the
/// credential helpers (`hmac_sha256_hex` and `eq_ct` are deterministic, but they
/// exist to handle secrets, which a compiled playbook never sees). A script
/// that names any of these cannot be compiled.
pub const HOST_ONLY_FUNCTIONS: &[&str] = &[
    "http_get",
    "http_post",
    "http_delete",
    "http_get_auth",
    "http_post_auth",
    "http_delete_auth",
    "get_gcp_token",
    "timestamp",
    "timestamp_ms",
    "sleep",
    "sleep_ms",
    "random_digits",
    "random_token",
    "hmac_sha256_hex",
    "eq_ct",
];

/// Ways to reach a function by name at run time, which a static check can't
/// see through. `timestamp` (rhai's own clock) is already in
/// [`HOST_ONLY_FUNCTIONS`].
pub const DYNAMIC_SYMBOLS: &[&str] = &["eval", "Fn", "call", "curry", "import"];

/// Log levels of the logging family, for the hook.
#[cfg(feature = "engine")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    Debug,
    Info,
    Warn,
    Error,
}

#[cfg(feature = "engine")]
/// Register the pure functions. `log` receives every `log`/`print`/`debug`/
/// `info`/`warn`/`error` call.
pub fn register_pure<L>(engine: &mut Engine, log: L)
where
    L: Fn(Level, &str) + Send + Sync + Clone + 'static,
{
    for (name, level) in [
        ("log", Level::Info),
        ("print", Level::Info),
        ("debug", Level::Debug),
        ("info", Level::Info),
        ("warn", Level::Warn),
        ("error", Level::Error),
    ] {
        let log = log.clone();
        engine.register_fn(name, move |msg: &str| log(level, msg));
    }

    engine.register_fn("parse_json", |s: &str| -> Dynamic {
        match serde_json::from_str::<serde_json::Value>(s) {
            Ok(v) => json_to_dynamic(&v),
            Err(_) => Dynamic::UNIT,
        }
    });

    engine.register_fn("to_json", |val: Dynamic| -> String {
        let json = dynamic_to_json(&val);
        serde_json::to_string(&json).unwrap_or_else(|_| "null".to_string())
    });

    engine.register_fn("contains", |s: &str, substr: &str| -> bool {
        s.contains(substr)
    });

    engine.register_fn("contains_any", |s: &str, substrs: rhai::Array| -> bool {
        for item in substrs {
            if let Ok(substr) = item.into_string() {
                if s.contains(substr.as_str()) {
                    return true;
                }
            }
        }
        false
    });

    engine.register_fn("sha256_hex", |s: &str| hex_lower(&sha256(s.as_bytes())));
}

#[cfg(feature = "engine")]
/// A compiled playbook's engine: `Engine::new()` plus the pure functions,
/// logging dropped, `eval` disabled.
pub fn pure_engine() -> Engine {
    let mut engine = Engine::new();
    register_pure(&mut engine, |_, _| {});
    engine.disable_symbol("eval");
    engine
}

#[cfg(feature = "engine")]
/// The step's scope, exactly as the rhai tool builds it: the `args`, then
/// every context variable, then `execution_id`, `step` and `server_url`.
pub fn scope<'a, V>(
    args: &HashMap<String, String>,
    variables: V,
    execution_id: i64,
    step: &str,
    server_url: &str,
) -> Scope<'static>
where
    V: IntoIterator<Item = (&'a String, &'a serde_json::Value)>,
{
    let mut scope = Scope::new();
    for (k, v) in args {
        scope.push(k.clone(), v.clone());
    }
    for (k, v) in variables {
        scope.push(k.clone(), json_to_dynamic(v));
    }
    scope.push("execution_id", execution_id);
    scope.push("step", step.to_string());
    scope.push("server_url", server_url.to_string());
    scope
}

#[cfg(feature = "engine")]
/// Run `code` and return its value as JSON. The error text is the rhai tool's.
pub fn eval(engine: &Engine, scope: &mut Scope, code: &str) -> Result<serde_json::Value, String> {
    engine
        .eval_with_scope::<Dynamic>(scope, code)
        .map(|r| dynamic_to_json(&r))
        .map_err(|e| format!("Rhai error: {e}"))
}

/// Why a script cannot be compiled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Impure {
    pub symbol: String,
    pub line: usize,
}

impl std::fmt::Display for Impure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "line {}: '{}' is not available in a compiled playbook (pure functions: {:?})",
            self.line, self.symbol, PURE_FUNCTIONS
        )
    }
}

/// The build-time purity check: every identifier in `code`, outside strings
/// and comments, must not be a host-only function or a dynamic symbol.
///
/// Deliberately conservative: a variable or map key written as a bare
/// identifier with one of those names (`#{ timestamp: 1 }`) is refused too.
/// Quote the key (`#{ "timestamp": 1 }`) or rename the variable.
pub fn scan(code: &str) -> Result<(), Impure> {
    for (ident, line) in identifiers(code) {
        if HOST_ONLY_FUNCTIONS.contains(&ident) || DYNAMIC_SYMBOLS.contains(&ident) {
            return Err(Impure {
                symbol: ident.to_string(),
                line,
            });
        }
    }
    Ok(())
}

/// Identifiers outside string, character and comment literals, with their
/// 1-based line. Handles `"..."` (with escapes), `` `...` `` (with `${...}`
/// interpolation scanned as code), `'c'`, `// ...` and nested `/* ... */`.
pub fn identifiers(code: &str) -> Vec<(&str, usize)> {
    let b = code.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let mut line = 1;
    // Stack of open backtick strings; each entry counts the `{` depth of the
    // `${ ... }` interpolation currently being scanned as code (0: in text).
    let mut ticks: Vec<usize> = Vec::new();
    while i < b.len() {
        let c = b[i];
        if c == b'\n' {
            line += 1;
        }
        // Inside the text part of a backtick string.
        if let Some(&0) = ticks.last() {
            match c {
                b'`' => {
                    ticks.pop();
                }
                b'\\' => i += 1,
                b'$' if b.get(i + 1) == Some(&b'{') => {
                    *ticks.last_mut().unwrap() = 1;
                    i += 1;
                }
                _ => {}
            }
            i += 1;
            continue;
        }
        match c {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                let mut depth = 1;
                i += 2;
                while i < b.len() && depth > 0 {
                    if b[i] == b'\n' {
                        line += 1;
                    }
                    if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                        depth += 1;
                        i += 1;
                    } else if b[i] == b'*' && b.get(i + 1) == Some(&b'/') {
                        depth -= 1;
                        i += 1;
                    }
                    i += 1;
                }
                continue;
            }
            b'"' | b'\'' => {
                let q = c;
                i += 1;
                while i < b.len() && b[i] != q {
                    if b[i] == b'\\' {
                        i += 1;
                    } else if b[i] == b'\n' {
                        line += 1;
                    }
                    i += 1;
                }
                i += 1;
                continue;
            }
            b'`' => {
                ticks.push(0);
                i += 1;
                continue;
            }
            b'{' => {
                if let Some(d) = ticks.last_mut() {
                    *d += 1;
                }
            }
            b'}' => {
                if let Some(d) = ticks.last_mut() {
                    *d -= 1; // back to text at 0
                }
            }
            _ => {}
        }
        if c == b'_' || c.is_ascii_alphabetic() {
            let start = i;
            while i < b.len() && (b[i] == b'_' || b[i].is_ascii_alphanumeric()) {
                i += 1;
            }
            out.push((&code[start..i], line));
            continue;
        }
        if c.is_ascii_digit() {
            // A number, including suffixes like 1e5 or 0x1f: not an identifier.
            while i < b.len() && (b[i] == b'_' || b[i].is_ascii_alphanumeric() || b[i] == b'.') {
                if b[i] == b'.' && !b.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    break;
                }
                i += 1;
            }
            continue;
        }
        i += 1;
    }
    out
}

// JSON <-> Dynamic, exactly as the rhai tool has always converted.

#[cfg(feature = "engine")]
pub fn json_to_dynamic(value: &serde_json::Value) -> Dynamic {
    match value {
        serde_json::Value::Null => Dynamic::UNIT,
        serde_json::Value::Bool(b) => Dynamic::from(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Dynamic::from(i)
            } else if let Some(f) = n.as_f64() {
                Dynamic::from(f)
            } else {
                Dynamic::UNIT
            }
        }
        serde_json::Value::String(s) => Dynamic::from(s.clone()),
        serde_json::Value::Array(arr) => {
            let rhai_arr: rhai::Array = arr.iter().map(json_to_dynamic).collect();
            Dynamic::from(rhai_arr)
        }
        serde_json::Value::Object(obj) => {
            let mut map = rhai::Map::new();
            for (k, v) in obj {
                map.insert(k.clone().into(), json_to_dynamic(v));
            }
            Dynamic::from(map)
        }
    }
}

#[cfg(feature = "engine")]
pub fn dynamic_to_json(value: &Dynamic) -> serde_json::Value {
    if value.is_unit() {
        serde_json::Value::Null
    } else if let Ok(b) = value.as_bool() {
        serde_json::Value::Bool(b)
    } else if let Ok(i) = value.as_int() {
        serde_json::Value::Number(i.into())
    } else if let Ok(f) = value.as_float() {
        serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    } else if let Ok(s) = value.clone().into_string() {
        serde_json::Value::String(s)
    } else if value.is_array() {
        let arr = value.clone().into_array().unwrap_or_default();
        serde_json::Value::Array(arr.iter().map(dynamic_to_json).collect())
    } else if value.is_map() {
        let map = value.clone().into_typed_array::<(String, Dynamic)>().ok();
        if let Some(entries) = map {
            let obj: serde_json::Map<String, serde_json::Value> = entries
                .into_iter()
                .map(|(k, v)| (k, dynamic_to_json(&v)))
                .collect();
            serde_json::Value::Object(obj)
        } else if let Some(rhai_map) = value.clone().try_cast::<rhai::Map>() {
            let obj: serde_json::Map<String, serde_json::Value> = rhai_map
                .into_iter()
                .map(|(k, v)| (k.to_string(), dynamic_to_json(&v)))
                .collect();
            serde_json::Value::Object(obj)
        } else {
            serde_json::Value::String(value.to_string())
        }
    } else {
        serde_json::Value::String(value.to_string())
    }
}

#[cfg(feature = "engine")]
pub fn sha256(data: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(data).into()
}

#[cfg(feature = "engine")]
pub fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(all(test, feature = "engine"))]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(code: &str, vars: serde_json::Value) -> serde_json::Value {
        let vars: HashMap<String, serde_json::Value> = serde_json::from_value(vars).unwrap();
        let mut s = scope(&HashMap::new(), &vars, 1, "s", "");
        eval(&pure_engine(), &mut s, code).unwrap()
    }

    #[test]
    fn the_pure_functions_work() {
        assert_eq!(
            run(r#"parse_json("{\"a\":[1,2]}").a[1]"#, json!({})),
            json!(2)
        );
        assert_eq!(
            run(r#"to_json(#{b: 1, a: "x"})"#, json!({})),
            json!(r#"{"a":"x","b":1}"#)
        );
        assert_eq!(
            run(r#"contains_any("snow day", ["rain", "snow"])"#, json!({})),
            json!(true)
        );
        assert_eq!(
            run(r#"sha256_hex("abc")"#, json!({})),
            json!("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
        assert_eq!(run(r#"log("x"); warn("y"); 7"#, json!({})), json!(7));
    }

    #[test]
    fn the_scope_carries_context_and_metadata() {
        assert_eq!(
            run(
                "start.cfg.model + ':' + step + ':' + execution_id",
                json!({"start": {"cfg": {"model": "m"}}})
            ),
            json!("m:s:1")
        );
    }

    #[test]
    fn the_compiled_engine_has_no_host_functions() {
        let mut s = Scope::new();
        for f in HOST_ONLY_FUNCTIONS.iter().filter(|f| **f != "timestamp") {
            let r = pure_engine().eval_with_scope::<Dynamic>(&mut s, &format!("{f}(\"x\")"));
            assert!(r.is_err(), "{f} is callable");
        }
        assert!(pure_engine().eval::<Dynamic>(r#"eval("1")"#).is_err());
    }

    #[test]
    fn scan_accepts_pure_code() {
        let code = r#"
            // http_get("x") in a comment is fine
            /* and timestamp() /* nested */ here */
            let s = "call http_get(\"y\") in a string";
            let c = 'e';
            let t = `text eval() ${ to_json(#{a: 1}) } more`;
            let call_llm = parse_json("{}");
            let n = 1e5 + 0x1f;
            if contains_any(s, ["a"]) { log(t) }
            call_llm
        "#;
        assert_eq!(scan(code), Ok(()));
    }

    #[test]
    fn scan_refuses_host_only_and_dynamic_symbols() {
        for (code, sym) in [
            ("let r = http_get(\"u\");", "http_get"),
            ("timestamp()", "timestamp"),
            ("let x = 1;\nsleep(1)", "sleep"),
            ("random_token(8)", "random_token"),
            ("eval(\"1\")", "eval"),
            ("let f = Fn(\"http_\" + \"get\"); f.call(1)", "Fn"),
            ("import \"m\" as m;", "import"),
            ("`a ${ timestamp_ms() } b`", "timestamp_ms"),
        ] {
            let e = scan(code).unwrap_err();
            assert_eq!(e.symbol, sym, "{code}");
        }
        assert_eq!(scan("let a = 1;\n\nget_gcp_token()").unwrap_err().line, 3);
    }
}
