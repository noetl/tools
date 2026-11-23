//! One process-wide `reqwest::Client` for tools that hold a default client.
//!
//! ⚠ Guard — do not replace calls to [`shared`] with `reqwest::Client::new()`.
//!
//! Building a `reqwest::Client` is not always cheap. When reqwest's
//! `rustls-tls-native-roots` feature is on — it is in the noetl CLI 6.4.1
//! binary, through feature unification, though not in this crate's own test
//! build — each build loads the operating system's trusted root certificates.
//! On macOS that measured **~120–130 ms per client** (Apple M1 Max, 2026-11-22,
//! release build, p50 of 10). `RhaiTool::new()` built one, and the local runtime built two
//! `RhaiTool`s per `kind: rhai` step, so a one-line rhai step cost ~240 ms while
//! the script itself ran in 0.1 ms.
//!
//! A `reqwest::Client` is an `Arc` handle around its connection pool and TLS
//! configuration, and reqwest's own documentation recommends building one and
//! reusing it. `Clone` is a reference-count bump. [`shared`] builds the client
//! once, on first use, and every caller gets a clone of it.
//!
//! Only tools that used the *default* configuration (`Client::new()`) use this.
//! A tool that needs its own timeout, proxy or TLS settings still builds its own
//! with `Client::builder()`, and should build it once per tool instance, not per
//! call.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

static SHARED: OnceLock<reqwest::Client> = OnceLock::new();

/// How many times the shared client has been built.  Read by the test that
/// guards the build-once property; a timing assertion cannot do that job,
/// because `Client::new()` is only slow when native roots are enabled, which
/// depends on the final binary's feature set, not on this crate's.
static BUILDS: AtomicUsize = AtomicUsize::new(0);

fn build() -> reqwest::Client {
    BUILDS.fetch_add(1, Ordering::Relaxed);
    reqwest::Client::new()
}

/// A clone of the process-wide default `reqwest::Client`, built on first call.
pub fn shared() -> reqwest::Client {
    SHARED.get_or_init(build).clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_client_is_built_once() {
        for _ in 0..100 {
            let _ = shared();
        }
        assert_eq!(
            BUILDS.load(Ordering::Relaxed),
            1,
            "shared() must build the client once per process, not per call"
        );
    }
}
