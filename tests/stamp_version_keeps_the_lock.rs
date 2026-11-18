//! **The release stamp must not re-resolve the dependency graph** (noetl/ai-meta#494).
//!
//! `ci/stamp-version.sh` runs in the release jobs before anything is built. It
//! used to end with `cargo generate-lockfile`, which re-resolves `Cargo.lock`
//! against the registry at release time — so the shipped artifact's graph was
//! not the one CI tested. It now edits only this package's own `version` line.

const STAMP: &str = include_str!("../ci/stamp-version.sh");

#[test]
fn the_stamp_step_does_not_re_resolve_the_graph() {
    assert!(STAMP.len() > 300, "stamp script read as {} bytes", STAMP.len());
    for line in STAMP.lines().map(str::trim).filter(|l| !l.starts_with('#')) {
        for forbidden in ["generate-lockfile", "cargo update"] {
            assert!(
                !line.contains(forbidden),
                "⚠ ci/stamp-version.sh runs `{forbidden}`, which re-resolves Cargo.lock at \
                 release time — the image would then build a graph CI never tested:\n  {line}"
            );
        }
    }
}

fn stamp_in(dir: &std::path::Path, version: &str) -> std::process::Output {
    std::process::Command::new("bash")
        .arg(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("ci/stamp-version.sh"))
        .arg(version)
        .current_dir(dir)
        .output()
        .expect("bash runs")
}

fn scratch(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("stamp-494-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    for f in ["Cargo.toml", "Cargo.lock"] {
        std::fs::copy(root.join(f), d.join(f)).unwrap();
    }
    d
}

/// The real script, on a copy: Cargo.lock changes in EXACTLY one line — the
/// root package's version — and Cargo.toml carries the stamp.
#[test]
fn stamping_changes_only_the_root_version_line_of_the_lock() {
    let d = scratch("ok");
    let before = std::fs::read_to_string(d.join("Cargo.lock")).unwrap();
    let out = stamp_in(&d, "9.8.7");
    assert!(
        out.status.success(),
        "stamp failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let after = std::fs::read_to_string(d.join("Cargo.lock")).unwrap();
    let changed: Vec<(&str, &str)> = before
        .lines()
        .zip(after.lines())
        .filter(|(a, b)| a != b)
        .collect();
    assert_eq!(
        before.lines().count(),
        after.lines().count(),
        "the lock gained or lost lines — something re-resolved"
    );
    assert_eq!(changed.len(), 1, "lock lines changed: {changed:?}");
    assert_eq!(changed[0].1, r#"version = "9.8.7""#);
    let toml = std::fs::read_to_string(d.join("Cargo.toml")).unwrap();
    assert!(toml.contains("version = \"9.8.7\""), "Cargo.toml not stamped");
    let _ = std::fs::remove_dir_all(&d);
}

/// A lock with no entry for this package cannot be made consistent by a text
/// stamp; the release must stop rather than ship an unverified lock.
#[test]
fn a_lock_without_the_root_entry_is_refused() {
    let d = scratch("noroot");
    let lock = std::fs::read_to_string(d.join("Cargo.lock")).unwrap();
    std::fs::write(
        d.join("Cargo.lock"),
        lock.replace("name = \"noetl-tools\"\n", "name = \"not-this-package\"\n"),
    )
    .unwrap();
    let out = stamp_in(&d, "9.8.7");
    assert!(!out.status.success(), "a lock with no root entry was accepted");
    let _ = std::fs::remove_dir_all(&d);
}
