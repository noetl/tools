//! Release-pipeline guards (noetl/ai-meta#330).
//!
//! Nothing here runs at runtime — these assert facts about the release
//! workflows that no other test could reach. Both failure modes they cover are
//! SILENT: nothing errors, nothing fails a build, and the wrong thing ships.

/// ⚠⚠ **Every job that builds or publishes MUST stamp the version first.**
///
/// The release bot no longer pushes a version bump to `main` — a required
/// status check rejects that push, because the commit is `[skip ci]` so the
/// check never runs on it and stays "expected" forever (GH006). The tag is
/// authoritative now, and the checked-out tree carries a stale floor in
/// `Cargo.toml`.
///
/// `CARGO_PKG_VERSION` is compiled into the binary and is what `cargo publish`
/// uploads. A job that skips the stamp still compiles, still publishes, still
/// deploys, still passes every health check — and carries the PREVIOUS version.
/// There is no failure to notice, which is why this is a guard and not a
/// comment.
#[test]
fn every_building_or_publishing_job_stamps_the_version() {
    let wf = include_str!("../.github/workflows/release.yml");

    // Split into top-level jobs: a line at exactly 2 spaces of indent ending
    // in `:` starts one.
    let mut jobs: Vec<(String, String)> = Vec::new();
    let mut name = String::new();
    let mut body = String::new();
    for line in wf.lines() {
        let is_header = line.starts_with("  ")
            && !line.starts_with("   ")
            && line.trim_end().ends_with(':')
            && !line.trim_start().starts_with('#');
        if is_header {
            if !name.is_empty() {
                jobs.push((name.clone(), std::mem::take(&mut body)));
            }
            name = line.trim().trim_end_matches(':').to_string();
        } else if !name.is_empty() {
            body.push_str(line);
            body.push('\n');
        }
    }
    if !name.is_empty() {
        jobs.push((name, body));
    }
    assert!(
        jobs.len() >= 4,
        "job extraction broke — found {} jobs, so this guard is not inspecting \
         what it claims to",
        jobs.len()
    );

    // ⚠ Strip comment lines before deciding what a job DOES. The
    // tag-authoritative comment in `verify-version` literally contains the
    // words "cargo publish"; matching prose classified that job as a publisher
    // and stamped it. Comments are not code — this repo has paid for that
    // lesson more than once.
    let builders: Vec<&(String, String)> = jobs
        .iter()
        .filter(|(_, b)| {
            let code: String = b
                .lines()
                .filter(|l| !l.trim_start().starts_with('#'))
                .collect::<Vec<_>>()
                .join("\n");
            code.contains("cargo publish")
                || code.contains("docker/build-push-action")
                || code.contains("gcloud builds submit")
        })
        .collect();
    assert!(
        !builders.is_empty(),
        "no building/publishing job found in release.yml — the detector is \
         matching nothing, which would make this guard vacuously green"
    );

    for (job, b) in builders {
        assert!(
            b.contains("ci/stamp-version.sh"),
            "release.yml job `{job}` builds or publishes but never runs \
             ci/stamp-version.sh. The artifact would ship carrying the version \
             in the committed Cargo.toml floor, which is stale by design since \
             the bot stopped pushing to main."
        );
    }
}

/// ⚠ The release must not reintroduce a push to `main`.
///
/// `@semantic-release/git` is the plugin that committed the version bump and
/// pushed it. Re-adding it re-breaks the release the moment a required status
/// check is on `main` — which is the whole reason this shape exists.
#[test]
fn semantic_release_does_not_push_to_main() {
    let rc = include_str!("../.releaserc.json");
    // ⚠ Match the QUOTED name. `@semantic-release/git` is a prefix of
    // `@semantic-release/github`, which is still in use — a plain `contains`
    // reports the plugin as present when it is not.
    assert!(
        !rc.contains("\"@semantic-release/git\""),
        "@semantic-release/git is back in .releaserc.json. It pushes the \
         version bump to `main`, which a required status check rejects \
         (GH006) — the commit is [skip ci], so the check never runs on it and \
         stays `expected` forever."
    );
}

/// ⚠⚠ **Every `cargo publish` must tolerate the stamped dirty tree.**
///
/// This pipeline stamps the real version into `Cargo.toml` **in the runner and
/// never commits it** — that is the point of the shape introduced in
/// noetl/ai-meta#330: nothing is pushed to `main`, the tag is authoritative,
/// and the committed `version` is a floor. `ci/stamp-version.sh` regenerates
/// `Cargo.lock` too.
///
/// So by the time `publish-crate` runs the tree is **guaranteed dirty**, and
/// `cargo publish` refuses a dirty tree by default:
///
/// ```text
/// error: 2 files in the working directory contain changes that were not yet
/// committed into git:
/// Cargo.lock
/// Cargo.toml
/// ```
///
/// ⚠ Why the obvious alternative is wrong. "Commit the stamp so the tree is
/// clean" is what the OLD pipeline did, and committing it only helps if it is
/// pushed — which is exactly the push a required status check rejects (GH006).
/// A runner-only commit would also write an unfetchable SHA into the published
/// crate's `.cargo_vcs_info.json`; `--allow-dirty` records `main`'s real SHA
/// plus `"dirty": true`, which is better provenance, and that was verified on
/// the packaged 4.0.2 crate rather than assumed.
///
/// ⚠ Why the shape's own guards did not catch this. noetl/ai-meta#330 was
/// proven on noetl/worker, and **worker publishes no crate at all** — its
/// publish step is an `echo` saying the artifact is a container image. This is
/// the first repo under this shape that runs `cargo publish`, so the conflict
/// could not surface upstream. v4.0.2 was tagged and GitHub-released while
/// crates.io stayed at 4.0.1.
#[test]
fn every_cargo_publish_tolerates_the_stamped_dirty_tree() {
    let wf = include_str!("../.github/workflows/release.yml");

    // ⚠ Strip comment lines before matching, for the same reason the stamp
    // guard above does: several comments here contain the words "cargo
    // publish" while describing it, and counting prose as an invocation makes
    // this guard assert about sentences instead of commands.
    let code: Vec<&str> = wf
        .lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .collect();

    let invocations: Vec<&&str> = code
        .iter()
        .filter(|l| l.contains("cargo publish"))
        .collect();

    // Print the denominator: a zero here makes the loop below vacuously green,
    // and this repo has shipped that shape before.
    assert!(
        invocations.len() >= 3,
        "found {} `cargo publish` invocation(s) in release.yml; expected at \
         least 3 (noetl-directives, noetl-locator, noetl-tools). The matcher \
         is not seeing the commands, so this guard proves nothing.",
        invocations.len()
    );

    for line in invocations {
        assert!(
            line.contains("--allow-dirty"),
            "`cargo publish` without --allow-dirty in release.yml:\n    {}\n\n\
             ci/stamp-version.sh rewrites Cargo.toml and regenerates Cargo.lock \
             in the runner and never commits them, so the tree is always dirty \
             here and cargo refuses to publish. This fails the release AFTER \
             the tag and the GitHub Release already exist, leaving crates.io \
             behind.",
            line.trim()
        );
    }
}
