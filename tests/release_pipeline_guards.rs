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
