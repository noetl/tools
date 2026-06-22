//! NoETL Resource Locator — the stable logical name for every data asset
//! and its derivation to a physical cell/shard location.
//!
//! NoETL runs as a super-cluster spanning cloud providers, regions, zones,
//! hybrid datacenters, and Kubernetes clusters. A result's **name must
//! resolve to where its bytes live** — without hard-coding a mutable physical
//! placement into the name. This module is the single source of truth for
//! that naming so the server, the worker, and the materialiser pool share one
//! implementation rather than the divergent string-formatting they use today
//! (`noetl://execution/<eid>/result/<name>/<id>` built in the worker, parsed
//! again in the server).
//!
//! Two layers, per the
//! [Global Hybrid Supercluster Blueprint](https://github.com/noetl/docs/blob/main/docs/architecture/noetl_global_hybrid_cloud_grid_distributed_architecture_blueprint.md)
//! (§4 Regional Cell + Shard, §7 Object Store, §8 Resource Locator):
//!
//! 1. **Logical URI** — `noetl://<tenant>/<project>/<kind>/<logical_path>@<version>`.
//!    Location-independent; never renamed on replication, migration, or
//!    failover. This is what execution state carries and what dedup/replay
//!    key on. See [`ResourceLocator`].
//! 2. **Topology resolution** — `shard_key = hash(tenant + project + affinity)
//!    % shard_count → region + cell + shard`, yielding the §7 physical object
//!    prefix. Derivable from `(tenant, project, execution_id)` with zero
//!    central lookup; only the small, slow-changing cell endpoint map needs a
//!    registry. See [`shard_key`] and [`ResultCoordinates::physical_key`].
//!
//! ## Stability contract
//!
//! The shard hash MUST be reproducible by any consumer, any binary version,
//! on any architecture, forever — a result written by `cell A` today must be
//! findable by `cell B` running a different build next year. So this module
//! uses a fixed FNV-1a hash, **not** [`std::collections::hash_map::DefaultHasher`]
//! (SipHash with a per-process random seed). The locked test
//! `shard_key_is_stable` pins an exact value; changing the hash is a breaking
//! change to the storage layout.

use std::fmt;

/// URI scheme prefix for every NoETL resource locator.
pub const SCHEME: &str = "noetl://";

/// Default logical shard count per region (§4 recommended starting point).
pub const DEFAULT_SHARD_COUNT: u32 = 256;

/// Tenant used when a deployment is single-tenant and has not assigned one.
pub const DEFAULT_TENANT: &str = "default";
/// Project used when a deployment is single-project and has not assigned one.
pub const DEFAULT_PROJECT: &str = "default";

/// The `kind` segment for execution result assets.
pub const KIND_RESULTS: &str = "results";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Failure parsing a locator URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocatorError {
    /// The string did not start with `noetl://`.
    MissingScheme(String),
    /// Fewer than the required `<tenant>/<project>/<kind>/<logical_path>` segments.
    TooFewSegments(String),
    /// An empty segment where a value was required.
    EmptySegment(String),
    /// The locator is well-formed but is not a `results` locator (its `kind`
    /// segment is something else) — surfaced by [`ResultCoordinates::from_locator`].
    NotResultLocator(String),
}

impl fmt::Display for LocatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LocatorError::MissingScheme(s) => {
                write!(f, "locator must start with '{SCHEME}', got: {s:?}")
            }
            LocatorError::TooFewSegments(s) => write!(
                f,
                "locator must have at least tenant/project/kind/logical_path segments: {s:?}"
            ),
            LocatorError::EmptySegment(s) => write!(f, "locator has an empty required segment: {s:?}"),
            LocatorError::NotResultLocator(s) => {
                write!(f, "locator is not a '{KIND_RESULTS}' locator: {s:?}")
            }
        }
    }
}

impl std::error::Error for LocatorError {}

// ---------------------------------------------------------------------------
// Logical locator (§8)
// ---------------------------------------------------------------------------

/// A parsed NoETL Resource Locator — the stable logical name (§8).
///
/// `noetl://<tenant>/<project>/<kind>/<logical_path>@<version>`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceLocator {
    pub tenant: String,
    pub project: String,
    /// Asset class — `results`, `events`, `models`, `datasets`, …
    pub kind: String,
    /// Everything between `<kind>` and the optional `@<version>`; may contain
    /// `/`.
    pub logical_path: String,
    /// The `@<version>` suffix (`v1`, `sha256-…`, `final`), without the `@`.
    pub version: Option<String>,
}

impl ResourceLocator {
    /// Construct a locator from its parts.
    pub fn new(
        tenant: impl Into<String>,
        project: impl Into<String>,
        kind: impl Into<String>,
        logical_path: impl Into<String>,
        version: Option<String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            project: project.into(),
            kind: kind.into(),
            logical_path: logical_path.into(),
            version,
        }
    }

    /// Render the logical URI.
    pub fn to_uri(&self) -> String {
        let mut s = format!(
            "{SCHEME}{}/{}/{}/{}",
            self.tenant, self.project, self.kind, self.logical_path
        );
        if let Some(v) = &self.version {
            s.push('@');
            s.push_str(v);
        }
        s
    }

    /// Parse a logical URI in the canonical `noetl://<tenant>/<project>/<kind>/<logical_path>[@<version>]`
    /// shape.
    pub fn parse(uri: &str) -> Result<Self, LocatorError> {
        let rest = uri
            .strip_prefix(SCHEME)
            .ok_or_else(|| LocatorError::MissingScheme(uri.to_string()))?;

        // Split the optional trailing `@version` off the FULL remainder first
        // (logical_path itself never contains '@').
        let (path, version) = match rest.rsplit_once('@') {
            Some((p, v)) if !v.contains('/') => (p, Some(v.to_string())),
            _ => (rest, None),
        };

        let parts: Vec<&str> = path.split('/').collect();
        if parts.len() < 4 {
            return Err(LocatorError::TooFewSegments(uri.to_string()));
        }
        let tenant = parts[0];
        let project = parts[1];
        let kind = parts[2];
        let logical_path = parts[3..].join("/");
        if tenant.is_empty() || project.is_empty() || kind.is_empty() || logical_path.is_empty() {
            return Err(LocatorError::EmptySegment(uri.to_string()));
        }

        Ok(Self::new(
            tenant,
            project,
            kind,
            logical_path,
            version,
        ))
    }

    /// Stable shard key for this locator's `(tenant, project)` with optional
    /// co-celling `affinity`.
    pub fn shard_key(&self, affinity: Option<&str>, shard_count: u32) -> u32 {
        shard_key(&self.tenant, &self.project, affinity, shard_count)
    }
}

impl fmt::Display for ResourceLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_uri())
    }
}

// ---------------------------------------------------------------------------
// Result coordinates (§7 physical mapping)
// ---------------------------------------------------------------------------

/// The execution-scoped coordinates that address one result, collision-free
/// across the two-level cursor fan-out (`frame`, `row`) and retries (`attempt`).
///
/// A `mode: cursor` loop fans out twice: the orchestrator claims a **frame** of
/// rows, then dispatches one body command per **row** in that frame
/// (`metadata.cursor = {frame, row}` on the body command). Each body command
/// produces one result, so `(frame, row)` is the coordinate that makes a
/// result's name unique within its step — `frame` alone collides across the
/// rows of a frame. Both default to `0` for a step that does not fan out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultCoordinates {
    pub tenant: String,
    pub project: String,
    pub execution_id: i64,
    pub step: String,
    /// Cursor frame / claim index — `0` when the step does not fan out.
    pub frame: u64,
    /// Row index within the frame (the body command's `cursor.row`) — `0` when
    /// the frame holds a single result or the step does not fan out.
    pub row: u64,
    /// 1-based retry attempt.
    pub attempt: u32,
}

impl ResultCoordinates {
    /// Construct coordinates, defaulting tenant/project for single-tenant
    /// deployments that have not assigned them.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tenant: Option<&str>,
        project: Option<&str>,
        execution_id: i64,
        step: impl Into<String>,
        frame: u64,
        row: u64,
        attempt: u32,
    ) -> Self {
        Self {
            tenant: tenant.unwrap_or(DEFAULT_TENANT).to_string(),
            project: project.unwrap_or(DEFAULT_PROJECT).to_string(),
            execution_id,
            step: step.into(),
            frame,
            row,
            attempt,
        }
    }

    /// The stable logical locator for this result:
    /// `noetl://<tenant>/<project>/results/<execution_id>/<step>/<frame>/<row>/<attempt>`.
    pub fn to_locator(&self) -> ResourceLocator {
        ResourceLocator::new(
            self.tenant.clone(),
            self.project.clone(),
            KIND_RESULTS,
            format!(
                "{}/{}/{}/{}/{}",
                self.execution_id, self.step, self.frame, self.row, self.attempt
            ),
            None,
        )
    }

    /// The logical URI string (convenience over `to_locator().to_uri()`).
    pub fn logical_uri(&self) -> String {
        self.to_locator().to_uri()
    }

    /// Parse the canonical results logical URI back into coordinates — the
    /// inverse of [`logical_uri`](Self::logical_uri).
    ///
    /// The result materialiser (noetl/ai-meta#104 Phase B) derives the §7
    /// physical key from the worker-stamped `reference.uri`, so it must recover
    /// `(tenant, project, execution_id, step, frame, row, attempt)` from the
    /// stable name. This is the single source of that inversion so the
    /// materialiser and any future consumer agree with the producer
    /// ([`logical_uri`](Self::logical_uri)) byte-for-byte.
    ///
    /// Accepts `noetl://<tenant>/<project>/results/<eid>/<step>/<frame>/<row>/<attempt>`.
    pub fn parse(uri: &str) -> Result<Self, LocatorError> {
        Self::from_locator(&ResourceLocator::parse(uri)?)
    }

    /// Recover result coordinates from an already-parsed [`ResourceLocator`].
    ///
    /// `step` may itself contain `/`, so the numeric tail
    /// (`frame`/`row`/`attempt`) is taken from the END and `execution_id` from
    /// the front; everything between is the step. Rejects a non-`results`
    /// locator ([`LocatorError::NotResultLocator`]) and a tail that does not
    /// parse as integers.
    pub fn from_locator(loc: &ResourceLocator) -> Result<Self, LocatorError> {
        if loc.kind != KIND_RESULTS {
            return Err(LocatorError::NotResultLocator(loc.to_uri()));
        }
        let segs: Vec<&str> = loc.logical_path.split('/').collect();
        if segs.len() < 5 {
            return Err(LocatorError::TooFewSegments(loc.to_uri()));
        }
        let n = segs.len();
        let parse_int_err = || LocatorError::EmptySegment(loc.to_uri());
        let execution_id = segs[0].parse::<i64>().map_err(|_| parse_int_err())?;
        let frame = segs[n - 3].parse::<u64>().map_err(|_| parse_int_err())?;
        let row = segs[n - 2].parse::<u64>().map_err(|_| parse_int_err())?;
        let attempt = segs[n - 1].parse::<u32>().map_err(|_| parse_int_err())?;
        let step = segs[1..n - 3].join("/");
        if step.is_empty() {
            return Err(LocatorError::EmptySegment(loc.to_uri()));
        }
        Ok(Self::new(
            Some(&loc.tenant),
            Some(&loc.project),
            execution_id,
            step,
            frame,
            row,
            attempt,
        ))
    }

    /// Stable shard key, co-celling all results of one execution by feeding
    /// `execution_id` as the affinity (§4 `optional_dataset_or_execution_affinity`).
    pub fn shard_key(&self, shard_count: u32) -> u32 {
        shard_key(
            &self.tenant,
            &self.project,
            Some(&self.execution_id.to_string()),
            shard_count,
        )
    }

    /// The §7 physical object-store key for this result under a resolved cell
    /// placement. `date` is the UTC partition date (`YYYY-MM-DD`); `ext` is the
    /// payload extension (`feather`, `json`, `parquet`).
    pub fn physical_key(&self, placement: &CellPlacement, date: &str, ext: &str) -> String {
        format!(
            "noetl/env={env}/region={region}/cell={cell}/shard={shard}/\
             tenant={tenant}/project={project}/date={date}/execution={eid}/\
             results/{step}/{frame}/{row}/{attempt}.{ext}",
            env = placement.env,
            region = placement.region,
            cell = placement.cell,
            shard = placement.shard,
            tenant = self.tenant,
            project = self.project,
            date = date,
            eid = self.execution_id,
            step = self.step,
            frame = self.frame,
            row = self.row,
            attempt = self.attempt,
            ext = ext,
        )
    }
}

/// A resolved cell placement (§4) — the physical home of a shard. Produced by
/// resolving `shard_key` against the cell topology / endpoint map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellPlacement {
    /// Deployment environment — `prod`, `staging`, `dev`.
    pub env: String,
    /// Region code — `usw2`.
    pub region: String,
    /// Cell within the region — `usw2-a`.
    pub cell: String,
    /// Logical shard label — `s0042`.
    pub shard: String,
}

impl CellPlacement {
    /// Build a placement, formatting the shard id as the `s####` label used in
    /// the §4 naming convention.
    pub fn new(
        env: impl Into<String>,
        region: impl Into<String>,
        cell: impl Into<String>,
        shard_id: u32,
    ) -> Self {
        Self {
            env: env.into(),
            region: region.into(),
            cell: cell.into(),
            shard: format!("s{shard_id:04}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Stable hashing
// ---------------------------------------------------------------------------

/// FNV-1a 64-bit — a fixed, dependency-free, architecture-independent hash.
/// Used for shard derivation so the mapping is reproducible across binaries
/// and time. Do not swap for `DefaultHasher` (random-seeded SipHash).
fn fnv1a_64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Stable shard key for a `(tenant, project)` pair with optional co-celling
/// `affinity` (§4: `hash(tenant_id + project_id + optional_execution_affinity)
/// % shard_count`). The unit separator (`0x1f`) prevents segment-boundary
/// collisions (`a|bc` vs `ab|c`).
pub fn shard_key(tenant: &str, project: &str, affinity: Option<&str>, shard_count: u32) -> u32 {
    debug_assert!(shard_count > 0, "shard_count must be non-zero");
    let shard_count = shard_count.max(1);
    let mut buf = Vec::with_capacity(tenant.len() + project.len() + 8);
    buf.extend_from_slice(tenant.as_bytes());
    buf.push(0x1f);
    buf.extend_from_slice(project.as_bytes());
    if let Some(a) = affinity {
        buf.push(0x1f);
        buf.extend_from_slice(a.as_bytes());
    }
    (fnv1a_64(&buf) % shard_count as u64) as u32
}

// ---------------------------------------------------------------------------
// Legacy compatibility
// ---------------------------------------------------------------------------

/// Parsed legacy `noetl://execution/<eid>/result/<name>/<id>` reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegacyExecutionRef {
    pub execution_id: i64,
    pub name: String,
    pub result_id: i64,
}

/// `true` if `uri` is the legacy execution-scoped result reference shape
/// (first path segment `execution`) rather than the canonical locator.
pub fn is_legacy_execution_ref(uri: &str) -> bool {
    uri.strip_prefix(SCHEME)
        .map(|rest| rest.starts_with("execution/"))
        .unwrap_or(false)
}

/// Parse the legacy `noetl://execution/<eid>/result/<name>/<id>` shape so the
/// resolve path accepts both during the migration. New producers emit
/// [`ResultCoordinates::logical_uri`] instead.
pub fn parse_legacy_execution_ref(uri: &str) -> Result<LegacyExecutionRef, LocatorError> {
    let path = uri
        .strip_prefix(SCHEME)
        .ok_or_else(|| LocatorError::MissingScheme(uri.to_string()))?;
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 5 || parts[0] != "execution" || parts[2] != "result" {
        return Err(LocatorError::TooFewSegments(uri.to_string()));
    }
    let execution_id = parts[1]
        .parse::<i64>()
        .map_err(|_| LocatorError::EmptySegment(uri.to_string()))?;
    let result_id = parts[parts.len() - 1]
        .parse::<i64>()
        .map_err(|_| LocatorError::EmptySegment(uri.to_string()))?;
    let name = parts[3..parts.len() - 1].join("/");
    if name.is_empty() {
        return Err(LocatorError::EmptySegment(uri.to_string()));
    }
    Ok(LegacyExecutionRef {
        execution_id,
        name,
        result_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_uri_round_trips() {
        let loc = ResourceLocator::new("t_acme", "p_gen", "results", "exec_1/align/main", Some("v1".into()));
        let uri = loc.to_uri();
        assert_eq!(uri, "noetl://t_acme/p_gen/results/exec_1/align/main@v1");
        assert_eq!(ResourceLocator::parse(&uri).unwrap(), loc);
    }

    #[test]
    fn logical_uri_round_trips_without_version() {
        let uri = "noetl://t/p/datasets/market/snap";
        let loc = ResourceLocator::parse(uri).unwrap();
        assert_eq!(loc.version, None);
        assert_eq!(loc.logical_path, "market/snap");
        assert_eq!(loc.to_uri(), uri);
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert!(matches!(
            ResourceLocator::parse("https://x/y/z/w"),
            Err(LocatorError::MissingScheme(_))
        ));
        assert!(matches!(
            ResourceLocator::parse("noetl://t/p/results"),
            Err(LocatorError::TooFewSegments(_))
        ));
        assert!(matches!(
            ResourceLocator::parse("noetl://t//results/x"),
            Err(LocatorError::EmptySegment(_))
        ));
    }

    #[test]
    fn result_coordinates_build_the_logical_uri() {
        // frame 2, row 4 — a cursor body result.
        let c = ResultCoordinates::new(Some("t_acme"), Some("p_gen"), 325, "load_next_facility", 2, 4, 1);
        assert_eq!(
            c.logical_uri(),
            "noetl://t_acme/p_gen/results/325/load_next_facility/2/4/1"
        );
        // Round-trips back through the generic parser.
        let loc = ResourceLocator::parse(&c.logical_uri()).unwrap();
        assert_eq!(loc.kind, "results");
        assert_eq!(loc.logical_path, "325/load_next_facility/2/4/1");
    }

    #[test]
    fn result_coordinates_parse_round_trips_logical_uri() {
        // The producer stamps `logical_uri`; the materialiser parses it back.
        // The inversion must be exact for every field.
        let c = ResultCoordinates::new(Some("t_acme"), Some("p_gen"), 325, "load_next_facility", 2, 4, 1);
        let parsed = ResultCoordinates::parse(&c.logical_uri()).unwrap();
        assert_eq!(parsed, c);
        // No-fan-out default coordinates round-trip too.
        let d = ResultCoordinates::new(None, None, 7, "start", 0, 0, 1);
        assert_eq!(ResultCoordinates::parse(&d.logical_uri()).unwrap(), d);
    }

    #[test]
    fn result_coordinates_parse_keeps_every_attempt() {
        // Keep-every (OQ1): a retry bumps `attempt`, which is in the name — so
        // parsing two attempts of the same (eid, step, frame, row) yields
        // distinct coordinates AND distinct physical keys (never overwritten).
        let a1 = ResultCoordinates::parse("noetl://t/p/results/1/s/0/0/1").unwrap();
        let a2 = ResultCoordinates::parse("noetl://t/p/results/1/s/0/0/2").unwrap();
        assert_eq!(a1.attempt, 1);
        assert_eq!(a2.attempt, 2);
        assert_ne!(a1, a2);
        let pl = CellPlacement::new("dev", "local", "local-0", 0);
        assert_ne!(
            a1.physical_key(&pl, "2026-06-22", "feather"),
            a2.physical_key(&pl, "2026-06-22", "feather")
        );
    }

    #[test]
    fn result_coordinates_parse_handles_step_with_slash() {
        // A step name containing '/' must still parse: eid from the front,
        // frame/row/attempt from the tail, the slash-bearing remainder is step.
        let c = ResultCoordinates::parse("noetl://t/p/results/9/a/b/c/3/5/2").unwrap();
        assert_eq!(c.execution_id, 9);
        assert_eq!(c.step, "a/b/c");
        assert_eq!(c.frame, 3);
        assert_eq!(c.row, 5);
        assert_eq!(c.attempt, 2);
    }

    #[test]
    fn result_coordinates_parse_rejects_malformed() {
        // Wrong kind.
        assert!(matches!(
            ResultCoordinates::parse("noetl://t/p/datasets/9/s/0/0/1"),
            Err(LocatorError::NotResultLocator(_))
        ));
        // Too few tail segments.
        assert!(matches!(
            ResultCoordinates::parse("noetl://t/p/results/9/s/0"),
            Err(LocatorError::TooFewSegments(_))
        ));
        // Non-integer attempt.
        assert!(matches!(
            ResultCoordinates::parse("noetl://t/p/results/9/s/0/0/x"),
            Err(LocatorError::EmptySegment(_))
        ));
        // Not a locator at all.
        assert!(matches!(
            ResultCoordinates::parse("https://x/y/results/9/s/0/0/1"),
            Err(LocatorError::MissingScheme(_))
        ));
    }

    #[test]
    fn result_coordinates_default_tenant_project() {
        // No fan-out: frame 0, row 0.
        let c = ResultCoordinates::new(None, None, 7, "s", 0, 0, 1);
        assert_eq!(c.tenant, "default");
        assert_eq!(c.project, "default");
        assert_eq!(c.logical_uri(), "noetl://default/default/results/7/s/0/0/1");
    }

    #[test]
    fn physical_key_matches_blueprint_layout() {
        let c = ResultCoordinates::new(Some("t_acme"), Some("p_gen"), 325, "align_reads", 3, 7, 2);
        let placement = CellPlacement::new("prod", "usw2", "usw2-a", 42);
        let key = c.physical_key(&placement, "2026-06-16", "feather");
        assert_eq!(
            key,
            "noetl/env=prod/region=usw2/cell=usw2-a/shard=s0042/\
             tenant=t_acme/project=p_gen/date=2026-06-16/execution=325/\
             results/align_reads/3/7/2.feather"
        );
    }

    #[test]
    fn frame_row_and_attempt_are_collision_free() {
        let base = ResultCoordinates::new(Some("t"), Some("p"), 1, "s", 0, 0, 1);
        let other_frame = ResultCoordinates { frame: 5, ..base.clone() };
        let other_row = ResultCoordinates { row: 3, ..base.clone() };
        let other_attempt = ResultCoordinates { attempt: 2, ..base.clone() };
        // Distinct frame / row / attempt → distinct logical names AND keys —
        // two rows of the same frame (the cursor case) must not collide.
        assert_ne!(base.logical_uri(), other_frame.logical_uri());
        assert_ne!(base.logical_uri(), other_row.logical_uri());
        assert_ne!(base.logical_uri(), other_attempt.logical_uri());
        let pl = CellPlacement::new("prod", "usw2", "usw2-a", 0);
        for other in [&other_frame, &other_row, &other_attempt] {
            assert_ne!(
                base.physical_key(&pl, "d", "feather"),
                other.physical_key(&pl, "d", "feather")
            );
        }
    }

    #[test]
    fn shard_key_is_stable() {
        // LOCKED VALUE — the shard hash is a forever-stable storage contract.
        // If this assertion changes, the object-store layout has shifted and
        // every previously-written result is stranded. Do not "fix" it to
        // match a new hash; that is a breaking migration.
        assert_eq!(shard_key("t_acme", "p_gen", Some("325"), 256), 235);
        assert_eq!(shard_key("t_acme", "p_gen", None, 256), 244);
        // Same inputs → same key, every call.
        assert_eq!(
            shard_key("t", "p", Some("e"), 256),
            shard_key("t", "p", Some("e"), 256)
        );
    }

    #[test]
    fn shard_key_separator_prevents_boundary_collision() {
        // `a|bc` and `ab|c` must not hash the same despite concatenating to
        // the same bytes without a separator.
        assert_ne!(
            shard_key("a", "bc", None, 256),
            shard_key("ab", "c", None, 256)
        );
    }

    #[test]
    fn shard_key_distributes_across_the_space() {
        use std::collections::HashSet;
        let mut buckets = HashSet::new();
        for i in 0..2000 {
            buckets.insert(shard_key("t", "p", Some(&i.to_string()), 256));
        }
        // 2000 distinct executions should touch most of the 256 shards.
        assert!(
            buckets.len() > 200,
            "expected wide shard spread, hit only {}",
            buckets.len()
        );
    }

    #[test]
    fn shard_key_respects_count() {
        for i in 0..500 {
            let k = shard_key("t", "p", Some(&i.to_string()), 16);
            assert!(k < 16, "shard {k} out of range for count 16");
        }
    }

    #[test]
    fn legacy_ref_parses_and_is_detected() {
        let uri = "noetl://execution/123/result/my_step/456";
        assert!(is_legacy_execution_ref(uri));
        assert!(!is_legacy_execution_ref("noetl://t/p/results/1/s/0/1"));
        let r = parse_legacy_execution_ref(uri).unwrap();
        assert_eq!(r.execution_id, 123);
        assert_eq!(r.name, "my_step");
        assert_eq!(r.result_id, 456);
    }
}
