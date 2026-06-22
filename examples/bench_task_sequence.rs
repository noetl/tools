//! Micro-benchmark for the task_sequence per-sub-task hot path
//! (noetl/ai-meta#127).
//!
//! Isolates the *code* cost of draining sub-tasks — the
//! `running_ctx.clone()` + per-render context rebuild that the issue
//! flags — from the I/O-bound end-to-end batch run.  Uses the `noop`
//! sub-tool so each sub-task does no real work: the measured time is
//! dominated by the loop's context clones / minijinja rebuilds, which
//! is exactly what the optimization targets.
//!
//! Runs identically on the pre-optimization baseline and the optimized
//! tree (it only touches public API), so:
//!
//!   cargo run --release --example bench_task_sequence        # optimized
//!   git stash push -- src/ && cargo run --release --example bench_task_sequence  # baseline
//!   git stash pop
//!
//! gives a clean before/after with the machine held constant.

use std::time::Instant;

use noetl_tools::tools::TaskSequenceTool;
use noetl_tools::{ExecutionContext, Tool, ToolConfig};

/// Build a sizeable running context, mimicking the accumulated batch
/// state a real PFT drain carries between sub-tasks (workload config +
/// several large sibling payloads).
fn seed_context() -> ExecutionContext {
    let mut ctx = ExecutionContext::new(987654321, "process_pft_action_batches", "http://server");
    ctx.worker_id = Some("worker-bench-1".to_string());
    ctx.command_id = Some("cmd-bench-1".to_string());

    // A workload blob the templates read from.
    ctx.set_variable(
        "workload",
        serde_json::json!({
            "pft_batch_size": 25,
            "pft_batch_concurrency": 16,
            "data_type": "mds",
            "base_url": "http://paginated-api/api/v1/pft/batch",
            "page_size": 10,
        }),
    );

    // Drain loop counter (the policy `when:` compares against it).
    ctx.set_variable("iter", serde_json::json!({"i": 0}));

    // Accumulated sibling-style payloads — each carries a chunk of
    // patient rows, the kind of growing context the drain clones every
    // sub-task.
    for s in 0..8 {
        let rows: Vec<serde_json::Value> = (0..40)
            .map(|i| {
                serde_json::json!({
                    "patient_id": format!("patient_{s}_{i:04}"),
                    "facility": format!("facility_{}", s % 10),
                    "mds": {"a01": i, "b02": "value", "c03": [1, 2, 3, 4]},
                    "detail": "lorem ipsum dolor sit amet consectetur adipiscing",
                })
            })
            .collect();
        ctx.set_variable(
            format!("prior_batch_{s}"),
            serde_json::json!({"rows": rows, "count": 40, "data_type": "mds"}),
        );
    }
    ctx
}

/// Drain iterations a single `execute()` runs before breaking.  The
/// task_sequence rebuilds the tool registry once per `execute()`; a
/// long self-jump drain amortizes that fixed cost across many
/// sub-tasks so the measured per-sub-task figure reflects the loop's
/// context clone / render cost — the thing noetl/ai-meta#127 changes —
/// rather than the one-off registry build.
const DRAIN_ITERS: u64 = 300;

/// A self-jumping sub-task shaped like the PFT per-batch drain
/// (claim/fetch/save fused), carrying `input:` + `set:` + a
/// `spec.policy.rules` block — the full per-sub-task render surface.
/// It loops `DRAIN_ITERS` times via `do: jump` then `do: break`,
/// matching the real fixture's "jump claim_batch until the queue
/// drains" control flow.
fn drain_config() -> ToolConfig {
    let when = format!("{{{{ iter.i < {DRAIN_ITERS} }}}}");
    let tasks = serde_json::json!([
        {
            "drain_step": {
                "kind": "noop",
                "input": {
                    "url": "{{ workload.base_url }}/{{ workload.data_type }}",
                    "batch_size": "{{ workload.pft_batch_size }}",
                    "page_size": "{{ workload.page_size }}"
                },
                "result": {
                    "data": {"records": [{"patient_id": "p1"}, {"patient_id": "p2"}]},
                    "batch_id": "b-001"
                },
                "set": {
                    "iter.fetched": "{{ output.data.records }}",
                    "iter.batch_id": "{{ output.batch_id }}"
                },
                "spec": {
                    "policy": {
                        "rules": [
                            {
                                "when": when,
                                "then": {
                                    "do": "jump",
                                    "to": "drain_step",
                                    "set": {"iter.i": "{{ iter.i + 1 }}"}
                                }
                            },
                            {"else": {"then": {"do": "break"}}}
                        ]
                    }
                }
            }
        }
    ]);
    ToolConfig {
        kind: "task_sequence".to_string(),
        config: tasks,
        timeout: None,
        retry: None,
        auth: None,
    }
}

fn main() {
    // One `run` = one execute() draining DRAIN_ITERS iterations.
    let runs: u64 = std::env::var("BENCH_RUNS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400);
    // DRAIN_ITERS jumps + the final break iteration.
    let subtasks_per_run = DRAIN_ITERS + 1;

    let tool = TaskSequenceTool::new();
    let ctx = seed_context();
    let config = drain_config();

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime");

    // Warm up (registry build, template compile caches, allocator).
    for _ in 0..20 {
        let _ = rt.block_on(tool.execute(&config, &ctx)).expect("warmup");
    }

    let start = Instant::now();
    let acc = std::hint::black_box(rt.block_on(async {
        let mut acc = 0u64;
        for _ in 0..runs {
            let r = tool.execute(&config, &ctx).await.expect("execute");
            acc += r.is_success() as u64;
        }
        acc
    }));
    let elapsed = start.elapsed();
    assert_eq!(acc, runs, "every drain must succeed");

    let subtasks = runs * subtasks_per_run;
    let per_drain_us = elapsed.as_secs_f64() * 1e6 / runs as f64;
    let per_subtask_us = elapsed.as_secs_f64() * 1e6 / subtasks as f64;

    println!("runs={runs} subtasks={subtasks} total={:.3}s", elapsed.as_secs_f64());
    println!("per_drain={per_drain_us:.2}us  per_subtask={per_subtask_us:.2}us");
    println!(
        "drains_per_sec={:.0}  subtasks_per_sec={:.0}",
        runs as f64 / elapsed.as_secs_f64(),
        subtasks as f64 / elapsed.as_secs_f64()
    );
}
