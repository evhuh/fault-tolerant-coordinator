# The plan-batching experiment

Measured on a **real 3-node `cmd/coordinator` cluster**: raft over HTTP between nodes, client load over gRPC, no simulator and no `simnet` anywhere in the path.
Run on the tree with the worker→task index fix (`internal/state` `tasksByWorker`), which the companion `results/scale-profile/` explains.

## Configuration

- **Host:** Apple M3 Pro, 11 cores, go1.26.5, darwin/arm64.
- **Cluster:** 3 coordinators, gRPC on 127.0.0.1:8081-8083, raft on 127.0.0.1:7001-7003, metrics on 127.0.0.1:9091-9093.
- **fsync:** always on. The WAL syncs once per batch and every snapshot calls `File.Sync`; there is no unsafe mode in this build.
- **Workers:** 8 processes × `-concurrency 32` = **256 slots**, all idle at the moment the measurement starts.
- **Backlog:** 20 jobs × 100 tasks = **2,000 tasks**, pre-submitted with **no workers connected**, so every slot is free when the pool appears.
- **Snapshot threshold** raised to 500,000 entries so no snapshot occurs inside a measurement. The synchronous-snapshot exposure is a separate, already-documented finding; measuring both at once would answer neither.
- **Independent variable:** `-max-plan-size` ∈ {1, 10, 100}. 3 runs per arm, median reported.
- **Dependent variable: time to drain the fixed backlog.** Not rate-in-a-window — a window measurement is floor-limited the moment an arm finishes early, which is exactly what happened in the pilot: the P=100 arm drained all 1,000 tasks and then sat idle, so its "33.3/s" was the backlog's rate, not the system's.

### Why the backlog has to be few jobs with many tasks

`sched.computePlan` iterates `snap.PendingTaskIDs(eval.JobID)` — scoped to the eval's **own job**.
A backlog of 2,000 single-task jobs therefore pins plan size at 1.00 whatever `-max-plan-size` says, which is what the first two pilots measured before the code was read.
Plan size is bounded by `min(pending tasks in this job, free slots, max-plan-size)`.

## Results

| | **P=1** | **P=10** | **P=100** |
|---|---|---|---|
| **drain time (median)** | **62.1s** | **18.0s** | **19.0s** |
| the three runs | 60.2 / 62.1 / 93.9 | 16.1 / 18.4 / 18.0 | 19.1 / 18.2 / 19.0 |
| measured plan size | 1.00 | 7.04 | 6.80 |
| ApplyPlan entries | 2000 | 284 | 290 |
| raft entries, per node | 5306 | 2666 | 2690 |
| plan rejections | 320 | 407 | 606 |
| completed | 2000/2000 | 2000/2000 | 2000/2000 |
| leadership changes | 0 | 0 | 0 |

### Entry breakdown, per node

| command | P=1 | P=10 | P=100 |
|---|---|---|---|
| `complete_task` | 2004 | 2005 | 2003 |
| `apply_plan` | **2000** | **284** | **290** |
| `register_worker` | 992 | 296 | 304 |
| `tick` | 310 | 91 | 96 |
| `submit_job` + `create_eval` | 40 | 40 | 40 |

## What it means

**Log-entry reduction is 2.0× and that is the ceiling.** 5,306 → 2,666 entries, because `apply_plan` collapses 2000 → 284 while `complete_task` stays pinned near 2,004: each completion is its own entry and batching cannot touch it.

**Drain time improves 3.4×, which is more than the entry reduction allows, so the win is not log volume.**
At P=1 the planner is serial — one plan per commit round trip — and the arithmetic closes to within 2% on every run:

| run | commit latency mean | plans × mean | observed drain |
|---|---|---|---|
| P=1 r1 | 29.9ms | 59.8s | 60.2s |
| P=1 r2 | 30.7ms | 61.4s | 62.1s |
| P=1 r3 | 47.5ms | 95.0s | 93.9s |

Batching removes 1,716 serial round trips. Completions never paid that cost because they arrive concurrently and batch into shared fsyncs.

**r3 was not an outlier in behaviour.** Its commit latency distribution was simply slower — p50 48ms, p90 62ms, p99 112ms, against p50 29ms / p90 55-56ms / p99 63ms for r1 and r2 — and drain time tracked it exactly, as the serial model predicts.
What the harness **cannot** say is why the disk was slower for that run: unlike the simulator's checkpoint, this bench takes no bracketing fsync or CPU probes, so host attribution is unavailable.
That is a harness gap, recorded as a follow-up rather than guessed at.

**P=100 is marginally worse than P=10** (19.0s vs 18.0s), and the histogram says why: the cap stops binding somewhere below 10.
Rejections rise monotonically with the cap — 320 → 407 → 606 — because a larger plan is computed against state that has moved further by the time it applies.

### Plan-size distribution, P=100, run 3 (300 plans)

```
sched_plan_size_bucket{le="1"}     7
sched_plan_size_bucket{le="2"}    29
sched_plan_size_bucket{le="4"}    81
sched_plan_size_bucket{le="8"}   278     <- 93% of plans carry 8 or fewer
sched_plan_size_bucket{le="16"}  297
sched_plan_size_bucket{le="32"}  297
sched_plan_size_bucket{le="64"}  298
sched_plan_size_bucket{le="128"} 300     <- 2 plans above 64
sched_plan_size_bucket{le="+Inf"} 300
```

**93% of plans carry ≤8 assignments**, and only 2 of 300 exceed 64.
The mean of 6.80 is dominated by small **completion-triggered** evals: every completion emits an eval for its own job, which then finds only a task or two pending in that job.
The two large plans are the initial per-job evals, which found 100 pending tasks against 256 idle slots.

**Follow-up: eval coalescing.** Nomad keeps at most one pending evaluation per job, coalescing further triggers into it. Doing the same here would let a job's completions accumulate into one eval rather than firing one per completion, which should raise effective plan size well above 7 and is the change most likely to move this number. Not attempted here.

## What is in here

- `sweep.txt` — the nine runs as the harness printed them.
- `d0-<P>-<run>-<node>.txt` / `d1-...` — raw `/metrics` scrapes from all three nodes before and after each measurement. Everything above is a diff of these.
- `smoke-openloop.json` — the open-loop `cmd/loadgen` smoke run with its disclosure block (200/s offered, p50 33.1ms, p99 99.3ms from intended send time, generator lag p99 0.7ms).
- `drain.sh` — the experiment. `batch.sh` — the earlier rate-in-a-window version, kept because the pilot that exposed the floor-limiting problem used it. `up.sh` / `down.sh` — the cluster. `profile.sh` — the stall reproduction. `analyze.py` — the scrape differ.

Leader-only metrics are summed across nodes (leadership can move mid-run); per-node log metrics are read from the busiest node and labelled as per-node. Summing those would triple-count the same log.
