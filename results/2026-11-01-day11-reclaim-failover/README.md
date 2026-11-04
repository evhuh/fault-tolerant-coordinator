# Day 11 — worker reclaim and leader failover, 2026-10-31 / 11-01

Measured on a **real 3-node `cmd/coordinator` cluster**: raft over HTTP between nodes, client load over gRPC, no simulator and no `simnet` anywhere in the path.
Host: Apple M3 Pro, 11 cores, go1.26.5, darwin/arm64, fsync always on.

Five runs are recorded here, and they do not all mean the same thing.
Read the `-snapshot-entries` column before quoting any number: that flag decides whether the raft log compacts, and the log decides how long a restarted node spends applying before it can vote.

| run | file | `-snapshot-entries` | status |
|---|---|---|---|
| Worker reclaim, 100 trials | `reclaim-trials.jsonl` | 500,000 | **quotable** |
| Leader failover, bounded, 100 trials | `failover-bounded-trials.jsonl` | 500,000 | **quotable at a stated state size** |
| Leader failover, shipped default, 55 trials | `failover-shipped-trials.jsonl` | **10,000** | **quotable; the control** |
| Leader failover, unbounded, 100 trials | `failover-unbounded-superseded.jsonl` | 500,000 | **SUPERSEDED** — kept as a finding |
| Leader failover, no resets, 45 trials | `failover-noreset-45.jsonl` | 500,000 | elimination experiment |
| Steady-state control, no kills, 10 min | `steady-state-control/` | 10,000 | **control** |
| Interleaved A/B, 40 runs | `ab-interleaved.jsonl` | n/a (simulator) | attribution of one red |

## Worker reclaim latency — 100 trials, 100 valid

```
from LAST HEARTBEAT   n=100  p50 2121ms  p95 2217ms  max 2246ms
from the KILL         n=100  p50 1909ms  p95 2109ms  max 2124ms
reassigned, from last heartbeat   n=77  p50 2139ms  p95 2248ms  max 2280ms
reassigned, from the kill         n=77  p50 1931ms  p95 2126ms  max 2150ms
free slots at release: min 14, median 14
trial-index drift: first half p50 1905ms, second half p50 1914ms
0 INVALID, 0 excluded on host probes
```

**t=0 is the last heartbeat RECEIVED, derived from the POST-KILL lease.**
`/debug/workers` reports `lease_expiry_ms`, and last-heartbeat is that minus the 2000ms lease TTL.
The reading taken at victim-selection time is stale — the worker heartbeats every 500ms and renews again during the bracketing probe — so using it overstated this figure by up to one heartbeat interval, measured as p50 2522ms against a from-kill p50 of 1916ms, a 606ms gap.
Once the worker is dead the lease stops moving, so the last value observed IS last-heartbeat + TTL by definition.

Two figures because they answer different questions: from last heartbeat is what the **budget** is about, and from the kill is what a user sees — up to one heartbeat shorter, because the worker was alive for part of a lease it had already renewed.

**Consistent with the ~2250ms budget** (lease 2000 + tick 200 + commit); the worst case of **2246ms sits at its edge**, and the measurement carries ±10ms of poll error, so this is deliberately not reported as a pass count.

**Preconditions, asserted against the cluster before t=0**, each failure writing an INVALID row with its reason:
- the victim holds a **RUNNING** task, not merely an ASSIGNED one — killing a worker that was handed a task but has not started it measures a dispatch race, not a lease expiry;
- a **free slot exists somewhere other than the victim**, or the reassignment leg measures queueing for a slot instead of the reclaim.

**In-flight work is capped at 6 non-terminal tasks** (`CEILING` in `reclaim.py`), topped up from `/debug/tasks`. Without a cap the `sleep 30` tasks accumulate on the spare worker and its free capacity decays: measured at 14, 11, 8, 5, 2, 0 across eight trials, with one reassignment taking 13.7s waiting for a slot rather than for the reclaim.

The spare worker is named `z-spare` so it sorts AFTER the victims. `pickWorker` starts at index 0 of the sorted worker ids on every plan, so named `spare` it absorbed every task and 36 of 57 trials had no victim holding anything.

**23 of 100 trials have no reassignment, lawfully**: the task hit `MaxAttempts` and went terminal, so there was nothing left to dispatch. The reassignment percentiles are over the 77 where a reassignment was possible.

## Leader failover latency

### Shipped default (`-snapshot-entries 10000`) — the control, 55 trials, 55 valid

```
kill -> first task dispatched   p50 1441ms  p95 2090ms  max 2584ms
first half p50 1384ms, second half p50 1554ms  (+12%)
tasks_in_store grew 1,980 -> 19,080 across the run
```

t=0 is the kill; t=1 is the first assignment committed after it, polled at 10ms from `sched_assignments_total` on the survivors. **Precondition: pending backlog ≥ 50** (`MIN_BACKLOG`), asserted per trial — the new leader can only be timed to its first dispatch if there is something to dispatch.

### Bounded (`-snapshot-entries 500000`, cluster rebuilt every 10 trials) — 100 trials, 98 valid

```
kill -> first task dispatched   p50 1303ms  p95 1905ms  max 2066ms
of which BROKER REBUILD         p50 0.64ms  p95 1.98ms  max 2.94ms
tasks_in_store: p50 990 (min 180, max 1800), median 990 in every decile
trial-index drift: first half p50 1317ms, second half p50 1260ms
2 excluded on host probes, 0 INVALID
```

This is **failover at a median of 990 task records with compaction effectively disabled**, not "failover is 1.3s". The cluster is rebuilt from empty every 10 trials precisely so the log stays short; without that, see below.

### Unbounded — SUPERSEDED, and the reason it is kept

```
trials  1-10   dispatch p50  1602ms   est. 1,800 tasks
trials 41-50   dispatch p50  3074ms   est. 16,200 tasks
trials 91-100  dispatch p50 17060ms   est. 34,200 tasks
```

Monotonic, 10x. `unbounded-dispatch-vs-state.txt` has the per-trial series; its x-axis is **DERIVED** (360 tasks submitted per trial), not measured, because these rows predate the `tasks_in_store` field.

**The cause is restart replay inside the Ready loop, and four other explanations were eliminated by measurement:**

- **not task count** — a cluster built fresh to 30,000 records failed over in **1132ms**;
- **not restart cycles** — 45 consecutive kills with a bounded backlog stayed flat at 986-2062ms over the 44 valid rows, decile medians 1241 / 1455 / 1395 / 1377 / 1489ms (`failover-noreset-45.jsonl`);
- **not broker rebuild** — it grew 1.3ms to 29.4ms, which is 28ms of a 15,400ms increase, 0.18%;
- **not WAL read** — a restart against a 4-5MB WAL served HTTP in 50ms against 31ms on an empty one;
- **it is APPLY on restart** — `newRaft` reported `commit: 23490, applied: 0`, and applying those **~23,500 log entries** took **6.3 seconds**, during which the node cannot vote or ack. The entry count is the quantity that matters, not the ~19,800 task records it rebuilt: replay cost is paid per entry.

Reproduced deliberately with `UNBOUNDED=1 RESET_EVERY=9999`: 1127ms at trial 10 to 4690ms by trial 55, a **4.2×** degradation, knee near trial 40-45.
**That reproduction's rows are NOT in this directory** — only its endpoints were recorded, in `docs/architecture.md` and the roadmap. The degradation is reproducible from the superseded run's rows above, which are here; the 4.2× figure itself is quotable only from those endpoints.

**The harness configuration is what let the log grow**: `-snapshot-entries 500000` was set to keep snapshot stalls out of the measurement and disabled compaction entirely — zero snapshots on any node. Under the shipped 10,000 the same workload cost only 12%, replay fell to 2357ms for ~2,900 entries, and stalls fell to 0.3-0.5 per restart. **Bounded by compaction, not removed by it**: a node a full 10,000 entries behind would still exceed the 1s election timeout.

## Steady-state control — no kills, 10 minutes, shipped config

```
75 batches submitted -> 27,000 task records, 4 snapshots per node
node 1: 0 stalls   node 2: 0 stalls   node 3: 0 stalls
```

**Zero stalls**, so there is no unexplained steady-state apply stall: every stall this project has recorded on the real cluster is attributable to restart replay or to the snapshot fsync tail. It also confirms independently that 27,000 task records with no restarts cause no degradation.

## Measurement caveats that apply to everything here

- **±10ms** on every latency: the state and metrics are polled, not pushed.
- **Reclaim's worst case sits at the budget's edge** (2246ms against ~2250ms), which is why it is reported as "consistent with" rather than as a pass count.
- **Stall durations recorded before `17b792a` are FLOORS.** The watchdog polled at `threshold/2` and reported once per stall, so its `Duration` was elapsed-at-detection: quantized into `[threshold, 1.5x threshold]` whatever the real blockage. A six-second stall was reported as roughly 500ms. `17b792a` added a completed-duration report; the 6.3s restart figure above is unaffected because it came from polling the applied task count, not from a stall duration.
- **The host probes bracket every trial** and are in the rows. Two failover trials were excluded on them; no reclaim trial was.
- `ab-interleaved.jsonl` is the interleaved A/B that attributed one `ready-loop-stall` red to host degradation **by rate, not mechanism** — 2/9 on a host at 5.9ms fsync against 0/40 at 2.60-3.85ms.

## Harnesses

`reclaim.py`, `failover.py` (env: `UNBOUNDED`, `RESET_EVERY`, `SNAPENTRIES`, `TRIALS`), `steady.sh`, `profile-failover.py`, and `report.py` which refuses inputs that carry no `RUN_ID` unless `--allow-legacy` is passed, stamping any such output "legacy, unscoped".
