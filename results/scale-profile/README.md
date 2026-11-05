# The O(state) read-path defect

Two CPU profiles from a real 3-node `cmd/coordinator` cluster, taken either side of the worker→task index fix.
The rule it produced and the before/after numbers are in the top-level README, under "The two findings the benchmarks produced".

## `cpu-10k-tasks-before-fix.pprof` — the defect

Captured on the leader for 20s while 8 workers × concurrency 32 polled a cluster holding 10,000 tasks.

```
Duration: 20.02s, Total samples = 3.60s (17.99%)
     1.69s 46.94%  runtime.pthread_cond_signal
     0.43s 11.94%  runtime.pthread_cond_wait
     0.34s  9.44%  runtime.kevent
     0.18s  5.00%  cmpbody
     0.15s  4.17%  server.(*Service).heldTask.func1      (0.58s cum, 16.11%)
```

**18% CPU with 59% of samples in mutex wait and wake** — so not a CPU problem. The call graph names the whole path:

```
Service.RequestTask → Service.heldTask → Coordinator.read   0.58s  16.11%
    ├─ StateStore.TaskIDs  0.32s (55%)  → sortedTaskIDs → pdqsortOrdered  0.35s
    └─ StateStore.Task     0.11s (19%)
```

`heldTask` sorted **every task id in the store** to answer a question about one worker, on every pass of the long poll, under the apply lock.
Observed consequence: Ready-loop stalls of **540–695ms in `apply`** on multiple nodes, **7–9 leadership changes per 30s window**, commit latency **322–696ms**.

Three plausible per-apply culprits were checked against this profile and cleared: `applyRegisterWorker`'s scan per heartbeat (absent), `recomputeJobState` per completion (absent), tick scans (`applyTick` at 0.83%).

## `cpu-150k-tasks-after-fix.pprof` — the limit that remains

Captured the same way at 150,000 tasks, after the fix. No stalls, no leadership changes, but the process is now CPU-bound.

```
Duration: 20.12s, Total samples = 27.86s (138.49%)
    4.32s 15.51%  slices.partitionOrdered[string]   (7.64s cum, 27.42%)
    3.44s 12.35%  cmpbody
    1.62s  5.81%  state.(*Snapshot).InFlight        (5.15s cum, 18.49%)
    0.51s  1.83%  state.(*Snapshot).PendingTaskIDs
```

Attribution of the 9.17s spent sorting: **`sortedTaskIDs` 8.03s (87.6%)** on the tick paths, **`StateStore.Snapshot` 1.14s (12.4%)** building each planning snapshot.
`Snapshot.InFlight` is called per worker from `computePlan` and scans every task each time — O(workers × tasks) per plan.

So the cost moved from the RPC path to the scheduler and tick paths, and from a cliff to a slope.

## Scale, as measured

| tasks in store | stalls | leadership changes | commit latency | assignments/s |
|---|---|---|---|---|
| 10,000 | 0 (was 6/13/10) | 1 (initial) | 30.8ms (was 322–696ms) | 29.4 (was 4.1) |
| 50,000 | 0 | 1 (initial) | 35.9ms | 23.8 |
| 150,000 | 0 | 1 (initial) | 220.8ms | 3.8 |

**No stall boundary up to 150,000 tasks.** Correct and stall-free to at least that size, with useful throughput to roughly 50,000.
Beyond that the scheduler's per-plan scans dominate: throughput falls about 6× and latency rises about 6×.
The next fix is an in-flight count index plus a cheaper planning snapshot; deliberately not done here.

Read either profile with `go tool pprof -top <file>` (the binary is `cmd/coordinator` built from the commit under test).
