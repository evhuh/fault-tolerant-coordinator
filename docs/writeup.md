# Four things this coordinator taught me

This is a 3-node task coordinator whose control plane is a replicated state machine on etcd/raft.
Jobs and tasks live in the Raft log, workers dial in and long-poll for work, and a leader can be `SIGKILL`ed mid-flight without losing or duplicating a committed effect.
The measured numbers are in the [README](../README.md); the design decisions, one by one, are in [architecture.md](architecture.md).
This is the part that fits in neither: the four problems that were actually hard, and what the verification does and does not establish.

---

## 1. You cannot read the clock in the apply path

Lease expiry is the first thing a coordinator needs and the first place a replicated state machine bites you.

A worker holds a task under a lease, heartbeating every 500ms; if the coordinator hasn't heard from it within the 2000ms TTL, the task is reclaimed and the next attempt dispatched.
The obvious implementation — compare `worker.LeaseExpiryMs` against `time.Now()` in the lease check — is wrong in a way no single-node test can show you.

`Apply` runs on **every replica**, against the same log, and must reach the same state; `time.Now()` returns a different value on each one.
A follower applying entry 4,000 three milliseconds later than the leader would reclaim a task the leader considers live, and the replicas now disagree about who holds what.
That isn't a race that shows up under load. It's a divergence that compounds silently until a snapshot or a leader change exposes it.

The fix is to make time a **decided fact** rather than an observation.
The leader proposes a `Tick{nowMs}` command every 200ms, carrying its own wall clock reading, and the apply path reads lease deadlines against *that* timestamp.
Every replica applies the same `Tick` and makes the same decision.
Wall-clock time becomes an input to the state machine instead of an ambient property of the machine running it.

That one rule propagates further than it looks.

- **Retry backoff deadlines** are absolute `NotBeforeMs` values computed at apply time from the tick, not durations evaluated later. Their **jitter is derived from `(taskID, attempt)`**, not drawn randomly: jitter exists to desynchronize retries, not to be unpredictable, and a replica drawing its own random number diverges exactly like `time.Now()` would.
- **The zero clock is a real state, and it cost me a bug.** The clock starts at 0, so anything applied before the first `Tick` carries `createdMs = 0` — meaning *no time yet*, but looking infinitely old against a real timestamp. Dedup sessions created between winning an election and the first `Tick` committing were destroyed by the first session sweep, so submission dedup silently failed for that window. It surfaced as a snapshot bug, because that is where a test happened to look. Both fixes stay deterministic: the ticker fires immediately on election, and the first `Tick` **rebases** zero timestamps to now rather than exempting them, so they still expire on schedule.

The general shape: an RPC is a *request*, a log command is a *decided fact*.
A `SubmitJobRequest` carries no job id — the leader assigns one, then proposes a command containing it. Anything nondeterministic gets decided once, by the leader, and baked into the entry.
Hold that line and replication becomes boring, which is the goal.

---

## 2. What batching decisions actually buys

Scheduling is modelled on Nomad, and the shape exists for a throughput reason rather than an aesthetic one.

A state change creates an **evaluation** — a note saying "job X may need scheduling". Evaluations go into a priority **broker**. N **scheduler workers** dequeue them concurrently, each reading an immutable **snapshot** and producing a **plan**: a set of assignments. A single-threaded **planner** verifies each plan against *current* state — is that worker still alive, does it still have a free slot, is that task still pending — and proposes the accepted part as **one** `ApplyPlan` entry.

So the expensive part (deciding) is concurrent and lock-free, and only the cheap part (committing) serializes.
A plan can be rejected whole or accepted **partially**, still deterministically, because its assignments apply in log order and the apply path re-checks each one.

The eval broker is **derived, not replicated**: a queue of *intentions*, reconstructible from the facts, so replicating it would mean paying consensus for scheduling bookkeeping.
A new leader rebuilds it by scanning state, measured at p50 0.64ms and worst case 2.94ms — a rounding error inside the ~1.3s failover it sits in.

### The part I got wrong first

The intuition is that batching helps because it shrinks the log.
I measured it, and that's mostly not where the win comes from.

Draining a fixed 2,000-task backlog, with `-max-plan-size` swept over {1, 10, 100}, three runs per arm:

| | P=1 | P=10 | P=100 |
|---|---|---|---|
| drain time (median) | **62.1s** | **18.0s** | **19.0s** |
| `apply_plan` entries | 2000 | 284 | 290 |
| raft entries per node | 5306 | 2666 | 2690 |
| measured plan size | 1.00 | 7.04 | 6.80 |
| plan rejections | 320 | 407 | 606 |

**Log-entry reduction is 2.0×, and that is its ceiling.**
`apply_plan` collapses from 2,000 entries to 284, but `complete_task` stays pinned near 2,004 — every completion is its own entry, and batching cannot touch it.

**Drain time improves 3.4×, which is more than the entry reduction allows**, so the win isn't log volume.
At `P=1` the planner is *serial*: one plan, one commit round trip, wait, repeat.
Multiply each run's mean commit latency by its plan count and you get its drain time to within 2% on all three runs — including the slow one, whose commit distribution was simply worse (p50 48ms against 29ms) and whose drain time tracked it exactly.
Batching removes **1,716 serial round trips**; that's the mechanism.
Completions never paid that cost, because they arrive concurrently and batch into shared fsyncs for free.

**And `P=100` is slightly worse than `P=10`**, because the cap stops binding somewhere below 10: 93% of plans carry eight or fewer assignments, and only 2 of 300 exceed 64.
Rejections rise monotonically with the cap — 320 → 407 → 606 — since a larger plan is computed against state that has moved further by the time it applies.
Optimistic concurrency costs something, and the cost scales with how much you optimistically assume.

Why plans are small is the interesting part: `computePlan` is scoped to **one job's** pending tasks, and most evaluations are triggered by a single task completing — so the typical eval finds one or two pending tasks in its own job and emits a plan that size.
The large plans are the initial per-job evaluations, which found 100 pending tasks against 256 idle slots.

The follow-up this points at is **eval coalescing**: Nomad keeps at most one pending evaluation per job and folds further triggers into it, which here would let a job's completions accumulate into one evaluation instead of firing one per completion.
That is the change most likely to move plan size above 7, and it isn't done.

---

## 3. At-least-once delivery, effectively-once effects

These get muddled constantly, so here is the distinction this system implements.

**Delivery is at-least-once, and overlapping execution is by design.**
A worker that goes slow, gets descheduled or is partitioned loses its lease; the coordinator reclaims the task and dispatches attempt 2 — and attempt 1 is *still running*, because nothing can reach out and stop it.
Two workers executing one task simultaneously is this system working correctly. Any design claiming otherwise is either lying or paying for a distributed lock it hasn't mentioned.

**Committed effects are unique, and that's what `(taskID, attempt)` buys.**
Each attempt writes to its own directory, `<workerDir>/<taskID>/<attempt>/`, and reports back the pair it ran as.
The state machine accepts a result **only if the attempt it names is still the live one**.
A late report from a reclaimed attempt names a dead attempt, so it's a deterministic no-op on every replica — not an error, not a conflict, just ignored identically everywhere.
Exactly one attempt's result is ever committed, even though two executions happened.
Across the 64-seed checkpoint that path fired **5,071 times**.

**External side effects are at-least-once, full stop.**
If a payload posts to a third-party API or writes to a database the coordinator doesn't know about, that happens once per *execution* — twice if two attempts run.
Nothing here prevents it: those writes are outside the result tree, and the coordinator doesn't learn an attempt exists until it reports.
Preventing it needs **fencing** — the payload carrying a monotonic token the downstream system rejects when stale — which this system doesn't implement and doesn't claim.
What it does instead is pass `FTC_TASK_ID` and `FTC_ATTEMPT` into the executor's environment, so a payload needing single-execution semantics has what it needs to be idempotent itself.

### The caller's half: idempotency keys

`SubmitJob` takes `(clientID, requestID)` and the coordinator remembers the response it produced, which is what makes submission safe to retry through a failover.
Two rules govern the key, both the caller's: it must be **unique across clients and restarts**, and **derived from the work item rather than from who sends it**.

The first rule has teeth. A per-process counter reissues `job-1` after a restart, and the coordinator faithfully returns the *first* `job-1`'s answer — so the submission silently creates nothing, which looks exactly like the cluster dropping work.
My own load generator did this, and it cost 138 benchmark trials before I found the cause.

So the server now defends it as far as it can: each dedup session stores a **hash of the payload**, and a reused key carrying *different* work is rejected with `InvalidArgument` rather than silently answered.
The hash covers the task specs and priority but not job ids, task ids or timestamps, which are leader-assigned per proposal — that exclusion is what lets a genuine retry, arriving with a fresh job id, still replay correctly.
It is a new log command version, and the guard keys off what the **entry** declares, never what the running binary supports, so older entries replay under the old behaviour forever.
The full contract is in the [README](../README.md#idempotency-keys-the-callers-half-of-the-contract).

---

## 4. What the verification proves, and what it doesn't

The verification is the most valuable thing here, which is exactly why its limits need stating rather than leaving to be discovered.

The headline: 64 seeds × 600 steps, 2h47m of cumulative run time, **1,770 coordinator kills, 1,770 restarts, 7,986 partitions**, and **128,830 committed effects over 151,161 executions with zero duplicate commits and zero lost tasks** across the 63 runs that drained to quiescence.

### Coverage is the load-bearing number, not the verdict

"Zero violations" is only evidence about code that *executed*.
Early on every run came back green while the entire retry path was unreachable, because the workload never failed a task.
So each run reports what it reached, and a claim with a zero in its coverage column is **INCONCLUSIVE rather than a pass** — which is why the checkpoint quotes 308 *genuinely overlapping executions* and 558 *stale rejections actually discarded*, rather than asserting that idempotency works.

The same discipline produces the most counterintuitive line in the report: the checkpoint's 64 linearizability histories are clean, and **0 of 64 covered**.
Under the shipped configuration CheckQuorum steps a cut-off leader down in about one election timeout, so the split-brain read window never opens by chance — and 296,674 operations of healthy-cluster history prove nothing, because a healthy cluster is trivially linearizable.

The evidence is the **targeted** sweeps, where the window is injected rather than awaited:

| adversary | read mode | Illegal | covered |
|---|---|---|---|
| pinned (CheckQuorum off) | `local` | **8/8** | 8/8 |
| pinned | `readindex` | **0/8** | 8/8 |
| shipped + process-pause fault | `local` | **8/8** | 8/8 |
| shipped + process-pause fault | `readindex` | **0/8** | 8/8 |

Two clean A/Bs: same adversary, same coverage, opposite verdicts, read mode the only variable.
And the distinction those rows exist to make: **CheckQuorum narrows the stale-read window; `ReadIndex` closes it.** A cut-off leader steps down after ~230ms while a successor needs a randomized 250–500ms, so the intervals overlap at their edges — measured as 1/12 Illegal on the shipped configuration with no pause fault at all, at a wider step count. `ReadIndex` makes a cut-off leader **fail rather than lie**: `Unavailable`, never a stale `NotFound`.

### What the harness cannot see

**This is seeded fault injection, not deterministic simulation testing.**
Faults land **between steps, not mid-apply** — the state machine has no hook to pause inside it.
Time is compressed, not virtualized, so runs are host-sensitive; every checkpoint brackets itself with fsync and CPU probes and refuses to quote rates from a run whose host broke the premise.
The simulator **doesn't run the worker binary** — it calls the gRPC service directly, so the real heartbeat loop, long-poll backoff and subprocess executor are covered by unit tests only.
And it never kills below quorum: a cluster that can't commit stops testing anything.

**Porcupine cannot prove linearizability**; it builds confidence that an observed history isn't obviously violating it. 12% of operations in those histories were ambiguous — timed out, or returned without a definitive answer — and are recorded as PENDING rather than dropped, since dropping them is how a failing history quietly becomes a passing one.

### The bugs it found

The distinction between *found* and *targeted* matters: the stale read was **targeted**, documented as deliberate before Porcupine existed, because a checker that has never caught anything is indistinguishable from one that cannot.

**An unreported `MsgSnap` parked a follower forever** — the best bug in the project.
etcd/raft puts a follower in `StateSnapshot` and pauses *every* log probe to it until the application calls `Node.ReportSnapshot`. Nothing in this build ever called it, in either transport; the sender swallowed its error under the comment "peer down or partitioned; raft's own protocol retries", which is true of `MsgApp` and false of `MsgSnap`.
It is silent in the worst possible way: the parked follower keeps answering heartbeats, so `RecentActive` stays true, CheckQuorum is satisfied and the leader is never deposed — it just holds its term and commits nothing.
Reproduced deterministically from a single dropped message, fixed below both transports, and the frozen-clock symptom went from 4 of 64 seeds to 0 of 64 under the unchanged adversary. The original five windows stay formally unattributed, because they predate the progress-table instrumentation that would have shown a parked follower.

Two more turned up on the simulator's **first day** — a deadlock between the Ready loop and the scheduler on leadership revocation, and a publish-before-run race on `c.raft` — and both are worth reading for how they hid rather than for what they were: the deadlock self-healed after a 5s proposal timeout, so its symptom was not a hang but a node processing no raft messages for five seconds on every leadership change; the race needed a node restart to open its window, and the cluster tests start each node exactly once. Both are in the [README](../README.md#what-the-harness-found).

**An O(state) scan under the apply lock**, found by a pprof profile during benchmarking rather than by the simulator.
A worker's long poll called `heldTask`, which sorted every task id in the store to answer a question about one worker — 5.19ms per call at 50,000 tasks, under the apply lock, on every pass of every poll.
At 10,000 tasks that produced Ready-loop stalls, 7–9 leadership changes per 30-second window and commit latency of 322–696ms, at **18% CPU** with 59% of samples in mutex wait, which is what said this was not a CPU problem.
An index maintained inside `Apply` took the call to a flat 72ns.
The rule is "no O(state) work under the apply lock on an RPC path", but the better lesson is the second one: my property test for that index shared a blind spot with the index itself, since both asked "which tasks have `WorkerID == w`" and neither asked "and are they still running".
An oracle written from the same mental model as the code inherits its errors.

### What I'd lead with

Correct and stall-free to at least 150,000 tasks, usefully fast to about 50,000.
Failover is ~1.4s p50 on the shipped configuration, and that figure is **bounded by log compaction, not made safe by it**: a restarting node applies its backlog inside the Ready loop and can neither vote nor acknowledge for the whole window — 6.3 seconds for 23,500 entries with compaction disabled.
Snapshots are synchronous and expose the Ready loop to disk tail latency.
Every "max" is an observed worst case from a finite trial count on one host, never a bound.
One simulator red remains attributed to host degradation by rate comparison, with no mechanism found and none claimed.

A system whose limits you can state precisely is worth more than one whose author believes it has none.
