# Fault-Tolerant Coordinator — v2: Build Above Raft

**What this version is.**
You do not implement Raft.
You run consensus on [etcd/raft](https://github.com/etcd-io/raft) and spend the whole budget on the two things that actually differentiate: a **coordinator architecture modeled on Nomad's control plane**, and a **verification harness** that produces evidence the system is correct under failure.

**Why "coordinator" and not "task scheduler."**
Scheduling is one thing this control plane does. What you're actually building is the general machinery — a replicated state machine, lease-based failure detection, idempotent dispatch, derived-not-replicated working state — and task scheduling is the first workload run on top of it. Naming it accurately matters, because [v3](distributed-inference-gateway-roadmap-v3.md) reuses that machinery for a completely different workload, and the reuse is the point.

**Why not implement Raft.**
Nobody implements Raft in production.
[etcd/raft](https://github.com/etcd-io/raft) alone powers etcd, Kubernetes, Docker Swarm, CockroachDB, TiDB, Calico, Flannel, and Hyperledger — "tens of thousands of clusters each day."
hashicorp/raft powers Consul, Nomad, and Vault.
The industry wrote consensus seriously about a dozen times, ever, and then imported it.
Writing it yourself is a library-authoring exercise; the distributed-systems *engineering* happens above it.

"Implemented Raft" is a credential, and one every 6.824 student has.
"Here is the evidence my system stays linearizable under partition, and here is the seed to reproduce any failure" is evidence, and it's rare.
Under a time constraint, evidence wins.

---

## Budget

The invariant is **hours, not days: ~84 hours across 12 working days.**

| Track | Pace | Calendar |
|---|---|---|
| **A — two focused weeks** | 12 days × 7 hrs | ~2 weeks |
| **B — part-time** | 15 hrs/week | ~5.5 weeks |

Days are checkpoint boundaries, not calendar obligations.

---

## The shape of it

```
Day 0    Day 1          Days 2–7          Days 8–10        Days 11–12
prep  →  etcd/raft   →  the coordinator →  verification  →  numbers + writeup
         (the Ready      (Nomad-shaped     (faults, seeds,   (the evidence)
          contract)       control plane)    Porcupine)
```

The phases each answer a different interview question.
Phase 1: *can you host a consensus module correctly?*
Phase 2: *can you design a system on top of it?*
Phase 3: *how do you know it's correct?*
Most candidates can gesture at the first. Very few have anything for the third.

---

## The scope contract

**In scope:**
- Consensus on etcd/raft — you implement the storage, transport, and apply contract it requires.
- A Raft-replicated coordinator: job and task state survive leader death, nothing lost, nothing duplicated.
- **Nomad-shaped scheduling**: evaluations, an eval broker, plans verified under optimistic concurrency before commit.
- Independent tasks (a job fans out to N tasks, no ordering between them).
- Worker pool with registration, leases, gRPC heartbeat failure detection.
- At-least-once dispatch, idempotent execution keyed on `(taskID, attempt)`.
- Client retry with `(clientID, requestID)` dedup.
- Snapshots and log truncation.
- **A verification harness**: seeded fault injection plus Porcupine linearizability checking.
- Prometheus metrics and benchmarks.

**Out of scope, deliberately:**
- **Implementing Raft.** You read the paper and answered the four questions below; that's the understanding this project needs.
- **DAG dependencies between tasks.** The biggest scope trap; roughly doubles coordinator complexity.
- **Cluster membership changes.** Fixed cluster, configured at startup.
- **Full deterministic simulation testing.** See Day 8 — Go fights you on this, and overclaiming it is worse than not doing it.
- Sharding, web UI, multi-tenancy, auth, quotas.

---

## Day 0 — Prep (4 hrs) ✅

- [The Raft paper](https://raft.github.io/raft.pdf): §5 closely (election, replication, safety). Skim §6 (membership) and §7 (snapshots). ✅
- **Figure 2 is the spec.** Print it. ✅
- [The Secret Lives of Data](https://thesecretlivesofdata.com/raft/) — 10 minutes, and it makes §5 stick. ✅
- [Part 0](https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/) of the series — concepts and the layering (consensus module / log / state machine). ✅
- Clone [github.com/eliben/raft](https://github.com/eliben/raft) and run `go test -race ./...` in `part1/`. ✅
- Read the [etcd/raft README](https://github.com/etcd-io/raft/blob/main/README.md), specifically the `Ready` loop contract. That's Day 1's material.

**Answer these in writing before you code.** ✅

1. A leader appends an entry, replicates it to a majority, and dies before telling anyone it committed. Is the entry committed? What does the next leader do with it?
- yes. The next leader (which my have that entry by election restriction, up-to-date ness), commits it implicitly as it commits an entry from its own term (RPC does the work getting all followers up-to-date)
  - *Two sharpenings.* It's **must** have, not may — the election restriction guarantees it, because the entry is on a majority and no candidate wins without a log at least as up-to-date as a majority. And the mechanism isn't the RPC repair work; it's the **`prevLogIndex`/`prevLogTerm` consistency check** (Log Matching). Any follower that accepted the current-term entry above it provably holds the identical entry below. So one `commitIndex` advance sweeps it in — commitment needs a majority, not all followers. Repair of laggards happens afterward, on its own schedule.
2. Why can a new leader not commit an entry from a *previous* term merely because it sits on a majority? (§5.4.2 — this is the one that catches people, and the one Figure 8 exists to explain.)
- an entry from a prev term sitting on a majority can still be overwritten
  - *Add the mechanism:* up-to-dateness compares `(lastLogTerm, lastLogIndex)` with **term first**, so one node holding a higher-term entry at that index out-votes the entire majority holding the older one.
3. Why are election timeouts randomized? What happens if they aren't?
- solves the liveness problem: split votes (if same timeouts, all followers become candidates and vote for themselves)
  - *Safety comes from the voting rule* (one vote per server per term, majorities intersect); randomization is purely liveness.
4. Two nodes on opposite sides of a partition both believe they lead. Why can only one make progress?
- committing requires replicating to a majority, and only one side of a parition can hold a majority
  - *Consequence to carry forward:* the minority leader can't **commit**, but it will happily serve **reads** from local state. That's a linearizability violation, and Day 9's Porcupine harness exists to catch exactly it.

---

## Phase 1 — Hosting consensus (Day 1, ~7 hrs)

### Day 1 — etcd/raft (7 hrs) ✅

**Why etcd/raft rather than hashicorp/raft.**
etcd/raft is deliberately minimal: it implements the algorithm and hands you a `Ready` struct — *you* implement persistence, transport, and application.
That's the real contract Raft has with its host system, and implementing it teaches you the operationally relevant half.
hashicorp/raft hides exactly that behind a convenience interface.

The loop you must get right:

```go
for {
    select {
    case <-ticker.C:
        node.Tick()                       // drives election + heartbeat timeouts
    case rd := <-node.Ready():
        saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)  // 1. PERSIST
        send(rd.Messages)                                     // 2. then send
        applyToStateMachine(rd.CommittedEntries)              // 3. then apply
        node.Advance()                                        // 4. ask for more
    }
}
```

**Persist before you send.** That ordering *is* the durability guarantee — a message implying "I have this entry" must never precede the entry hitting disk, or a crash makes your node a liar and safety is gone. Getting to implement this by hand is the reason for the library choice.

Note also that `ElectionTick`/`HeartbeatTick` are counted in **ticks, not durations** — your ticker interval defines the time unit.

**Build a replicated counter first.** Three nodes, propose increments, all converge, kill one and restart it. Roughly 200 lines. Prove the wiring works before the coordinator makes failures ambiguous.

**Checkpoint:** 3-node counter converges under leader kill and restart-from-disk.

---

## Architecture

```
   CLI / client              Coordinator cluster (3 or 5 nodes)              Workers (N)
 ┌────────────┐        ┌──────────────────────────────────────────┐       ┌──────────┐
 │ submit job │─gRPC──▶│ LEADER ONLY                              │◀─gRPC─│ worker 1 │
 │ (clientID, │        │  ┌────────────┐   ┌───────────┐          │ pull  │ worker 2 │
 │  requestID)│◀NOT_   │  │ EvalBroker │──▶│ scheduler │──plan──┐ │  +hb  │ worker 3 │
 └────────────┘ LEADER │  │ (priority) │   │ workers×N │        │ │       └──────────┘
                +addr  │  └────────────┘   └───────────┘        ▼ │
                       │        ▲          (read snapshot)  ┌────────┐
                       │        │                           │Planner │ optimistic
                       │   evaluations                      │(serial)│ concurrency
                       │        │                           └───┬────┘
                       │  ┌─────┴──────────────────────────┐    │ valid plans
                       │  │  StateStore  ◀── apply ────────┼────┘
                       │  └────────────────────────────────┘
                       │            ▲ CommittedEntries            │
                       │      ┌─────┴──────┐                      │
                       │      │ etcd/raft  │──── replicate ───────┼──▶ followers
                       │      └─────┬──────┘                      │
                       └────────────┼─────────────────────────────┘
                                    ▼  WAL + snapshots on disk
```

**Pull, not push.** Workers long-poll for work; the coordinator never dials out. This removes address tracking, reconnection, and NAT handling, and gives backpressure for free. Heartbeats flow worker→coordinator, which is what failure detection reads.

**Every state mutation goes through Raft.** The command set stays small and closed:

```
SubmitJob{clientID, requestID, jobID, tasks[]}
CreateEval{evalID, jobID, priority, triggeredBy}
ApplyPlan{evalID, assignments[], stops[]}
CompleteTask{taskID, attempt, workerID, resultRef, exitCode}
FailTask{taskID, attempt, workerID, err, retryable}
RegisterWorker{workerID}
Tick{nowMs}
```

### The detail worth more than the rest of the build

**No wall-clock reads in the apply path.**
`applyToStateMachine` runs on *every* replica and must be deterministic.
`time.Now()` in there silently diverges state across nodes, and the divergence stays invisible until a leader change makes a follower's fantasy authoritative.

Instead the leader proposes `Tick{nowMs}` every ~200ms, and lease expiry uses the timestamp **carried in the log entry**.
Every replica expires the same leases at the same log position.

This is the class of bug determinism requirements create, most people have never had to reason about it, and it's the sentence that earns follow-up questions.

---

## Phase 2 — The coordinator (Days 2–7, ~40 hrs) ⭐️

### Day 2 — Protobuf and the pure state machine (7 hrs)  ✅
`proto/scheduler.proto`: `SubmitJob`, `GetJob`, `ListJobs`, `RegisterWorker`, `RequestTask`, `ReportResult`, `Heartbeat`. Generate with [protoc-gen-go-grpc](https://grpc.io/docs/languages/go/quickstart/).
Layout: `cmd/{coordinator,worker,schedctl,loadgen,simulator}`, `internal/{state,sched,server,worker,client}`.
Build the `StateStore` as a **pure, single-node, in-memory** state machine over the command set — no Raft, no network. Jobs, tasks, workers, evaluations.
Unit-test it directly: submit, evaluate, assign, complete, expire a lease, exhaust retries.

**Why state-machine-first.** Every state-transition bug found here costs 10× to find through a 3-node cluster. Highest-leverage day of the phase.

### Day 3 — Single-node end to end (7 hrs) ✅
gRPC server over the state machine. Worker binary: register, long-poll `RequestTask`, execute, renew lease via `Heartbeat`, `ReportResult`.
Executor: subprocess with timeout, captured stdout/stderr, exit code.
**Task output does not go in the Raft log** — write it to the worker's disk and put only a reference plus exit code in the log. Keeping the replicated log small and replicating references rather than payloads is a real production pattern and a deliberate decision worth documenting.
`schedctl` with `submit` / `status`.
(`watch` is deferred — polling `status` demos identically and Day 3 is the fullest day in the phase.)

**Four things to settle before the fun part.**
Each is small, each is load-bearing, and three of them are Day 6 seams that cost 10× to retrofit.

1. **Serialize the apply path — today, not on Day 6.**
`StateStore` has no lock and is correct only under a single writer.
A gRPC server puts N concurrent handler goroutines in front of it, and `-race` will find that the moment 100 tasks run across 3 workers.
Build one `propose(cmd) (Result, error)` that every handler funnels through — reads included, since they touch the same maps.
That function *is* the Day 6 seam: its body becomes `node.Propose()` and no caller moves.

2. **Build the `Tick` proposer now.**
Day 3's checkpoint depends on lease expiry, and lease expiry is driven only by `Tick`.
A goroutine calling `propose(Tick{now})` every 200ms is enough; Day 6 relocates it under `establishLeadership` without changing it.

3. **Decide what produces `TASK_STATE_RUNNING`.**
Nothing does yet, and the command set is deliberately closed — so `RequestTask` must not mutate.
The answer already exists in the schema: `HeartbeatRequest.running_task_ids`, which `applyRegisterWorker` currently ignores.
Promote `ASSIGNED → RUNNING` for the tasks a heartbeat names, and registration, renewal, and acknowledgement stay one idempotent path.
Until something writes `RUNNING`, Day 10's "no task `RUNNING` on two workers" invariant is vacuous.

4. **Fix an ID-generation scheme and write it down.**
Every ID is assigned by the leader before proposing, which means a re-propose after an ambiguous outcome must produce the *same* ID or idempotency evaporates — `applyCreateEval` is a no-op only on a repeated `eval_id`.
Derive eval IDs deterministically from what triggered them, not from a random UUID.
`SubmitJob` is already covered by `(clientID, requestID)`; nothing else is.

**The assignment placeholder — the seam that keeps Day 4 honest.**
The checkpoint needs tasks to reach workers, but the scheduler — eval broker, scheduler workers, planner — isn't until Day 4. So today you build the one primitive underneath all of it and nothing more: *task, meet worker.* The discipline is **build the assignment mechanism, not the assignment policy.** The mechanism is "produce an `ApplyPlan` and apply it." The policy — which task, which worker, in what order, respecting priority and conflicts — is Day 4.

Concretely: one deletable function, `assign(workerID) (*ApplyPlan, bool)`, that scans the state store for the first `PENDING` task over **sorted** IDs (deterministic, reusing your Day 2 discipline) and returns a one-assignment `ApplyPlan{taskID, workerID, attempt: task.Attempt+1, leaseExpiryMs: now+TTL}`. Grab-the-next-pending-one. That is the entire scheduler today. (Everything applies directly to the single-node state machine — Raft isn't wired until Day 6, so "apply the plan" is a direct `Apply` call for now.)

Structure `RequestTask` in two clauses, because the shape is the seam: **(1)** return any task already `ASSIGNED` to this worker — a pure read; **(2)** only if there's none, call `assign()`, apply the plan, return the new task. On Day 3 clause (1) is almost always empty and the tail does the work. On Day 4 you *delete the tail*: the planner produces `ASSIGNED` tasks proactively, and the handler keeps only its read half. The seam is a branch you remove, not a handler you rewrite.

Get one detail right now, because it's mechanism and Day 4 shouldn't retrofit it: **the attempt number is `task.Attempt+1`.** That's what `applyPlan` validates, and it's what makes a reclaimed task's next dispatch distinguishable from a slow worker's stale report.

Three things **not** to build — each is a shabby Day-4 component you'd otherwise have to detect and tear out:
- **No pending-work queue outside the state machine.** The source of truth for "what's pending" is the `StateStore`; the placeholder *scans* it. A mutable side list the handler pops from is a wrong proto-eval-broker — the real broker is derived from committed state and rebuilt on leadership change.
- **No policy.** No priority ordering, no conflict checking beyond what `applyPlan` already does, no batching several assignments to reason about together.
- **No refereeing.** Let `applyPlan` be the backstop: if two workers race for the same task, the second `ApplyPlan` fails its `PENDING` check and that worker just polls again. The placeholder is allowed to be naive precisely because the state machine already rejects an invalid assignment.

**The seam test:** on Day 4 you must be able to replace `assign()`'s body with the eval-broker → scheduler → planner pipeline, delete its call from the poll path, and leave the worker binary, the executor, the `ApplyPlan`/`applyPlan` command, and the proto surface untouched — with every checkpoint below still passing. If wiring the planner forces anything more than that, you built too much today.

**Checkpoint:** 100 tasks across 3 workers complete. `SIGKILL` a worker mid-task; its tasks reschedule after lease expiry and finish. **Demoable — commit and tag.** Tag it before Day 4, so "the naive scheduler worked end to end" stays a recoverable point in history.

### Day 4 — Nomad-shaped scheduling (7 hrs) ⭐️ ✅

This is the day that separates a coordinator from a distributed queue — and, placed right, it's a **slot-in at the `assign()` seam**, not a rewrite. Day 3 left you one deletable function that scanned committed state for a `PENDING` task and emitted a one-assignment `ApplyPlan` reactively, inside the poll path. Today you replace it with a pipeline that does the same job with structure and produces assignments *proactively*: evals name what changed, scheduler workers decide on snapshots in parallel, and a planner verifies and batches before committing. The control flow inverts — assignment stops happening on the worker's poll and starts happening when an eval fires — but the worker, the executor, and the command layer don't move, and `RequestTask` simply sheds its Day-3 tail and becomes the pure read it always should have been.

Model it on [Nomad's control plane](https://deepwiki.com/hashicorp/nomad/2-server-(control-plane)).

- **Evaluations.** State changes don't assign work directly — they create an `Evaluation` ("something changed, someone should think about this job"). Job submitted, worker died, task failed: all produce evals. These *are* the `EvalTrigger`s your Day 2 `Apply` already returns; the leader turns each into a `CreateEval` command. (One command path still — a Raft entry once Day 6 wires it.)
- **EvalBroker.** Leader-only, in-memory, priority-ordered queue of pending evals. **Not replicated** — it's *derived* from committed state, and rebuilt by scanning the state store on leadership acquisition. This is what the placeholder's "scan for the first PENDING task" grows into: same source of truth, now ordered and structured instead of a linear sweep. Understanding why derived-not-replicated is correct here is worth an interview answer on its own, and it's the idea v3 reuses wholesale.
- **Scheduler workers.** N goroutines dequeue evals, read a **snapshot** of state, and compute a `Plan{assignments, stops}`. These run concurrently and touch nothing shared — which is exactly why the deciding can be parallel now, where the placeholder's was inline on the poll.
- **Planner.** Single-threaded, serializing. Verifies each plan against *current* state — worker already busy, task already assigned, lease already expired — and accepts, partially accepts, or rejects. Only accepted plans commit as one `ApplyPlan`. This is the referee role you deliberately left to `applyPlan`'s validation on Day 3, now promoted to a real optimistic-concurrency check *before* the command is proposed.

**Build `StateStore.Snapshot()` first.**
Scheduler workers need a consistent point-in-time read they can hold while deciding, and the current `Task`/`Job` accessors give per-object copies, not a coherent view.
This is the same primitive Day 7 needs for state-machine serialization — write it once here and Day 7 gets materially cheaper.

**Why this shape.** Without it, every scheduling decision is a full Raft round-trip and you're pinned at hundreds per second by fsync. With it, the expensive part (deciding) runs in parallel on snapshots, and only the cheap verify-and-commit step serializes — and one `ApplyPlan` entry carries many assignments, amortizing fsync across them.

That gives you a real answer to *"how does this scale past one decision per consensus round-trip?"* and it gives Day 11 its best graph.

**Checkpoint:** a plan carrying 100 assignments commits as one entry. A deliberately stale plan (built against a snapshot, then invalidated before submission) is correctly rejected by the planner. **And the seam held:** Day 3's checkpoints still pass — the change was replacing `assign()` with the pipeline and deleting its call from `RequestTask`; the worker, executor, `ApplyPlan`/`applyPlan`, and proto surface didn't move.


### Day 5 — Delivery semantics (6 hrs) ✅
- `(clientID, requestID)` dedup in the state machine so a retried `SubmitJob` returns the original response. Bound the map — expire sessions on a TTL carried by `Tick`, or it grows forever.
- Retry policy: per-task attempt counter, capped retries, exponential backoff with jitter, terminal `FAILED`.
- **Where the backoff deadline lives.** Jitter is leader-only randomness baked into a plan, which is fine — but a `PENDING` task carrying a "not before" time has nowhere to be stored today.
If that deadline lives only in the leader's memory, failover loses it and the new leader redispatches immediately.
Add `not_before_ms` to `Task` (a new field number is always safe) or document the gap deliberately; decide before the log holds real data.
- **Idempotent execution keyed on `(taskID, attempt)`.** Results are written under `<taskID>/<attempt>/`, so each attempt leaves its own durable record and a re-run of the *same* attempt overwrites rather than appends.
This is Day 10's effect-ledger model, not an overwrite-under-taskID one: both readings appeared in this doc, and the ledger wins because it is the evidence Day 10's invariant checker needs.
The effect is singular not because only one artifact exists, but because only one attempt is ever live, so only one `result_ref` is ever committed.

**Checkpoint:** the same request submitted 5× concurrently yields exactly one job. A worker killed *after* doing the work but *before* `ReportResult` lands yields one observable result, not two.

### Day 6 — Wire in Raft (7 hrs) ✅
Replace direct state-machine calls with `node.Propose()`, and drive application from `rd.CommittedEntries` in the Ready loop.
Non-leader RPCs return gRPC `NOT_LEADER` plus the leader's address; client and worker redirect.
Leader-only components start on election and stop on loss — eval broker, scheduler workers, planner, `Tick` proposer. Nomad calls these `establishLeadership`/`revokeLeadership`, and stopping cleanly is what prevents split-brain scheduling.

**Carry the broker's wake-up triggers across intact.**
Day 4's scheduling only makes progress because two things outside the eval
path poke the broker: a task reaching a terminal state frees a slot, and a
worker being *admitted* (newly registered or revived from `LOST`, not merely
heartbeating) adds one. Both live in `afterApply` today, which Day 6 moves
into the Ready loop so it runs on committed entries.
If either is dropped in the move, nothing crashes — the cluster just stops
scheduling once the first eval blocks, which a short smoke test will not
catch. Add a failover test that submits more tasks than slots and asserts the
job still drains after the leader is killed.

**Budget an hour for promoting the spike.**
`spike/counter/` is its own Go module and the main `go.mod` has no `etcd/raft` dependency, so the WAL, the transport, and the `Ready` loop have to move into the product module and get their first tests.
The "swap, not a rewrite" framing is true of the *product* code; this plumbing is real work the day has to pay for.

**This is a swap, not a rewrite** — but only because Days 2–5 routed every mutation through one command path. If state is being mutated anywhere else, fix that first.

**Checkpoint:** `SIGKILL` the leader mid-flight; a new leader rebuilds its eval broker from state and continues, nothing lost or duplicated. Restart the dead node; it catches up and rejoins.

### Day 7 — Snapshots, restart, schema evolution (6 hrs) ✅
- **Snapshots.** etcd/raft handles the `InstallSnapshot` protocol; you implement state-machine serialization, `ApplySnapshot` into storage, and log compaction via `MemoryStorage.Compact`. Much cheaper than writing §7 yourself, and it stops being optional — an always-on coordinator's log grows forever and restart replay grows with it.
- **Log schema versioning.** The problem no demo ever hits: once real data is in your log, changing a command struct breaks replay on restart, because the log holds yesterday's encoding. Version your commands and keep old decoders. It's not hard, but it only shows up when the system has a past — and having solved it reads as having *operated* something.

**Checkpoint:** kill all three nodes, restart from disk, state is intact. Force a low snapshot threshold so `Restore` is genuinely exercised. Add a field to a command, replay an old log, and watch it not break.

---

## Phase 3 — Verification (Days 8–10, ~20 hrs)

The part almost nobody has.

**Why it's a whole phase.** Consider the §5.4.2 bug from question 2 — a leader committing prior-term entries by counting replicas. That's a genuine safety violation, and it can survive *hundreds* of green random test runs, because Figure 8 needs a specific interleaving of crashes and elections that random scheduling rarely produces. A test suite that passes tells you very little about a distributed system. Seeded fault injection is how you go looking on purpose.

### Day 8 — Seeded fault injection (7 hrs) ✅

Build `cmd/simulator`: all coordinator nodes in one process, with the chaotic parts under your control.

- **One seeded RNG.** `SCHED_SEED` env var; every random decision draws from it. Print the seed on every run and on every failure.
- **Injected clock.** A `Clock` interface everywhere — no bare `time.Now()` outside it. You already have this discipline from the `Tick` design.
- **Simulated network.** A transport shim between nodes with configurable drop rate, delay distribution, and partition sets. Cleanest hook is a custom dialer or a gRPC interceptor.
- **Fault schedule from the seed.** Kill nodes, partition halves, delay messages, pause a process mid-apply — all drawn from the seeded RNG so a given seed replays the same *schedule*.
- **Assert invariants after every step**, then print the seed on failure.

**State this honestly, because it's a real limit.** This is **seeded fault injection, not deterministic simulation testing.** [Go is a genuinely hard language for real DST](https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html) — blocking I/O and goroutine scheduling stay nondeterministic; Polar Signals had to compile to WASM and fork the runtime to get there. A seed reproduces the fault *schedule*, not byte-identical execution. Say exactly that in the README. Precision about what your testing does and doesn't guarantee is itself the signal; overclaiming DST would be worse than not doing it.

For context on why this matters: FoundationDB pioneered real DST, and Kyle Kingsbury declined to Jepsen-test them because their simulator already stressed the system harder than Jepsen could.

**Checkpoint:** a failing seed reproduces the same fault schedule on rerun.

### Day 8 — completed

**Built.**
`internal/clock` — injectable wall time for the four DRIVER paths (raft ticker, `Tick` proposer, long-poll deadlines, worker heartbeats). The apply path had no clock before this and still does not; that property is what makes replicas deterministic and does not depend on this package.
`internal/simnet` — in-memory raft transport with seeded drop / delay / partition / corruption. Still marshals and unmarshals, so the drop-on-corrupt branch is real. Corruption defaults to **link integrity**: a mangled frame is dropped before delivery, as a failed TCP checksum would.
`internal/sim` — fault schedule from one seed; two-tier invariants (mid-chaos every step, convergence once at the end); coverage reporting; `MinComparisons` with INCONCLUSIVE as a distinct outcome; Ready-loop stalls as violations; frozen-clock streak invariant.
`cmd/simulator` — `-seed`, `-runs`, `-steps`, `-drop-rate`, `-corrupt-rate`, `-deliver-corrupt`, `-failure-rate`, `-v`.

**Product defects found, with fixes.**
- *Leadership-revocation deadlock* — `onLeadershipChanged` runs on the Ready-loop goroutine and waited for scheduler workers that were waiting on proposals only that loop could deliver. Self-healed after the proposal timeout, so the symptom was an unexplained election, not a hang. **Fix:** release pending waiters before waiting for the scheduler.
- *Publish-before-run race* — `StartRaft` launched the Ready loop, then assigned `c.raft`, which the loop immediately read. **Fix:** `raftnode.New`/`Run` separate construction from running.
- *Teardown race* — `StopRaft` nil'd `c.raft` while the ticker read it mid-propose. **Fix:** do not nil it; a stopped node already reports `IsLeader()==false`.
- *Incomplete ticker-panic fix* — `node.Propose` returning its own context deadline was a different error value for the same situation, and panicked the process. **Fix:** classify propose timeouts at the source, and invert the ticker to **deny-by-default** — non-fatal unless the error is on an explicit allowlist (`state.ErrMalformedCommand`).

**Harness bugs found.** Convergence compared replicas at mismatched applied indices (reported divergence on every healthy run); drop metric folded switched-off-node drops into random drops (33% reported against 2% configured); the workload drew from the fault RNG, so a timing-dependent number of draws broke schedule reproducibility; `processingSince` cleared only on the happy path, producing phantom growing stalls on halted nodes; corrupt accounting compared sends against discards, showing a phantom leak; `dump()` omitted sessions, leaving the determinism test blind to dedup; the `MsgHup` experiment compared `node.Status().Term`, a `*uint64`, so it compared pointer addresses.

**Claims retracted.** The `MsgHup` "injection" is **not a defect**. etcd/raft already drops local message types arriving over the network (`node.Step` returns early on `IsLocalMsg`), proven empirically — a node settled as leader at term 2 is fed 50 network `MsgHup` messages and stays at term 2. The transport check remains as **defense in depth**, rewritten from a non-empty-body/non-zero-`To` heuristic to: reject `raft.IsLocalMsg`, require `To == selfID`, require `From` to be a configured peer.

**Rules now in force.**
1. No `time.Now` in driver paths — everything goes through `internal/clock`. The apply path has no clock at all.
2. **Disk I/O sets a floor on clock compression.** `StallThreshold` must sit below the election timeout *and above* the snapshot fsync cost (23–40ms measured), so `ElectionTick × TickInterval` needs ≈4× that. A simulated cluster cannot outrun its own fsyncs.
3. Workload randomness is **derived by hashing `(seed, taskID, attempt)`**, never drawn from the fault RNG — a timing-dependent number of draws destroys schedule reproducibility.
4. **Correctness runs stay inside raft's fault model.** `-deliver-corrupt` is off by default and excluded from every claim; a run with nonzero `CorruptReachedStep` is reported INCONCLUSIVE.
5. State-machine `Policy` (session TTL, retry backoff) is **one per cluster**, identical on every replica; it is not a coordinator flag.
6. **An ambiguous outcome is recorded PENDING, never dropped.** A timeout, `ErrProposalLost`, or any error where the entry may still commit goes into the history as pending. Dropping them turns a failing history into a passing one — the ambiguity is precisely where a violation hides.

**Known gaps carried into Day 9.** Snapshot + WAL compaction blocks the Ready loop (23–40ms, grows with state; etcd does this asynchronously). The simulator does not run the worker binary — it calls the gRPC service directly, so the executor, heartbeat loop and local claim dedup are covered only by unit tests. `simnet` calls `node.Step` directly, so the transport guard (`IsLocalMsg` / `To == self` / `From` is a peer) **never runs in the simulator** and is pinned only by `raftnode` unit tests; the follow-up is to move it out of the HTTP handler into `raftnode`'s receive path so both transports share one guard. `proposalTimeout` is 5s against a 1s election timeout; tightening to ≈2× is deferred. Undetectable corruption is out of scope.

**Checkpoint met:** a failing seed reproduces the same fault schedule — `simulator -seed 1234 -v` prints an identical kill/restart/partition/heal sequence every run, and a different seed differs.
Note that the *particular* schedule quoted for seed 1234 is a pre-Day-9 one: Day 9 added a held leader-partition fault and changed the fault weights, so every seed now replays something different from what it replayed on Day 8.
A seed reproducing its own schedule is the claim; a seed reproducing the same schedule across a change to the adversary was never the claim.

**Day 9 entry point.** The target already exists and is deliberate: the leader answers reads from local state, and a minority-side leader keeps doing so — `simnet.Partition` produces exactly that. Model two operations, `SubmitJob` and `GetJob`, against a sequential spec whose state is the jobs map; `RequestTask` delivers a decision already committed and the worker RPCs are not client operations. Run the histories **under seeded faults**, never on a healthy cluster, which is trivially linearizable. Day 9 owes one decision — reads through the log, a `ReadIndex` barrier, or documented as explicitly non-linearizable — and whichever it is lands in `Coordinator.read`, which is why every read already funnels through that one function. Fuller version, with the design context, in [`architecture.md`](architecture.md) under "Day 9 entry point — Porcupine".

### Day 9 — Linearizability checking (7 hrs) ✅

[Porcupine](https://github.com/anishathalye/porcupine) — a Go linearizability checker used by etcd and TiDB, 1,000–10,000× faster than Jepsen's Knossos. [Integration is moderate](https://notes.eatonphil.com/2024-10-31-checking-linearizability-in-go.html): define a model, record a history, check it.

- **Model.** A sequential spec of your coordinator as executable Go: state is the jobs map; `SubmitJob` and `GetJob` step it.
- **History.** Record every client operation with invocation time and response time. Operations that time out or return ambiguously must be recorded as *pending*, not dropped — that ambiguity is exactly where violations hide.
- **Check.** `porcupine.CheckOperations`; on failure use `CheckOperationsVerbose` + `Visualize` for the HTML timeline.
- **Run it under Day 8's faults**, not on a healthy cluster. A healthy cluster is trivially linearizable.

**The specific bug to hunt: stale reads from a partitioned leader.** That's question 4 made concrete — if `GetJob` answers from local state on a minority-side leader, Porcupine will catch it. Decide deliberately whether reads go through the log, use a `ReadIndex` barrier, or are documented as explicitly non-linearizable.

**State the caveat:** Porcupine cannot *prove* linearizability. It builds confidence you aren't obviously violating it. Say that in the README too.

### Day 9 — completed

**Built.**
`internal/sim/linmodel.go` — the sequential spec, as executable Go.
State is the jobs map plus the `(clientID, requestID)` dedup bindings, encoded as two sorted delimited strings so it is comparable and immutable, which porcupine's backtracking search requires.
`SubmitJob` and `GetJob` are the whole model: `RequestTask` delivers a decision the log already committed, and the worker RPCs are the execution substrate reporting in, not callers.
`internal/sim/history.go` — the recorder, with ambiguity as a first-class outcome and the ambiguous SHARE reported on every run.
`internal/sim/linclient.go` — clients that address leadership CLAIMANTS rather than "the leader", paced off the fault-step counter.
`internal/raftnode/readindex.go` — the quorum read barrier.
`internal/server` — `ReadMode` (`local` | `readindex`), `Coordinator.readClient`, `ErrReadBarrier`.
`internal/raftnode` — `CheckQuorum` and `PreVote`, on by default; Ready-loop phase instrumentation.

**The finding: a partitioned leader serves stale local reads.**
Raft revokes leadership only when a higher term arrives, and a leader cut off from the majority never hears one — so it keeps its term, keeps passing its own `requireLeader` guard, and keeps answering `GetJob` from state the cluster has already moved past.

Proven two ways, and the order matters.
A **deterministic scenario** builds the sequence by hand and reproduces in 0.6s: partition the leader, wait for the majority to elect a successor, commit a job on the majority side, then ask the old leader about it.

```
split brain: node 2 (cut off) and node 3 both claim leadership
  Submit(pre)    -> job-dd4a…      Get(job-dd4a…) -> found       <- control
  Submit(during) -> job-2ffc…      Get(job-2ffc…) -> NOT_FOUND   <- the violation
                                   Get(job-2ffc…) -> found       <- majority side
Verdict: Illegal over 4 operations
```

The control read is what makes it unambiguous: the old leader answers correctly about everything it has and wrongly about everything committed after it was cut off.
The **seeded sweep** also finds it, on some seeds — 777, 778 and 779 all report Illegal under `local` — but not reliably, because the violation needs four things to line up in order and a short run often never rolls a leader partition at all.
A 25-step run reached a cut-off leader 68 times and still came back Ok.
The lesson is that the fuzzer was never the detector here; the 0.6s scenario is.

**The fix: `ReadIndex` closes it; `CheckQuorum` only narrows it.**
Under `ReadOnlySafe` the leader records its commit index, heartbeats, and receives a `ReadState` only once a majority acknowledges; the read is then served only after the local state machine has applied up to that index.
A cut-off leader never gets the acknowledgement, so the read **fails instead of lying** — `Unavailable`, never `NotFound`, because a refused read must not look like an answer.
It lands in `Coordinator.readClient`, split from `read()` so the unbarriered accessor survives for callers that need it: the invariant checker reading followers, and the worker RPCs reading state they are about to propose against, where the state machine re-checks the lease at apply time.
The barrier runs outside the apply lock, because holding it across a quorum round-trip would block the very heartbeats the barrier waits for.

`CheckQuorum` makes a leader that stops hearing from a majority step down after an election timeout — measured at **230ms against a 250ms timeout** — shrinking the window from *forever* to *one timeout*.
Reads served inside that window are still stale.
`PreVote` stops a node that was partitioned away from deposing a healthy leader on rejoin: the leader's term holds **2 → 2** with it and climbs **2 → 9** without it.
Neither makes reads linearizable, which is why they landed as a separate commit from the barrier — together, a clean Porcupine result could not have been attributed to either.

**The A/B, and why the adversary is pinned.**
`ReadLocal` is kept and kept tested, because the two modes are each other's evidence.
The negative control asserts the history is **Illegal** — that is a test of the CHECKER, not of a bug left in, since a harness that only ever runs against the fixed configuration cannot distinguish "correct" from "no longer looking", and both produce identical green output.
Its counterpart asserts **Ok** under `readindex` across four seeds, and includes a majority-side read that must come back FOUND: a barrier that refused every read would otherwise produce the same green over a history of nothing but refusals.

Both variants set `DisableCheckQuorum`, pinning the configuration the bug lived in.
The scenario's premise is a leader that still believes it leads, and with CheckQuorum on that premise RACES the step-down.
Racing it was tried: the control passed, then failed within twenty runs reporting `Ok` — the most dangerous failure available here, because a green control reads as "the bug is fixed" when it means "the harness stopped looking".
`DisableCheckQuorum` and `DisablePreVote` are expressed as negatives so the safe configuration is the zero value, are exposed by no CLI flag, and are pinned on by `TestShippedConfigEnablesCheckQuorumAndPreVote` in both `raftnode` and `sim` — the second because the gate sweep runs `sim.DefaultConfig`, and without it the gate could be clearing a configuration production does not run.

**Harness blindness found — seven ways the checker reported `Ok` and meant nothing.**
This is the substance of Day 9.
Every one of them produced a green result, and each is now a number printed next to the verdict rather than an assumption.

- *The history was a PREFIX, not the run.* Clock-paced clients recorded 11,000 operations and hit the cap in the first 9 seconds of a 63-second run, so the verdict covered the quiet early steps before most faults had fired. Pacing is now off the fault-step counter, so the history spans the schedule by construction.
- *Two thirds of operations were ambiguous for no reason.* They hit followers, which refuse, and a refusal is indistinguishable from one returned after a proposal. Clients now route to leadership claimants, which is what a real pool does and what keeps a stale leader in the target set.
- *ZERO operations ever reached a cut-off node.* `s.leader()` returns nil while two nodes claim leadership, so the workload stood down for exactly the window the bug lives in. A held leader-partition fault was added, with `Partitioned` and `SplitBrain` counters so an Ok over a healthy cluster can no longer pass for evidence.
- *`linBudget` was spent on an ATTEMPT, not an operation issued.* A client that found no leader consumed its budget anyway; on short steps the cluster is between leaders whenever the budget refreshes, so it burned in microseconds and the run recorded **zero** operations.
- *An empty history was reported as a search timeout* — "search did not finish in 20s over 0 operations", a sentence describing a search that never ran. Both are inconclusive; they have different causes and different fixes.
- *A below-quorum guard was a selection effect.* A frozen-clock violation fired on a cluster with no quorum — correct behaviour misreported — and the first fix stopped the ADVERSARY from entering that state. That narrows the fault space to quiet an invariant. Below quorum is a real fault state the claim must cover; the invariant is what had to learn that no quorum means no progress. Reverting it also IMPROVED coverage, since refusing partitions to protect quorum had been suppressing the faults the claim depends on.
- *Zero-coverage runs counted as passes.* A history that never reached a cut-off node or a split brain is green about a healthy cluster, which is trivially linearizable and needs no checker. Such runs are now INCONCLUSIVE on the MinComparisons precedent, and the sweep reports covered / inconclusive / illegal instead of passed / failed.

Two further defects fell out of the gating, both exposed by Day 9's own held-partition fault.
A **false divergence**: `raftnode` advanced its applied index after `Apply` returned and released the apply lock, so a sampler could read index N and encode state already containing N+1, and a detector comparing replicas only at shared indices was comparing different positions. `Handler.Apply` now carries the index and the coordinator records it in the same critical section as the mutation.
And **stalls that did not name their cause**: "a blocking call on the loop's own goroutine" names a class of bug, not a line of code. Ready batches now record their phase, which immediately refuted two of my own guesses — the real event was **3.42s in `persist`**, an fsync on a loaded host, not the snapshot compaction inferred from a low-threshold profile. `wal.Save` is one append plus one `Sync` per batch, the same as etcd, so the code was right and the INVARIANT was wrong to call a slow disk a concurrency bug. I/O phases now report INCONCLUSIVE with the timing premise they broke; logic phases still fail hard, which is what caught Day 8's revocation deadlock.

**Rules now in force.**
1. **An ambiguous outcome is recorded PENDING, never dropped**, and its SHARE is reported. An ambiguous operation constrains nothing, so a history that is mostly ambiguous checked very little regardless of its verdict.
2. **A verdict without coverage is not evidence.** A run that reached no fault cannot count toward "N histories, zero violations"; it is INCONCLUSIVE, which is a distinct outcome from a pass and from a violation.
3. **Never narrow the adversary to quiet an invariant.** Below quorum, corrupt frames and slow disks are real conditions; the checker is what must understand them. Excluding them from the fault space is a selection effect, not a fix.
4. **Every test asserts only its own property**, matched on violation kind. A comparison-floor test must not report "failed its own floor" over an illegal history, and nothing may be nondeterministically red.
5. **A stall must name its phase.** I/O-bound phases are the host; logic-bound phases are the bug the invariant exists to catch. Reporting them identically lets a real deadlock be dismissed as a slow disk.
6. **A floor is calibrated, not chosen.** "At least one" is the floor already rejected for `MinComparisons`; measure the rate, then set the minimum from it, and widen the sample if no stable floor exists.
7. **The negative control is permanent.** A checker that only runs against the fixed configuration cannot tell correct from no-longer-looking.

**Gate, on covered runs only.**
Five consecutive full-suite passes, every package, zero failures.
A 24-seed sweep under the shipped configuration: **20 covered and linearizable, 4 inconclusive, 0 NOT LINEARIZABLE**.
Across those 20 covered runs: 2,021 recorded operations, median 108 per run; ambiguous share median 15%, max 23%; 205 operations landed on a partitioned node, median 10 per run.
**And zero landed in a split-brain window — none, in any covered run.**
That is CheckQuorum doing its job: the cut-off leader steps down before the majority elects a successor, so two nodes no longer both claim leadership.
It also means **the sweep covers partitions, and the stale-read path is covered by the pinned deterministic scenario only**.
A sweep verdict under the shipped configuration is evidence that reads behave under partition; it is not evidence about the window the bug actually lived in, and it must not be quoted as though it were.
Under `local`, the pinned scenario still reports Illegal.

**State the caveat.** Porcupine cannot *prove* linearizability.
It builds confidence that an observed history is not obviously violating it, and that confidence is bounded by what the history covered — which is why coverage is printed next to every verdict.

**Day 10 entry point.**
The honest weakness to carry forward is that **the sweep no longer exercises the stale-read path at all**, and the numbers above say so: zero split-brain operations across twenty covered runs.
Three things follow, in this order.

**1. Tighten `Covered` to require a split-brain READ.**
It currently counts `partitioned || splitBrain`, which overstates what it measures now that CheckQuorum is on: an operation sent to a node that has already stepped down returns NOT_LEADER, which constrains nothing, yet marks the run covered.
A run should count toward the linearizability claim only when it contained at least one READ served by a node that believed it led while another node also did — the single shape that can produce a stale answer.
Everything else is a partition test, which is worth having under its own name and must not be banked as linearizability evidence.

**2. Build a targeted workload, and validate it by its ability to FAIL.**
Hold a leader partition, write through the majority, and read the job just committed from the cut-off node — the deterministic scenario's sequence, driven continuously under the seeded schedule rather than once by hand.
The acceptance criterion is not that it passes under `readindex`; it is that it finds Illegal under `local`.
A targeted workload that cannot reliably catch a bug that is definitely present is not a workload, and shipping one that comes back green on both modes would quietly retire the only check that covers this path.

**The validation must name its adversary, because under the shipped configuration the split-brain window may be empty BY CONSTRUCTION.**
CheckQuorum steps the cut-off leader down after about one election timeout — measured at 230ms — while etcd/raft randomizes a successor's election timeout over [250ms, 500ms), so the new leader almost always appears *after* the old one has already stopped believing it leads.
That, rather than bad luck, is the likely explanation for the zero split-brain count above, and it means a single catch rate would be meaningless without saying which cluster produced it.
Record three, separately:

- **Pinned adversary (`DisableCheckQuorum`).** Should find Illegal on most seeds. This is the configuration the bug lived in, and a workload that cannot catch it here is broken.
- **Shipped config plus a PROCESS-PAUSE fault.** Should also find Illegal under `local` — because a leader that is not ticking cannot run CheckQuorum. Freezing the cut-off leader's ticker for longer than an election timeout models a GC pause, a VM stall or a `SIGSTOP`, and it re-opens the window that CheckQuorum closes, since CheckQuorum is driven by a timer the paused process is not running. `ReadIndex` must still return Ok here, and the distinction is the whole point: the barrier needs a quorum ROUND-TRIP, not a local timer, so a paused leader cannot satisfy it no matter how long it sleeps.
- **Shipped config, no pause.** Record the rate honestly, and expect roughly zero. That is CheckQuorum working, not the workload failing, and writing it down is what stops a future reader concluding the targeted workload is broken.

Measure each rate and record it, the way the conclusive rate was measured before the sweep floor was set.

This needs **pause as a seeded fault type**, which also closes a gap carried since Day 8: the schedule kills, restarts, partitions, drops, delays and corrupts, but never pauses, and "pause a process mid-apply" has been on the deferred list since then.
Pausing the ticker is the cheap half and is enough for this; pausing inside the apply path still needs a hook the state machine does not have.

**The pause will trip our own invariants, so specify it before building it.**
A frozen leader is exactly what the Day 8 frozen-clock invariant was written to catch, and it cannot tell an injected pause from the silent tick-dropping it exists to detect.

- **Implement it as a TICKER FREEZE, not a blocked Ready loop.** Blocking the loop would also trip the stall detector, and would trip it in a logic phase, where a stall means "a blocking call on the loop's own goroutine" — the one reading that must stay trustworthy. Freezing the ticker models the same outcome (no heartbeats, no CheckQuorum, no `Tick` proposals) without lying to a detector about its cause.
- **A ticker freeze alone is NOT a pause, and would make `local` pass for the wrong reason.** `simnet` delivers by calling `node.Step` directly, so a node whose ticker is frozen still processes inbound raft messages: it receives the new leader's higher-term heartbeat, steps down DURING the pause, and no stale leader remains at wake. The run would then return Ok because the scenario never happened — the exact shape of failure this whole day was spent eliminating. So the pause must also **HOLD the paused node's inbound raft messages: buffer them, never drop them.** Dropping is a partition, which is a different fault the cluster already defends against; a real paused process has its packets queued by the kernel and delivered when it resumes, and queueing is what preserves the deposed-but-doesn't-know-it state the window depends on.
- **On wake, serve the pending client read BEFORE releasing the buffered messages.** That ordering IS the GC-pause window: the process is running again, has not yet drained the queue that would tell it it has been deposed, and answers from state it still believes is current. Releasing first would deliver the higher term, step it down, and close the window before the read.
- **Make the frozen-clock invariant pause-aware the way it is already partition-aware**, and bound the exemption. Exempt a node ONLY while the fault schedule says it is paused, and **FAIL if the clock is still frozen after the scheduled pause has ended**. An unbounded exemption would be the same mistake as the below-quorum guard in a different costume: an invariant quieted rather than taught. The bounded version is strictly stronger than what exists today, because "it thawed when it was supposed to" is an assertion the harness currently cannot make at all.
- **Use the realistic ordering for the validation**, which is also the classic bug: pause the leader, let the majority elect a successor and commit, then WAKE the old leader and read from it before its CheckQuorum check fires. A paused process cannot answer anything, so a read issued during the pause tests nothing; the dangerous window is the moment after it resumes and before it discovers it has been deposed. That is the GC-pause lease-read bug as it actually happens in production.
`ReadIndex` must still return Ok through all of it, and it has two independent ways to get there, which is the robustness worth demonstrating: if the buffered messages are still held, the barrier's heartbeats go unacknowledged and the read times out as `Unavailable`; if they have been released, the quorum round-trip surfaces the higher term and the read is refused as NOT_LEADER. Either way it declines to answer, because the barrier depends on a quorum round-trip rather than on a timer the paused process was not running.

**3. Longer runs, as a supplement rather than the fix.**
Conclusive runs measured 15 of 36 at 8 steps against roughly 87% at 12 steps, because a short run often never rolls a leader partition; the seed sweep was widened to 16 seeds with a calibrated floor of 3 as a stopgap.
More steps raises coverage across the board and is worth doing, but it raises the odds of stumbling into the window rather than aiming at it — which is why it comes after the targeted workload, not instead of it.

Day 10's own subjects sit on top of that: an effect ledger would catch what between-step sampling misses, and the contention sweep is where `Unblock` releasing every blocked eval should finally show up as measurable waste.

### Day 10 — Invariants at scale (6 hrs) ✅ COMPLETE (2026-10-30)
- A live invariant checker: no task `RUNNING` on two workers at once (check this over the **effect ledger**, not coordinator state — one `Task` struct holds one `WorkerID`, so the state machine makes the coordinator-side version true by construction; the interesting case is two workers *executing* the same task, which at-least-once permits by design); completed tasks never revert; every submitted task reaches a terminal state; **exactly one committed effect per `taskID` regardless of attempt count.**
- The effect ledger: each attempt appends to a durable side-effect log keyed by `(taskID, attempt)`. The invariant is one *committed* effect per task ID.
- Long runs across many seeds. Everything under `-race`.

**Checkpoint:** hours of continuous seeded chaos, zero invariant violations, zero races, zero linearizability violations — and a count of induced faults you can quote.

### Day 10 — items 1–2 complete

**Why these came before the invariant checker.** Day 9's sweep had stopped being able to reach the fault it was clearing.
Under the shipped configuration a 24-seed sweep recorded **zero split-brain operations** and reported it as 20 covered runs — green about a healthy cluster, which needs no checker to establish.

**1. `Covered` means a TARGETED READ.** ✅
Not "an operation touched a partitioned node": a node that has stepped down answers NOT_LEADER, which constrains nothing.
Not "a read during split brain": a read of a *pre-partition* job is correct on both sides and proves nothing.
It means a read from the cut-off node, of a job the majority committed **during** that partition.

Two counters, because coverage is relative to the claim being made and collapsing them is wrong in both directions.
`targetedReads` is the OPPORTUNITY — such a read was issued — and is the right notion for the barrier, where a **refusal is the correct outcome**; demanding an answer there made a working system report 0 of 8 covered.
`targetedAnswered` is the narrower COULD-HAVE-FAILED — it came back found or not-found — and is the right notion for validating the detector under local reads, where a refusal gives it nothing to detect.

**2. The targeted workload.** ✅
Write through the majority, read through the minority, in that order, so the write is acknowledged before the read begins and no reordering of the history can make a `NOT_FOUND` legal.
The window is **injected at a seeded step, not awaited**: a targeted workload that waits for the schedule to roll a leader partition is not targeted.

**The pause fault**, the only adversary that defeats the shipped configuration.
`clock.Pausable` freezes a node's tickers — CheckQuorum is driven by ticks, so a process that is not running never performs the check and still believes it leads on waking.
`simnet.Hold` **queues** its inbound raft messages rather than dropping them: the kernel buffers packets for a descheduled process, and delivering them on resume is what keeps the node unaware it was deposed. Dropping would be a partition, which the cluster already defends against.
Outbound is **suppressed**, because a descheduled process generates nothing.
On wake, the targeted read is served **before** the queue is released; that gap is the GC-pause window.

**Catch rates.** n = 8, seeds 1–8, 7 steps.

| adversary | reads | Illegal | covered |
|---|---|---|---|
| pinned (`DisableCheckQuorum`) | `local` | **8/8** | 8/8 |
| pinned | `readindex` | **0/8** | 8/8 |
| shipped + pause | `local` | **8/8** | 8/8 |
| shipped + pause | `readindex` | **0/8** | 8/8 |
| shipped, no pause | `local` | 0/8 | 0/8 |

Two clean A/Bs: same adversary, same coverage, opposite verdicts, the read mode the only variable.
That is the attribution CheckQuorum was kept out of the ReadIndex commit to protect.

**The no-pause row is the informative zero, and it is only zero at this width.**
At **twelve** steps and twelve seeds it measures **1/12 Illegal with 3/12 covered** — a stale read reachable on the shipped configuration with no pause at all.
CheckQuorum steps a cut-off leader down after about one election timeout (measured 230ms) while a successor needs a randomized one to two of them (250–500ms), so the two intervals **overlap at their edges**.
That is the empirical form of "CheckQuorum narrows the window and ReadIndex closes it", which until then had only been a timing argument.
At seven steps the window never opens at all, so that row runs at twelve steps deliberately; a zero there would otherwise be unreadable, since a zero meaning "no bug" and a zero meaning "no window" look identical.

**Three pause defects, each with its fix.**
The pause row covered 4 of 8 against the pinned row's 8 of 8, and adding seeds would have buried all three.

1. `Hold` queued only **inbound**, so the paused node's goroutines kept proposing and its `MsgApp` kept resetting every follower's election timer. Diagnosed from node states at wake: followers sitting at term 2 still pointing at the paused leader, never having become candidates. The majority never elected, so there was nothing newer to be stale about. **Fix:** suppress outbound from a held node.
2. The schedule **killed a node mid-pause**, leaving one reachable node of three, which can never elect. **Fix:** suppress the schedule's kills and partitions while the window is open.
3. On one seed the pause **never fired**, deferred past the end of the run because membership never recovered between kills. **Fix:** restore full membership before opening the window.

Scope, which matters more than the fixes: (1) is **not** a narrowing — it makes the pause faithful rather than suppressing other faults, and would be correct unconditionally; it simply has no effect elsewhere because nothing else calls `Hold`.
(2) and (3) **do** trade adversary breadth for aim, and both are confined to `DriveWindow`: `protect` is literally `DriveWindow && windowPending`, and the membership restore sits inside `injectTargetedWindow`, which returns immediately unless `DriveWindow` or `ReplayWindow` set the step.
Verified rather than asserted: a default verbose run fires zero `TARGETED`, `PAUSE` or restart lines.

**Reproducibility, scoped.**
An ordinary run reproduces its **fault schedule from the seed** — the Day 8 checkpoint, still holding: `TestSameSeedReproducesTheFaultSchedule` passed **25 of 25** consecutive runs.
A `DriveWindow` run does **not**.
Its injected window must find the current leader and defers when there is none, so the step it lands on and the node it hits depend on timing rather than the seed; two runs of one seed logged different schedules, intermittently, which is why injecting is off by default and gated behind `DriveWindow` rather than riding along with `TargetStaleReads`.
What a `DriveWindow` seed reproduces is the adversary's **intent**, not its realization.
So the realization is recorded — `Result.Window` carries whether the injection fired, at which step, against which node, and whether it was a pause or a partition — and `Config.ReplayWindow` feeds it back to force the same step and victim.
`TestReplayWindowReproducesTheRealizedWindow` pins that for both window kinds and passed **10 of 10** consecutive runs; without it, "replay" was an unverified claim in a comment.
**Any failure found under `DriveWindow` must be reduced to a deterministic scenario test before it is treated as fixed.**
A replayed window is the same adversary, not the same execution — a weaker claim than the seeded schedule's.

**Every healthy-looking aggregate hid something.**
Four times in this day's work, and the thing that broke each one open was printing the per-run columns the aggregate was computed from.
The pause diagnosis now prints unconditionally, pass or fail, for that reason.

| the aggregate | what it hid |
|---|---|
| "20 covered runs" | zero split-brain operations: coverage counted ops to nodes that had already stepped down |
| "4 of 8 covered" | three distinct pause defects, listed above |
| "3 covered, 2 Illegal" | targeted reads that were *refused* counting as coverage |
| "8 of 8 covered" | every seed injecting at step 2 and waking at step 5 — eight samples of **one** timing, because the span was `Steps/4`, which is 1 at seven steps. Span now derives from the hold duration (`Steps - hold - 1`), giving steps 2–4 across all three node ids |

**"CHECKED" and "COVERED" are different words now.**
The seed sweep reports *checked* / *unchecked*: the per-step invariants and the convergence check ran and found nothing.
Linearizability reports *covered*: the history contained a read that could have been stale.
They were briefly sharing one word, which is how a general fault sweep gets quoted as linearizability evidence — it is not, and under the shipped configuration it structurally cannot be, because the window never opens by chance.
Tightening `Covered` took `TestSeedSweep` to 0 of 16 "conclusive" on runs that had exercised every invariant and converged, which was it failing on another test's property.

**The sweeps are gated behind `make sweeps` (`FTC_SWEEPS=1`).**
They are validation of the workload and calibration of its floors, not per-commit guards; what guards the behaviour on every commit is the sub-second deterministic scenario under both read modes plus the four liveness tests, all in `make test`.
The gating was forced rather than chosen: five sweeps put the package at 577s against Go's 600s default timeout — a whole-package failure with no test failing — and shortening runs to recover it took the no-pause adversary to 0 of 8 covered, a minute spent asserting nothing.
`make sweeps` is **required before tagging, and before quoting any rate anywhere**, and every quoted rate carries its **n and seed range**: "8/8 Illegal" means nothing alone, "8/8 Illegal over seeds 1–8 at 7 steps" can be reproduced and argued with.
`make release-check` is `vet test race sweeps` — everything a tag asserts.

### Day 10 — entry point at the time (history)

Items 1–2 are done; what Day 10 was actually for has not started.
The targeted workload exists to make the linearizability claim mean something, and the four items below are about EFFECTS rather than reads.
Read item 2 before writing any checker: the obvious invariant there is one the system is *supposed* to violate.

**1. The effect ledger.** The foundation, and nothing currently checks it.
Each attempt appends to a durable side-effect log keyed by `(taskID, attempt)`.
The invariant is **exactly one COMMITTED effect per `taskID`, regardless of attempt count** — which is the claim Day 5's effectively-once design actually rests on, and which no test currently makes.
At-least-once delivery means several attempts may *execute*; what must be unique is the committed effect.

**2. A live invariant checker over the LEDGER — and read this before writing it.**

**"No task `RUNNING` on two workers at once" is NOT a promisable invariant, and a checker that asserts it will fire on correct behaviour.**
Coordinator-side it is true by construction: one `Task` struct holds one `WorkerID`.
Worker-side it is **expected to be violated** — a slow or paused worker keeps executing after its lease has been reclaimed and the task has been handed to someone else.
That is at-least-once delivery working as designed.
Preventing it would require **fencing** (the executor checking a monotonic token before committing its effect), which this system does not have and has not claimed to.
So overlap is not a defect to catch; it is the condition the idempotency exists to survive.

What the checker ASSERTS:
- **Exactly one COMMITTED effect per `taskID`, across all attempts.** This is the claim Day 5's effectively-once design rests on, and nothing currently tests it.
- **The committed effect matches the attempt the coordinator recorded as completing.** One effect is not enough on its own — it has to be *that* attempt's effect, or the ledger and the state machine disagree about what happened.
- **A completed task never reverts.**
- **Every submitted task reaches a terminal state.** Asserted only after a QUIESCENCE phase: heal every partition and hold, restart every killed node, stop injection entirely, then run until the queue drains within a bound. Exceeding the bound is reported as **DID-NOT-DRAIN**, a distinct outcome from a lost task — a task still retrying under ongoing faults has not been lost, and asserting liveness mid-chaos would make the checker fire on a system that is merely slow.

What the checker MEASURES, and must never assert on:
- **Overlapping executions per run.** Report the count. **Nonzero overlap is evidence the idempotency was exercised; zero means it was not** — so a run with no overlap proves nothing about effectively-once, the same way a run with no split-brain read proves nothing about linearizable reads. By the coverage rule established on Day 9 and 10, a zero here makes the effectively-once claim INCONCLUSIVE for that run, not confirmed.

**3. The ledger needs WORKER-SIDE EXECUTION in the simulator, which does not exist yet.**
This is a gap carried since Day 8 and it blocks everything above: the simulator calls the gRPC service directly, so the real worker's executor, heartbeat loop and local claim dedup are exercised only by their own unit tests.
A ledger keyed by `(taskID, attempt)` records what an EXECUTOR did, and there is currently no executor in the loop to record.

So the first ledger step is a decision, not code: a **worker-side execution shim** inside the simulator, or **running the real worker code in-process** the way the coordinators already are.
Whichever it is, it has to include a **slow worker that keeps executing after its lease is reclaimed** — otherwise overlap stays at zero, and the headline invariant is tested only against the case that cannot violate it.

**4. Long multi-seed runs, everything under `-race`.**
The sweeps so far are minutes, not hours, and `-race` has been run on the default suite rather than on the long runs.
Day 9's item 3 — longer runs per seed — folds in here as a **supplement**: it raises coverage across the board but raises the odds of *stumbling into* a window where the targeted workload *aims* at one, which is why it comes after, not instead.

**Checkpoint:** hours of continuous seeded chaos, zero invariant violations, zero races, zero linearizability violations, and a **quotable count of induced faults** — carrying its n and seed range, per the rule above.
Note what the checkpoint does NOT say: zero overlapping executions. Overlap is measured and reported alongside the fault count, because a run that never overlapped did not test effectively-once at all.

### Day 10 — COMPLETE: the checkpoint passed under additive isolation

**Day 10 is met.** The checkpoint passed on 2026-10-30 against the additive-isolation adversary on recalibrated floors, commit `e633366`, after two failed runs that uncovered a real product defect and one attribution run.
The completion summary, the three-checkpoint sequence and the operational notes are in "DAY 10 IS COMPLETE — compaction handoff" below; the subsections between here and there record how it got there and should be read as history rather than instructions.
**Next is Day 11 (benchmarks), not asynchronous snapshotting** — that is follow-up 1, deliberately not the next work.

#### What the checkpoint found: a snapshot-reporting defect in the product

Fixed in **`29644d1` — `fix(raft): report snapshot outcomes, or a dropped MsgSnap parks a follower`**.

etcd/raft puts a follower in `StateSnapshot` when it sends it a snapshot and pauses every log probe to it — `Progress.IsPaused()` is unconditionally true in that state — until the application calls `Node.ReportSnapshot`.
**Nothing in this build ever called it**, in either transport, and `ReportUnreachable` was never called either.
raft's own documentation names the consequence: the follower "could end up in a limbo, never getting any updates from the leader".

It is silent in the worst way.
The parked follower keeps answering heartbeats, so `Progress.RecentActive` stays true, CheckQuorum is satisfied, and the leader is never deposed — it simply holds its term and commits nothing.
`httpTransport.Send` swallowed its POST error under the comment *"peer down or partitioned; raft's own protocol retries"*, which is true of `MsgApp` and false of `MsgSnap`.

The fix lives in `raftnode` so both transports get it: `Transport.Send` now reports whether the send ATTEMPT completed, and `Node.send` reports `ReportSnapshot(Finish|Failure)` for every `MsgSnap` plus `ReportUnreachable` on any failed send.
`simnet` returns nil on a drop deliberately — a real network drop looks like success to its sender too.
No `PendingSnapshot` timeout is needed: both outcomes call `BecomeProbe`, so probing resumes either way, and a snapshot that never arrived is caught by the follower rejecting the next probe.

**The deterministic reproduction** is `TestDroppedSnapshotDoesNotParkAFollower` (`internal/sim/snapshotlimbo_test.go`), built on `simnet.DropSnapshotsTo(id, n)`, which loses exactly one `MsgSnap` and nothing else.
It fails without the report and passes with it:

```
before:  term=2  ackingQuorum=false  commit=486
         node1: StateSnapshot  match=0  pendingSnapshot=481  recentActive=TRUE  paused=TRUE
         -> 6s of "proposal lost", leader holds its term
after:   term=2  ackingQuorum=true   commit=489
         node1: StateReplicate  match=489  pendingSnapshot=0  paused=false
```

#### THE CLAIM, stated exactly

The mechanism is **proven**.
The fix is **verified** by a deterministic reproduction.
And **the failure did not recur across 64 post-fix seeds under the same adversary** — 0 of 64 against 4 of 64 before the fix.

That last part is a **rate argument, not a captured progress table**, and the difference matters.
The five original frozen-clock windows were recorded **before** the raft progress table was instrumented, so none of them shows a follower parked in `StateSnapshot`, and none ever will now.
Those five windows therefore stay **formally unattributed**.
What is settled is narrower and still worth having: the failure mode the checkpoint kept hitting is gone, and the mechanism capable of producing it is fixed.

The arithmetic behind "0 of 64": the pre-fix rate was 4 of 64, seed 54 ended at step 343 of 600 on a host stall so the basis is **63.6 full seeds**, and zero windows on that basis is a **~1.7%** outcome by chance (0.9375^63.6).
The reading was fixed in advance — zero is evidence, one or two would have been AMBIGUOUS and reported as such — so the conclusion was not fitted to the data afterwards.

#### (a) — DONE, 2026-10-29

64 seeds x 600 steps on `29644d1` under the unchanged REPLACE semantics, so the snapshot fix was the only variable.
**PASS**, 2h36m20s of cumulative run time.
Every finding in all 64 rows was INCONCLUSIVE, of three kinds only: `linearizability-coverage` (64, expected by construction), `ledger-coverage` (6), `host-io` (1).
No `invariant`, `convergence`, `readyloop-stall`, `effect-ledger`, `did-not-drain` or `comparison-floor` finding occurred anywhere in the run.

**The coverage numbers are floors-met, NOT quotable**: 299 overlapping executions against a floor of 160, 467 stale rejections against a floor of 200.
Seed 54 took a 175ms host-I/O stall in `persist`, which trips the harness's own rule that rates measured on a host that did not sustain the run's timings are re-measured before being quoted.
They are **superseded by (b)'s checkpoint**, which is where quotable coverage comes from — re-running (a) on a quiet host would buy a number that the next checkpoint replaces anyway.

The whole record is committed under **`results/2026-10-29-checkpoint-a/`**: `rows.jsonl` (64 rows, seeds 1–64), the 64 per-seed fault logs, the aggregate `report.txt`, `per-seed.txt`, and a README with the commit, the invocation and the caveat.
`/tmp` is cleared on reboot, so nothing diagnostic is left there.

#### (b) Additive isolation, recalibration, fresh checkpoint — DONE, 2026-10-30

Landed as `e633366` (`feat(sim): additive partition isolation`) and `0979e4f` (`docs: record additive-isolation calibration`).

The recalibrated floors are **200 overlapping executions and 300 stale rejections** over 64 seeds x 600 steps, measured on the additive adversary over seeds 101-108 and 201-208, recorded in `results/2026-10-30-calibration/`.
They were set to hold even at the LOWER sweep's rate, which projects to about 280 and 512, rather than at the pooled mean — the two sweeps disagreed by about 2x (35 vs 72 overlaps) and the A/B gap is roughly 2.3 standard errors, so variance estimated from 16 seeds probably understates the spread.
That choice is why the checkpoint was not a coin flip: it came in at 308 and 558, below the pooled projection of 428 and 680.


The decided semantics (see `docs/architecture.md`, "Partition semantics"):

- `partition X` **ADDS** X to the isolated set; isolating one node never releases another.
- A **held partition survives later partition events** and ends only at its own scheduled heal.
- **Heals release only the nodes they name**, with heal-everything as its own explicit event rather than the degenerate `Partition()` with no arguments.
- **The RNG draw order must not change.** The schedule's heal branch needs a new explicit heal-all event, and if the number or order of draws shifts then every seed replays a different schedule and the Day 8 checkpoint goes red for an unrelated reason. Verify with `make repro` (25x) before recalibrating.

Then recalibrate the floors on the same two-sweep basis as the first calibration, and only then run a fresh checkpoint.

**Every number from all three checkpoints already run was measured under REPLACE semantics**, including the fault counts, under which a reported leader partition was often not held as long as it claimed.
Those figures are labelled as such in `docs/architecture.md`; the floors derived from them are **void** for the additive adversary.
The recalibration numbers are reviewed before any floor is set and before the final checkpoint starts.

#### The exact invocation

The checkpoint is gated and parameterised entirely by environment variables.
Absolute paths, because the row file and the per-seed fault logs are the whole diagnostic record and a relative path puts them wherever the test happened to run:

```
cd /Users/evazheng/Desktop/d-sys/v2/fault-tolerant-coordinator
mkdir -p /tmp/ftc-checkpoint

FTC_CHECKPOINT=1 \
FTC_CHECKPOINT_SEEDS=64 \
FTC_CHECKPOINT_STEPS=600 \
FTC_CHECKPOINT_MIN_OVERLAPS=160 \
FTC_CHECKPOINT_MIN_STALE=200 \
FTC_CHECKPOINT_OUT=/tmp/ftc-checkpoint/rows.jsonl \
  go test -count=1 ./internal/sim/ -run TestDay10Checkpoint -v -timeout 300m \
  > /tmp/ftc-checkpoint/run.log 2>&1
```

- `FTC_CHECKPOINT_SEED_FROM=N` runs a single seed or a range, for re-examining one finding without paying for the whole sweep.
- `FTC_CHECKPOINT_OUT` holds one durable JSON row per seed, `fsync`ed before the next seed starts, and **the run resumes from it**: re-invoking the same command skips seeds already recorded and rebuilds the report from the file.
- **Per-seed fault logs land next to the row file** as `faults-seed-NNN.log`, one per seed, never `io.Discard` — the first checkpoint sent them nowhere and its findings could not be diagnosed afterwards.
- The host gate (`hostio.Fsync`, mean ≤ 12ms, worst ≤ 100ms) runs inside the test before seed 1 and refuses the run on a busy disk.
- Expect **~2.5–2.7 hours** for 64 seeds at ~146s per seed.

**THE MACHINE MUST STAY AWAKE WITH THE LID OPEN.**
Use Amphetamine (or `caffeinate -dimsu`) for the whole run.
A sleep mid-run does not merely pause it: the host probes bracketing each seed will record the stall, the frozen-clock and stall invariants measure wall time against a 250ms election timeout, and the run comes back with findings that describe the laptop rather than the system.

**Do not run anything else on the machine during a checkpoint.**
Measured: back-to-back race suites pushed fsync latency from ~3ms to 1–13 MINUTES, took 20 of 25 reproducibility checkpoints to SKIP, and produced two Ready-loop stalls that were really the pileup behind the disk.

#### DAY 10 IS COMPLETE — compaction handoff

**The Day 10 checkpoint PASSED on 2026-10-30** against the additive-isolation adversary on recalibrated floors, commit `e633366`.
64 seeds x 600 steps, 2h47m38s of cumulative run time, record in `results/2026-10-30-checkpoint-additive/`.

```
LEDGER COVERAGE: 308 overlapping executions (floor 200), 558 stale rejections (floor 300)
DRAIN: 63 of 64 reached quiescence, 63 of those drained; 63 completed all 600 steps
STALLS: 0 host-io, 0 unattributable
SNAPSHOT BUDGET: 1 stall (known product limitation)
LINEARIZABILITY: 64 histories, 0 Illegal, 296,674 ops checked, 12% ambiguous
--- PASS: TestDay10Checkpoint (10109.20s)
```

**Coverage 308/558 is quotable, over 63 complete seeds; seed 3 truncated at step 308 by the synchronous-snapshot limit.**
Zero frozen-clock windows, the second consecutive checkpoint without one and the first against the harsher adversary.
No `invariant`, `convergence`, `readyloop-stall`, `effect-ledger`, `did-not-drain` or `comparison-floor` finding in any of the 64 rows.

**The three checkpoints, in order, so the sequence is not re-derived later:**

1. Two runs failed on the frozen-clock invariant (4 of 64, then 5 of 64 on a degraded host) and uncovered the snapshot-reporting defect, fixed in `29644d1`.
2. (a), 2026-10-29, `results/2026-10-29-checkpoint-a/`: 64 seeds on the OLD replace semantics, the snapshot fix the only variable. Zero windows. Its coverage numbers (299/467) are floors-met and NOT quotable, superseded by (b).
3. (b), 2026-10-30, `results/2026-10-30-checkpoint-additive/`: additive isolation, recalibrated floors. PASS, numbers above.

**What the snapshot claim is, exactly.** Mechanism proven, fix verified by a deterministic reproduction (`TestDroppedSnapshotDoesNotParkAFollower`), and the failure did not recur across 128 post-fix seeds over two adversaries.
Still a RATE argument: the five original windows predate the raft progress instrumentation, so they remain formally unattributed and always will.

#### Open follow-ups

1. **Asynchronous snapshotting — the top follow-up.** `maybeSnapshot` serializes, fsyncs and compacts inside the Ready loop, so **the loop is exposed to disk tail latency**: an fsync tail inside the snapshot stops the node processing raft messages for its whole duration. Two worst-case observations on healthy disks, one per checkpoint: **91ms at 1.43MB** under replace semantics and **133ms at 1.60MB** under additive isolation, against a **125ms** threshold the second crossed, truncating seed 3 at step 308.

   **These are two worst cases, not a growth trend, and the cause is not size.** Serialization runs at about 6.5ms/MB, so 1.6MB accounts for roughly 10ms, and the step from 1.43MB to 1.60MB is about 1ms — neither the 91ms nor the gap to 133ms is explained by state size. Seed 3's post-run worst probe was 19ms, the highest of the 64 seeds, which is the shape of a tail. Asynchronous snapshotting removes the exposure; a quieter machine only shortens the tail. Separately and still true, **single-run state growth beyond ~1,500 jobs is unmeasured**. NOT the next work: Day 11 is.
2. **Periodic worker pause** — agreed as a separate change. The pause fires once per run (`s.result.WorkerPause.Fired` gates it), so a 6,000-step run would get one aimed window. Measured: this does not starve coverage, because ordinary chaos generates lease lapses by itself — a 150-step run produced 8 overlaps and 15 stale rejections from a single pause window — so making it periodic raises coverage density rather than enabling it.
3. **The frozen-clock invariant's exemption is still built on the simulator's connectivity view**, which counts nodes that are up and unpartitioned rather than nodes that can ACK. `RaftInfo.AckingQuorum` exists and is recorded in every window sample; neither (a) nor (b) produced a window, so nothing forces the switch and it stays open rather than resolved. Eva's constraint stands: keep a tenure-independent liveness invariant, exempt windows explained by injected faults as partitions and pauses already are, and keep failing on unexplained churn that freezes the clock.
4. **`TestShippedConfigWithoutPauseFindsNoStaleRead` is down to 1 of 8 covered** (from 2 of 8) under additive isolation. Its asserted property is that the shipped config finds nothing, which 0 of 8 Illegal satisfies, so it passes — but its coverage is incidental luck by construction, and at 0 of 8 it would stop exercising its own premise while still passing quietly. It should report INCONCLUSIVE when coverage reaches zero.
5. **Day 10's `make sweeps` ledger sweep is insensitive at 150 steps.** It returned statistically identical numbers before and after the adversary change (14 overlaps, 23 stale, 6/8 reaching overlap), because at 150 steps a held partition rarely has room to change the outcome. It is a smoke test, not a measurement; the 600-step sweeps are the measurement.

#### Operational notes for the next checkpoint

`make checkpoint` runs it into a **run-stamped directory** (`results/checkpoint-<UTC stamp>/`), gated by the fsync probe, under `caffeinate`.
Resuming an interrupted run means `make checkpoint DIR=<that directory>`; rows already recorded are skipped and the report is rebuilt from the file.
Overrides: `SEED_FROM`, `SEEDS`, `STEPS`, `MIN_OVERLAPS`, `MIN_STALE`.

Four traps, each of which cost a cycle:

- **`SEEDS` is the LAST SEED, not a count.** `SEED_FROM=101 SEEDS=8` used to run zero seeds and print a full report of zeros at exit 0. Now refused at startup, and a report over zero rows is refused too.
- **`DIR` must be absolute.** `go test` runs the binary with the PACKAGE directory as its working directory, so a relative path puts rows and fault logs under `internal/sim/` while the run log lands at the repo root. The Makefile uses `$(CURDIR)`.
- **Every report prints its own provenance** — run id, report time, row file, the span of the rows it read, and a count of rows CARRIED OVER from an earlier run. Two sweeps were once read off a stale log; this is what makes that visible.
- **Nothing else runs on the machine during a sweep whose numbers will be quoted.** Measured: back-to-back race suites pushed fsync from ~3ms to 1–13 MINUTES. Keep the lid open; `caffeinate` holds off idle sleep but not a lid close.

---

## Phase 4 — The numbers (Days 11–12, ~13 hrs)

### Day 11 — Benchmarks and instrumentation (7 hrs)

`cmd/loadgen` must be **open-loop**: submit at a fixed target rate without waiting for completion. A closed-loop generator measures your own client and hides queueing — the most common way portfolio benchmarks end up meaningless.

**Prometheus metrics** ([client_golang](https://github.com/prometheus/client_golang)):

```
sched_tasks_total{state="completed|failed|retried"}   counter   → throughput
sched_queue_depth{state="pending|assigned|running"}   gauge     → queue depth
sched_evals_pending                                   gauge
sched_plan_size                                       histogram → assignments per Raft entry
sched_plan_rejections_total                           counter   → optimistic-concurrency conflicts
sched_failover_latency_seconds                        histogram
sched_worker_reclaim_latency_seconds                  histogram
raft_commit_latency_seconds                           histogram
raft_leader_elections_total                           counter
```

**Methodology, stated in the README:** warm up 10s and discard; 3 runs per config, report the median; [hdrhistogram-go](https://github.com/HdrHistogram/hdrhistogram-go) for percentiles, recording *intended* send time to avoid coordinated omission; disclose hardware, Go version, cluster size, fsync setting.

**The headline experiment: plan batching.** Measure throughput with `ApplyPlan` carrying one assignment vs. many. The single-assignment case is fsync-bound at roughly hundreds to low thousands per second on laptop SSD; batching moves it by about an order of magnitude. That gap is Day 4's architecture paying off, and explaining it as fsync amortization is the best result you'll produce.

#### Day 11 — the plan-batching result, and what it opened

**Measured on a real 3-node `cmd/coordinator` cluster** (raft over HTTP, client load over gRPC, no simulator), 2,000 tasks drained from a fixed backlog, 3 runs per arm, median:

| | P=1 | P=10 | P=100 |
|---|---|---|---|
| drain time | **62.1s** | **18.0s** | **19.0s** |
| measured plan size | 1.00 | 7.04 | 6.80 |
| raft entries per node | 5306 | 2666 | 2690 |
| plan rejections | 320 | 407 | 606 |

Record is in `results/2026-10-30-day11-batching/`; the bullet for the writeup is **3.4x faster drain at a measured plan size of ~7**, not "an order of magnitude from fsync amortization".

**The mechanism is not what the roadmap predicted.** Log-entry reduction is exactly **2.0x** and that is its ceiling: `apply_plan` collapses 2000 to 284 while `complete_task` stays pinned near 2,004, since each completion is its own entry. Drain time nonetheless improves 3.4x, because at P=1 the planner is **serial** — one plan per commit round trip — and `plans x commit-latency-mean` predicts every run within 2% (29.9ms/59.8s predicted/60.2s observed; 30.7/61.4/62.1; 47.5/95.0/93.9). Batching removes 1,716 serial round trips rather than shortening the log. Completions never paid that cost because they arrive concurrently and batch into shared fsyncs.

**P=100 is marginally WORSE than P=10**, and the cap stops binding below 10. 93% of plans carry 8 or fewer assignments and only 2 of 300 exceed 64.

#### Day 11 follow-ups

0. **Failover degrades when the raft log grows while nodes keep restarting**, because a restarted node must APPLY the whole log before it is a useful voter. Measured, after four wrong attributions of mine:

   - **Not task count.** A cluster built fresh to 30,000 task records failed over in **1132ms**, against 17,060ms in the degrading run at a similar estimated count.
   - **Not restart cycles.** 45 consecutive leader kills with a bounded backlog stayed flat at 1127-2045ms with no trend.
   - **Not broker rebuild.** It grew 1.3ms to 29.4ms, which is 28ms of a 15,400ms increase — 0.18%. Evidence that state grew, not a cause.
   - **Not WAL read.** A restart against a 4-5MB WAL served HTTP in 50ms, against 31ms on an empty one.
   - **It is APPLY on restart.** The startup line says it plainly: `commit: 23490, applied: 0`. The listener is up in 50ms while ~23,500 entries remain to be applied, and applying them back to ~19,800 tasks took **6.3 seconds**, progressing incrementally. With a kill every ~10 seconds the cluster spends much of its life with one node still catching up, so the next election and first commit run against a thin quorum.

   **Reproduced** with `UNBOUNDED=1 RESET_EVERY=9999` on the failover harness: 1127ms at trial 10 rising to 4690ms by trial 55, knee near trial 40-45, matching the original run.

   **The benchmark configuration is what let the log grow**, and that part is the harness's fault rather than the product's: `-snapshot-entries 500000` was set to keep snapshot stalls out of the measurement, which disabled compaction entirely — zero snapshots on any node, confirmed from the logs. Under the shipped default of 10,000 a restarting node would restore from a snapshot rather than replaying everything, which should bound catch-up. **That is untested and is the obvious next measurement.**

   The bounded measurement therefore stands as "failover at a stated state size", with `tasks_in_store` reported alongside every figure.

0a. **Decouple apply from the Ready loop — one product fix behind two measured limitations.**

   Today the Ready loop persists, sends, applies committed entries, and takes snapshots, all on one goroutine. etcd does not: its Ready loop persists and sends, then hands committed entries to a separate apply goroutine. Everything that blocks apply therefore blocks raft message processing here, and two separate measurements are the same root cause:

   - **Restart replay: 6.3 seconds.** A node restarting ~23,500 entries behind applies them inside the Ready loop, so it cannot vote or ack for the whole window. The stall detector saw it as 3-5 stalls of 562-739ms per node, each under the 1s election timeout, so no single stall deposed anyone and the damage was aggregate unavailability. This degraded failover from 1127ms to 4690ms over 55 trials with compaction disabled; under the shipped `-snapshot-entries 10000` the same workload cost only 12% (1384ms to 1554ms), replay fell to 2357ms for ~2,900 entries, and stalls fell to 0.3-0.5 per restart at a 501ms median. Bounded by compaction, not removed.
   - **The snapshot fsync tail: 133ms at 1.60MB** against a 125ms threshold, on a healthy disk, which truncated seed 3 of the Day 10 checkpoint. `maybeSnapshot` serializes, fsyncs and compacts in the same loop.

   **Interaction with follow-up 1 (asynchronous snapshotting): this subsumes most of it.** Snapshots are triggered off applied state, so moving apply off the Ready loop moves the snapshot's serialization and compaction with it. What that does NOT automatically fix is the snapshot file's own fsync, which must still be kept off any path raft needs — so follow-up 1 remains as the narrower guarantee, and this is the structural change that makes it cheap. Do this one first; doing 1 alone leaves the restart gap untouched.

   **The hard part is not the goroutine, it is the invariant.** `applyCommitted` currently advances `appliedIndex` inside the same `c.mu` critical section as the mutation, deliberately, so that no reader can observe `(state, appliedIndex)` skewed — a false "nodes 1 and 3 DIVERGED at applied index 555" on one run in twenty-two is why. A separate apply goroutine has to preserve that pairing, remain the single writer of state, and keep `Advance` ordered correctly against it (or move to raft's async storage writes API). Divergence is the one failure this system has no recovery from, so this change needs the differential and ledger oracles run against it, not just a passing suite.

0b. **Terminal tasks are never collected.** `sessions` are swept by `Tick` on a TTL; tasks are not, so state grows with THROUGHPUT rather than with backlog. Real, and worth fixing — but NOT the failover mechanism above, since 30,000 fresh records failed over in 1132ms.

   **A retention policy has to answer three couplings**, which is why this is a follow-up and not a fix:
   - **`GetJob` semantics.** A client that submitted a job expects to read its outcome, so collection makes that read fail or return a partial job. Retention must outlive the window in which a client may still ask, and how long that is is a product decision.
   - **Dedup sessions.** `(clientID, requestID)` replays a Result naming job and task ids. Collecting the tasks a live session points at leaves a session replaying a reference to nothing, so tasks must outlive the sessions that name them.
   - **The linearizability model.** It is written over job and task state and partitions on dedup keys and job ids. A task vanishing mid-history is a transition the model has no operation for, so collection must be modelled explicitly or excluded from checked runs — and excluding it means the checker never sees the production configuration.

   Likely shape: time-based retention on terminal tasks, strictly greater than `SessionTTLMs`, swept in `Tick` as sessions already are, with collection modelled as an explicit operation. Not attempted.

1. **Eval coalescing, Nomad-style.** The reason plans stay small is that every completion emits an eval for its own job, which then finds a task or two pending in that job; the only large plans are the initial per-job evals, which found 100 pending tasks against 256 idle slots. Keeping at most ONE pending eval per job and coalescing further triggers into it should raise effective plan size well above 7, and is the single change most likely to move this number. The histogram is in the results README.
2. **The benchmark harness takes no host probes.** The simulator's checkpoint brackets every seed with fsync and CPU probes; this bench does not, so when P=1 run 3 came in at 93.9s against 60.2s and 62.1s, its commit latency distribution could be reported (p50 48ms, p90 62ms, p99 112ms against p50 29ms / p99 63ms) but the slower disk behind it could NOT be attributed. Any rate quoted from this harness carries that gap until it has the same probes.
3. **The scheduler path is the next scaling limit**, measured rather than projected. After the index fix there are no Ready-loop stalls up to 150,000 tasks, but throughput falls about 6x and latency rises about 6x between 50,000 and 150,000, and a profile attributes it to `sortedTaskIDs` on tick paths (87.6% of sort time), `StateStore.Snapshot` building each planning snapshot (12.4%), and `Snapshot.InFlight` scanning every task per worker inside `computePlan` (18.5% cumulative, O(workers x tasks) per plan). The fix is an in-flight count index plus a cheaper planning snapshot. See `results/2026-10-30-day11-profile/`.
4. **`RequestTask`'s long poll spins on a 50ms interval rather than waiting on a notification.** Deliberate — `pollInterval` exists so the poll never holds the apply lock while waiting — but it puts a floor of ~5,120 lock acquisitions per second under 256 pollers regardless of state size. Cheap now that each costs ~100ns; it was the multiplier that turned the old O(state) scan into a leadership-losing stall.

### The second experiment: where optimism stops paying

Plan batching (above) shows optimistic concurrency winning. This shows it
losing, which is the more interesting half — and it's the measurement that
answers "when does your design break?" before an interviewer has to ask.

**Independent variable:** contention ratio = pending tasks : idle **slots**.
Slots, not workers — Day 4 gave workers a declared capacity, so 8 workers at
`-concurrency 4` is 32 slots, and dividing by 8 would put the crossover in the
wrong place by 4×. Hold the pool fixed (say 8 workers × 4 = 32 slots) and
sweep offered load so the ratio moves across roughly 0.5× → 1× → 2× → 5× →
20×. Fix scheduler-worker count per sweep; run the sweep again at 2, 4, and 8
scheduler workers.

**Expect `Unblock` to be part of what you measure.** A freed slot releases
*every* blocked eval, not the one it suits, so at high contention the broker
manufactures scheduler work that places nothing. That is a real contributor to
the flattening, and distinguishing it from plan rejections is worth doing —
otherwise the curve has two causes and one label.

**Dependent variables, from existing metrics:**
- `sched_plan_rejections_total` → rejections per accepted plan
- `sched_tasks_total{state="completed"}` → useful throughput
- `sched_evals_pending` → is the broker draining or growing
- eval dequeue → commit latency (add a histogram if not present)

**The plot:** rejection rate and throughput vs. contention ratio, one line
per scheduler-worker count. Expect throughput to rise with scheduler count
at low contention and flatten — or invert — at high contention, where extra
schedulers only manufacture rejections. Find and quote the crossover.

**Why it belongs in the writeup.** It converts "I used optimistic
concurrency" into "I measured the regime where it wins and the regime where
it degrades, and here is the ratio at which they cross." Same character as
v3's cache-affinity-vs-load crossover — showing the failure mode of your own
design choice reads as engineering judgment rather than advocacy.

**Bullet slot:** *sustained [T] tasks/sec at [k] assignments per entry;
identified the contention ratio ([R] pending tasks per idle slot) beyond
which additional scheduler concurrency produced rejections rather than
throughput.*

**Cheap version if short on time:** two points, not a sweep — a low-contention
and a high-contention run, same config otherwise. One number each. The claim
survives; you lose the curve.

### Day 11 — COMPLETE: handoff

Read this before touching Day 11 or Day 12. Everything below is measured; where it is not, it says so.

#### Commits this session

| hash | what |
|---|---|
| `7927ff3` | `fix(state)` index tasks by worker, so a long poll stops scanning all state |
| `0c633d1` | `feat(bench)` open-loop loadgen and Prometheus metrics |
| `849f4e0` | `fix(state)` drop terminal tasks from the worker index |
| `e0eb949` | `feat(bench)` debug endpoints, JSON host probes, task args |
| `17b792a` | `fix(raftnode)` report a stall's completed duration, not the censored floor |
| `371103b` | `fix(bench)` make a prefill's idempotency key independent of the submitter |
| `2d9bb57` | `feat(state)` reject a reused idempotency key that carries different work |

**Three hashes were rewritten by a rebase** that reworded a mislabelled commit: `155a53d`→`e0eb949`, `25c3b78`→`17b792a`, `5a09e85`→`371103b`. `849f4e0` and earlier are unchanged. No prose cites the old ones; checked.

#### Results, with the configuration that makes them mean something

**Always read `-snapshot-entries` first.** It decides whether the raft log compacts, and the log decides how long a restarted node spends applying before it can vote. The failover numbers moved by an order of magnitude on that flag alone.

| measurement | number | `-snapshot-entries` |
|---|---|---|
| Plan batching, drain 2,000 tasks | **62.1s at P=1, 18.0s at P=10, 19.0s at P=100** | 500,000 |
| Worker reclaim, from last heartbeat | **p50 2121 / p95 2217 / max 2246ms** | 500,000 |
| Leader failover, shipped config | **p50 1441 / p95 2090 / max 2584ms**, +12% to ~19k tasks | **10,000** |
| Leader failover, bounded | p50 1303 / p95 1905 / max 2066ms at median 990 tasks | 500,000 |
| Steady-state control, no kills, 10 min | **0 stalls** at 27,000 records | 10,000 |

**Plan batching is 3.4x on drain time and the mechanism is not what the roadmap predicted.** Log entries fall **exactly 2.0x** (5306 to 2666) and that is the ceiling, because `apply_plan` collapses 2000 to 284 while `complete_task` stays pinned near 2004. The win is removing 1,716 SERIAL commit round trips: at P=1 `plans x commit-latency` predicts every run within 2%. **Plan size saturates near 7** whatever the cap — 93% of plans carry 8 or fewer — so P=100 is marginally WORSE than P=10 and rejections rise 320/407/606 with the cap. Record: `results/2026-10-30-day11-batching/`.

**Reclaim is consistent with the ~2250ms budget** (lease 2000 + tick 200 + commit) with the worst case **at its edge** and ±10ms of poll error, so it is deliberately not reported as a pass count. t=0 is the last heartbeat RECEIVED, derived from the POST-KILL lease: the pre-kill reading is stale by up to one heartbeat and overstated this by 606ms.

Full detail, preconditions and caveats: `results/2026-11-01-day11-reclaim-failover/`.

#### Findings

1. **`heldTask` did O(state) work under the apply lock.** Answering "which task does worker W hold" sorted every task id in the store, on every pass of the `RequestTask` long poll, with 256 pollers. At 10,000 tasks: apply stalls, **7-9 leadership changes per 30s**, commit latency 322-696ms. The CPU profile was decisive — 18% CPU, 59% of samples in mutex wait — and cleared the three per-apply suspects anyone would have guessed. Fixed by a worker->tasks index; read path went from 5.19ms to **72ns** at 50,000 tasks.
2. **That index then leaked, and the property test shared its blind spot.** `applyCompleteTask` sets `WorkerID` as provenance and never clears it, so every completed task stayed in the index forever. The oracle did not catch it because the oracle asked the same wrong question as the code — both "which tasks have `WorkerID == w`", neither "and are they still running". Found by a benchmark that knew nothing about the index and only knew a task should stop being attributed to a dead worker.
3. **Failover degradation is restart replay inside the Ready loop.** A restarted node applies its backlog on the goroutine that processes raft messages, so it cannot vote for the whole window: **6.3s** for ~23,500 entries. Four other explanations were eliminated by measurement (task count, restart cycles, broker rebuild at 0.18% of the growth, WAL read). Bounded but not removed by compaction.
4. **Every stall duration the detector ever reported was a floor.** It polled at `threshold/2` and reported once per stall, so `Duration` was elapsed-at-DETECTION: all 500-748ms with a 501ms median whatever the real blockage, and `raft_ready_stall_seconds` could not reach its upper buckets. Fixed in `17b792a` with a second, completed report; durations quoted before it are annotated as floors.
5. **The payload-hash guard shipped INERT.** `DefaultConfig` still proposed command version 3, and every version-gated rule reads the entry's version — so five passing state-machine tests covered a rule that did nothing over gRPC. An end-to-end test caught it; a config test now pins the default to `CommandVersionCurrent`.
6. **`cmd/loadgen` reused its idempotency keys** and cost **70 of 100 reclaim trials and 68 of 100 failover trials**, each prefill reporting success and creating nothing. Diagnosed only after printing the state; two plausible theories were wrong first. The key also folded in the submitter index, making dedup depend on a scheduling race.

#### Rules now in force

- **Measure the mechanism before theorizing about it.** Four wrong attributions this session were each killed by one measurement. A failure should also explain itself: the harnesses capture the cluster's state at a timeout.
- **Prefer an independent oracle** to a reimplementation of the thing under test. The effect ledger is this project's; a differential test can share the implementation's blind spots.
- **Every trial asserts its preconditions against the cluster** before t=0, and a failure is INVALID with a reason rather than a silent exclusion.
- **Run-stamped inputs, with refusal.** One directory per run holding logs, rows, probes and the config; an analyzer refuses inputs without the current run id. Shared append-mode logs once made an analysis divide a four-run stall count by a one-run denominator.
- **Idempotency keys are globally unique and derived from the work item**, never from who sends it. Documented in the README and enforced by `2d9bb57`.
- **`DefaultConfig.CommandVersion` tracks `CommandVersionCurrent`**, pinned by a test, equality not `>=`.
- **No commits by the assistant.** Stage, hand over the diff stat and a message; Eva commits.

#### State of the results/ fold-in

**Done.** `results/2026-11-01-day11-reclaim-failover/` holds the reclaim 100, the bounded failover 100, the shipped-default 55, the superseded unbounded 100, the no-reset 45, the steady-state control, the interleaved A/B rows, the derived dispatch-vs-state plot, all five harnesses, and a README stating `-snapshot-entries` per run, the t=0 derivation, the preconditions, the INVALID and exclusion counts, the in-flight cap of 6 and why, and the three measurement caveats. Earlier Day 11 records are in `results/2026-10-30-day11-batching/` and `results/2026-10-30-day11-profile/`.

**Left:** nothing for the fold-in. UNSTAGED and uncommitted at handoff: this roadmap section, the README idempotency subsection, `results/2026-11-01-day11-reclaim-failover/`, and the remaining `docs/architecture.md` hunk (the ready-loop-stall rate attribution). Also uncommitted: `cmd/coordinator/main.go` and `internal/sim/sim_test.go`, which carry the `/debug/tasks` held-state fields and the per-seed goroutine instrumentation.

#### Open follow-ups

1. **Decouple apply from the Ready loop** — one fix behind two measured limitations: 6.3s restart replay, and the 133ms snapshot fsync tail. Subsumes most of follow-up 2. The hard part is the invariant: `appliedIndex` advances inside the same `c.mu` section as the mutation so no reader sees them skewed, and a separate apply goroutine must preserve that.
2. **Asynchronous snapshotting** — the narrower guarantee; doing it alone leaves the restart gap untouched.
3. **Task GC with a retention policy.** Terminal tasks are never collected, so state grows with throughput. Three couplings decide the policy: `GetJob` semantics, dedup sessions outliving the tasks they name, and the linearizability model having no operation for a task that vanishes.
4. **Eval coalescing, Nomad-style** — one pending eval per job. The reason plans stay near 7 is that every completion emits an eval for its own job; this is the change most likely to move that number.
5. **The unreproduced `ready-loop-stall`** — `TestSeedSweep` seed 4, 2 failures in 9 runs on a degraded host, 0 in 40 interleaved A/B runs on a rested one. **Attributed by RATE, not mechanism**; CPU contention specifically is NOT implicated (scheduler overshoot overlapped: 242µs failing against 139-274µs passing). A CPU-probe classifier is recorded as an idea only.
6. **Periodic worker pause**, and **the benchmark harness still takes no host probes on the batching arm**.

#### Next

Finish nothing — the fold-in is complete. Stage the remaining docs with Eva, then **Day 12: the writeup**. The résumé-facing numbers are in the table above; every one carries its `-snapshot-entries` and its caveats, and the writeup should carry them too.

### Day 12 — The asset (6 hrs)
README: architecture diagram, scope contract, benchmark table, verification results, both failure demos as terminal GIFs.
Writeup covering four things: the deterministic-apply / `Tick` problem; the eval-broker + optimistic-plan design and why it beats one-decision-per-round-trip; at-least-once with `(taskID, attempt)` idempotency; and what your verification does and does not prove.
Keep the last 2 hours as slack.

**The two demos that sell it:**
1. Submit a batch, `SIGKILL` the coordinator leader mid-flight → new leader rebuilds its broker and resumes, nothing lost or duplicated.
2. `SIGKILL` a worker mid-task → lease expires, task redispatches, exactly one observable result.

---

## Producing the résumé numbers

Each claim needs a defined measurement, not an estimate.

### Worker reclaim — "within 3s of a missed heartbeat"

A budget you design, then validate:

| Component | Setting | Contribution |
|---|---|---|
| Heartbeat interval `H` | 500ms | — |
| Lease TTL `L` (4 missed heartbeats) | 2000ms | 2000ms |
| `Tick` period `T` | 200ms | ≤200ms |
| Eval → plan → commit | — | ~10–50ms |
| **Worst case, last heartbeat → redispatch** | | **≈ 2.25s** |

**Define t=0 explicitly** — "last heartbeat received" and "first missed deadline" differ by a full lease, and reviewers will ask. Report **p50 / p95 / max over ≥50 kill trials** and quote the max, since "within 3s" is a bound.

### Failover — leader `SIGKILL` → first task dispatched by the new leader
p50/p95/max over ≥50 trials. State your `ElectionTick` and tick interval alongside the number, or it's uninterpretable. Note that your number includes eval-broker rebuild time, which is real and worth measuring separately.

### Effectively-once
Prose can't support this; the effect ledger can. Quote the shape of the evidence: *N tasks, M induced coordinator and worker failures, zero duplicate effects, zero lost tasks* — across both failure modes.

### Linearizability
*N operation histories checked under M injected partitions, zero violations, every failure reproducible from seed.*

### The bullet, with slots

> **Fault-Tolerant Coordinator & Task Scheduler** — Go, etcd/raft, gRPC, Protocol Buffers, Prometheus, Linux
>
> - Built a Raft-replicated control plane (etcd/raft) where job and task state live in the replicated log, so coordinator failover preserves the task queue — **[X]ms p50 / [Y]ms p99** to first redispatch across **[N]** induced leader kills.
> - Designed Nomad-style scheduling — evaluations, a priority eval broker, and plans verified under optimistic concurrency before commit — batching **[k]** assignments per consensus entry for a **[m]×** throughput gain (**[T] tasks/sec** sustained on a 3-node cluster) over one-decision-per-round-trip.
> - Implemented at-least-once dispatch with idempotent execution keyed on (task ID, attempt number), yielding effectively-once observable effects: **zero duplicate or lost effects across [N] tasks under [M] induced coordinator and worker failures**.
> - Verified correctness with seeded fault injection and Porcupine linearizability checking — **[N] histories under [M] injected partitions, zero violations**, every failure reproducible from a recorded seed.
> - Instrumented a gRPC/Protocol Buffers job-submission API and CLI with Prometheus metrics for throughput, queue depth, and failover latency.

### The bullet, as measured — SUPERSEDES the slots above

Written on Day 12 from `results/`, and it differs from the slotted version in four places on purpose.
Each number cites the directory holding its rows and its harness.

> **Fault-Tolerant Coordinator & Task Scheduler** — Go, etcd/raft, gRPC, Protocol Buffers, Prometheus
>
> - Built a Raft-replicated control plane (etcd/raft) where job and task state live in the replicated log, so coordinator failover preserves the task queue — **1441ms p50 / 2090ms p95** to first task dispatched by the new leader across **55** induced leader kills; worker death reclaimed at **2121ms p50 / 2246ms max** from last heartbeat over **100** kill trials (±10ms poll error), consistent with the designed 2.25s lease budget, and reassigned to another worker within **2280ms** worst case over the **77** trials where a reassignment was possible. *(`results/2026-11-01-day11-reclaim-failover/`)*
> - Designed Nomad-style scheduling — evaluations, a priority eval broker, and plans verified under optimistic concurrency before commit — cutting time to drain a fixed 2,000-task backlog from **62.1s to 18.0s (3.4×)** by removing 1,716 serial consensus round trips; measured plan size saturates near **7** assignments per entry, so the batching cap stops binding well below its setting. *(`results/2026-10-30-day11-batching/`)*
> - Implemented at-least-once dispatch with execution idempotent on (task ID, attempt), yielding effectively-once committed effects: **128,830 committed effects over 151,161 executions with zero duplicate commits**, and **zero lost tasks across the 63 of 64 runs that drained to quiescence**, under **1,770** coordinator kills and **7,986** partitions — including **308 genuinely overlapping executions**, so the idempotency path was exercised rather than assumed. *(`results/2026-10-30-checkpoint-additive/report.txt`; the 64th seed was truncated at step 308 by a synchronous-snapshot stall and contributes no claim)*
> - Verified with seeded fault injection and Porcupine linearizability checking: a stale read from a partitioned leader **occurs on 8 of 8 seeds with local reads and 0 of 8 behind a ReadIndex barrier** — 8 of 8 covered in both arms, so the read mode is the only variable — reproduced both against a pinned adversary and on the shipped configuration under a process-pause fault, which is what separates CheckQuorum narrowing the window from ReadIndex closing it. The harness also found an unreported `MsgSnap` failure that parked a follower in `StateSnapshot` and froze commits while the leader held its term: reproduced deterministically, fixed in the transport-agnostic layer, and the matching symptom went **4 of 64 seeds to 0 of 64** under the unchanged adversary. Every ordinary run's fault schedule reproduces from its seed (**25 of 25**).
> - Instrumented a gRPC/Protocol Buffers API and CLI with Prometheus metrics for throughput, queue depth, commit latency, phase-labelled Ready-loop stalls and failover latency; a pprof CPU profile of the leader under load then found an O(state) scan under the apply lock costing **5.19ms per long poll at 50,000 tasks** — 59% of samples in mutex wait at 18% CPU — fixed to a flat **72ns** by an index maintained inside `Apply`. *(`results/2026-10-30-day11-profile/`)*

**The four deliberate departures from the slots.**

1. **"[T] tasks/sec sustained" is gone.** The honest headline is time-to-drain a fixed backlog, because a rate-in-a-window measurement is floor-limited the moment an arm finishes early — which is exactly what the pilot did, reporting the backlog's rate as the system's.
2. **"[m]× throughput gain" is stated as a drain-time ratio, not a throughput ratio**, and the mechanism named is round trips rather than log volume. Log-entry reduction is 2.0× and that is its ceiling, since each completion is its own entry; the 3.4× came from the planner no longer being serial.
3. **"[N] histories, zero violations" is replaced by the targeted A/B.** The 64-seed checkpoint's 64 clean histories are 0-of-64 *covered*, so quoting them as linearizability evidence would be a misuse of the project's own reporting.
4. **p95 rather than p99**, and failover rather than "redispatch": at 55 trials a p99 is one observation.
5. **Reclaim and redispatch are two numbers, not one.** Reclaim is the dead worker releasing the task (2246ms max from last heartbeat, n=100); redispatch is another worker receiving it (2280ms max, n=77, because 23 trials lawfully had nothing left to dispatch once the task hit `MaxAttempts`). Quoting the reclaim figure as a redispatch is the kind of slip the results README exists to prevent.

**One claim in the verification bullet is deliberately bounded.** The parked-follower *mechanism* is proven and its fix is verified by a deterministic reproduction; the 4-of-64 → 0-of-64 pair is a RATE argument about the symptom, measured under the unchanged REPLACE adversary.
The five original frozen-clock windows were captured before the raft progress table was instrumented, so none of them shows a follower in `StateSnapshot`, and their attribution stays formally unproven.
If asked in an interview, that is the honest answer and it is a better one than the bullet.

### The 4-line résumé version

This is the one that goes on the résumé.
The long version above is the interview backing for it, and `README.md` is the backing for that.

> **Fault-Tolerant Coordinator** — Go, etcd/raft, gRPC, Protocol Buffers, Prometheus
> - Built a Raft-replicated job coordinator where all scheduling state lives in the log; leader failover reaches first redispatch in 1.44s p50 / 2.09s p95 over 55 kills, and a dead worker's task is released in 2.12s p50 over 100 kills, consistent with a 2.25s lease budget.
> - Designed Nomad-style scheduling (eval broker, parallel planners, optimistic-concurrency plan verification), batching assignments per consensus entry to cut backlog drain 3.4× (62s → 18s, 2,000 tasks) by removing serial commit round trips.
> - Built a seeded fault-injection simulator with Porcupine linearizability checking and an effect ledger: zero duplicate commits across 64 runs (1,770 node kills, 7,986 partitions); it caught a partitioned-leader stale read (8/8 → 0/8 with ReadIndex) and a liveness defect where unreported snapshot failures parked a follower (matching symptom 4/64 → 0/64 runs after the fix).
> - Profiled an O(state) scan under the apply lock that destabilized the cluster at 10k tasks; a worker→task index cut the long-poll read from 5.19ms to 72ns at 50k tasks.

**Two phrasings to be ready to defend**, since the compression hides a caveat each.

- *"leader failover reaches first redispatch in 1.44s p50"* — that is the **shipped** `-snapshot-entries 10000`, and the figure is sensitive to it. The controlled comparison: with compaction off, failover degraded **4.2× (1127 → 4690ms) over the 55-trial reproduction**; under the shipped default, **+12% (1384 → 1554ms) over 55 trials**. The honest one-line answer is "bounded by compaction, not removed by it", and the mechanism is restart replay inside the Ready loop. Note that the **45-trial no-reset run is the flat one** (986–2062ms, ratio 1.07) — it is the evidence that restart cycles alone degrade nothing, so it is the wrong run to cite for the 4.2×.
- *"zero duplicate commits across 64 runs"* — the committed-effect claim covers all 64; the **no-lost-task** claim covers the 63 that drained to quiescence. If someone asks "across 64 runs of what?", the answer is 600 steps each, 2h47m of cumulative run time, 128,830 committed effects over 151,161 executions.


## The "why" — how to talk about this project

**Why a coordinator.**
It's the smallest system that forces every hard distributed-systems problem at once — consensus, failure detection, delivery semantics, idempotency, backpressure — while having a *crisp, mechanically checkable* correctness criterion: no task lost, no effect twice. Most side projects can't tell you whether they're correct. This one can, under induced failure. That's why the verification phase carries as much weight as the coordinator itself.

**Why use a Raft library instead of writing one.**
Because the interesting work is above consensus, not inside it — and knowing *when not to build something* is the more valuable judgment. The honest framing: I read the paper closely enough to reason about the safety properties, then used the library the industry uses, and spent the time on the parts that were actually mine to get right. If pushed on internals, you have the four questions — including why a majority-replicated prior-term entry still isn't safe to commit, and why that bug can hide behind a hundred green test runs.

**What it demonstrates that a CRUD project can't.**
Reasoning about partial failure — the thing separating distributed systems from concurrency.
Determinism constraints in replicated state machines (the `Tick` problem).
The difference between at-least-once delivery and effectively-once effects, the most commonly muddled idea in the space.
Throughput architecture above consensus — why you batch decisions into plans instead of paying a round-trip each.
And measurement and verification discipline: open-loop load, percentiles over averages, linearizability checking, reproducible seeds.

**The 60-second version, out loud:**
> "A fault-tolerant coordinator whose control plane is a replicated state machine on etcd/raft. Job and task state live in the Raft log, so when the leader dies a new one picks up mid-flight — [X]ms p50 to first redispatch. The scheduling design is modeled on Nomad: state changes create evaluations, scheduler workers compute plans against a snapshot in parallel, and a single-threaded planner verifies each plan against current state before committing it — so the expensive part is concurrent and only the commit serializes, which batches [k] assignments per consensus entry and buys about [m]× throughput. Delivery is at-least-once but execution is idempotent on task ID and attempt, so effects are effectively-once — verified with a Porcupine linearizability checker running under seeded fault injection, [N] histories, zero violations, every failure reproducible from its seed. The subtlest thing I hit was that you can't read the wall clock in the apply path, because it runs on every replica and has to be deterministic — so lease expiry is driven by a timestamp the leader writes into the log itself."

**Questions to answer cold:**
Why not two-phase commit? Why can't you get true exactly-once? What happens if a worker completes a task but the leader dies before the completion commits? Why is the eval broker derived rather than replicated? What does optimistic concurrency buy you, and when does it lose? What's your write throughput bounded by? Why 3 nodes rather than 5? What breaks first as workers scale? What does Porcupine *not* prove?

**The limits, stated first rather than extracted.**
Moved here from the writeup, because this is the paragraph to open with when someone asks how good the system actually is.

> Correct and stall-free to at least 150,000 tasks, usefully fast to about 50,000.
> Failover is ~1.4s p50 on the shipped configuration, and that figure is **bounded by log compaction, not made safe by it**: a restarting node applies its backlog inside the Ready loop and can neither vote nor acknowledge for the whole window — 6.3 seconds for 23,500 entries with compaction disabled.
> Snapshots are synchronous and expose the Ready loop to disk tail latency.
> Every "max" is an observed worst case from a finite trial count on one host, never a bound.
> One simulator red remains attributed to host degradation by rate comparison, with no mechanism found and none claimed.

Leading with that is stronger than being walked to it, and it earns the numbers that come after.

---

## If you fall behind

Never cut Day 6 or Day 12 — a Raft-less coordinator doesn't tell the story, and an undocumented one doesn't get read.

1. **Day 7 schema versioning.** Document the gap.
2. **Day 9 Porcupine.** Painful to lose — it's the differentiator — but Day 10's invariant checker carries a weaker version of the claim.
3. **Day 4's planner sophistication.** A plan that batches assignments without full conflict verification still gives you the batching win; note the simplification.
4. The worker-count sweep — one config measured well beats five measured badly.
5. Grafana. The benchmark table carries it.

**If you're ahead:** implement `ReadIndex` for linearizable reads and show Porcupine going from violation to clean. That's a beautiful before/after.

---

## How to know you're on track

- **End of Day 1:** a 3-node replicated counter surviving leader kill and restart-from-disk.
- **End of Day 3:** kill a worker, task reschedules and completes. Demoable.
- **End of Day 4:** a 100-assignment plan commits as one entry; a stale plan is rejected.
- **End of Day 6:** kill the leader mid-flight, nothing lost or duplicated.
- **End of Day 10:** hours of seeded chaos, zero invariant and zero linearizability violations.
- **End of Day 12:** every bracket in the bullet holds a number you measured and can defend.

---

## What comes after

[v3 — Distributed Inference Gateway](distributed-inference-gateway-roadmap-v3.md).

The reason to name this project a *coordinator* rather than a task scheduler: v3 keeps the etcd/raft control plane, the worker pool and lease-based failure detection, the idempotency machinery, the seeded fault injector, the Porcupine harness, and the Prometheus setup — and points all of it at a completely different workload, routing LLM inference requests across a pool of model servers.

That reuse is the strongest thing the pair says about you. One project proves you can build a replicated control plane. Two prove you built the *right* abstraction, because the second one didn't have to rebuild it.

---

## References

**Consensus:**
- [etcd/raft](https://github.com/etcd-io/raft) · [README and Ready contract](https://github.com/etcd-io/raft/blob/main/README.md) · [godoc](https://pkg.go.dev/go.etcd.io/raft/v3)
- [The Raft paper](https://raft.github.io/raft.pdf) — Figure 2 is the spec; §5.4.2 and Figure 8 are the ones worth rereading
- [eliben/raft](https://github.com/eliben/raft) and [the series](https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/) — the clearest readable Go implementation if you want to see one

**Architecture:**
- [Nomad's control plane](https://deepwiki.com/hashicorp/nomad/2-server-(control-plane)) — evaluations, eval broker, plan queue, leader-only components
- [gRPC for Go](https://grpc.io/docs/languages/go/) · [Protocol Buffers](https://protobuf.dev/)

**Verification:**
- [Porcupine](https://github.com/anishathalye/porcupine) · [Testing Distributed Systems for Linearizability](https://anishathalye.com/testing-distributed-systems-for-linearizability/) · [Checking linearizability in Go](https://notes.eatonphil.com/2024-10-31-checking-linearizability-in-go.html)
- [What's the big deal about DST?](https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html) — read before writing the README's honesty paragraph
- [Testing Distributed Systems](https://asatarin.github.io/testing-distributed-systems/) — the curated list, if you want to go deeper

**Measurement:**
- [hdrhistogram-go](https://github.com/HdrHistogram/hdrhistogram-go) · [prometheus/client_golang](https://github.com/prometheus/client_golang)
