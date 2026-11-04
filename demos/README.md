# The two demos

Both run against a **real 3-node cluster** — three `cmd/coordinator` processes, raft over HTTP, workers over gRPC, no simulator in the path.
Each one kills a process with `SIGKILL` and then checks what it claims rather than narrating it.

| | shows | script | tape |
|---|---|---|---|
| **Failover** | leader dies mid-flight, a survivor resumes the queue, every task accounted for | `failover.sh` | `failover.tape` |
| **Reclaim** | worker dies mid-task, its lease expires, the task redispatches as attempt 2 | `reclaim.sh` | `reclaim.tape` |

## Running them by hand

```sh
make bin
./demos/up.sh 3 4          # 3 workers, concurrency 4
TASKS=24 ./demos/failover.sh
./demos/down.sh

./demos/up.sh 2 2
./demos/reclaim.sh
./demos/down.sh
```

Everything a run writes lands in `demos/run/` (WALs, worker data, logs, pidfiles), which is gitignored and deleted by `down.sh`.
`./demos/down.sh --keep-logs` keeps the logs and deletes only the state.

## Recording the GIFs

```sh
brew install vhs            # also installs ttyd and ffmpeg
vhs demos/failover.tape     # writes demos/failover.gif
vhs demos/reclaim.tape      # writes demos/reclaim.gif
```

Run `vhs` from the **repository root**, not from `demos/` — the tapes `cd` to the git root themselves, and the `Output` paths are repo-relative.

The tapes need **vhs ≥ 0.7** for `Wait`, which holds the recording until the shell prompt returns.
A fixed `Sleep` would be the alternative, and it is worse in both directions: too short truncates the demo on a slow run, too long pads every GIF with dead air.
Build and cluster startup are inside `Hide` blocks, so the recording opens on the demo itself.

Expect roughly 40–70 seconds of real time per GIF, which is a large file.
If you need them smaller, cut `TASKS` in `failover.tape` rather than speeding up playback — a demo whose timings are visibly wrong undercuts the numbers it is there to support.

## What the demos do and do not establish

They are **one trial each, on one host**.
The distributions are in [`results/2026-11-01-day11-reclaim-failover/`](../results/2026-11-01-day11-reclaim-failover/): failover p50 1441ms / p95 2090ms over 55 trials, reclaim p50 2121ms / max 2246ms over 100 trials.
The demo prints its single measurement and says so in its last line.

Three things the scripts check rather than assume, because each one has a failure mode that looks like success:

- **The victim is RUNNING its task, not merely ASSIGNED.** Killing a worker that was handed a task but has not started it demonstrates a dispatch race, not a lease expiry.
- **A free slot exists on another worker.** Without one, the redispatch leg measures queueing for a slot.
- **The kill took effect.** `kill -0` on the recorded pid, after the fact.

Two measurement notes the scripts print inline:

- **t=0 for reclaim is the last heartbeat RECEIVED**, derived as `lease_expiry_ms - 2000ms` from committed state *after* the kill. Read before the kill it is stale by up to one heartbeat interval, which overstated this figure by 606ms in the Day 11 trials.
- **Everything is polled**, at 10ms, so every number carries ±10ms. Reclaim and redispatch can land in the same poll iteration, because reassignment follows release by roughly 15ms.

## Two things that bit me writing these

**`sched_assignments_total` is leader-only and per-process.** A follower never increments it, and a new leader starts at zero, because the counter lives in the process rather than in the log. The first version read it from "any live node" and compared against a pre-kill baseline — which can never fire, so the demo reported no dispatch for twenty seconds while the survivor was dispatching fine.

**Node 1's metrics port is not a safe default** in a demo that kills a coordinator: node 1 may be the corpse, every read silently returns empty, and the final accounting reported "0 tasks terminal" about a cluster that had finished the job correctly. `live_metrics` asks for a node that answers.

Both are the same class of error as the `pkill -f` miss recorded in `docs/architecture.md` — **measure the mechanism before theorizing about it**, and print the state you are branching on.
