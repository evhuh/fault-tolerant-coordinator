# The two demos

Both run against a **real 3-node cluster** — three `cmd/coordinator` processes, raft over HTTP, workers over gRPC, no simulator in the path.
Each one kills a process with `SIGKILL` and then checks what it claims rather than narrating it.

| | shows | script |
|---|---|---|
| **Failover** | leader dies mid-flight, a survivor resumes the queue, every task accounted for | `failover.sh` |
| **Reclaim** | worker dies mid-task, its lease expires, the task redispatches as attempt 2 | `reclaim.sh` |

A captured run of each is pasted into the top-level [README](../README.md#the-two-failure-demos) as **text**, because the output is the evidence and a code block can be read, searched, diffed and quoted.

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

## Capturing output to paste

```sh
./demos/up.sh 3 4 && TASKS=24 ./demos/failover.sh 2>&1 | sed 's/\x1b\[[0-9;]*m//g'
```

The `sed` strips the colour escapes, which are emitted only onto a terminal anyway.
`failover.sh`'s progress line redraws with `\r` on a terminal and prints nothing when redirected, so a captured run has one line per stage rather than every poll smeared onto one.

Only the task count varies between runs of the failover demo; keep `TASKS` small enough that the run finishes in well under a minute.

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
