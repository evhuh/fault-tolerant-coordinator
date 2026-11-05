# Chaos checkpoint after the snapshot-reporting fix

The post-fix attribution run for the snapshot-reporting defect.
Its one job was to answer whether the frozen-clock failure recurs once a dropped `MsgSnap` is reported, under the *same* adversary that produced it.

- **Commit under test:** `29644d1` — `fix(raft): report snapshot outcomes, or a dropped MsgSnap parks a follower`, with no other change in the working tree.
- **Adversary:** REPLACE partition semantics (`simnet.Partition` replaces the isolated set), i.e. the old behaviour, deliberately unchanged so the snapshot fix is the only variable.
- **Verdict:** PASS, 9426.17s wall, 2h36m20s of cumulative run time.

## The invocation

```
FTC_CHECKPOINT=1 \
FTC_CHECKPOINT_SEEDS=64 \
FTC_CHECKPOINT_STEPS=600 \
FTC_CHECKPOINT_MIN_OVERLAPS=160 \
FTC_CHECKPOINT_MIN_STALE=200 \
FTC_CHECKPOINT_OUT=/tmp/ftc-checkpoint/rows.jsonl \
  go test -count=1 ./internal/sim/ -run TestDay10Checkpoint -v -timeout 300m
```

Run under `caffeinate -dimsu` with nothing else on the machine.
Host gate before seed 1: fsync x40, mean 3.276ms, worst 6.762ms, against a budget of 12ms mean and 100ms worst.

## The headline

**Zero frozen-clock windows across 64 seeds**, against 4 of 64 before the fix.
Every finding in all 64 rows is INCONCLUSIVE, of exactly three kinds: `linearizability-coverage` (64, expected by construction), `ledger-coverage` (6), and `host-io` (1).
No `invariant`, `convergence`, `readyloop-stall`, `effect-ledger`, `did-not-drain` or `comparison-floor` finding occurred.

**Coverage floors were met but the numbers are NOT quotable:** 299 overlapping executions against a floor of 160, and 467 late reports rejected as stale against a floor of 200.
Seed 54 took a 175ms host-I/O stall in `persist` and ended at step 343 of 600, which trips the harness's own rule that rates measured on a host that did not sustain the run's timings must be re-measured before being quoted.
These figures are superseded by the checkpoint under additive isolation; quotable coverage comes from there.

## What is in here

- `rows.jsonl` — one durable JSON row per seed, 64 rows, seeds 1 through 64 complete. The primary record.
- `faults-seed-NNN.log` — the injected fault schedule for each seed, one file per seed.
- `report.txt` — the harness's host gate line and full aggregate report, including every per-seed finding.
- `per-seed.txt` — the one-line-per-seed progress log.

The full 23MB `go test -v` log, mostly raft INFO chatter, is not tracked.
Everything in it that bears on a finding is in `report.txt` and the per-seed fault logs.
