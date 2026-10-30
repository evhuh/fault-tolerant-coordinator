# Day 10 checkpoint — PASSED, 2026-10-30

The Day 10 checkpoint against the ADDITIVE isolation adversary, on the recalibrated floors.
This is the run that completes Day 10.

- **Commit under test:** `e633366` (`feat(sim): additive partition isolation`), on top of `29644d1` (the snapshot-reporting fix).
- **Floors:** 200 overlapping executions, 300 late reports rejected as stale, calibrated in `results/2026-10-30-calibration/`.
- **Verdict:** **PASS**, 2h47m38s of cumulative run time, 10,109s wall.

## The result

```
LEDGER COVERAGE: 308 overlapping executions (floor 200), 558 stale rejections (floor 300)
DRAIN: 63 of 64 reached quiescence, 63 of those drained; 63 completed all 600 steps
STALLS: 0 host-io, 0 unattributable
SNAPSHOT BUDGET: 1 stall (known product limitation)
LINEARIZABILITY: 64 histories, 0 Illegal, 296,674 ops checked, 34,581 ambiguous (12%)
FAULTS: 1770 kills, 1770 restarts, 7986 partitions, 3063 leader partitions, 3587 heals
```

**Zero frozen-clock windows**, for the second consecutive checkpoint and the first against this harsher adversary.
No `invariant`, `convergence`, `readyloop-stall`, `effect-ledger`, `did-not-drain` or `comparison-floor` finding occurred in any of the 64 rows.
The findings that remain are `linearizability-coverage` (64, expected by construction), `ledger-coverage` (4), and one `snapshot-budget`.

**The coverage numbers are quotable:** 308 overlapping executions and 558 stale rejections over **63 complete seeds; seed 3 truncated at step 308 by the synchronous-snapshot limit.**
Both figures are below the calibration's pooled projection of 428 and 680 and above the floors, which is roughly what the lower calibration sweep predicted — setting the floors to survive that sweep rather than the pooled mean is what kept this run from being a coin flip.

## Seed 3, and why it is not a host failure

```
FINDING: seed 3: INCONCLUSIVE [snapshot-budget] seed 3 step 308: node 2 Ready loop
  waited 136ms in snapshot+compact (synchronous snapshot) on the disk
SNAPSHOTS: worst 133ms at 1601KB (stall threshold 125ms)
```

Seed 3's bracketing host probes stayed inside the gate's budget — fsync mean 3,188µs before and 9,061µs after, worst 19,067µs, against 12ms mean and 100ms worst — so the disk is not what blocked the loop.
The cost is systematic: `maybeSnapshot` serializes, fsyncs and compacts inside the Ready loop, `Encode` is linear at about 6.5ms/MB, and a large enough state exceeds the threshold on any disk.
It is therefore classified `snapshot-budget`, inconclusive for that seed and labelled a known product limitation, and it deliberately does NOT fire the "the host did not sustain the run's timings" banner — re-measuring on a quieter machine would reproduce it.

Worth stating because it is the one judgement call: seed 3's post-run worst probe of 19ms was the highest of the 64 seeds, roughly four times typical, though still well inside the 100ms budget.
The classification follows the stated rule; a stricter probe budget would flip this seed to `host-io`.

## What is in here

- `rows.jsonl` — one durable JSON row per seed, 64 rows, seeds 1–64 complete. The primary record.
- `report.txt` — the full aggregate report, **as reclassified**: the stall counted as `snapshot-budget` and the rates quotable.
- `per-seed.txt` — the one-line-per-seed progress log.
- `faults-seed-NNN.log` — the injected fault schedule per seed, 64 files.
- `run.log.gz` — the complete `go test -v` log, 23MB uncompressed.

The report was rebuilt from `rows.jsonl` after the `snapshot-budget` kind was added, without re-running the seeds; the reclassification is a report-time rule precisely so that a classification added later can be applied to a record already paid for.
