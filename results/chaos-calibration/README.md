# Floor recalibration under additive isolation

The two sweeps the chaos checkpoint's coverage floors are derived from, measured against the ADDITIVE isolation adversary.
The previous floors (160 and 200) were calibrated against the replace adversary and were void once isolation became additive.

- **Commit under test:** `29644d1` plus the additive-isolation change, which landed as `e633366` (`feat(sim): additive partition isolation`); this record landed as `0979e4f`.
- **Configuration:** 600 steps, real workers, `FTC_CHECKPOINT_MIN_OVERLAPS=0` and `FTC_CHECKPOINT_MIN_STALE=0` so the calibration could not fail on the numbers it exists to measure.
- **Approved floors:** 200 overlapping executions, 300 late reports rejected as stale, over 64 seeds x 600 steps.

## The measurement

| sweep | seeds | run-steps | overlaps | per 100 | stale | per 100 |
|---|---|---|---|---|---|---|
| A | 101–108 | 4,800 | 35 | 0.729 | 64 | 1.333 |
| B | 201–208 | 4,800 | 72 | 1.500 | 106 | 2.208 |
| pooled | both | 9,600 | 107 | 1.115 | 170 | 1.771 |

Per-run overlaps: A `[3 1 4 4 9 4 7 3]`, B `[5 7 12 10 14 5 5 14]`.
Per-run stale: A `[6 4 12 11 7 13 7 4]`, B `[13 11 17 16 15 6 10 18]`.

Pooled, that projects to about 428 overlaps and 680 stale over 64 seeds, and the floors sit near half of it.
**The floors hold even at the lower sweep's rate:** sweep A alone projects to about 280 and 512.
Dropping sweep A's richest seed as well still projects 238 overlaps.
The justification is deliberately not a count of standard deviations from the pooled mean — the A/B gap is about 2.3 standard errors, so 16 seeds probably understate the real spread.

## Health, and whether sweep A was perturbed

16 of 16 seeds completed all 600 steps, 16 of 16 reached quiescence and drained, `0 host-io` and `0 unattributable` stalls in both sweeps, worst snapshot 65ms against the 125ms threshold.
Held partitions therefore do not starve the drain, which was the open question about the harsher adversary.

**Sweep A was not perturbed, as far as the probes can tell.**
Three short verification runs were started on the machine during sweep A, inside seed 105's window — seed 105 ran 07:32:28 to 07:34:55 and the runs began at 07:33:00, 07:33:01 and 07:33:10.
Seed 105's fixed-compute probe read 12,509µs before and 9,590µs after, the second of which is the FASTEST of the eight seeds, and its scheduler overshoot of 142/138µs is at the best end of the sweep.
Contention would have made those readings slower, not faster.
Seed 105 is nonetheless sweep A's richest seed at 9 overlaps, so a causal link cannot be excluded from probe data alone; the robustness check above drops it and the floors still hold.

The 2x gap between the sweeps is seed luck rather than the host: mean fsync per seed was flat across both (2,775–3,370µs), fault counts were comparable (1,004 vs 1,032 partitions, 378 vs 386 leader partitions), and the compute probe averaged 13,295µs in A against 13,735µs in B.
Sweep B's pre-run host gate recorded a 78ms worst probe, a single outlier that left no trace in any seed.

## What is in here

- `rows-101.jsonl`, `rows-201.jsonl` — one durable JSON row per seed, 8 each.
- `report-101.txt`, `report-201.txt` — each sweep's host gate line, per-seed lines and full aggregate report.
- `faults-seed-NNN.log` — the injected fault schedule per seed, 16 files.
