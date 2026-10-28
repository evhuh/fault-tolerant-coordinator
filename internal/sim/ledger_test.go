package sim

import (
	"context"
	"testing"
)

// THE FIRST LEDGER RESULT.
//
// This is the assertion Day 5's effectively-once design rests on and nothing
// tested until now: across however many attempts executed, exactly ONE had
// its result committed, it was the attempt the coordinator recorded as
// completing, and its result directory is on that worker's disk.
//
// It runs over real workers, because a ledger written by a simulated worker
// is evidence about the simulator.
//
// Overlap and late reports are MEASURED, never asserted on. Two workers
// executing one task is at-least-once working as designed — preventing it
// would need fencing, which this system does not have and does not claim — so
// a nonzero overlap is evidence the idempotency was exercised and a zero
// makes the claim inconclusive for that run.
func TestEffectLedger(t *testing.T) {
	// GATED like the adversary sweeps, and for the same reason: real workers
	// fork and exec every attempt, a third of tasks sleep six leases, and
	// quiescence then waits for the backlog — about 70s for three seeds,
	// against a package that already runs for three minutes. What guards the
	// behaviour on every commit is the deterministic scenario in
	// internal/server (TestRunningTaskIsReclaimedWhenItsWorkerStopsClaimingIt
	// and the stale-report tests); this is validation that the ledger can see
	// the situation at all.
	requireSweeps(t)

	var effects, overlaps, execs, stale, dup int
	drained, runs := 0, 0
	var overlapPerRun, stalePerRun []int
	earlyExits := 0
	downgradedStalls, hostIOStalls := 0, 0

	// EIGHT SEEDS, to measure the distribution rather than sample it. Three
	// was enough to show the assertions hold and far too few to say anything
	// about coverage: two successive sweeps of the same code measured 5
	// overlaps and then 1, which is the spread, not a change.
	//
	// NO FLOOR IS SET HERE YET. The per-seed columns below are the
	// measurement a floor would have to be calibrated from, and setting one
	// off the first promising sweep is how a floor ends up asserting the
	// weather.
	const seeds = 8
	for seed := int64(1); seed <= seeds; seed++ {
		// LONGER THAN THE OTHER SWEEPS, and it has to be. The sequence this
		// measures is: a slow task starts (2.5 leases of sleep), its worker
		// freezes, the lease lapses (one lease), the planner redispatches,
		// and only then do two executions overlap. At ten steps of 60ms the
		// whole run was 600ms — two leases — so the sequence could not
		// finish inside it and the first attempt measured zero overlap with
		// the pause firing but never waking.
		cfg := quickConfig(seed, 40)
		cfg.RealWorkers = true
		cfg.LinClients = 0
		cfg.WorkerDir = t.TempDir()

		res, err := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
		l := res.Ledger
		runs++
		w := res.WorkerPause
		t.Logf("seed %d: worker pause fired=%v step=%d worker=%s attempt=%d resumed=%d reclaimed=%v",
			seed, w.Fired, w.Step, w.Worker, w.Attempt, w.Resumed, w.Reclaimed)
		t.Logf("seed %d: committed effects=%d | executions=%d overlaps=%d max-concurrent=%d | stale-rejected=%d duplicate-ignored=%d | drained=%v undrained=%d in %s",
			seed, l.CommittedEffects, l.Executions, l.Overlaps, l.MaxConcurrent,
			l.StaleRejected, l.DuplicateIgnored, l.Drained, l.Undrained,
			l.DrainWaited.Round(1e6))
		t.Logf("seed %d: terminal with no effect, by attempt budget=%d (correct, and measured)",
			seed, l.NoEffectByBudget)
		// STALL ACCOUNTING ON EVERY ROW. A downgraded stall is a run the
		// harness could not attribute, and a sweep that quotes rates over
		// such runs is quoting the host. Printed whether or not it is zero,
		// because a zero is the thing being claimed.
		t.Logf("seed %d: stalls: host-io=%d unattributable=%d",
			seed, res.HostIOStalls, res.DowngradedStalls)
		downgradedStalls += res.DowngradedStalls
		hostIOStalls += res.HostIOStalls
		t.Logf("seed %d: quiesced=%v drain budget %s | run outcome: %v",
			seed, l.Quiesced, l.DrainBound, err)

		if !l.Checked {
			t.Fatalf("seed %d: the ledger did not run", seed)
		}
		// THE VERDICT COVERS EVERY REAL VIOLATION, not only this test's own.
		//
		// This is the one place the each-test-asserts-only-its-own-property
		// rule has to bend, and it bends because of what this test
		// PUBLISHES. It reports distributions that a floor will be
		// calibrated from, and a run that ended on a genuine violation
		// stopped early: one sweep's seed 8 contributed 11 executions and 6
		// effects against a normal run's ~100 and ~70, and the sweep still
		// said PASS. That is not a test ignoring somebody else's property,
		// it is a measurement quietly averaging in a run that did not
		// happen.
		//
		// INCONCLUSIVE violations stay reported rather than fatal — a host
		// whose fsync blew past the run's timings, a run that left the fault
		// model, a window that never opened. They say the harness could not
		// look, which is exactly what the coverage columns above are for.
		for _, v := range res.Violations {
			if v.Inconclusive {
				t.Logf("seed %d: INCONCLUSIVE %v", seed, v)
				continue
			}
			t.Errorf("seed %d ended on a real violation, so its ledger numbers are a partial run: %v", seed, v)
		}
		if !l.Quiesced {
			earlyExits++
			t.Logf("seed %d: ENDED BEFORE QUIESCENCE — its executions and effects are a prefix, and its zeros in the distributions below are not evidence of anything",
				seed)
		}

		effects += l.CommittedEffects
		overlaps += l.Overlaps
		overlapPerRun = append(overlapPerRun, l.Overlaps)
		stalePerRun = append(stalePerRun, l.StaleRejected)
		execs += l.Executions
		stale += l.StaleRejected
		dup += l.DuplicateIgnored
		if l.Drained {
			drained++
		}
	}

	t.Logf("TOTAL over %d runs: committed effects=%d executions=%d overlaps=%d stale-rejected=%d duplicate-ignored=%d drained=%d/%d",
		runs, effects, execs, overlaps, stale, dup, drained, runs)
	// The two distributions a floor would be calibrated from, printed as
	// counts per run rather than as a mean: "0.9 overlaps per run" hides
	// whether half the runs measured none, and that is the only number that
	// matters for a floor.
	t.Logf("DISTRIBUTION overlaps per run: %v", overlapPerRun)
	t.Logf("DISTRIBUTION stale rejections per run: %v", stalePerRun)
	t.Logf("runs that reached overlap: %d/%d | runs that saw a late report: %d/%d",
		countNonZero(overlapPerRun), runs, countNonZero(stalePerRun), runs)
	// Printed next to the distributions, always, because a distribution that
	// silently includes truncated runs reads lower than the truth and the
	// reader cannot tell.
	t.Logf("runs that ended before quiescence: %d/%d", earlyExits, runs)
	t.Logf("STALLS across the sweep: %d host-io, %d unattributable%s",
		hostIOStalls, downgradedStalls, quotableNote(hostIOStalls, downgradedStalls))

	if execs == 0 {
		t.Fatal("no attempt executed at all: the ledger checked nothing")
	}
	// Reported, not asserted — but a sweep where this is zero is a sweep that
	// says nothing about effectively-once, and that has to be visible.
	if overlaps == 0 {
		t.Logf("INCONCLUSIVE for effectively-once: no overlapping executions across %d runs; the idempotency was never exercised", runs)
	}
	if stale == 0 {
		t.Logf("INCONCLUSIVE for the (taskID, attempt) rejection: no late report reached a coordinator across %d runs", runs)
	}
}

// The worker pause must reproduce its own aim.
//
// Mirrors TestReplayWindowReproducesTheRealizedWindow, for the same reason and
// with the same honesty about what is reproduced. The injection is REACTIVE:
// it looks for an attempt the payload log shows in flight and defers when
// there is none, so the step it lands on and the worker it freezes depend on
// timing rather than on the seed. The seed reproduces the INTENT; only the
// recorded realization reproduces the aim.
//
// Without this, "Result.WorkerPause can be replayed" is an unverified claim in
// a comment — which is exactly what the window's replay was until it got a
// test.
func TestReplayWorkerPauseReproducesTheRealizedPause(t *testing.T) {
	requireSweeps(t)

	base := func() Config {
		cfg := quickConfig(4242, 20)
		cfg.RealWorkers = true
		cfg.LinClients = 0
		cfg.WorkerDir = t.TempDir()
		return cfg
	}

	first, _ := New(base(), dirsFor(t, 3)).Run(context.Background())
	if !first.WorkerPause.Fired {
		t.Skipf("no worker was executing at any step after the scheduled one, so there is nothing to replay: %+v",
			first.WorkerPause)
	}
	t.Logf("realized: %+v", first.WorkerPause)

	replayCfg := base()
	plan := first.WorkerPause
	replayCfg.ReplayWorkerPause = &plan
	second, _ := New(replayCfg, dirsFor(t, 3)).Run(context.Background())

	if !second.WorkerPause.Fired {
		t.Fatalf("the replayed pause never fired: %+v", second.WorkerPause)
	}
	// STEP AND VICTIM, not the whole struct. Resumed is a step number and
	// Reclaimed is an outcome of the live cluster — asserting those would be
	// asserting identical execution, which replay explicitly does not claim.
	if second.WorkerPause.Step != first.WorkerPause.Step ||
		second.WorkerPause.Worker != first.WorkerPause.Worker {
		t.Errorf("replay aimed somewhere else:\n  first:  step %d worker %s\n  second: step %d worker %s",
			first.WorkerPause.Step, first.WorkerPause.Worker,
			second.WorkerPause.Step, second.WorkerPause.Worker)
	}
}

func countNonZero(xs []int) int {
	n := 0
	for _, x := range xs {
		if x > 0 {
			n++
		}
	}
	return n
}

// quotableNote says plainly whether the sweep's numbers can be quoted.
//
// A sweep with host-io stalls measured a host that could not sustain the
// run's compressed timings, so its rates describe the disk as much as the
// system. Saying so next to the number is cheaper than discovering it later
// from a figure already in a README.
func quotableNote(hostIO, downgraded int) string {
	if hostIO == 0 && downgraded == 0 {
		return " — the host sustained the run's timings, so these rates are quotable"
	}
	return " — THE HOST DID NOT SUSTAIN THE RUN'S TIMINGS; re-measure on a quiet machine before quoting these rates"
}
