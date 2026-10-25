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

	for seed := int64(1); seed <= 3; seed++ {
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
		t.Logf("seed %d: worker pause %+v", seed, w)
		t.Logf("seed %d: committed effects=%d | executions=%d overlaps=%d max-concurrent=%d | stale-rejected=%d duplicate-ignored=%d | drained=%v undrained=%d in %s",
			seed, l.CommittedEffects, l.Executions, l.Overlaps, l.MaxConcurrent,
			l.StaleRejected, l.DuplicateIgnored, l.Drained, l.Undrained,
			l.DrainWaited.Round(1e6))
		t.Logf("seed %d: drain budget %s", seed, l.DrainBound)

		if !l.Checked {
			t.Fatalf("seed %d: the ledger did not run", seed)
		}
		// This test owns the ledger's ASSERTIONS. The coverage kinds and
		// DID-NOT-DRAIN are its own measurements and are reported above, not
		// failed on; everything else is somebody else's property.
		for _, v := range res.Violations {
			if v.Kind == KindEffectLedger {
				t.Errorf("seed %d: %v", seed, v)
			}
		}
		// notMine now covers the ledger kinds too, since every other test
		// has to ignore them; this test owns KindEffectLedger and asserts on
		// it in the loop above, and its own coverage kinds are reported
		// rather than failed on.
		if err != nil && !notMine(err) {
			t.Fatalf("seed %d: %v", seed, err)
		}

		effects += l.CommittedEffects
		overlaps += l.Overlaps
		execs += l.Executions
		stale += l.StaleRejected
		dup += l.DuplicateIgnored
		if l.Drained {
			drained++
		}
	}

	t.Logf("TOTAL over %d runs: committed effects=%d executions=%d overlaps=%d stale-rejected=%d duplicate-ignored=%d drained=%d/%d",
		runs, effects, execs, overlaps, stale, dup, drained, runs)

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
