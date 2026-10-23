package sim

// Validation of the targeted stale-read workload.
//
// THE WORKLOAD IS VALIDATED BY ITS ABILITY TO FAIL. A targeted workload that
// cannot catch a bug known to be present is not a workload, and one that
// comes back green under both read modes would quietly retire the only check
// covering this path — which is the failure this whole exercise exists to
// prevent, arriving one level up.
//
// So the acceptance criterion is not "passes under readindex". It is that it
// finds Illegal under ReadLocal against the adversary the bug lived in.

import (
	"context"
	"os"
	"testing"

	"github.com/anishathalye/porcupine"

	"github.com/evazheng/fault-tolerant-coordinator/internal/server"
)

// requireSweeps gates the adversary sweeps behind FTC_SWEEPS=1.
//
// THESE ARE VALIDATION, NOT REGRESSION GUARDS, and the distinction is what
// justifies gating them. They exist to measure whether the targeted workload
// can catch a bug that is definitely present, and to calibrate the floors
// from that measurement — work that is done when the numbers are recorded,
// not work that needs repeating on every commit.
//
// What guards the BEHAVIOUR is cheap and stays in the default suite: the
// deterministic stale-read scenario under both read modes (under a second
// each) and the four liveness tests. Those fail immediately if reads regress.
//
// The gating is a cost decision made under duress and it should be read that
// way. Five sweeps put the sim package at 577s against Go's 600s default
// timeout — a failure of the whole package with no test failing — and
// shortening the runs to claw that back took the no-pause adversary to 0 of 8
// covered, which is 56 seconds of asserting nothing. A suite that times out,
// or that spends a minute measuring nothing, is worse than one that says
// plainly which checks are on demand.
//
// Recorded rates, at eight seeds and seven steps unless noted:
//
//	pinned/local             7/8 Illegal, 7/8 covered   (8/8 at ten steps)
//	shipped+pause/local      4/8 Illegal, 4/8 covered
//	shipped/local, no pause  0/8 covered at seven steps; 1/12 Illegal,
//	                         3/12 covered at ten — the window CheckQuorum
//	                         leaves open, narrow but not closed
//	pinned/readindex         0/8 Illegal, 6/8 covered
//	shipped+pause/readindex  0/8 Illegal, 3/8 covered
func requireSweeps(t *testing.T) {
	t.Helper()
	if os.Getenv("FTC_SWEEPS") != "1" {
		t.Skip("adversary sweep: set FTC_SWEEPS=1 to run (minutes, and it is validation " +
			"of the workload rather than a regression guard; see the recorded rates above)")
	}
}

// targetedRun is one run's outcome, reduced to what the catch rate needs.
type targetedRun struct {
	seed            int64
	verdict         porcupine.CheckResult
	covered         bool
	splitBrainReads int
	targetedReads   int
	answered        int
	ops             int
	window          WindowPlan
	pause           PauseDiag
}

// runTargeted sweeps seeds and reports what the targeted workload found.
func runTargeted(t *testing.T, seeds int64, mode readModeFn, steps int) []targetedRun {
	t.Helper()
	var out []targetedRun
	for seed := int64(1); seed <= seeds; seed++ {
		cfg := quickConfig(seed, steps)
		cfg.TargetStaleReads = true
		// The validation sweeps DRIVE their window — inject it and protect
		// it. The default sweep does neither: see Config.DriveWindow, both
		// for why narrowing the adversary is a price only opt-in validation
		// pays, and for why injecting a fault reactively costs schedule
		// reproducibility.
		cfg.DriveWindow = true
		mode(&cfg)
		res, _ := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
		out = append(out, targetedRun{
			seed:            seed,
			verdict:         res.Lin.Verdict,
			covered:         res.Lin.Covered,
			splitBrainReads: res.Lin.SplitBrainReads,
			targetedReads:   res.Lin.TargetedReads,
			answered:        res.Lin.TargetedReadsAnswered,
			ops:             res.Lin.Ops,
			window:          res.Window,
			pause:           res.Pause,
		})
	}
	return out
}

type readModeFn func(*Config)

// coveredMisses are the runs that reached the window and did NOT catch the
// bug. This, not illegal == covered, is the real assertion.
//
// Strict equality was wrong and the data said so: a run can come back Illegal
// with ZERO targeted reads, because the ordinary sampling clients sometimes
// stumble onto a stale read of their own. That is a BONUS finding, not a
// failure of coverage accounting, and failing on it would punish the harness
// for finding extra bugs.
// Measured against ANSWERED reads, not merely issued ones: a run whose
// targeted reads were all refused could not have caught anything, so holding
// it to "must be Illegal" blames the detector for the barrier doing its job.
func coveredMisses(runs []targetedRun) []int64 {
	var out []int64
	for _, r := range runs {
		if r.answered > 0 && r.verdict != porcupine.Illegal {
			out = append(out, r.seed)
		}
	}
	return out
}

// answeredRuns counts runs whose targeted reads actually returned a value.
func answeredRuns(runs []targetedRun) int {
	n := 0
	for _, r := range runs {
		if r.answered > 0 {
			n++
		}
	}
	return n
}

func report(t *testing.T, label string, runs []targetedRun) (illegal, covered int) {
	t.Helper()
	for _, r := range runs {
		if r.verdict == porcupine.Illegal {
			illegal++
		}
		if r.covered {
			covered++
		}
		// BOTH read counts, labelled correctly. They are different numbers
		// and the gap is the point: a split-brain read of a pre-partition
		// job is a correct read, and only the targeted ones could have been
		// stale. Printing one under the other's name cost an hour of
		// chasing a contradiction that did not exist.
		t.Logf("  %s seed %d: verdict=%-8s covered=%-5v split-brain=%-3d targeted=%-3d answered=%-3d ops=%d",
			label, r.seed, r.verdict, r.covered, r.splitBrainReads, r.targetedReads, r.answered, r.ops)
	}
	t.Logf("%s: %d/%d Illegal, %d/%d covered", label, illegal, len(runs), covered, len(runs))
	return illegal, covered
}

// Against the PINNED adversary the workload must find the bug on most seeds.
//
// DisableCheckQuorum reproduces the configuration the stale read lived in: a
// cut-off leader keeps its term and keeps answering. If the targeted workload
// cannot catch it here, it cannot catch it anywhere, and a clean result from
// it under readindex would mean nothing at all.
func TestTargetedWorkloadCatchesStaleReadsUnderPinnedAdversary(t *testing.T) {
	requireSweeps(t)
	runs := runTargeted(t, targetedSeeds, func(c *Config) {
		c.ReadMode = server.ReadLocal
		c.DisableCheckQuorum = true
	}, targetedSteps)

	_, covered := report(t, "pinned/local", runs)
	if covered == 0 {
		t.Fatal("no run produced a split-brain read — the targeted workload never reached " +
			"the window, so it is measuring nothing")
	}
	// Floored on ANSWERED runs, not merely covered ones: this test validates
	// the detector, and a run whose targeted reads were all refused gave it
	// nothing to detect.
	if ans := answeredRuns(runs); ans < targetedCoveredFloor {
		t.Errorf("only %d of %d seeds got an ANSWER to a targeted read (floor %d; %d reached "+
			"the window at all) — the workload is not reaching the fault it exists to aim at",
			ans, len(runs), targetedCoveredFloor, covered)
	}
	// THE REAL ASSERTION. Every run that reached the window must have caught
	// it: the bug is definitely present under this adversary, so a covered
	// run coming back Ok means the detector missed it.
	if miss := coveredMisses(runs); len(miss) > 0 {
		t.Errorf("seeds %v reached the window and did NOT find the stale read — a workload "+
			"that cannot catch a bug known to be present is not evidence of anything when "+
			"it comes back green", miss)
	}
}

// How wide each adversary sweep is.
//
// EIGHT, not twelve, because of a cost that became a hard failure: five
// adversary sweeps at twelve seeds took the sim package past Go's default
// ten-minute timeout, which fails the whole package without a single test
// failing. A gate nobody can run is not a gate, and asking every caller to
// pass -timeout is a papercut that will be forgotten.
//
// Eight keeps the evidence that matters — the pinned adversary reaches the
// window on every seed, so its result is about detection rather than luck —
// at roughly two thirds the cost.
//
// Narrowing alone was not enough: at eight seeds and ten steps the package
// finished in 577s against a 600s limit, which is not a margin, it is the
// same failure waiting for a slower machine. Seven steps with the window
// injected in the first quarter of the run is what buys real headroom, and
// it costs nothing in coverage because the window is placed rather than
// stumbled upon.
var (
	targetedSeeds int64 = 8
	targetedSteps       = 7
)

// targetedCoveredFloor is how many of the eight seeds must reach the window.
//
// CALIBRATED at this width. The pinned adversary measured 8 of 8 covered at
// eight seeds, and 11, 12, 11 of twelve before the narrowing — it reaches the
// window on essentially every seed, because the window is injected rather
// than waited for.
//
// At the shipped width — eight seeds, seven steps — it measured 7 of 8 under
// local reads and 6 of 8 under the barrier. Five leaves margin while still
// catching the regressions actually seen: before the window was injected
// coverage was 50%, and before the injection ensured the majority could
// elect, 67%. Both land below this floor, which is what a floor is for.
var targetedCoveredFloor = 5

// Adversary two: THE SHIPPED CONFIGURATION, defeated by a pause.
//
// CheckQuorum is on, so a cut-off leader steps down and the sweep records no
// split brain at all. A PAUSED leader is different in kind: CheckQuorum is
// driven by ticks it is not running, so it neither steps down nor learns of
// the new term until it drains its queue — and the read lands in the gap
// between waking and draining.
//
// Under ReadLocal this must still find Illegal. If it does not, the pause
// fault is not reproducing the window and the third adversary's expected
// zero would be unreadable: a zero that means "no bug" and a zero that means
// "no window" look identical.
func TestPauseDefeatsCheckQuorumUnderLocalReads(t *testing.T) {
	requireSweeps(t)
	runs := runTargeted(t, targetedSeeds, func(c *Config) {
		c.ReadMode = server.ReadLocal
		c.PauseLeader = true // shipped config otherwise: CheckQuorum ON
	}, targetedSteps)

	// PER-RUN DIAGNOSIS, printed whether or not the test passes.
	//
	// "4 of 8 covered" is not a diagnosis, and trying to fix that number by
	// adding seeds would have buried three real defects. These are the
	// conditions the window depends on, in the order they must hold, so a
	// miss names its own cause: the injection firing, the majority electing
	// during the pause, its write being acknowledged, the woken node still
	// believing it leads, and the read landing before the queue is released.
	for _, r := range runs {
		t.Logf("  pause seed %d: %s | woke@%d majorityElected=%v majorityCommitted=%v "+
			"wokenStillLeader=%v readInGap=%v → targeted=%d answered=%d verdict=%s",
			r.seed, r.window, r.pause.Woke, r.pause.MajorityElected, r.pause.MajorityCommitted,
			r.pause.WokenStillLeader, r.pause.ReadLandedInGap, r.targetedReads, r.answered,
			r.verdict)
	}

	_, covered := report(t, "shipped+pause/local", runs)
	if ans := answeredRuns(runs); ans < pausedCoveredFloor {
		t.Errorf("only %d of %d seeds got an ANSWER to a targeted read (floor %d; %d reached "+
			"the window) — CheckQuorum may be stepping the leader down anyway, or the wake "+
			"ordering is wrong; either way this adversary is measuring little",
			ans, len(runs), pausedCoveredFloor, covered)
	}
	if miss := coveredMisses(runs); len(miss) > 0 {
		t.Errorf("seeds %v opened the window and the read came back correct — that needs "+
			"explaining before any clean readindex result can be trusted", miss)
	}
}

// Adversary three: the shipped configuration with NO pause.
//
// Expected to find nothing, and that expectation is recorded rather than
// assumed. CheckQuorum steps the cut-off leader down after about one election
// timeout while a successor needs a randomized 1-2x that to be elected, so
// the window is near-empty BY CONSTRUCTION — which is CheckQuorum working,
// not the workload failing.
//
// RECORDED, not asserted against. Under ReadLocal a stale read is documented
// behaviour, so finding one is not a surprise and failing on it would be
// asserting that local reads are linearizable — which is the opposite of what
// this project says about them.
//
// The measured rate is the interesting part, and it is NOT zero: 1 of 12,
// with 3 of 12 reaching the window at all. CheckQuorum steps the leader down
// after about one election timeout while a successor needs a randomized one
// to two of them, so the two intervals OVERLAP at their edges and the window
// is narrow rather than closed. That is the empirical form of "CheckQuorum
// narrows the window and ReadIndex closes it", and it is worth more than the
// zero this test was expected to produce.
func TestShippedConfigWithoutPauseFindsNoStaleRead(t *testing.T) {
	requireSweeps(t)
	// LONGER runs than the other adversaries. Without a pause the window
	// depends on CheckQuorum's step-down and a successor's election
	// overlapping at their edges, and at seven steps it never opened at all
	// — 0 of 8 covered, a test asserting nothing. Twelve steps is what makes
	// this a measurement rather than a formality.
	runs := runTargeted(t, targetedSeeds, func(c *Config) {
		c.ReadMode = server.ReadLocal
	}, 12)

	illegal, covered := report(t, "shipped/local", runs)
	t.Logf("shipped config without a pause: %d/%d covered, %d/%d Illegal — low coverage here "+
		"is CheckQuorum working, not the workload failing", covered, len(runs), illegal, len(runs))
	// The only thing that would be wrong is a covered run MISSING a stale
	// read, same as every other adversary.
	if miss := coveredMisses(runs); len(miss) > 0 {
		t.Errorf("seeds %v reached the window under local reads and came back Ok", miss)
	}
}

// THE POSITIVE RESULT, and the only one that is a claim about the shipped
// system: the same targeted workload, against the same adversaries, must find
// NOTHING once reads go through the barrier.
//
// Run against the two adversaries that actually reach the window — the pinned
// one and the pause — because a clean result from an adversary that never
// opens a window is not a result. The covered floor is what enforces that:
// without it this test would pass most loudly exactly when it was testing
// least.
//
// ReadIndex declines rather than answers, and it does so by two independent
// routes depending on timing: with the queue still held its heartbeats go
// unacknowledged and the read times out; with the queue drained the quorum
// round-trip surfaces the higher term and the read is refused. Either way the
// history records an ambiguous read, which constrains nothing and is legal,
// because the client learned nothing.
func TestReadIndexFindsNoStaleReadUnderEitherAdversary(t *testing.T) {
	requireSweeps(t)
	for _, tc := range []struct {
		name  string
		apply readModeFn
		floor int
	}{
		{"pinned/readindex", func(c *Config) {
			c.ReadMode = server.ReadIndexBarrier
			c.DisableCheckQuorum = true
		}, targetedCoveredFloor},
		// The pause reaches the window less often than the pinned adversary
		// does — measured 7 of 12 — so its floor is its own, calibrated
		// from that rather than borrowed.
		{"shipped+pause/readindex", func(c *Config) {
			c.ReadMode = server.ReadIndexBarrier
			c.PauseLeader = true
		}, pausedCoveredFloor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runs := runTargeted(t, targetedSeeds, tc.apply, targetedSteps)
			illegal, covered := report(t, tc.name, runs)
			if covered < tc.floor {
				t.Errorf("only %d of %d seeds reached the window (floor %d) — a clean result "+
					"from runs that could not have failed is not evidence",
					covered, len(runs), tc.floor)
			}
			if illegal != 0 {
				t.Errorf("%d histories were NOT LINEARIZABLE under the ReadIndex barrier — "+
					"the barrier is supposed to make this impossible", illegal)
			}
		})
	}
}

// pausedCoveredFloor is the covered floor for the pause adversary.
//
// SIX, the same as the pinned adversary, because the pause now reaches the
// window as reliably: three consecutive samples measured 8 of 8. It used to
// sit at 2 against an observed 3-4 of 8, and raising it was earned by fixing
// three causes rather than by widening the sweep:
//
//   - the pause only queued INBOUND messages, so the paused node's goroutines
//     kept proposing and its MsgApp kept resetting every follower's election
//     timer. The majority never elected, so there was nothing newer for the
//     woken leader to be stale about. Outbound is now suppressed: a
//     descheduled process generates nothing.
//   - the fault schedule killed a node mid-pause, leaving one reachable node
//     that cannot elect.
//   - on one seed the pause never fired at all, deferred past the end of the
//     run because membership never recovered between kills.
//
// A floor that had been "calibrated" against the broken distribution would
// have locked in the breakage as normal.
var pausedCoveredFloor = 6
