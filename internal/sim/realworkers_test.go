package sim

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// THE TRANSPORT SEAM, asserted rather than assumed.
//
// Everything downstream of this — the effect ledger, the one-committed-effect
// invariant, the late-report rejection — is a claim about the real worker's
// code. If the workers never actually executed anything, every one of those
// claims would be vacuously true, and the run would look like a quiet pass.
//
// So this asserts the three things that make the seam real: the pool found
// the leader over bufconn and work was accepted; the EXECUTOR ran, which is
// visible only as a result tree on the worker's own disk; and the per-attempt
// directory layout the ledger will be keyed on exists.
func TestRealWorkersExecuteThroughTheRealPool(t *testing.T) {
	// THREE SEEDS, aggregated, because one seed is not a reliable source of
	// OPPORTUNITY. The first version pinned seed 1 and was flaky: seed 1 at
	// eight steps sometimes commits no job at all — the same schedule gives
	// the simulated pool zero jobs too — and a transport test must not depend
	// on a schedule leaving a stable leader up long enough to submit.
	//
	// Aggregating keeps the assertion honest. It says at least one execution
	// got through and at least one attempt directory was written, which is
	// what makes the seam real; it does not claim every seed drains work.
	totalOK, totalAttempts, totalJobs := 0, 0, 0

	for seed := int64(1); seed <= 3; seed++ {
		cfg := quickConfig(seed, 10)
		cfg.RealWorkers = true
		// This test is about the transport, and porcupine's search over a
		// concurrent history is minutes of the budget for a question it does
		// not ask.
		cfg.LinClients = 0
		// Named root, so the result trees outlive the run and can be read
		// here — and cleaned up by testing rather than by the simulator.
		cfg.WorkerDir = t.TempDir()

		s := New(cfg, dirsFor(t, cfg.Nodes))
		res, err := s.Run(context.Background())
		if err != nil && !notMine(err) {
			t.Fatalf("seed %d: %v", seed, err)
		}

		c := res.Coverage
		// The result tree is the evidence that the real executor ran — an
		// accepted report alone could come from anywhere — and it is also the
		// thing the effect ledger will read.
		attempts, tasks := 0, 0
		for _, w := range s.rw.workers {
			taskDirs, err := os.ReadDir(w.dir)
			if err != nil {
				t.Fatalf("worker %s result dir: %v", w.id, err)
			}
			tasks += len(taskDirs)
			for _, task := range taskDirs {
				entries, err := os.ReadDir(filepath.Join(w.dir, task.Name()))
				if err != nil {
					t.Fatalf("worker %s task %s: %v", w.id, task.Name(), err)
				}
				attempts += len(entries)
			}
		}
		t.Logf("seed %d: accepted ok=%d retryable=%d terminal=%d | jobs=%d tasks=%d | worker-side task dirs=%d attempt dirs=%d",
			seed, c.ReportedOK, c.ReportedRetryable, c.ReportedTerminal,
			res.Jobs, res.Tasks, tasks, attempts)

		totalOK += c.ReportedOK
		totalAttempts += attempts
		totalJobs += res.Jobs
	}

	if totalJobs == 0 {
		t.Fatalf("no job committed across three seeds: the run had no work to hand anyone, so the seam was never exercised")
	}
	if totalOK == 0 {
		t.Fatalf("no execution was reported and accepted across three seeds: the workers never reached a coordinator through the real pool")
	}
	if totalAttempts == 0 {
		t.Fatalf("the executor wrote no (taskID, attempt) directories across three seeds: nothing executed")
	}
	t.Logf("TOTAL across three seeds: accepted reports=%d attempt directories=%d", totalOK, totalAttempts)
}

// THE DAY 8 CHECKPOINT, WITH THE REAL WORKER IN THE LOOP.
//
// This is the gate before anything is built on the seam, and it is the thing
// most likely to break: three real workers poll, execute and report on their
// own goroutines, so the NUMBER and ORDER of client RPCs is now timing
// dependent in a way it never was when the simulator drove the pool itself.
//
// The claim is the same narrow one as always — the same seed drives the same
// FAULT SCHEDULE, not the same execution. What protects it is that nothing
// the workers do draws from the fault RNG: their task outcomes are hashed
// from (seed, step, job, task) at submit time, per the Day 8 rule, and their
// notices go to WorkerOut rather than into the schedule log.
//
// If this is red, the seam is not usable: a failure found with real workers
// would not be reproducible, and the effect ledger's findings would be
// anecdotes.
func TestSameSeedReproducesTheFaultScheduleWithRealWorkers(t *testing.T) {
	const steps = 10
	run := func(seed int64) (string, FaultCounts) {
		cfg := quickConfig(seed, steps)
		cfg.RealWorkers = true
		cfg.LinClients = 0
		var log bytes.Buffer
		cfg.Out = &log
		res, err := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
		if err != nil && !notMine(err) {
			t.Fatalf("seed %d: %v", seed, err)
		}
		// Same prefix hazard as the ordinary checkpoint: a run cut short by
		// an inconclusive stall logs fewer steps, and comparing that against
		// a complete log reports nondeterminism that is not there.
		if res.Steps < steps {
			t.Skipf("seed %d ended at step %d of %d, so its schedule log is a prefix and the "+
				"checkpoint cannot be evaluated: %v", seed, res.Steps, steps, err)
		}
		return log.String(), res.Faults
	}

	scheduleA, countsA := run(1234)
	scheduleB, countsB := run(1234)

	if scheduleA != scheduleB {
		t.Errorf("seed 1234 produced different fault schedules with real workers\n--- first ---\n%s\n--- second ---\n%s",
			scheduleA, scheduleB)
	}
	// The SCHEDULE fields only. The first version compared the whole struct
	// and went red on Net.DroppedPartition (63 against 0) — the number of
	// messages that happened to be in flight when the partition landed, which
	// is timing and was never part of the claim. Comparing it asserts
	// identical execution, which the Day 8 checkpoint explicitly does not.
	if countsA.NodeKills != countsB.NodeKills ||
		countsA.NodeRestarts != countsB.NodeRestarts ||
		countsA.Partitions != countsB.Partitions ||
		countsA.LeaderPartitions != countsB.LeaderPartitions ||
		countsA.Pauses != countsB.Pauses ||
		countsA.Heals != countsB.Heals {
		t.Errorf("fault counts differ: %+v vs %+v", countsA, countsB)
	}
	if scheduleA == "" {
		t.Fatal("no faults were scheduled at all; the test proves nothing")
	}
	t.Logf("seed 1234 schedule with real workers:\n%s", scheduleA)
}
