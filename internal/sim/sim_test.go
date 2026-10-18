package sim

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/simnet"
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
)

func dirsFor(t *testing.T, n int) map[uint64]string {
	t.Helper()
	out := map[uint64]string{}
	for i := 1; i <= n; i++ {
		out[uint64(i)] = t.TempDir()
	}
	return out
}

func quickConfig(seed int64, steps int) Config {
	cfg := DefaultConfig()
	cfg.Seed = seed
	cfg.Steps = steps
	cfg.StepTime = 60 * time.Millisecond
	cfg.Faults = simnet.Faults{DropRate: 0.02, MaxDelay: 2 * time.Millisecond}
	return cfg
}

// A short run must finish, and finish CLEAN. Anything that wedges shows up
// here as a test timeout with a full goroutine dump, which is how the
// leadership-revocation deadlock was found.
func TestShortRunCompletes(t *testing.T) {
	cfg := quickConfig(42, 10)
	res, err := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
	if err != nil {
		t.Fatalf("seed %d: %v", cfg.Seed, err)
	}
	if len(res.Violations) != 0 {
		t.Errorf("violations: %v", res.Violations)
	}
	t.Logf("seed=%d kills=%d restarts=%d partitions=%d heals=%d jobs=%d tasks=%d",
		res.Seed, res.Faults.NodeKills, res.Faults.NodeRestarts,
		res.Faults.Partitions, res.Faults.Heals, res.Jobs, res.Tasks)
}

// THE DAY 8 CHECKPOINT: a seed reproduces the same fault schedule.
//
// Note precisely what is asserted — the SCHEDULE, not the execution. Two runs
// of one seed kill the same nodes at the same steps, partition the same
// nodes, and heal at the same points. They do not produce identical state:
// goroutine scheduling decides who wins each race, and no seeding changes
// that in Go.
//
// That weaker claim is still the one that matters. A failure you can put the
// same adversary back in front of is investigable; one you cannot is an
// anecdote.
func TestSameSeedReproducesTheFaultSchedule(t *testing.T) {
	run := func(seed int64) (string, FaultCounts) {
		cfg := quickConfig(seed, 14)
		var log bytes.Buffer
		cfg.Out = &log
		res, err := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		return log.String(), res.Faults
	}

	scheduleA, countsA := run(1234)
	scheduleB, countsB := run(1234)

	if scheduleA != scheduleB {
		t.Errorf("seed 1234 produced different fault schedules\n--- first ---\n%s\n--- second ---\n%s",
			scheduleA, scheduleB)
	}
	if countsA.NodeKills != countsB.NodeKills ||
		countsA.Partitions != countsB.Partitions ||
		countsA.Heals != countsB.Heals ||
		countsA.NodeRestarts != countsB.NodeRestarts {
		t.Errorf("fault counts differ: %+v vs %+v", countsA, countsB)
	}
	if scheduleA == "" {
		t.Fatal("no faults were scheduled at all; the test proves nothing")
	}

	// A different seed must actually differ, or the assertion above is
	// satisfied by every run being empty.
	scheduleC, _ := run(5678)
	if scheduleC == scheduleA {
		t.Error("seeds 1234 and 5678 produced identical schedules; the seed is not driving anything")
	}
	t.Logf("seed 1234 schedule:\n%s", scheduleA)
}

// A sweep of seeds, each short. This is the shape Day 10 scales up: breadth
// across adversaries beats depth on one.
func TestSeedSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("sweep is slow; -short runs the single-seed test only")
	}
	for seed := int64(1); seed <= 6; seed++ {
		cfg := quickConfig(seed, 8)
		res, err := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
		if err != nil {
			t.Errorf("seed %d FAILED: %v  (reproduce: simulator -seed %d -steps 8 -v)", seed, err, seed)
			continue
		}
		if len(res.Violations) > 0 {
			t.Errorf("seed %d: %v", seed, res.Violations)
		}
	}
}

// The invariant checker has to be able to FAIL, or a green run means nothing.
// This plants a contradiction directly in a node's state machine and asserts
// the checker notices.
func TestInvariantCheckerDetectsCorruptedState(t *testing.T) {
	cfg := quickConfig(99, 3)
	s := New(cfg, dirsFor(t, cfg.Nodes))
	defer s.shutdown()

	for i := 1; i <= cfg.Nodes; i++ {
		if err := s.start(uint64(i)); err != nil {
			t.Fatalf("start: %v", err)
		}
	}
	lead := s.awaitLeader(10 * time.Second)
	if lead == nil {
		t.Fatal("no leader")
	}
	s.registerWorkers()
	s.offerWork(1)
	time.Sleep(200 * time.Millisecond)

	if vs := s.checkInvariants(1); len(vs) > 0 {
		t.Fatalf("healthy cluster already violating: %v", vs)
	}

	// Corrupt it: mark a task PENDING while leaving a worker attached, which
	// no command path can produce.
	var corrupted bool
	lead.coord.ReadState(func(st *state.StateStore) {
		for _, id := range st.TaskIDs() {
			if tk, _ := st.Task(id); tk.State == state.TaskAssigned || tk.State == state.TaskRunning {
				st.CorruptForTesting(id)
				corrupted = true
				return
			}
		}
	})
	if !corrupted {
		t.Skip("no assigned task to corrupt in this run")
	}

	vs := s.checkInvariants(1)
	if len(vs) == 0 {
		t.Error("checker passed a state no command path can produce")
	}
	for _, v := range vs {
		if v.Seed != cfg.Seed || v.Step != 1 {
			t.Errorf("violation lost its attribution: %+v", v)
		}
	}
	t.Logf("checker caught: %v", vs)
}
