package sched

import (
	"testing"

	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// buildState assembles a store with one job of n tasks and the given workers.
func buildState(t *testing.T, nTasks int, workers map[string]int32) *state.StateStore {
	t.Helper()
	st := state.New()
	entries := make([]*commandpb.TaskEntry, 0, nTasks)
	for i := 0; i < nTasks; i++ {
		entries = append(entries, &commandpb.TaskEntry{
			TaskId: taskName(i),
			Spec:   &schedulerpb.TaskSpec{Command: "/bin/true", MaxAttempts: 3},
		})
	}
	if _, err := st.Apply(&commandpb.Command{Kind: &commandpb.Command_SubmitJob{
		SubmitJob: &commandpb.SubmitJob{JobId: "job-1", Tasks: entries},
	}}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	for id, capacity := range workers {
		if _, err := st.Apply(&commandpb.Command{Kind: &commandpb.Command_RegisterWorker{
			RegisterWorker: &commandpb.RegisterWorker{
				WorkerId: id, LeaseExpiryMs: 1_000_000, Capacity: capacity,
			},
		}}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
	return st
}

func taskName(i int) string {
	const digits = "0123456789"
	return "job-1/" + string([]byte{digits[(i/10)%10], digits[i%10]})
}

func newTestScheduler(cfg Config) *Scheduler {
	return NewScheduler(cfg, NewBroker(), nil, nil)
}

// The scheduler must never hand a worker more than its capacity, counting the
// assignments it is itself making — nothing is committed yet, so the snapshot
// cannot tell it.
func TestComputePlanRespectsCapacity(t *testing.T) {
	st := buildState(t, 10, map[string]int32{"w-1": 2, "w-2": 3})
	cfg := DefaultConfig()
	cfg.LeaseTTLMs = 2000
	s := newTestScheduler(cfg)

	plan := s.computePlan(st.Snapshot(), eval("e-1", 1))

	if len(plan.Assignments) != 5 {
		t.Fatalf("placed %d tasks, want 5 (2 + 3 slots)", len(plan.Assignments))
	}
	if plan.Unplaced != 5 {
		t.Errorf("unplaced = %d, want 5 — those tasks must block the eval", plan.Unplaced)
	}
	per := map[string]int{}
	for _, a := range plan.Assignments {
		per[a.WorkerID]++
	}
	if per["w-1"] != 2 || per["w-2"] != 3 {
		t.Errorf("per-worker = %v, want w-1:2 w-2:3", per)
	}
}

// Every assignment must ask for exactly Attempt+1: applyPlan validates it,
// and it is what distinguishes a redispatch from a stale report.
func TestComputePlanUsesNextAttempt(t *testing.T) {
	st := buildState(t, 1, map[string]int32{"w-1": 1})
	s := newTestScheduler(DefaultConfig())
	plan := s.computePlan(st.Snapshot(), eval("e-1", 1))
	if len(plan.Assignments) != 1 {
		t.Fatalf("placed %d, want 1", len(plan.Assignments))
	}
	if plan.Assignments[0].Attempt != 1 {
		t.Errorf("attempt = %d, want 1 for a task at attempt 0", plan.Assignments[0].Attempt)
	}
}

// A dead worker is not a placement target. applyPlan would reject it anyway,
// so proposing one only manufactures a rejection.
func TestComputePlanSkipsLostWorkers(t *testing.T) {
	st := buildState(t, 2, map[string]int32{"w-1": 2})
	// Advance past the lease so Tick marks w-1 LOST.
	if _, err := st.Apply(&commandpb.Command{Kind: &commandpb.Command_Tick{
		Tick: &commandpb.Tick{NowMs: 2_000_000},
	}}); err != nil {
		t.Fatalf("tick: %v", err)
	}

	s := newTestScheduler(DefaultConfig())
	plan := s.computePlan(st.Snapshot(), eval("e-1", 1))
	if len(plan.Assignments) != 0 {
		t.Errorf("placed %d tasks on a LOST worker, want 0", len(plan.Assignments))
	}
	if plan.Unplaced != 2 {
		t.Errorf("unplaced = %d, want 2", plan.Unplaced)
	}
}

// MaxPlanSize caps one entry. The overflow is unplaced, not dropped, so the
// eval blocks and the rest commit in a later plan.
func TestComputePlanCapsPlanSize(t *testing.T) {
	st := buildState(t, 10, map[string]int32{"w-1": 10})
	cfg := DefaultConfig()
	cfg.MaxPlanSize = 4
	s := newTestScheduler(cfg)

	plan := s.computePlan(st.Snapshot(), eval("e-1", 1))
	if len(plan.Assignments) != 4 {
		t.Errorf("plan size = %d, want 4", len(plan.Assignments))
	}
	if plan.Unplaced != 6 {
		t.Errorf("unplaced = %d, want 6", plan.Unplaced)
	}
}

// Batching is the throughput argument: one plan carries every placement it
// can make, rather than one plan per decision.
func TestComputePlanBatchesManyAssignments(t *testing.T) {
	st := buildState(t, 100, map[string]int32{"w-1": 50, "w-2": 50})
	cfg := DefaultConfig()
	cfg.MaxPlanSize = 1000
	s := newTestScheduler(cfg)

	plan := s.computePlan(st.Snapshot(), eval("e-1", 1))
	if len(plan.Assignments) != 100 {
		t.Errorf("plan carries %d assignments, want 100 in one entry", len(plan.Assignments))
	}
	if plan.Unplaced != 0 {
		t.Errorf("unplaced = %d, want 0", plan.Unplaced)
	}
}

// Leases are computed from the SNAPSHOT's clock, never time.Now() — the
// scheduler has no business reading a wall clock.
func TestComputePlanLeaseComesFromSnapshotClock(t *testing.T) {
	st := buildState(t, 1, map[string]int32{"w-1": 1})
	if _, err := st.Apply(&commandpb.Command{Kind: &commandpb.Command_Tick{
		Tick: &commandpb.Tick{NowMs: 500_000},
	}}); err != nil {
		t.Fatalf("tick: %v", err)
	}
	// The tick expired w-1's lease, so revive it past the new clock.
	if _, err := st.Apply(&commandpb.Command{Kind: &commandpb.Command_RegisterWorker{
		RegisterWorker: &commandpb.RegisterWorker{WorkerId: "w-1", LeaseExpiryMs: 900_000, Capacity: 1},
	}}); err != nil {
		t.Fatalf("register: %v", err)
	}

	cfg := DefaultConfig()
	cfg.LeaseTTLMs = 2000
	s := newTestScheduler(cfg)
	plan := s.computePlan(st.Snapshot(), eval("e-1", 1))
	if len(plan.Assignments) != 1 {
		t.Fatalf("placed %d, want 1", len(plan.Assignments))
	}
	if want := int64(500_000 + 2000); plan.Assignments[0].LeaseExpiryMs != want {
		t.Errorf("lease = %d, want %d (snapshot clock + TTL)", plan.Assignments[0].LeaseExpiryMs, want)
	}
}
