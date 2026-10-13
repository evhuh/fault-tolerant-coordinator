package state

import "sort"

// Snapshot is a coherent, read-only, point-in-time copy of the state machine.
//
// It exists because scheduler workers decide CONCURRENTLY. Each one reads a
// snapshot and computes a plan against it while others do the same and while
// the planner commits. That is the whole throughput argument: the expensive
// part (deciding) runs in parallel, and only the cheap verify-and-commit step
// serializes.
//
// A snapshot is therefore allowed to go STALE, and that is not a bug — it is
// the premise of optimistic concurrency. The planner re-verifies every
// assignment against CURRENT state before committing, and the state machine
// re-checks again in applyPlan. A plan built against an outdated snapshot is
// rejected rather than corrupting anything.
//
// Everything here is a VALUE, not a pointer into live state, so a snapshot
// cannot observe a mutation that happened after it was taken.
//
// Day 7 reuses this for Raft snapshots: serializing the state machine and
// serializing a Snapshot are the same problem.
type Snapshot struct {
	NowMs int64

	jobs    map[string]Job
	tasks   map[string]Task
	workers map[string]Worker

	// Precomputed sorted ids. A scheduler that iterates these makes the same
	// decision on every replica, and the sort cost is paid once per snapshot
	// rather than once per scan.
	jobIDs    []string
	taskIDs   []string
	workerIDs []string
}

// Snapshot copies the store. The caller must hold whatever lock guards the
// store; internal/server takes it under the same mutex as propose.
func (s *StateStore) Snapshot() *Snapshot {
	sn := &Snapshot{
		NowMs:     s.nowMs,
		jobs:      make(map[string]Job, len(s.jobs)),
		tasks:     make(map[string]Task, len(s.tasks)),
		workers:   make(map[string]Worker, len(s.workers)),
		jobIDs:    make([]string, 0, len(s.jobs)),
		taskIDs:   make([]string, 0, len(s.tasks)),
		workerIDs: make([]string, 0, len(s.workers)),
	}
	for id, j := range s.jobs {
		cp := *j
		cp.TaskIDs = append([]string(nil), j.TaskIDs...)
		sn.jobs[id] = cp
		sn.jobIDs = append(sn.jobIDs, id)
	}
	for id, t := range s.tasks {
		cp := *t
		cp.Spec.Args = append([]string(nil), t.Spec.Args...)
		sn.tasks[id] = cp
		sn.taskIDs = append(sn.taskIDs, id)
	}
	for id, w := range s.workers {
		sn.workers[id] = *w
		sn.workerIDs = append(sn.workerIDs, id)
	}
	sort.Strings(sn.jobIDs)
	sort.Strings(sn.taskIDs)
	sort.Strings(sn.workerIDs)
	return sn
}

func (sn *Snapshot) Job(id string) (Job, bool) {
	j, ok := sn.jobs[id]
	return j, ok
}

func (sn *Snapshot) Task(id string) (Task, bool) {
	t, ok := sn.tasks[id]
	return t, ok
}

func (sn *Snapshot) Worker(id string) (Worker, bool) {
	w, ok := sn.workers[id]
	return w, ok
}

func (sn *Snapshot) JobIDs() []string    { return sn.jobIDs }
func (sn *Snapshot) TaskIDs() []string   { return sn.taskIDs }
func (sn *Snapshot) WorkerIDs() []string { return sn.workerIDs }

// InFlight counts tasks currently held by a worker — ASSIGNED or RUNNING.
// This is what the planner compares against Worker.Capacity to decide whether
// a worker has room.
func (sn *Snapshot) InFlight(workerID string) int32 {
	var n int32
	for _, id := range sn.taskIDs {
		t := sn.tasks[id]
		if t.WorkerID == workerID && (t.State == TaskAssigned || t.State == TaskRunning) {
			n++
		}
	}
	return n
}

// PendingTaskIDs returns the dispatchable PENDING tasks, in sorted order.
// Tasks at the attempt cap are excluded: applyPlan rejects any assignment
// asking for attempt > MaxAttempts, so proposing one could only ever be
// rejected and spun on.
//
// Passing a jobID narrows to that job; empty returns every job's.
func (sn *Snapshot) PendingTaskIDs(jobID string) []string {
	out := make([]string, 0, len(sn.taskIDs))
	for _, id := range sn.taskIDs {
		t := sn.tasks[id]
		if t.State != TaskPending {
			continue
		}
		if jobID != "" && t.JobID != jobID {
			continue
		}
		if t.Attempt >= t.Spec.MaxAttempts {
			continue
		}
		out = append(out, id)
	}
	return out
}

// HasPendingWork reports whether any job still has dispatchable tasks. The
// broker uses it on rebuild to rediscover work from committed state.
func (sn *Snapshot) HasPendingWork() bool {
	return len(sn.PendingTaskIDs("")) > 0
}
