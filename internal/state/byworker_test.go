package state

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
)

// fullScanByWorker computes the index the slow, obviously-correct way.
//
// This is the oracle: whatever the maintained index claims, a scan of every
// task has to agree with it. Keeping the oracle this dumb is the point —
// anything clever here could share a bug with the thing under test.
//
// TERMINAL TASKS ARE EXCLUDED, which is the invariant and not a detail.
// applyCompleteTask sets WorkerID as provenance and never clears it, so an
// index keyed on WorkerID alone keeps every completed task forever: an
// unbounded leak, and a held-task list that reports finished work as held.
// The first version of this oracle omitted the state half and therefore
// agreed with a leaking index.
func fullScanByWorker(s *StateStore) map[string][]string {
	out := map[string][]string{}
	for id, t := range s.tasks {
		if t.WorkerID == "" || terminal(t.State) {
			continue
		}
		out[t.WorkerID] = append(out[t.WorkerID], id)
	}
	for _, ids := range out {
		slices.Sort(ids)
	}
	return out
}

func indexSnapshot(s *StateStore) map[string][]string {
	out := map[string][]string{}
	for w := range s.tasksByWorker {
		if ids := s.TasksHeldBy(w); len(ids) > 0 {
			out[w] = ids
		}
	}
	return out
}

func assertIndexMatchesScan(t *testing.T, s *StateStore, step int, what string) {
	t.Helper()
	want, got := fullScanByWorker(s), indexSnapshot(s)
	if len(want) != len(got) {
		t.Fatalf("step %d (%s): index covers %d worker(s), a full scan finds %d\n index=%v\n scan =%v",
			step, what, len(got), len(want), got, want)
	}
	for w, ids := range want {
		if !slices.Equal(ids, got[w]) {
			t.Fatalf("step %d (%s): worker %q holds %v by scan, %v by index",
				step, what, w, ids, got[w])
		}
	}
	// Empty sets must be REMOVED, not merely empty: a worker that comes and
	// goes would otherwise leak one map per appearance.
	for w, set := range s.tasksByWorker {
		if len(set) == 0 {
			t.Fatalf("step %d (%s): worker %q left an empty set behind", step, what, w)
		}
	}
}

// The index must equal a full scan after EVERY transition, over random
// command sequences that exercise dispatch, completion, failure, lease
// expiry, worker loss, and snapshot+restore.
//
// Random sequences rather than hand-picked cases because the failure mode this
// guards against is a transition somebody forgot to route through
// setTaskWorker, and the one you forget is by definition the one you would not
// write a case for.
func TestTasksByWorkerIndexMatchesAFullScan(t *testing.T) {
	for seed := int64(1); seed <= 40; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			s := New()
			step := 0
			apply := func(what string, cmd *commandpb.Command) {
				step++
				_, _ = s.Apply(cmd) // errors are legitimate outcomes here
				assertIndexMatchesScan(t, s, step, what)
			}

			const nWorkers = 4
			for i := 0; i < nWorkers; i++ {
				apply("register worker", withCapacity(
					registerWorkerCmd(fmt.Sprintf("w-%d", i), s.nowMs, s.nowMs+2000), 3))
			}
			// One job with enough tasks that assignment choices matter.
			apply("submit job", submitJobCmd("j-1", "c", "r-1", nTaskIDs(12)...))

			for i := 0; i < 120; i++ {
				switch rng.Intn(6) {
				case 0: // dispatch whatever is pending
					var as []assign
					free := map[string]int32{}
					for w := 0; w < nWorkers; w++ {
						free[fmt.Sprintf("w-%d", w)] = 3
					}
					for _, id := range s.sortedTaskIDs() {
						tk := s.tasks[id]
						if tk.State != TaskPending || tk.NotBeforeMs > s.nowMs {
							continue
						}
						w := fmt.Sprintf("w-%d", rng.Intn(nWorkers))
						if free[w] <= 0 {
							continue
						}
						free[w]--
						as = append(as, assign{taskID: id, workerID: w,
							attempt: tk.Attempt + 1, leaseMs: s.nowMs + 1000})
						if len(as) >= 4 {
							break
						}
					}
					if len(as) == 0 {
						continue
					}
					apply("apply plan", applyPlanCmd(fmt.Sprintf("e-%d", i), as))
				case 1: // complete a held task
					id, tk := randomHeld(s, rng)
					if tk == nil {
						continue
					}
					apply("complete task", completeTaskCmd(id, tk.WorkerID, tk.Attempt, "ref"))
				case 2: // fail a held task, retryable or not
					id, tk := randomHeld(s, rng)
					if tk == nil {
						continue
					}
					apply("fail task", failTaskCmd(id, tk.WorkerID, tk.Attempt, rng.Intn(2) == 0))
				case 3: // let the clock run past leases, so Tick reclaims
					apply("tick past leases", tickCmd(s.nowMs+int64(200+rng.Intn(1500))))
				case 4: // a worker stops claiming its tasks — RUNNING follows the claim
					w := fmt.Sprintf("w-%d", rng.Intn(nWorkers))
					apply("worker heartbeats claiming nothing", withCapacity(
						heartbeatCmd(w, s.nowMs+2000), 3))
				case 5: // SNAPSHOT + RESTORE: derived state must come back
					blob, err := s.Encode()
					if err != nil {
						t.Fatalf("encode: %v", err)
					}
					restored := New()
					if err := restored.Restore(blob); err != nil {
						t.Fatalf("restore: %v", err)
					}
					step++
					assertIndexMatchesScan(t, restored, step, "after restore")
					// Continue from the RESTORED store, so a restore that
					// rebuilt the index wrongly poisons every later step
					// rather than being checked once and discarded.
					s = restored
				}
			}

			// The sequence has to have actually held something, or the test
			// passed by never exercising the index.
			if len(fullScanByWorker(s)) == 0 && step < 10 {
				t.Fatalf("seed %d held no task at any point; the index was never exercised", seed)
			}
		})
	}
}

// AssignedTaskFor must agree with the obvious scan, including which task it
// picks when a worker holds several.
func TestAssignedTaskForPicksTheLowestSortedAssignedTask(t *testing.T) {
	s := New()
	mustApply(t, s, withCapacity(registerWorkerCmd("w-1", 0, 2000), 4))
	mustApply(t, s, submitJobCmd("j-1", "c", "r", nTaskIDs(3)...))
	var as []assign
	for _, id := range s.sortedTaskIDs() {
		as = append(as, assign{taskID: id, workerID: "w-1", attempt: 1, leaseMs: 2000})
	}
	mustApply(t, s, applyPlanCmd("e-1", as))

	got, ok := s.AssignedTaskFor("w-1")
	if !ok {
		t.Fatal("worker holds three assigned tasks but AssignedTaskFor found none")
	}
	want := s.sortedTaskIDs()[0]
	if got.ID != want {
		t.Errorf("AssignedTaskFor = %q, want the lowest-sorted held task %q", got.ID, want)
	}
	if _, ok := s.AssignedTaskFor("w-nobody"); ok {
		t.Error("AssignedTaskFor invented a task for a worker that holds none")
	}
}

func randomHeld(s *StateStore, rng *rand.Rand) (string, *Task) {
	var ids []string
	for _, id := range s.sortedTaskIDs() {
		if t := s.tasks[id]; t.WorkerID != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return "", nil
	}
	id := ids[rng.Intn(len(ids))]
	return id, s.tasks[id]
}

func nTaskIDs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("t-%02d", i)
	}
	return out
}

// withCapacity gives a registration room for several tasks at once. A
// capacity-1 worker never holds a multi-id set, so it would not exercise the
// index's interesting case.
func withCapacity(cmd *commandpb.Command, capacity int32) *commandpb.Command {
	cmd.GetRegisterWorker().Capacity = capacity
	return cmd
}

// THE READ PATH MUST BE FLAT IN STATE SIZE.
//
// This is the regression test for the defect the scale profile found:
// answering "which task does worker W hold" used to sort every task id in the
// store, on every pass of the RequestTask long poll, under the apply lock.
// At 10,000 tasks with 256 pollers that produced 540-695ms Ready-loop stalls
// in the apply phase and 7-9 leadership changes per 30-second window.
//
// Asserted as a RATIO rather than an absolute duration, because an absolute
// bound would be a statement about this laptop. The index makes the answer
// cost O(tasks held by this worker); the old full scan made it O(n log n) in
// the whole store, so 50x the state would cost ~50x more. A 5x allowance
// absorbs cache effects and allocator noise while still failing by a wide
// margin on the old code: measured at ~70x before the fix.
func TestAssignedTaskForCostIsFlatInStateSize(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	const probes = 2000
	sizes := []int{1_000, 10_000, 50_000}
	per := make(map[int]time.Duration, len(sizes))

	for _, n := range sizes {
		s := storeWithTasks(t, n)
		// The worker under test holds one ASSIGNED task out of n. Timing a
		// hit rather than a miss is deliberate: a miss could be made fast by
		// an empty-set shortcut that says nothing about the scan.
		start := time.Now()
		for i := 0; i < probes; i++ {
			if _, ok := s.AssignedTaskFor("w-probe"); !ok {
				t.Fatalf("n=%d: the probe worker holds no assigned task; the test is measuring a miss", n)
			}
		}
		per[n] = time.Since(start) / probes
		t.Logf("n=%6d tasks: %v per AssignedTaskFor", n, per[n])
	}

	// Floor the baseline so a sub-microsecond measurement at the smallest
	// size cannot make the ratio meaningless.
	base := per[sizes[0]]
	if base < 100*time.Nanosecond {
		base = 100 * time.Nanosecond
	}
	for _, n := range sizes[1:] {
		if ratio := float64(per[n]) / float64(base); ratio > 5 {
			t.Errorf("AssignedTaskFor at %d tasks costs %.1fx its cost at %d tasks (%v vs %v): "+
				"the read path scales with total state, which is the defect this test guards",
				n, ratio, sizes[0], per[n], per[sizes[0]])
		}
	}
}

// storeWithTasks builds a store holding n pending tasks, with one worker
// ("w-probe") holding a single ASSIGNED task.
func storeWithTasks(t *testing.T, n int) *StateStore {
	t.Helper()
	s := New()
	mustApply(t, s, withCapacity(registerWorkerCmd("w-probe", 0, 1_000_000), 2))
	// Built in batches: one SubmitJob carrying 50,000 task entries is a
	// single enormous command, which is not the shape being measured.
	const batch = 1000
	made := 0
	for b := 0; made < n; b++ {
		ids := make([]string, 0, batch)
		for i := 0; i < batch && made < n; i++ {
			ids = append(ids, fmt.Sprintf("b%04d-t%05d", b, i))
			made++
		}
		mustApply(t, s, submitJobCmd(
			fmt.Sprintf("j-%04d", b), "c", fmt.Sprintf("r-%04d", b), ids...))
	}
	first := s.sortedTaskIDs()[0]
	mustApply(t, s, applyPlanCmd("e-probe", []assign{
		{taskID: first, workerID: "w-probe", attempt: 1, leaseMs: 1_000_000},
	}))
	if _, ok := s.AssignedTaskFor("w-probe"); !ok {
		t.Fatalf("setup failed: w-probe holds nothing at n=%d", n)
	}
	return s
}

// A COMPLETED task must leave the index while keeping its WorkerID.
//
// The regression this pins: tasksByWorker retained every completed task, so
// the map grew without bound and TasksHeldBy reported finished work as held.
// The reclaim benchmark hit it as a timeout — the harness waited for a
// task to stop being attributed to a dead worker, and it never did.
func TestTerminalTasksLeaveTheIndexButKeepTheirWorker(t *testing.T) {
	s := New()
	mustApply(t, s, withCapacity(registerWorkerCmd("w-1", 0, 100000), 2))
	mustApply(t, s, submitJobCmd("j-1", "c", "r", nTaskIDs(2)...))
	ids := s.sortedTaskIDs()
	mustApply(t, s, applyPlanCmd("e-1", []assign{
		{taskID: ids[0], workerID: "w-1", attempt: 1, leaseMs: 100000},
		{taskID: ids[1], workerID: "w-1", attempt: 1, leaseMs: 100000},
	}))
	if got := s.TasksHeldBy("w-1"); len(got) != 2 {
		t.Fatalf("after dispatch the worker holds %v, want 2 tasks", got)
	}

	mustApply(t, s, completeTaskCmd(ids[0], "w-1", 1, "ref"))
	if got := s.TasksHeldBy("w-1"); !slices.Equal(got, []string{ids[1]}) {
		t.Errorf("after completing %s the worker holds %v, want only %s — a completed task "+
			"left in the index is an unbounded leak", ids[0], got, ids[1])
	}
	// PROVENANCE SURVIVES. The completed task still records which worker ran
	// it; only the index membership goes.
	done, ok := s.Task(ids[0])
	if !ok || done.WorkerID != "w-1" {
		t.Errorf("completed task %s lost its WorkerID (%q); provenance must survive", ids[0], done.WorkerID)
	}
	if _, found := s.AssignedTaskFor("w-1"); !found {
		t.Error("the remaining assigned task is no longer findable")
	}

	// Terminal FAILURE, the other path.
	mustApply(t, s, failTaskCmd(ids[1], "w-1", 1, false))
	if got := s.TasksHeldBy("w-1"); len(got) != 0 {
		t.Errorf("after a terminal failure the worker still holds %v", got)
	}
	assertIndexMatchesScan(t, s, 0, "after terminal transitions")
}
