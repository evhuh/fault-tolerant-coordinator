package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/evazheng/fault-tolerant-coordinator/internal/client"
	"github.com/evazheng/fault-tolerant-coordinator/internal/sched"
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// testClock replaces the coordinator's only wall-clock read, so tests advance
// time explicitly instead of sleeping. Day 8 generalizes this into the
// injected Clock the whole simulator depends on.
type testClock struct{ ms atomic.Int64 }

func (c *testClock) now() time.Time  { return time.UnixMilli(c.ms.Load()) }
func (c *testClock) advance(d int64) { c.ms.Add(d) }
func (c *testClock) set(ms int64)    { c.ms.Store(ms) }

// newTestService starts the full leader-only pipeline: broker, planner, and
// scheduler workers. Day 4 inverted the control flow — assignment no longer
// happens on a worker's poll, it happens when an eval fires — so a test that
// does not run the pipeline will see nothing dispatched, ever.
//
// One scheduler worker by default, so ordering is deterministic. Tests that
// care about concurrent deciding ask for more.
func newTestService(t *testing.T, schedWorkers ...int) (*Service, *testClock) {
	t.Helper()
	clk := &testClock{}
	clk.set(1_700_000_000_000)
	cfg := DefaultConfig()
	cfg.Sched.Workers = 1
	if len(schedWorkers) > 0 {
		cfg.Sched.Workers = schedWorkers[0]
	}
	cfg.Sched.LeaseTTLMs = cfg.LeaseTTL.Milliseconds()

	c := New(cfg)
	c.now = clk.now

	ctx, cancel := context.WithCancel(context.Background())
	c.StartScheduling(ctx)
	t.Cleanup(func() {
		cancel()
		c.StopScheduling()
	})
	return NewService(c), clk
}

// awaitTask waits for the planner to assign something to this worker.
// Assignment is ASYNCHRONOUS now: SubmitJob returns as soon as the command
// commits, and the eval is decided on a scheduler goroutine afterwards.
func awaitTask(t *testing.T, s *Service, workerID string) *schedulerpb.Task {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if task := requestNow(t, s, workerID); task != nil {
			return task
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no task assigned to %s within the deadline", workerID)
	return nil
}

// awaitNoTask asserts nothing is assigned, giving the pipeline a moment to be
// wrong. Used where the correct behaviour is to dispatch nothing.
func awaitNoTask(t *testing.T, s *Service, workerID string) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	if task := requestNow(t, s, workerID); task != nil {
		t.Fatalf("dispatched %q to %s, want nothing", task.GetTaskId(), workerID)
	}
}

// awaitState waits for a task to reach a given state.
func awaitState(t *testing.T, s *Service, taskID string, want state.TaskState) state.Task {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var got state.Task
	for time.Now().Before(deadline) {
		s.c.read(func(st *state.StateStore) { got, _ = st.Task(taskID) })
		if got.State == want {
			return got
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("task %s is %s, want %s", taskID, got.State, want)
	return got
}

// tick drives the state machine's clock the way RunTicker does, but
// synchronously, so a test never races the ticker goroutine.
func tick(t *testing.T, s *Service) {
	t.Helper()
	if _, err := s.c.propose(tickCmd(s.c.nowMs())); err != nil {
		t.Fatalf("tick: %v", err)
	}
}

func submit(t *testing.T, s *Service, requestID string, n int) string {
	t.Helper()
	specs := make([]*schedulerpb.TaskSpec, 0, n)
	for i := 0; i < n; i++ {
		specs = append(specs, &schedulerpb.TaskSpec{Command: "/bin/true", MaxAttempts: 3})
	}
	resp, err := s.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId: "client-a", RequestId: requestID, Tasks: specs,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return resp.GetJobId()
}

// heartbeat renews a worker's lease. It is the same command as registration —
// there is no Heartbeat command — so it also REVIVES a worker Tick marked
// LOST, which is what wakes evals blocked for want of capacity.
func heartbeat(t *testing.T, s *Service, workerID string, runningTaskIDs ...string) {
	t.Helper()
	if _, err := s.Heartbeat(context.Background(), &schedulerpb.HeartbeatRequest{
		WorkerId: workerID, RunningTaskIds: runningTaskIDs,
	}); err != nil {
		t.Fatalf("heartbeat %s: %v", workerID, err)
	}
}

func register(t *testing.T, s *Service, workerID string, capacity ...int32) {
	t.Helper()
	var cap int32 = 1
	if len(capacity) > 0 {
		cap = capacity[0]
	}
	if _, err := s.RegisterWorker(context.Background(), &schedulerpb.RegisterWorkerRequest{
		WorkerId: workerID, Capacity: cap,
	}); err != nil {
		t.Fatalf("register %s: %v", workerID, err)
	}
}

// requestNow polls with wait_ms=0, so it returns whatever is available
// immediately rather than long-polling.
func requestNow(t *testing.T, s *Service, workerID string) *schedulerpb.Task {
	t.Helper()
	resp, err := s.RequestTask(context.Background(), &schedulerpb.RequestTaskRequest{WorkerId: workerID})
	if err != nil {
		t.Fatalf("request task %s: %v", workerID, err)
	}
	return resp.GetTask()
}

func TestSubmitDispatchCompleteEndToEnd(t *testing.T) {
	s, _ := newTestService(t)
	register(t, s, "w-1")
	jobID := submit(t, s, "req-1", 1)

	task := awaitTask(t, s, "w-1")
	if task.GetAttempt() != 1 {
		t.Errorf("attempt = %d, want 1 on first dispatch", task.GetAttempt())
	}
	if task.GetState() != schedulerpb.TaskState_TASK_STATE_ASSIGNED {
		t.Errorf("state = %s, want ASSIGNED", task.GetState())
	}

	// The heartbeat's claim is the worker's ack, and the only thing that
	// produces RUNNING.
	heartbeat(t, s, "w-1", task.GetTaskId())
	// RequestTask only offers ASSIGNED tasks — a RUNNING one is already being
	// executed — so the promotion is checked against state directly.
	if got := awaitState(t, s, task.GetTaskId(), state.TaskRunning); got.State != state.TaskRunning {
		t.Errorf("state = %s, want RUNNING after the heartbeat claim", got.State)
	}

	if _, err := s.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
		WorkerId: "w-1", TaskId: task.GetTaskId(), Attempt: task.GetAttempt(),
		ExitCode: 0, ResultRef: "/data/w-1/out",
	}); err != nil {
		t.Fatalf("report: %v", err)
	}

	resp, err := s.GetJob(context.Background(), &schedulerpb.GetJobRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if resp.GetJob().GetState() != schedulerpb.JobState_JOB_STATE_COMPLETE {
		t.Errorf("job state = %s, want COMPLETE", resp.GetJob().GetState())
	}
}

// A retried submission must return the ORIGINAL job, not a second one. The
// server mints a fresh job id on every call; dedup in the state machine is
// what throws the duplicate away.
func TestSubmitJobDedupsOnRequestID(t *testing.T) {
	s, _ := newTestService(t)
	first := submit(t, s, "req-1", 3)
	second := submit(t, s, "req-1", 3)
	if first != second {
		t.Errorf("job ids %q and %q differ; a retry must return the original", first, second)
	}

	list, err := s.ListJobs(context.Background(), &schedulerpb.ListJobsRequest{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetJobs()) != 1 {
		t.Errorf("job count = %d, want exactly 1", len(list.GetJobs()))
	}
}

// Clause (1) of RequestTask is a pure read: polling again before reporting
// returns the SAME task rather than dispatching a second one.
func TestRequestTaskReturnsTheTaskAlreadyHeld(t *testing.T) {
	s, _ := newTestService(t)
	register(t, s, "w-1")
	submit(t, s, "req-1", 5)

	first := awaitTask(t, s, "w-1")
	second := requestNow(t, s, "w-1")
	if first.GetTaskId() != second.GetTaskId() {
		t.Errorf("got %q then %q; a worker holding a task must be handed the same one",
			first.GetTaskId(), second.GetTaskId())
	}
	if second.GetAttempt() != 1 {
		t.Errorf("attempt = %d, want 1 — re-polling must not re-dispatch", second.GetAttempt())
	}
}

func TestRequestTaskIsEmptyWhenNothingPending(t *testing.T) {
	s, _ := newTestService(t)
	register(t, s, "w-1")
	if task := requestNow(t, s, "w-1"); task != nil {
		t.Errorf("got task %q, want none", task.GetTaskId())
	}
}

// An unregistered worker gets nothing rather than an assignment applyPlan
// would only reject.
func TestRequestTaskRefusesUnknownWorker(t *testing.T) {
	s, _ := newTestService(t)
	submit(t, s, "req-1", 1)
	awaitNoTask(t, s, "ghost")
}

// The Day 3 checkpoint, without the subprocesses: a worker goes away, its
// lease lapses, Tick reclaims the task, and another worker picks it up as the
// NEXT attempt.
func TestLeaseExpiryRedispatchesToAnotherWorker(t *testing.T) {
	s, clk := newTestService(t)
	register(t, s, "w-1")
	submit(t, s, "req-1", 1)
	task := awaitTask(t, s, "w-1")
	register(t, s, "w-2")

	// w-1 dies: no more heartbeats. Past the lease, Tick reclaims.
	clk.advance(DefaultConfig().LeaseTTL.Milliseconds() + 1)
	tick(t, s)

	// w-2 is alive and heartbeating. It has to be: the clock jumped past
	// every lease, so without a renewal Tick marks w-2 LOST as well and
	// there is nowhere to redispatch to.
	heartbeat(t, s, "w-2")

	// The reclaim emits an EvalTrigger, which becomes a CreateEval, which the
	// broker hands to a scheduler worker — so the redispatch happens without
	// anyone polling for it.
	redispatched := awaitTask(t, s, "w-2")
	if redispatched.GetTaskId() != task.GetTaskId() {
		t.Errorf("redispatched %q, want %q", redispatched.GetTaskId(), task.GetTaskId())
	}
	if redispatched.GetAttempt() != 2 {
		t.Errorf("attempt = %d, want 2 — a redispatch is a new attempt", redispatched.GetAttempt())
	}
}

// A report from a worker whose task was already reclaimed and redispatched
// names a stale attempt, and must be ignored rather than overwriting the live
// one. This is the at-least-once delivery / effectively-once effect boundary.
func TestStaleReportFromEvictedWorkerIsIgnored(t *testing.T) {
	s, clk := newTestService(t)
	register(t, s, "w-1")
	submit(t, s, "req-1", 1)
	slow := awaitTask(t, s, "w-1")

	register(t, s, "w-2")
	clk.advance(DefaultConfig().LeaseTTL.Milliseconds() + 1)
	tick(t, s)
	heartbeat(t, s, "w-2")
	fresh := awaitTask(t, s, "w-2")

	// w-1 was slow, not dead, and finally reports attempt 1.
	if _, err := s.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
		WorkerId: "w-1", TaskId: slow.GetTaskId(), Attempt: slow.GetAttempt(), ExitCode: 0,
		ResultRef: "/data/w-1/stale",
	}); err != nil {
		t.Fatalf("stale report: %v", err)
	}

	var after state.Task
	s.c.read(func(st *state.StateStore) { after, _ = st.Task(slow.GetTaskId()) })
	if after.State == state.TaskComplete {
		t.Error("a stale report completed the task; attempt 2 is the live one")
	}
	if after.Attempt != fresh.GetAttempt() {
		t.Errorf("attempt = %d, want %d", after.Attempt, fresh.GetAttempt())
	}

	// The live attempt still completes normally.
	if _, err := s.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
		WorkerId: "w-2", TaskId: fresh.GetTaskId(), Attempt: fresh.GetAttempt(), ExitCode: 0,
		ResultRef: "/data/w-2/out",
	}); err != nil {
		t.Fatalf("live report: %v", err)
	}
	s.c.read(func(st *state.StateStore) { after, _ = st.Task(slow.GetTaskId()) })
	if after.State != state.TaskComplete || after.ResultRef != "/data/w-2/out" {
		t.Errorf("state = %s ref = %q, want COMPLETE with w-2's result", after.State, after.ResultRef)
	}
}

// The reason this package owns a mutex at all, restated for Day 4.
//
// StateStore has no lock and is correct only under a single writer, while
// gRPC hands every RPC its own goroutine AND the scheduler now runs N more.
// This drives many workers against one task pool and asserts the property
// that must hold: NO TASK IS EVER HANDED TO TWO WORKERS. Under -race it is
// also what catches an unguarded read of the store's maps.
func TestConcurrentWorkersNeverDoubleDispatch(t *testing.T) {
	const (
		workers = 8
		tasks   = 200
	)
	s, _ := newTestService(t, 4) // 4 scheduler workers deciding in parallel

	ids := make([]string, 0, workers)
	for i := 0; i < workers; i++ {
		id := "w-" + string(rune('a'+i))
		ids = append(ids, id)
		register(t, s, id, 2)
	}
	submit(t, s, "req-1", tasks)

	var mu sync.Mutex
	owner := make(map[string]string, tasks)
	done := make(chan struct{})

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				task := requestNow(t, s, id)
				if task == nil {
					time.Sleep(2 * time.Millisecond)
					continue
				}
				mu.Lock()
				prev, seen := owner[task.GetTaskId()]
				if !seen {
					owner[task.GetTaskId()] = id
				}
				mu.Unlock()
				if seen && prev != id {
					t.Errorf("task %s handed to both %s and %s", task.GetTaskId(), prev, id)
					return
				}
				if _, err := s.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
					WorkerId: id, TaskId: task.GetTaskId(), Attempt: task.GetAttempt(),
					ExitCode: 0, ResultRef: "/data/" + id,
				}); err != nil {
					t.Errorf("report: %v", err)
					return
				}
			}
		}(id)
	}

	awaitAllTerminal(t, s, tasks, 10*time.Second)
	close(done)
	wg.Wait()

	if len(owner) != tasks {
		t.Errorf("dispatched %d distinct tasks, want %d", len(owner), tasks)
	}
	s.c.read(func(st *state.StateStore) {
		for _, id := range st.TaskIDs() {
			task, _ := st.Task(id)
			if task.State != state.TaskComplete {
				t.Errorf("task %s is %s, want COMPLETE", id, task.State)
			}
			if task.Attempt != 1 {
				t.Errorf("task %s attempt = %d, want 1 — nothing should have been retried", id, task.Attempt)
			}
		}
	})
}

func awaitAllTerminal(t *testing.T, s *Service, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		n := 0
		s.c.read(func(st *state.StateStore) {
			for _, id := range st.TaskIDs() {
				task, _ := st.Task(id)
				if task.State == state.TaskComplete || task.State == state.TaskFailed {
					n++
				}
			}
		})
		if n >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("not all %d tasks reached a terminal state within %s", want, within)
}

// ---------------------------------------------------------------------------
// Day 4 checkpoints
// ---------------------------------------------------------------------------

// THE BATCHING CHECKPOINT: one plan carrying 100 assignments commits as ONE
// entry. That is the whole throughput argument — the expensive part (deciding)
// ran in parallel on a snapshot, and a single fsync is amortized across every
// assignment in the batch.
func TestOnePlanCarriesManyAssignments(t *testing.T) {
	const (
		pool     = 10
		capacity = 10
		tasks    = pool * capacity // exactly fills the cluster
	)
	s, _ := newTestService(t)
	for i := 0; i < pool; i++ {
		register(t, s, fmt.Sprintf("w-%02d", i), capacity)
	}
	submit(t, s, "req-1", tasks)

	// Wait for the pool to fill.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, _, largest := s.c.Planner().Stats(); largest >= tasks {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	plans, accepted, rejected, largest := s.c.Planner().Stats()
	if largest != tasks {
		t.Errorf("largest plan = %d assignments, want %d in a single entry", largest, tasks)
	}
	if accepted != tasks {
		t.Errorf("accepted = %d, want %d", accepted, tasks)
	}
	if rejected != 0 {
		t.Errorf("rejected = %d, want 0 — nothing was contending", rejected)
	}
	// The point of batching: far fewer entries than assignments.
	if plans >= tasks {
		t.Errorf("committed %d plans for %d assignments; batching bought nothing", plans, tasks)
	}
	t.Logf("%d assignments committed in %d plan(s), largest %d", accepted, plans, largest)
}

// THE OPTIMISTIC-CONCURRENCY CHECKPOINT: a plan built against a snapshot and
// invalidated before submission is rejected rather than committed.
//
// This is what the planner is FOR. Scheduler workers decide in parallel
// against snapshots that can go stale; the planner is the referee that checks
// against current state before anything commits.
func TestStalePlanIsRejected(t *testing.T) {
	s, _ := newTestService(t)
	register(t, s, "w-1", 1)
	register(t, s, "w-2", 1)
	submit(t, s, "req-1", 1)

	// Let the pipeline place the task on someone.
	assigned := awaitTask(t, s, "w-1")

	// Now build a plan from a snapshot taken BEFORE that assignment — i.e.
	// one that still believes the task is PENDING at attempt 0.
	stale := &sched.Plan{
		EvalID: "eval/stale",
		JobID:  assigned.GetJobId(),
		Assignments: []sched.Assignment{{
			TaskID:        assigned.GetTaskId(),
			WorkerID:      "w-2",
			Attempt:       1, // the live attempt is already 1 and ASSIGNED
			LeaseExpiryMs: 1 << 40,
		}},
	}
	res := s.c.Planner().Submit(stale)

	if res.Accepted != 0 {
		t.Errorf("accepted = %d, want 0 — the task was no longer PENDING", res.Accepted)
	}
	if res.Rejected != 1 {
		t.Errorf("rejected = %d, want 1", res.Rejected)
	}
	// And the live assignment is untouched.
	after := awaitState(t, s, assigned.GetTaskId(), state.TaskAssigned)
	if after.WorkerID != "w-1" {
		t.Errorf("worker = %q, want w-1 — a stale plan must not steal a task", after.WorkerID)
	}
}

// A plan that partially holds commits the part that does. Rejecting the whole
// batch because one assignment went stale would throw away good work.
func TestPlannerPartiallyAcceptsPlan(t *testing.T) {
	s, _ := newTestService(t)
	register(t, s, "w-1", 1)
	submit(t, s, "req-1", 3)
	taken := awaitTask(t, s, "w-1")

	// Find a task that is still pending.
	var pending string
	s.c.read(func(st *state.StateStore) {
		for _, id := range st.TaskIDs() {
			if tk, _ := st.Task(id); tk.State == state.TaskPending {
				pending = id
				return
			}
		}
	})
	if pending == "" {
		t.Fatal("expected a pending task to remain")
	}

	register(t, s, "w-2", 1)
	mixed := &sched.Plan{
		EvalID: "eval/mixed",
		JobID:  taken.GetJobId(),
		Assignments: []sched.Assignment{
			{TaskID: taken.GetTaskId(), WorkerID: "w-2", Attempt: 1, LeaseExpiryMs: 1 << 40}, // stale
			{TaskID: pending, WorkerID: "w-2", Attempt: 1, LeaseExpiryMs: 1 << 40},           // good
		},
	}
	res := s.c.Planner().Submit(mixed)
	if res.Accepted != 1 || res.Rejected != 1 {
		t.Errorf("accepted = %d rejected = %d, want 1 and 1", res.Accepted, res.Rejected)
	}
	if got := awaitState(t, s, pending, state.TaskAssigned); got.WorkerID != "w-2" {
		t.Errorf("worker = %q, want w-2", got.WorkerID)
	}
}

// BLOCKED EVALUATIONS: more work than capacity must not starve.
//
// An eval that cannot place everything is blocked rather than dropped, and a
// completing task releases it. Without this the tasks beyond the first
// capacity-worth sit PENDING forever, because CompleteTask emits no trigger.
func TestBlockedEvalDrainsAsCapacityFrees(t *testing.T) {
	const tasks = 6
	s, _ := newTestService(t)
	register(t, s, "w-1", 1) // one slot for six tasks
	submit(t, s, "req-1", tasks)

	// First task placed; the rest have nowhere to go, so the eval blocks.
	first := awaitTask(t, s, "w-1")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, blocked := s.c.BrokerStats(); blocked > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if _, blocked := s.c.BrokerStats(); blocked == 0 {
		t.Fatal("no eval blocked; the remaining tasks would starve")
	}

	// Drain by completing one at a time. Each completion unblocks the eval,
	// which re-plans against the freed slot.
	seen := map[string]bool{first.GetTaskId(): true}
	current := first
	for i := 0; i < tasks; i++ {
		if _, err := s.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
			WorkerId: "w-1", TaskId: current.GetTaskId(), Attempt: current.GetAttempt(),
			ExitCode: 0, ResultRef: "/data/w-1",
		}); err != nil {
			t.Fatalf("report: %v", err)
		}
		if i == tasks-1 {
			break
		}
		current = awaitTask(t, s, "w-1")
		if seen[current.GetTaskId()] {
			t.Fatalf("task %s dispatched twice", current.GetTaskId())
		}
		seen[current.GetTaskId()] = true
	}

	awaitAllTerminal(t, s, tasks, 5*time.Second)
	if len(seen) != tasks {
		t.Errorf("ran %d distinct tasks, want %d", len(seen), tasks)
	}
}

// A worker JOINING is new capacity, and must wake a blocked eval. Otherwise a
// job submitted before any worker existed would never run.
func TestBlockedEvalWakesWhenAWorkerJoins(t *testing.T) {
	s, _ := newTestService(t)
	submit(t, s, "req-1", 2) // no workers at all yet

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, blocked := s.c.BrokerStats(); blocked > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if _, blocked := s.c.BrokerStats(); blocked == 0 {
		t.Fatal("eval did not block with no workers available")
	}

	register(t, s, "w-1", 2)
	if task := awaitTask(t, s, "w-1"); task == nil {
		t.Fatal("blocked eval never woke after a worker joined")
	}
}

// DERIVED, NOT REPLICATED: a new leader rebuilds the broker by SCANNING
// COMMITTED STATE, with none of the previous leader's in-memory queue.
//
// This simulates the failover Day 6 makes real: tear the pipeline down
// (losing every queued and blocked eval) and start a fresh one.
func TestBrokerRebuildsFromCommittedState(t *testing.T) {
	s, _ := newTestService(t)
	submit(t, s, "req-1", 4) // no workers; everything pends, eval blocks

	// Leadership lost: the entire in-memory queue goes away.
	s.c.StopScheduling()
	if ready, blocked := s.c.BrokerStats(); ready != 0 || blocked != 0 {
		t.Fatalf("broker survived teardown: ready=%d blocked=%d", ready, blocked)
	}

	// Leadership acquired: rebuild from state alone.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.c.StartScheduling(ctx)
	t.Cleanup(s.c.StopScheduling)

	register(t, s, "w-1", 4)
	if task := awaitTask(t, s, "w-1"); task == nil {
		t.Fatal("rebuilt broker found no pending work")
	}
	awaitAllTerminalAfterCompleting(t, s, "w-1", 4)
}

// awaitAllTerminalAfterCompleting drains a worker's queue by reporting
// success on whatever it is handed.
func awaitAllTerminalAfterCompleting(t *testing.T, s *Service, workerID string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		task := requestNow(t, s, workerID)
		if task == nil {
			done := 0
			s.c.read(func(st *state.StateStore) {
				for _, id := range st.TaskIDs() {
					if tk, _ := st.Task(id); tk.State == state.TaskComplete {
						done++
					}
				}
			})
			if done >= n {
				return
			}
			time.Sleep(2 * time.Millisecond)
			continue
		}
		if _, err := s.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
			WorkerId: workerID, TaskId: task.GetTaskId(), Attempt: task.GetAttempt(),
			ExitCode: 0, ResultRef: "/data/" + workerID,
		}); err != nil {
			t.Fatalf("report: %v", err)
		}
	}
	t.Fatalf("did not complete %d tasks in time", n)
}

// ---------------------------------------------------------------------------
// Day 5 checkpoints
// ---------------------------------------------------------------------------

// THE CHECKPOINT: the same request submitted 5× CONCURRENTLY yields exactly
// one job.
//
// Every handler mints its own job id before proposing — it has to, because
// ids are decided facts baked into the command. What makes this safe is that
// propose() serializes, so the first one through creates the job and records
// the session, and the other four hit dedup and get the original id back.
// Their minted ids are simply discarded, never having entered state.
func TestConcurrentIdenticalSubmitsYieldOneJob(t *testing.T) {
	s, _ := newTestService(t)

	const n = 5
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resp, err := s.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
				ClientId:  "client-a",
				RequestId: "req-1",
				Tasks: []*schedulerpb.TaskSpec{
					{Command: "/bin/true", MaxAttempts: 3},
				},
			})
			if err != nil {
				t.Errorf("submit %d: %v", i, err)
				return
			}
			ids[i] = resp.GetJobId()
		}(i)
	}
	wg.Wait()

	for i, id := range ids {
		if id != ids[0] {
			t.Errorf("submit %d returned %q, want %q — all five must agree", i, id, ids[0])
		}
	}

	list, err := s.ListJobs(context.Background(), &schedulerpb.ListJobsRequest{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetJobs()) != 1 {
		t.Errorf("job count = %d, want exactly 1", len(list.GetJobs()))
	}
	var tasks int
	s.c.read(func(st *state.StateStore) { tasks = st.TaskCount() })
	if tasks != 1 {
		t.Errorf("task count = %d, want 1 — four submissions created nothing", tasks)
	}
}

// A retryable failure must not redispatch instantly. Without a backoff a
// fast-failing task burns its whole attempt budget in milliseconds, which is
// exactly what Day 8's fault injection would surface as a spin.
func TestRetryIsDelayedByBackoff(t *testing.T) {
	s, clk := newTestService(t)
	register(t, s, "w-1", 1)
	submit(t, s, "req-1", 1)
	task := awaitTask(t, s, "w-1")

	if _, err := s.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
		WorkerId: "w-1", TaskId: task.GetTaskId(), Attempt: task.GetAttempt(),
		ExitCode: 1, Error: "boom", Retryable: true,
	}); err != nil {
		t.Fatalf("report: %v", err)
	}

	// The task is pending again, but held back.
	var backing state.Task
	s.c.read(func(st *state.StateStore) { backing, _ = st.Task(task.GetTaskId()) })
	if backing.State != state.TaskPending {
		t.Fatalf("state = %s, want PENDING", backing.State)
	}
	if backing.NotBeforeMs == 0 {
		t.Fatal("no backoff set; a crash-looping task would spin")
	}
	awaitNoTask(t, s, "w-1")

	// Advance past the deadline and tick: the sweep wakes the scheduler.
	clk.set(backing.NotBeforeMs + 1)
	heartbeat(t, s, "w-1")
	tick(t, s)

	retried := awaitTask(t, s, "w-1")
	if retried.GetAttempt() != 2 {
		t.Errorf("attempt = %d, want 2", retried.GetAttempt())
	}
}

// THE CHECKPOINT: work done but never reported yields ONE observable result.
//
// Both attempts ran — at-least-once delivery means that is normal, and the
// per-attempt ledger on disk records both. The effect is singular because only
// one attempt is ever live, so only one result_ref is ever committed.
func TestKilledBeforeReportYieldsOneObservableResult(t *testing.T) {
	s, clk := newTestService(t)
	register(t, s, "w-1", 1)
	register(t, s, "w-2", 1)
	submit(t, s, "req-1", 1)

	// w-1 is handed the task, does the work, and dies before reporting.
	lost := awaitTask(t, s, "w-1")

	clk.advance(DefaultConfig().LeaseTTL.Milliseconds() + 1)
	tick(t, s)
	heartbeat(t, s, "w-2")

	// w-2 picks it up as attempt 2 and reports.
	fresh := awaitTask(t, s, "w-2")
	if fresh.GetTaskId() != lost.GetTaskId() || fresh.GetAttempt() != 2 {
		t.Fatalf("redispatch = %s attempt %d, want %s attempt 2",
			fresh.GetTaskId(), fresh.GetAttempt(), lost.GetTaskId())
	}
	if _, err := s.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
		WorkerId: "w-2", TaskId: fresh.GetTaskId(), Attempt: 2,
		ExitCode: 0, ResultRef: "/data/w-2/attempt-2",
	}); err != nil {
		t.Fatalf("report: %v", err)
	}

	// w-1's report finally lands, carrying the dead attempt.
	if _, err := s.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
		WorkerId: "w-1", TaskId: lost.GetTaskId(), Attempt: 1,
		ExitCode: 0, ResultRef: "/data/w-1/attempt-1",
	}); err != nil {
		t.Fatalf("late report: %v", err)
	}

	final := awaitState(t, s, lost.GetTaskId(), state.TaskComplete)
	if final.ResultRef != "/data/w-2/attempt-2" {
		t.Errorf("result_ref = %q, want w-2's — exactly one committed effect", final.ResultRef)
	}
	if final.Attempt != 2 {
		t.Errorf("attempt = %d, want 2", final.Attempt)
	}
}

// A propose() failure must reach the client as a code it can ACT on.
//
// This mapping is silent when wrong: a lost proposal handed back as
// InvalidArgument makes the client pool give up instead of retrying, and the
// bug only appears during a failover — the one moment it matters.
func TestProposeErrorsMapToActionableCodes(t *testing.T) {
	s, _ := newTestService(t)

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			// Leadership can be lost between the guard and the commit. The
			// client must re-aim, not fail.
			name: "lost leadership mid-propose", err: ErrNotLeader,
			wantCode: codes.Unavailable, wantMsg: "NOT_LEADER",
		},
		{
			// The entry may still commit; retrying is safe because every
			// command is idempotent.
			name: "proposal timed out", err: ErrProposalLost,
			wantCode: codes.Unavailable, wantMsg: "retry",
		},
		{
			// The state machine said no. Retrying would be rejected again.
			name: "rejected by the state machine", err: errors.New("job already exists"),
			wantCode: codes.InvalidArgument, wantMsg: "job already exists",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := s.proposeError(tc.err, "submit job")
			if code := status.Code(got); code != tc.wantCode {
				t.Errorf("code = %s, want %s", code, tc.wantCode)
			}
			if msg := status.Convert(got).Message(); !strings.Contains(msg, tc.wantMsg) {
				t.Errorf("message = %q, want it to contain %q", msg, tc.wantMsg)
			}
		})
	}
}

// The two retryable outcomes must be ones the client pool actually acts on.
// Asserting the codes alone would not catch a mismatch between the two halves.
func TestRetryableProposeErrorsAreActedOnByTheClient(t *testing.T) {
	s, _ := newTestService(t)

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"not leader", ErrNotLeader},
		{"proposal lost", ErrProposalLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rpcErr := s.proposeError(tc.err, "submit job")
			_, isRedirect := client.IsNotLeader(rpcErr)
			retryable := isRedirect || status.Code(rpcErr) == codes.Unavailable
			if !retryable {
				t.Errorf("%v maps to %s, which the client pool treats as fatal", tc.err, status.Code(rpcErr))
			}
		})
	}
}

// A proposal can fail to LAND as well as fail to commit, and both must reach
// the caller as ErrProposalLost.
//
// Propose blocks until raft accepts the entry, so a busy or stalled node
// returns the context deadline from Propose itself rather than from the
// waiter. That path was returning a bare wrapped error, which the Tick
// proposer does not tolerate — so a slow node PANICKED the process. Same bug
// class as the original ticker panic, one layer down.
func TestProposeTimeoutsAllClassifyAsLost(t *testing.T) {
	s, _ := newTestService(t)

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"deadline from Propose", context.DeadlineExceeded},
		{"cancelled propose", context.Canceled},
		{"waiter timed out", ErrProposalLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rpcErr := s.proposeError(tc.err, "tick")
			// Whatever the source, the ticker must survive it and the client
			// must be told to retry.
			tolerated := errors.Is(tc.err, ErrProposalLost) ||
				errors.Is(tc.err, context.DeadlineExceeded) ||
				errors.Is(tc.err, context.Canceled)
			if !tolerated {
				t.Fatalf("precondition: %v is not a timeout", tc.err)
			}
			if code := status.Code(rpcErr); code == codes.Internal {
				t.Errorf("%v surfaced as Internal; clients will not retry", tc.err)
			}
		})
	}
}
