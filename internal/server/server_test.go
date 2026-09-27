package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func newTestService(t *testing.T) (*Service, *testClock) {
	t.Helper()
	clk := &testClock{}
	clk.set(1_700_000_000_000)
	cfg := DefaultConfig()
	c := New(cfg)
	c.now = clk.now
	return NewService(c), clk
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

func register(t *testing.T, s *Service, workerID string) {
	t.Helper()
	if _, err := s.RegisterWorker(context.Background(), &schedulerpb.RegisterWorkerRequest{WorkerId: workerID}); err != nil {
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
	jobID := submit(t, s, "req-1", 1)
	register(t, s, "w-1")

	task := requestNow(t, s, "w-1")
	if task == nil {
		t.Fatal("no task dispatched")
	}
	if task.GetAttempt() != 1 {
		t.Errorf("attempt = %d, want 1 on first dispatch", task.GetAttempt())
	}
	if task.GetState() != schedulerpb.TaskState_TASK_STATE_ASSIGNED {
		t.Errorf("state = %s, want ASSIGNED", task.GetState())
	}

	// The heartbeat's claim is the worker's ack, and the only thing that
	// produces RUNNING.
	if _, err := s.Heartbeat(context.Background(), &schedulerpb.HeartbeatRequest{
		WorkerId: "w-1", RunningTaskIds: []string{task.GetTaskId()},
	}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if got := requestNow(t, s, "w-1"); got.GetState() != schedulerpb.TaskState_TASK_STATE_RUNNING {
		t.Errorf("state = %s, want RUNNING after the heartbeat claim", got.GetState())
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
	submit(t, s, "req-1", 5)
	register(t, s, "w-1")

	first := requestNow(t, s, "w-1")
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
	if task := requestNow(t, s, "ghost"); task != nil {
		t.Errorf("dispatched %q to an unregistered worker", task.GetTaskId())
	}
}

// The Day 3 checkpoint, without the subprocesses: a worker goes away, its
// lease lapses, Tick reclaims the task, and another worker picks it up as the
// NEXT attempt.
func TestLeaseExpiryRedispatchesToAnotherWorker(t *testing.T) {
	s, clk := newTestService(t)
	submit(t, s, "req-1", 1)
	register(t, s, "w-1")
	register(t, s, "w-2")

	task := requestNow(t, s, "w-1")
	if task == nil {
		t.Fatal("no initial dispatch")
	}

	// w-1 dies: no more heartbeats. Past the lease, Tick reclaims.
	clk.advance(DefaultConfig().LeaseTTL.Milliseconds() + 1)
	tick(t, s)

	var reclaimed state.Task
	s.c.read(func(st *state.StateStore) { reclaimed, _ = st.Task(task.GetTaskId()) })
	if reclaimed.State != state.TaskPending {
		t.Fatalf("state = %s after lease expiry, want PENDING", reclaimed.State)
	}
	// Attempt is a dispatch counter: expiry must not bump it.
	if reclaimed.Attempt != 1 {
		t.Errorf("attempt = %d, want it unchanged at 1 across reclaim", reclaimed.Attempt)
	}

	// w-2 heartbeats to keep its own lease alive, then picks the task up.
	if _, err := s.Heartbeat(context.Background(), &schedulerpb.HeartbeatRequest{WorkerId: "w-2"}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	redispatched := requestNow(t, s, "w-2")
	if redispatched == nil {
		t.Fatal("reclaimed task was not redispatched")
	}
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
	submit(t, s, "req-1", 1)
	register(t, s, "w-1")
	register(t, s, "w-2")

	slow := requestNow(t, s, "w-1")
	clk.advance(DefaultConfig().LeaseTTL.Milliseconds() + 1)
	tick(t, s)
	if _, err := s.Heartbeat(context.Background(), &schedulerpb.HeartbeatRequest{WorkerId: "w-2"}); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	fresh := requestNow(t, s, "w-2")

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

// The reason this package owns a mutex at all.
//
// StateStore has no lock and is correct only under a single writer, while
// gRPC hands every RPC its own goroutine. This drives many workers at one
// task pool concurrently and asserts the property that must hold: NO TASK IS
// EVER DISPATCHED TO TWO WORKERS. Run under -race, it is also what catches an
// unguarded read of the store's maps.
func TestConcurrentPollsNeverDoubleDispatch(t *testing.T) {
	const (
		workers = 8
		tasks   = 200
	)
	s, _ := newTestService(t)
	submit(t, s, "req-1", tasks)

	var mu sync.Mutex
	owner := make(map[string]string, tasks)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		id := "w-" + string(rune('a'+i))
		register(t, s, id)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task := requestNow(t, s, id)
				if task == nil {
					return // pool drained
				}
				mu.Lock()
				prev, seen := owner[task.GetTaskId()]
				if !seen {
					owner[task.GetTaskId()] = id
				}
				mu.Unlock()
				if seen && prev != id {
					t.Errorf("task %s dispatched to both %s and %s", task.GetTaskId(), prev, id)
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
		}()
	}
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
				t.Errorf("task %s attempt = %d, want 1 — no task should have been retried", id, task.Attempt)
			}
		}
	})
}
