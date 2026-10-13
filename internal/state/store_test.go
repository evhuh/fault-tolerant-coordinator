package state

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// Helpers

func submitJobCmd(jobID, clientID, requestID string, taskIDs ...string) *commandpb.Command {
	tasks := make([]*commandpb.TaskEntry, 0, len(taskIDs))
	for _, id := range taskIDs {
		tasks = append(tasks, &commandpb.TaskEntry{
			TaskId: id,
			Spec: &schedulerpb.TaskSpec{
				Command:     "/bin/echo",
				Args:        []string{id},
				TimeoutMs:   1000,
				MaxAttempts: 3,
			},
		})
	}
	return &commandpb.Command{
		Version: 1,
		Kind: &commandpb.Command_SubmitJob{
			SubmitJob: &commandpb.SubmitJob{
				ClientId:  clientID,
				RequestId: requestID,
				JobId:     jobID,
				Tasks:     tasks,
				Priority:  50,
				CreatedMs: 1_700_000_000_000,
			},
		},
	}
}

func registerWorkerCmd(workerID string, registeredMs, leaseExpiryMs int64) *commandpb.Command {
	return &commandpb.Command{
		Version: 1,
		Kind: &commandpb.Command_RegisterWorker{
			RegisterWorker: &commandpb.RegisterWorker{
				WorkerId:      workerID,
				RegisteredMs:  registeredMs,
				LeaseExpiryMs: leaseExpiryMs,
			},
		},
	}
}

// heartbeatCmd is a RegisterWorker carrying a claim. There is no Heartbeat
// command: renewal and acknowledgement are the same idempotent path.
func heartbeatCmd(workerID string, leaseExpiryMs int64, runningTaskIDs ...string) *commandpb.Command {
	cmd := registerWorkerCmd(workerID, 500, leaseExpiryMs)
	cmd.GetRegisterWorker().RunningTaskIds = runningTaskIDs
	return cmd
}

func tickCmd(nowMs int64) *commandpb.Command {
	return &commandpb.Command{
		Version: 1,
		Kind:    &commandpb.Command_Tick{Tick: &commandpb.Tick{NowMs: nowMs}},
	}
}

type assign struct {
	taskID   string
	workerID string
	attempt  int32
	leaseMs  int64
}

func applyPlanCmd(evalID string, assigns []assign, stops ...string) *commandpb.Command {
	as := make([]*commandpb.Assignment, 0, len(assigns))
	for _, a := range assigns {
		as = append(as, &commandpb.Assignment{
			TaskId:        a.taskID,
			WorkerId:      a.workerID,
			Attempt:       a.attempt,
			LeaseExpiryMs: a.leaseMs,
		})
	}
	ss := make([]*commandpb.Stop, 0, len(stops))
	for _, id := range stops {
		ss = append(ss, &commandpb.Stop{TaskId: id, Reason: "stopped by plan"})
	}
	return &commandpb.Command{
		Version: 1,
		Kind: &commandpb.Command_ApplyPlan{
			ApplyPlan: &commandpb.ApplyPlan{EvalId: evalID, Assignments: as, Stops: ss},
		},
	}
}

func completeTaskCmd(taskID, workerID string, attempt int32, resultRef string) *commandpb.Command {
	return &commandpb.Command{
		Version: 1,
		Kind: &commandpb.Command_CompleteTask{
			CompleteTask: &commandpb.CompleteTask{
				TaskId: taskID, WorkerId: workerID, Attempt: attempt,
				ResultRef: resultRef, ExitCode: 0, CompletedMs: 2000,
			},
		},
	}
}

func failTaskCmd(taskID, workerID string, attempt int32, retryable bool) *commandpb.Command {
	return &commandpb.Command{
		Version: 1,
		Kind: &commandpb.Command_FailTask{
			FailTask: &commandpb.FailTask{
				TaskId: taskID, WorkerId: workerID, Attempt: attempt,
				Error: "boom", Retryable: retryable, ExitCode: 1, FailedMs: 2000,
			},
		},
	}
}

func createEvalCmd(evalID, jobID string, trigger commandpb.EvalTrigger) *commandpb.Command {
	return &commandpb.Command{
		Version: 1,
		Kind: &commandpb.Command_CreateEval{
			CreateEval: &commandpb.CreateEval{
				EvalId: evalID, JobId: jobID, Priority: 50,
				TriggeredBy: trigger, CreatedMs: 1500,
			},
		},
	}
}

// dump renders the store's full state in a canonical, sorted order. Two stores
// that applied the same commands must produce byte-identical dumps — that is
// the determinism property the whole design rests on, so it gets a test.
func dump(s *StateStore) string {
	var b strings.Builder
	fmt.Fprintf(&b, "nowMs=%d\n", s.nowMs)

	jobIDs := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
		jobIDs = append(jobIDs, id)
	}
	sort.Strings(jobIDs)
	for _, id := range jobIDs {
		j := s.jobs[id]
		fmt.Fprintf(&b, "job %s state=%s client=%s req=%s prio=%d created=%d tasks=%v\n",
			j.ID, j.State, j.ClientID, j.RequestID, j.Priority, j.CreatedMs, j.TaskIDs)
	}

	taskIDs := make([]string, 0, len(s.tasks))
	for id := range s.tasks {
		taskIDs = append(taskIDs, id)
	}
	sort.Strings(taskIDs)
	for _, id := range taskIDs {
		t := s.tasks[id]
		fmt.Fprintf(&b, "task %s job=%s state=%s attempt=%d worker=%s lease=%d cmd=%s args=%v maxAttempts=%d\n",
			t.ID, t.JobID, t.State, t.Attempt, t.WorkerID, t.LeaseExpiryMs,
			t.Spec.Command, t.Spec.Args, t.Spec.MaxAttempts)
	}

	for _, id := range s.sortedWorkerIDs() {
		w := s.workers[id]
		fmt.Fprintf(&b, "worker %s state=%s registered=%d lease=%d\n",
			w.ID, w.State, w.RegisteredMs, w.LeaseExpiryMs)
	}

	evalIDs := make([]string, 0, len(s.evals))
	for id := range s.evals {
		evalIDs = append(evalIDs, id)
	}
	sort.Strings(evalIDs)
	for _, id := range evalIDs {
		e := s.evals[id]
		fmt.Fprintf(&b, "eval %s job=%s prio=%d trigger=%s created=%d\n",
			e.ID, e.JobID, e.Priority, e.TriggeredBy, e.CreatedMs)
	}
	return b.String()
}

func mustApply(t *testing.T, s *StateStore, cmd *commandpb.Command) Result {
	t.Helper()
	res, err := s.Apply(cmd)
	if err != nil {
		t.Fatalf("Apply: unexpected error: %v", err)
	}
	return res
}

// SubmitJob
func TestSubmitJobCreatesJobAndPendingTasks(t *testing.T) {
	s := New()
	res := mustApply(t, s, submitJobCmd("job-1", "client-a", "req-1", "task-1", "task-2", "task-3"))

	if res.JobID != "job-1" {
		t.Errorf("Result.JobID = %q, want %q", res.JobID, "job-1")
	}

	job, ok := s.Job("job-1")
	if !ok {
		t.Fatal("job-1 not found after SubmitJob")
	}
	if job.State != JobPending {
		t.Errorf("job state = %s, want %s", job.State, JobPending)
	}
	// Submission order is semantic and must be preserved exactly.
	want := []string{"task-1", "task-2", "task-3"}
	if len(job.TaskIDs) != len(want) {
		t.Fatalf("job has %d tasks, want %d", len(job.TaskIDs), len(want))
	}
	for i := range want {
		if job.TaskIDs[i] != want[i] {
			t.Errorf("TaskIDs[%d] = %q, want %q", i, job.TaskIDs[i], want[i])
		}
	}

	for _, id := range want {
		task, ok := s.Task(id)
		if !ok {
			t.Fatalf("task %s not found", id)
		}
		if task.State != TaskPending {
			t.Errorf("task %s state = %s, want %s", id, task.State, TaskPending)
		}
		if task.Attempt != 0 {
			t.Errorf("task %s attempt = %d, want 0", id, task.Attempt)
		}
		if task.JobID != "job-1" {
			t.Errorf("task %s jobID = %q, want job-1", id, task.JobID)
		}
		if task.WorkerID != "" {
			t.Errorf("task %s already has worker %q", id, task.WorkerID)
		}
	}
}

func TestSubmitJobDedupsRepeatedRequest(t *testing.T) {
	s := New()
	first := mustApply(t, s, submitJobCmd("job-1", "client-a", "req-1", "task-1"))

	// Same (clientID, requestID), different jobID: the leader retried and
	// assigned a fresh ID, but the request was already applied. The original
	// response must come back, and no second job may be created.
	second := mustApply(t, s, submitJobCmd("job-2", "client-a", "req-1", "task-2"))

	if second.JobID != first.JobID {
		t.Errorf("retry returned JobID %q, want the original %q", second.JobID, first.JobID)
	}
	if got := s.JobCount(); got != 1 {
		t.Errorf("JobCount = %d, want 1 — the retry created a duplicate job", got)
	}
	if got := s.TaskCount(); got != 1 {
		t.Errorf("TaskCount = %d, want 1", got)
	}
	if _, ok := s.Job("job-2"); ok {
		t.Error("job-2 was created; the retry was not deduplicated")
	}
}

func TestSubmitJobDistinctRequestsBothApply(t *testing.T) {
	s := New()
	mustApply(t, s, submitJobCmd("job-1", "client-a", "req-1", "task-1"))
	mustApply(t, s, submitJobCmd("job-2", "client-a", "req-2", "task-2"))

	if got := s.JobCount(); got != 2 {
		t.Errorf("JobCount = %d, want 2", got)
	}
}

func TestSubmitJobRejectsInvalidCommands(t *testing.T) {
	tests := []struct {
		name string
		cmd  *commandpb.Command
		want string
	}{
		{
			name: "empty job id",
			cmd:  submitJobCmd("", "client-a", "req-1", "task-1"),
			want: "empty job_id",
		},
		{
			name: "no tasks",
			cmd:  submitJobCmd("job-1", "client-a", "req-1"),
			want: "no tasks",
		},
		{
			name: "empty task id",
			cmd:  submitJobCmd("job-1", "client-a", "req-1", ""),
			want: "empty task_id",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			_, err := s.Apply(tc.cmd)
			if err == nil {
				t.Fatalf("Apply(%s) = nil error, want error containing %q", tc.name, tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want it to contain %q", err, tc.want)
			}
			// A rejected command must leave no partial state behind.
			if s.JobCount() != 0 || s.TaskCount() != 0 {
				t.Errorf("rejected command mutated state: %d jobs, %d tasks",
					s.JobCount(), s.TaskCount())
			}
		})
	}
}

func TestSubmitJobRejectsDuplicateJobID(t *testing.T) {
	s := New()
	mustApply(t, s, submitJobCmd("job-1", "client-a", "req-1", "task-1"))

	// Different request ID, so dedup does not apply — this is a genuine
	// collision and must be rejected deterministically.
	_, err := s.Apply(submitJobCmd("job-1", "client-b", "req-9", "task-9"))
	if err == nil {
		t.Fatal("Apply with duplicate job_id = nil error, want rejection")
	}
	if s.JobCount() != 1 {
		t.Errorf("JobCount = %d, want 1", s.JobCount())
	}
}

func TestSubmitJobDefaultsMaxAttempts(t *testing.T) {
	s := New()
	cmd := &commandpb.Command{
		Version: 1,
		Kind: &commandpb.Command_SubmitJob{
			SubmitJob: &commandpb.SubmitJob{
				ClientId:  "client-a",
				RequestId: "req-1",
				JobId:     "job-1",
				Tasks: []*commandpb.TaskEntry{{
					TaskId: "task-1",
					Spec:   &schedulerpb.TaskSpec{Command: "/bin/echo"}, // MaxAttempts unset
				}},
			},
		},
	}
	mustApply(t, s, cmd)

	task, _ := s.Task("task-1")
	if task.Spec.MaxAttempts != defaultMaxAttempts {
		t.Errorf("MaxAttempts = %d, want default %d", task.Spec.MaxAttempts, defaultMaxAttempts)
	}
}

// RegisterWorker

func TestRegisterWorkerCreatesAliveWorker(t *testing.T) {
	s := New()
	res := mustApply(t, s, registerWorkerCmd("w-1", 1000, 3000))

	if res.WorkerID != "w-1" {
		t.Errorf("Result.WorkerID = %q, want w-1", res.WorkerID)
	}
	w, ok := s.Worker("w-1")
	if !ok {
		t.Fatal("worker w-1 not found")
	}
	if w.State != WorkerAlive {
		t.Errorf("state = %s, want ALIVE", w.State)
	}
	if w.LeaseExpiryMs != 3000 {
		t.Errorf("lease = %d, want 3000", w.LeaseExpiryMs)
	}
	if w.RegisteredMs != 1000 {
		t.Errorf("registered = %d, want 1000", w.RegisteredMs)
	}
}

func TestRegisterWorkerRenewsLeaseIdempotently(t *testing.T) {
	s := New()
	mustApply(t, s, registerWorkerCmd("w-1", 1000, 3000))
	mustApply(t, s, registerWorkerCmd("w-1", 9999, 5000)) // heartbeat

	if got := s.WorkerCount(); got != 1 {
		t.Errorf("WorkerCount = %d, want 1 — renewal created a second worker", got)
	}
	w, _ := s.Worker("w-1")
	if w.LeaseExpiryMs != 5000 {
		t.Errorf("lease = %d, want it renewed to 5000", w.LeaseExpiryMs)
	}
	// RegisteredMs records the FIRST registration and must not be overwritten.
	if w.RegisteredMs != 1000 {
		t.Errorf("registered = %d, want the original 1000", w.RegisteredMs)
	}
}

// The heartbeat's claim is what produces RUNNING. Nothing else does
func TestHeartbeatClaimPromotesAssignedToRunning(t *testing.T) {
	s := seedCluster(t, 2)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{
		{"task-1", "w-1", 1, 5000},
		{"task-2", "w-1", 1, 5000},
	}))

	// The worker picked up task-1 and says so; task-2 is still just assigned
	mustApply(t, s, heartbeatCmd("w-1", 12000, "task-1"))

	if task, _ := s.Task("task-1"); task.State != TaskRunning {
		t.Errorf("task-1 state = %s, want RUNNING (the worker claimed it)", task.State)
	}
	if task, _ := s.Task("task-2"); task.State != TaskAssigned {
		t.Errorf("task-2 state = %s, want ASSIGNED (unclaimed)", task.State)
	}
	// Renewal still extends BOTH, or Tick would reclaim work from a healthy worker that simply has not started one of its tasks yet
	for _, id := range []string{"task-1", "task-2"} {
		if task, _ := s.Task(id); task.LeaseExpiryMs != 12000 {
			t.Errorf("%s lease = %d, want 12000", id, task.LeaseExpiryMs)
		}
	}
}

// A worker may only speak for tasks it holds, and a claim never resurrects a terminal task. Both are silent no-ops: a heartbeat racing a reassignment is normal, not an error
func TestHeartbeatClaimIgnoresTasksTheWorkerDoesNotHold(t *testing.T) {
	s := seedCluster(t, 2)
	mustApply(t, s, registerWorkerCmd("w-2", 500, 9000))
	mustApply(t, s, applyPlanCmd("eval-1", []assign{
		{"task-1", "w-1", 1, 5000},
		{"task-2", "w-2", 1, 5000},
	}))
	mustApply(t, s, completeTaskCmd("task-1", "w-1", 1, "/out/1"))

	// w-2 claims a task it lost AND one that is already complete
	mustApply(t, s, heartbeatCmd("w-2", 12000, "task-1", "task-2"))

	if task, _ := s.Task("task-1"); task.State != TaskComplete {
		t.Errorf("task-1 state = %s, want COMPLETE — a foreign claim must not touch it", task.State)
	}
	if task, _ := s.Task("task-2"); task.State != TaskRunning {
		t.Errorf("task-2 state = %s, want RUNNING", task.State)
	}
}

// RUNNING is not a new lifecycle, just an earlier one: everything downstream of ASSIGNED must still work from RUNNING
func TestRunningTaskStillCompletesAndReclaims(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s, heartbeatCmd("w-1", 5000, "task-1"))

	// Reclaim out of RUNNING, attempt untouched
	mustApply(t, s, tickCmd(5001))
	task, _ := s.Task("task-1")
	if task.State != TaskPending || task.Attempt != 1 {
		t.Fatalf("state = %s attempt = %d, want PENDING/1", task.State, task.Attempt)
	}

	// Redispatch, claim, complete
	mustApply(t, s, registerWorkerCmd("w-1", 500, 20000))
	mustApply(t, s, applyPlanCmd("eval-2", []assign{{"task-1", "w-1", 2, 15000}}))
	mustApply(t, s, heartbeatCmd("w-1", 15000, "task-1"))
	mustApply(t, s, completeTaskCmd("task-1", "w-1", 2, "/out/2"))

	if task, _ := s.Task("task-1"); task.State != TaskComplete {
		t.Errorf("state = %s, want COMPLETE from RUNNING", task.State)
	}
	if job, _ := s.Job("job-1"); job.State != JobComplete {
		t.Errorf("job state = %s, want COMPLETE", job.State)
	}
}

func TestRegisterWorkerRejectsInvalid(t *testing.T) {
	s := New()
	if _, err := s.Apply(registerWorkerCmd("", 1000, 3000)); err == nil {
		t.Error("empty worker_id = nil error, want rejection")
	}
	if _, err := s.Apply(registerWorkerCmd("w-1", 1000, 0)); err == nil {
		t.Error("missing lease_expiry_ms = nil error, want rejection")
	}
	if s.WorkerCount() != 0 {
		t.Errorf("rejected commands mutated state: %d workers", s.WorkerCount())
	}
}

// Tick

func TestTickAdvancesClock(t *testing.T) {
	s := New()
	mustApply(t, s, tickCmd(5000))
	if s.NowMs() != 5000 {
		t.Errorf("NowMs = %d, want 5000", s.NowMs())
	}
}

func TestTickClampsBackwardsClock(t *testing.T) {
	s := New()
	mustApply(t, s, tickCmd(5000))

	// A new leader with a slow clock, or an NTP step. Must be a no-op, not an
	// error and not a regression: an expired lease must never un-expire.
	res, err := s.Apply(tickCmd(3000))
	if err != nil {
		t.Fatalf("backwards tick returned error %v, want silent clamp", err)
	}
	if len(res.EvalTriggers) != 0 {
		t.Errorf("backwards tick produced %d eval triggers, want 0", len(res.EvalTriggers))
	}
	if s.NowMs() != 5000 {
		t.Errorf("NowMs = %d, want it clamped at 5000", s.NowMs())
	}
}

func TestTickExpiresWorkerLease(t *testing.T) {
	s := New()
	mustApply(t, s, registerWorkerCmd("w-1", 1000, 3000))

	mustApply(t, s, tickCmd(2999)) // still inside the lease
	if w, _ := s.Worker("w-1"); w.State != WorkerAlive {
		t.Fatalf("worker state = %s at t=2999, want ALIVE", w.State)
	}

	mustApply(t, s, tickCmd(3001)) // lease lapsed
	if w, _ := s.Worker("w-1"); w.State != WorkerLost {
		t.Errorf("worker state = %s at t=3001, want LOST", w.State)
	}
}

func TestTickIsIdempotentOnRepeat(t *testing.T) {
	s := New()
	mustApply(t, s, registerWorkerCmd("w-1", 1000, 3000))
	mustApply(t, s, tickCmd(4000))

	before := dump(s)
	res := mustApply(t, s, tickCmd(4000)) // same timestamp again
	if len(res.EvalTriggers) != 0 {
		t.Errorf("repeat tick produced %d triggers, want 0", len(res.EvalTriggers))
	}
	if after := dump(s); after != before {
		t.Errorf("repeat tick mutated state\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

// ApplyPlan

// seedCluster gives one job with `nTasks` tasks and one alive worker, clock at 1000.
func seedCluster(t *testing.T, nTasks int) *StateStore {
	t.Helper()
	s := New()
	ids := make([]string, 0, nTasks)
	for i := 1; i <= nTasks; i++ {
		ids = append(ids, fmt.Sprintf("task-%d", i))
	}
	mustApply(t, s, submitJobCmd("job-1", "client-a", "req-1", ids...))
	mustApply(t, s, registerWorkerCmd("w-1", 500, 9000))
	mustApply(t, s, tickCmd(1000))
	return s
}

func TestApplyPlanAssignsAndIncrementsAttempt(t *testing.T) {
	s := seedCluster(t, 2)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{
		{"task-1", "w-1", 1, 5000},
		{"task-2", "w-1", 1, 5000},
	}))

	for _, id := range []string{"task-1", "task-2"} {
		task, _ := s.Task(id)
		if task.State != TaskAssigned {
			t.Errorf("%s state = %s, want ASSIGNED", id, task.State)
		}
		if task.Attempt != 1 {
			t.Errorf("%s attempt = %d, want 1", id, task.Attempt)
		}
		if task.WorkerID != "w-1" {
			t.Errorf("%s worker = %q, want w-1", id, task.WorkerID)
		}
		if task.LeaseExpiryMs != 5000 {
			t.Errorf("%s lease = %d, want 5000", id, task.LeaseExpiryMs)
		}
	}
	// First assignment promotes the job.
	if job, _ := s.Job("job-1"); job.State != JobRunning {
		t.Errorf("job state = %s, want RUNNING", job.State)
	}
}

func TestApplyPlanBatchesManyAssignmentsAsOneEntry(t *testing.T) {
	const n = 100
	s := seedCluster(t, n)

	assigns := make([]assign, 0, n)
	for i := 1; i <= n; i++ {
		assigns = append(assigns, assign{fmt.Sprintf("task-%d", i), "w-1", 1, 5000})
	}
	// One command, 100 assignments — one Raft entry once Day 6 wires it up.
	mustApply(t, s, applyPlanCmd("eval-1", assigns))

	for i := 1; i <= n; i++ {
		task, _ := s.Task(fmt.Sprintf("task-%d", i))
		if task.State != TaskAssigned {
			t.Fatalf("task-%d state = %s, want ASSIGNED", i, task.State)
		}
	}
}

func TestApplyPlanRejectsStalePlanWholesale(t *testing.T) {
	s := seedCluster(t, 2)
	// task-1 was assigned by an earlier plan; a scheduler working from a stale
	// snapshot still believes it is PENDING.
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))

	before := dump(s)
	_, err := s.Apply(applyPlanCmd("eval-2", []assign{
		{"task-2", "w-1", 1, 5000}, // valid
		{"task-1", "w-1", 1, 5000}, // stale: task-1 is ASSIGNED, not PENDING
	}))
	if err == nil {
		t.Fatal("stale plan = nil error, want rejection")
	}
	// All-or-nothing: the valid assignment must NOT have landed.
	if after := dump(s); after != before {
		t.Errorf("rejected plan partially applied\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestApplyPlanRejectsWrongAttempt(t *testing.T) {
	s := seedCluster(t, 1)
	if _, err := s.Apply(applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 2, 5000}})); err == nil {
		t.Error("attempt=2 on a fresh task = nil error, want rejection")
	}
	if _, err := s.Apply(applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 0, 5000}})); err == nil {
		t.Error("attempt=0 = nil error, want rejection")
	}
}

func TestApplyPlanRejectsDeadWorkerAndExpiredLease(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, tickCmd(9500)) // w-1's lease (9000) lapses → LOST

	if _, err := s.Apply(applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 20000}})); err == nil {
		t.Error("assigning to a LOST worker = nil error, want rejection")
	}

	s2 := seedCluster(t, 1)
	if _, err := s2.Apply(applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 500}})); err == nil {
		t.Error("lease already in the past = nil error, want rejection")
	}
}

func TestApplyPlanStopReturnsTaskToPending(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s, applyPlanCmd("eval-2", nil, "task-1"))

	task, _ := s.Task("task-1")
	if task.State != TaskPending {
		t.Errorf("state = %s, want PENDING", task.State)
	}
	if task.WorkerID != "" {
		t.Errorf("worker = %q, want cleared", task.WorkerID)
	}
	// A stop does NOT roll back the dispatch counter.
	if task.Attempt != 1 {
		t.Errorf("attempt = %d, want it to stay at 1", task.Attempt)
	}
}

func TestTickExpiredLeaseReclaimsTaskWithoutTouchingAttempt(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))

	res := mustApply(t, s, tickCmd(5001))

	task, _ := s.Task("task-1")
	if task.State != TaskPending {
		t.Errorf("state = %s, want PENDING after lease expiry", task.State)
	}
	if task.WorkerID != "" {
		t.Errorf("worker = %q, want cleared", task.WorkerID)
	}
	// Attempt is a DISPATCH counter — expiry must not bump it, or a slow
	// worker's late report could not be told apart from a fresh one.
	if task.Attempt != 1 {
		t.Errorf("attempt = %d, want it to stay at 1 across expiry", task.Attempt)
	}
	if len(res.EvalTriggers) != 1 || res.EvalTriggers[0].TaskID != "task-1" {
		t.Errorf("EvalTriggers = %+v, want one for task-1", res.EvalTriggers)
	}

	// The reclaimed task can now be dispatched again, as attempt 2.
	mustApply(t, s, registerWorkerCmd("w-1", 500, 20000))
	mustApply(t, s, applyPlanCmd("eval-2", []assign{{"task-1", "w-1", 2, 15000}}))
	task, _ = s.Task("task-1")
	if task.Attempt != 2 {
		t.Errorf("attempt = %d after redispatch, want 2", task.Attempt)
	}
}

// A task whose lease expires on its LAST permitted attempt must go terminal,
// not back to PENDING. applyPlan rejects any assignment asking for
// attempt > MaxAttempts, so a PENDING task at the attempt cap is
// undispatchable forever: a scheduler scanning for PENDING work would pick it,
// propose, be rejected, and spin. It would also never reach a terminal state,
// which is one of the invariants the whole project claims.
func TestTickExhaustedAttemptsGoesTerminalInsteadOfPending(t *testing.T) {
	s := seedCluster(t, 1) // MaxAttempts = 3

	// Attempts 1 and 2 are reclaimed normally: budget remains.
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s, tickCmd(5001))
	mustApply(t, s, applyPlanCmd("eval-2", []assign{{"task-1", "w-1", 2, 8000}}))
	res := mustApply(t, s, tickCmd(8001))
	if task, _ := s.Task("task-1"); task.State != TaskPending {
		t.Fatalf("state = %s after attempt 2 expired, want PENDING (budget remains)", task.State)
	}
	if len(res.EvalTriggers) != 1 {
		t.Fatalf("EvalTriggers = %+v, want one while the task is still retryable", res.EvalTriggers)
	}

	// Attempt 3 is the last one MaxAttempts permits. Its expiry is terminal.
	mustApply(t, s, registerWorkerCmd("w-1", 500, 20000))
	mustApply(t, s, applyPlanCmd("eval-3", []assign{{"task-1", "w-1", 3, 15000}}))
	res = mustApply(t, s, tickCmd(15001))

	task, _ := s.Task("task-1")
	if task.State != TaskFailed {
		t.Errorf("state = %s after the last attempt expired, want FAILED", task.State)
	}
	if task.WorkerID != "" || task.LeaseExpiryMs != 0 {
		t.Errorf("worker = %q lease = %d, want both cleared", task.WorkerID, task.LeaseExpiryMs)
	}
	if task.LastError == "" {
		t.Error("LastError is empty; a terminal reclaim should record why")
	}
	// No eval: there is nothing left for a scheduler to decide about this task.
	if len(res.EvalTriggers) != 0 {
		t.Errorf("EvalTriggers = %+v, want none for an exhausted task", res.EvalTriggers)
	}
	// The job is derived from its tasks, so it must settle too.
	if job, _ := s.Job("job-1"); job.State != JobFailed {
		t.Errorf("job state = %s, want FAILED", job.State)
	}
}

// CompleteTask / FailTask

func TestCompleteTaskMarksCompleteAndFinishesJob(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s, completeTaskCmd("task-1", "w-1", 1, "/var/out/task-1"))

	task, _ := s.Task("task-1")
	if task.State != TaskComplete {
		t.Errorf("state = %s, want COMPLETE", task.State)
	}
	if task.ResultRef != "/var/out/task-1" {
		t.Errorf("resultRef = %q, want the path", task.ResultRef)
	}
	if task.LeaseExpiryMs != 0 {
		t.Errorf("lease = %d, want cleared", task.LeaseExpiryMs)
	}
	if job, _ := s.Job("job-1"); job.State != JobComplete {
		t.Errorf("job state = %s, want COMPLETE", job.State)
	}
}

// a slow worker's late report must not produce a second observable effect
func TestStaleReportFromSlowWorkerIsIgnored(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))

	// Worker A goes slow; its lease lapses and the task is reclaimed.
	mustApply(t, s, tickCmd(5001))
	// Worker B picks it up as attempt 2 and completes it.
	mustApply(t, s, registerWorkerCmd("w-2", 5000, 20000))
	mustApply(t, s, applyPlanCmd("eval-2", []assign{{"task-1", "w-2", 2, 15000}}))
	mustApply(t, s, completeTaskCmd("task-1", "w-2", 2, "/var/out/from-w2"))

	// Worker A finally reports attempt 1. Must be silently ignored.
	res, err := s.Apply(completeTaskCmd("task-1", "w-1", 1, "/var/out/from-w1"))
	if err != nil {
		t.Fatalf("stale report returned error %v, want silent no-op", err)
	}
	if res.JobID != "" {
		t.Errorf("stale report produced a Result %+v, want zero", res)
	}
	task, _ := s.Task("task-1")
	if task.ResultRef != "/var/out/from-w2" {
		t.Errorf("resultRef = %q — the stale report overwrote the real one", task.ResultRef)
	}
	if task.Attempt != 2 {
		t.Errorf("attempt = %d, want 2", task.Attempt)
	}
}

func TestCompleteTaskIsIdempotentOnRedelivery(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s, completeTaskCmd("task-1", "w-1", 1, "/var/out/task-1"))

	before := dump(s)
	mustApply(t, s, completeTaskCmd("task-1", "w-1", 1, "/var/out/task-1"))
	if after := dump(s); after != before {
		t.Errorf("redelivered completion mutated state\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestFailTaskRetriesUntilExhausted(t *testing.T) {
	s := seedCluster(t, 1) // MaxAttempts = 3

	now := int64(1000)
	for attempt := int32(1); attempt <= 3; attempt++ {
		// A retryable failure now sets a backoff, so the clock has to pass it
		// before the next dispatch is legal. Tick clears the deadline and
		// emits a BACKOFF_ELAPSED eval; without that sweep the task would sit
		// PENDING with nothing coming to dispatch it.
		if attempt > 1 {
			now += maxBackoffMs * 2
			mustApply(t, s, tickCmd(now))
		}
		mustApply(t, s, registerWorkerCmd("w-1", 500, now+90000))
		mustApply(t, s, applyPlanCmd("eval-x", []assign{{"task-1", "w-1", attempt, now + 50000}}))
		res := mustApply(t, s, failTaskCmd("task-1", "w-1", attempt, true))

		task, _ := s.Task("task-1")
		if attempt < 3 {
			if task.State != TaskPending {
				t.Fatalf("attempt %d: state = %s, want PENDING (retry)", attempt, task.State)
			}
			if len(res.EvalTriggers) != 1 || res.EvalTriggers[0].Reason != EvalTaskFailed {
				t.Errorf("attempt %d: triggers = %+v, want one TASK_FAILED", attempt, res.EvalTriggers)
			}
		} else {
			// Retries exhausted at MaxAttempts.
			if task.State != TaskFailed {
				t.Fatalf("attempt %d: state = %s, want FAILED (exhausted)", attempt, task.State)
			}
			if len(res.EvalTriggers) != 0 {
				t.Errorf("exhausted task produced triggers %+v, want none", res.EvalTriggers)
			}
		}
	}
	if job, _ := s.Job("job-1"); job.State != JobFailed {
		t.Errorf("job state = %s, want FAILED", job.State)
	}
}

func TestFailTaskNonRetryableIsTerminalImmediately(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s, failTaskCmd("task-1", "w-1", 1, false))

	task, _ := s.Task("task-1")
	if task.State != TaskFailed {
		t.Errorf("state = %s, want FAILED on first non-retryable failure", task.State)
	}
	if task.Attempt != 1 {
		t.Errorf("attempt = %d, want 1 — FailTask must not bump the dispatch counter", task.Attempt)
	}
}

func TestJobFailsIfAnyTaskFails(t *testing.T) {
	s := seedCluster(t, 2)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{
		{"task-1", "w-1", 1, 5000},
		{"task-2", "w-1", 1, 5000},
	}))
	mustApply(t, s, completeTaskCmd("task-1", "w-1", 1, "/out/1"))

	// One task still running — job must not be terminal yet.
	if job, _ := s.Job("job-1"); job.State != JobRunning {
		t.Errorf("job state = %s, want RUNNING while task-2 is outstanding", job.State)
	}

	mustApply(t, s, failTaskCmd("task-2", "w-1", 1, false))
	if job, _ := s.Job("job-1"); job.State != JobFailed {
		t.Errorf("job state = %s, want FAILED", job.State)
	}
}

func TestCompleteUnknownTaskIsAnError(t *testing.T) {
	s := seedCluster(t, 1)
	if _, err := s.Apply(completeTaskCmd("nope", "w-1", 1, "/out")); err == nil {
		t.Error("completing an unknown task = nil error, want rejection")
	}
}

// CreateEval

func TestCreateEvalStoresEval(t *testing.T) {
	s := New()
	mustApply(t, s, submitJobCmd("job-1", "client-a", "req-1", "task-1"))
	res := mustApply(t, s, createEvalCmd("eval-1", "job-1",
		commandpb.EvalTrigger_EVAL_TRIGGER_JOB_SUBMITTED))

	if res.EvalID != "eval-1" {
		t.Errorf("Result.EvalID = %q, want eval-1", res.EvalID)
	}
	e, ok := s.Eval("eval-1")
	if !ok {
		t.Fatal("eval-1 not found")
	}
	if e.JobID != "job-1" || e.TriggeredBy != EvalJobSubmitted || e.Priority != 50 {
		t.Errorf("eval = %+v, want job-1 / JOB_SUBMITTED / 50", e)
	}
}

func TestCreateEvalIsIdempotent(t *testing.T) {
	s := New()
	mustApply(t, s, submitJobCmd("job-1", "client-a", "req-1", "task-1"))
	mustApply(t, s, createEvalCmd("eval-1", "job-1",
		commandpb.EvalTrigger_EVAL_TRIGGER_JOB_SUBMITTED))

	before := dump(s)
	// A leader that crashed mid-propose may re-propose the same eval.
	mustApply(t, s, createEvalCmd("eval-1", "job-1",
		commandpb.EvalTrigger_EVAL_TRIGGER_TASK_FAILED))

	if s.EvalCount() != 1 {
		t.Errorf("EvalCount = %d, want 1", s.EvalCount())
	}
	if after := dump(s); after != before {
		t.Errorf("re-proposed eval mutated state\nbefore:\n%s\nafter:\n%s", before, after)
	}
}

func TestCreateEvalRejectsUnknownJob(t *testing.T) {
	s := New()
	if _, err := s.Apply(createEvalCmd("eval-1", "nope",
		commandpb.EvalTrigger_EVAL_TRIGGER_JOB_SUBMITTED)); err == nil {
		t.Error("eval for unknown job = nil error, want rejection")
	}
	if s.EvalCount() != 0 {
		t.Errorf("EvalCount = %d, want 0", s.EvalCount())
	}
}

// SubmitJob emits the trigger the leader turns into that CreateEval.
func TestSubmitJobEmitsEvalTrigger(t *testing.T) {
	s := New()
	res := mustApply(t, s, submitJobCmd("job-1", "client-a", "req-1", "task-1"))
	if len(res.EvalTriggers) != 1 {
		t.Fatalf("EvalTriggers = %+v, want exactly one", res.EvalTriggers)
	}
	if res.EvalTriggers[0].Reason != EvalJobSubmitted || res.EvalTriggers[0].JobID != "job-1" {
		t.Errorf("trigger = %+v, want JOB_SUBMITTED for job-1", res.EvalTriggers[0])
	}

	// A deduplicated retry must NOT re-trigger: the eval already exists.
	retry := mustApply(t, s, submitJobCmd("job-2", "client-a", "req-1", "task-2"))
	if len(retry.EvalTriggers) != 0 {
		t.Errorf("dedup retry produced triggers %+v, want none", retry.EvalTriggers)
	}
}

// Determinism — the property the entire replicated design rests on

func TestApplyIsDeterministicAcrossReplicas(t *testing.T) {
	// A log exercising all seven commands and the interesting interleavings.
	// Every new command belongs here — this is the regression net for the one
	// property the whole replicated design rests on.
	log := []*commandpb.Command{
		submitJobCmd("job-1", "client-a", "req-1", "task-1", "task-2"),
		submitJobCmd("job-2", "client-b", "req-1", "task-3"),
		submitJobCmd("job-3", "client-a", "req-2", "task-4", "task-5", "task-6"),
		submitJobCmd("job-9", "client-a", "req-1", "task-9"), // dedup: no-op

		createEvalCmd("eval-1", "job-1", commandpb.EvalTrigger_EVAL_TRIGGER_JOB_SUBMITTED),
		createEvalCmd("eval-1", "job-1", commandpb.EvalTrigger_EVAL_TRIGGER_TASK_FAILED), // dup: no-op

		registerWorkerCmd("w-1", 100, 10_000),
		registerWorkerCmd("w-2", 100, 3_000),
		tickCmd(1_000),

		// Batch across two workers in a single entry.
		applyPlanCmd("eval-1", []assign{
			{"task-1", "w-1", 1, 8_000},
			{"task-2", "w-2", 1, 2_500},
			{"task-3", "w-1", 1, 8_000},
			{"task-4", "w-1", 1, 8_000},
		}),

		completeTaskCmd("task-1", "w-1", 1, "/out/task-1"),
		failTaskCmd("task-3", "w-1", 1, true),  // retryable → PENDING
		failTaskCmd("task-4", "w-1", 1, false), // terminal → FAILED, job-3 not done yet

		// w-2's lease (3000) lapses → LOST; task-2 reclaimed → PENDING.
		tickCmd(3_500),
		tickCmd(3_000), // backwards: clamped, no-op

		// task-3 redispatched as attempt 2, then completed.
		applyPlanCmd("eval-1", []assign{{"task-3", "w-1", 2, 8_000}}),
		completeTaskCmd("task-3", "w-1", 2, "/out/task-3"),

		// A slow worker's stale report for attempt 1 — must be ignored.
		completeTaskCmd("task-3", "w-1", 1, "/out/stale"),

		// Stop an assigned task, then let the clock run past everything.
		registerWorkerCmd("w-2", 100, 20_000),
		applyPlanCmd("eval-1", []assign{{"task-2", "w-2", 2, 15_000}}),
		applyPlanCmd("eval-1", nil, "task-2"),
		tickCmd(30_000),
	}

	// Two independent "replicas" apply the same log prefix.
	a, b := New(), New()
	for _, cmd := range log {
		if _, err := a.Apply(cmd); err != nil {
			t.Fatalf("replica A: %v", err)
		}
	}
	for _, cmd := range log {
		if _, err := b.Apply(cmd); err != nil {
			t.Fatalf("replica B: %v", err)
		}
	}

	if da, db := dump(a), dump(b); da != db {
		t.Errorf("replicas diverged\n--- A ---\n%s\n--- B ---\n%s", da, db)
	}
}

func TestReadsReturnCopies(t *testing.T) {
	s := New()
	mustApply(t, s, submitJobCmd("job-1", "client-a", "req-1", "task-1"))

	job, _ := s.Job("job-1")
	job.TaskIDs[0] = "mutated"
	job.State = JobFailed

	fresh, _ := s.Job("job-1")
	if fresh.TaskIDs[0] != "task-1" {
		t.Error("mutating a returned Job corrupted live state")
	}
	if fresh.State != JobPending {
		t.Error("mutating a returned Job corrupted live state")
	}

	task, _ := s.Task("task-1")
	task.Spec.Args[0] = "mutated"
	freshTask, _ := s.Task("task-1")
	if freshTask.Spec.Args[0] != "task-1" {
		t.Error("mutating a returned Task's Args corrupted live state")
	}
}

// The closed command set

func TestMalformedCommandsAreRejected(t *testing.T) {
	s := New()
	if _, err := s.Apply(nil); err == nil {
		t.Error("Apply(nil) = nil error, want error")
	}
	if _, err := s.Apply(&commandpb.Command{}); err == nil {
		t.Error("Apply(empty command) = nil error, want error")
	}
}

// Day 5 — backoff, session bounding, effect semantics

// Jitter must be DETERMINISTIC. apply runs on every replica, so a real random
// roll would place the same task at different times on different nodes — the
// exact divergence the Tick design exists to prevent.
func TestBackoffIsDeterministicAndDecorrelated(t *testing.T) {
	// Same input, same answer, every time and on every replica.
	for i := 0; i < 50; i++ {
		if a, b := backoffFor("task-1", 2), backoffFor("task-1", 2); a != b {
			t.Fatalf("backoffFor is not deterministic: %d vs %d", a, b)
		}
	}

	// Different tasks failing together must not retry in lockstep — that is
	// the property jitter is actually for.
	seen := make(map[int64]int)
	for i := 0; i < 32; i++ {
		seen[backoffFor(fmt.Sprintf("task-%d", i), 1)]++
	}
	if len(seen) < 8 {
		t.Errorf("only %d distinct backoffs across 32 tasks; retries are correlated", len(seen))
	}

	// Exponential in the attempt, and capped.
	a1, a2, a3 := backoffFor("t", 1), backoffFor("t", 2), backoffFor("t", 3)
	if !(a1 < a2 && a2 < a3) {
		t.Errorf("not increasing: %d, %d, %d", a1, a2, a3)
	}
	if got := backoffFor("t", 30); got > maxBackoffMs+maxBackoffMs/2 {
		t.Errorf("backoff %d exceeded the cap", got)
	}
}

// A retryable failure backs off; a lease expiry does NOT.
//
// The distinction is deliberate and load-bearing: a failed task RAN and may be
// crash-looping, but an expired lease means the WORKER died and the task never
// got its chance. Backing off there would blow the reclaim budget the résumé
// number depends on.
func TestFailureBacksOffButLeaseExpiryDoesNot(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s, failTaskCmd("task-1", "w-1", 1, true))

	failed, _ := s.Task("task-1")
	if failed.State != TaskPending {
		t.Fatalf("state = %s, want PENDING", failed.State)
	}
	if failed.NotBeforeMs <= s.NowMs() {
		t.Errorf("NotBeforeMs = %d at now = %d; a retryable failure must back off",
			failed.NotBeforeMs, s.NowMs())
	}

	// Now the lease-expiry path, on a fresh task.
	s2 := seedCluster(t, 1)
	mustApply(t, s2, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s2, tickCmd(5001))
	reclaimed, _ := s2.Task("task-1")
	if reclaimed.State != TaskPending {
		t.Fatalf("state = %s, want PENDING", reclaimed.State)
	}
	if reclaimed.NotBeforeMs != 0 {
		t.Errorf("NotBeforeMs = %d, want 0 — a dead worker's task redispatches immediately", reclaimed.NotBeforeMs)
	}
}

// A task in backoff cannot be dispatched, and Tick is what releases it. The
// release must emit an eval: time passing is otherwise invisible to the
// scheduler, so the task would sit PENDING with nothing coming for it.
func TestBackoffBlocksDispatchUntilTickReleasesIt(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s, failTaskCmd("task-1", "w-1", 1, true))
	task, _ := s.Task("task-1")
	deadline := task.NotBeforeMs

	// Too early: the backstop refuses.
	mustApply(t, s, registerWorkerCmd("w-1", 500, 90000))
	if _, err := s.Apply(applyPlanCmd("eval-2", []assign{{"task-1", "w-1", 2, 50000}})); err == nil {
		t.Fatal("applyPlan accepted a task still serving its backoff")
	}

	// A tick before the deadline releases nothing.
	res := mustApply(t, s, tickCmd(deadline-1))
	for _, trig := range res.EvalTriggers {
		if trig.Reason == EvalBackoffElapsed {
			t.Error("backoff released early")
		}
	}

	// A tick past it clears the deadline and wakes the scheduler.
	res = mustApply(t, s, tickCmd(deadline+1))
	var woke bool
	for _, trig := range res.EvalTriggers {
		if trig.Reason == EvalBackoffElapsed && trig.TaskID == "task-1" {
			woke = true
		}
	}
	if !woke {
		t.Error("no BACKOFF_ELAPSED eval; the task would never be rescheduled")
	}
	if task, _ := s.Task("task-1"); task.NotBeforeMs != 0 {
		t.Errorf("NotBeforeMs = %d, want cleared", task.NotBeforeMs)
	}

	// And now it dispatches.
	mustApply(t, s, registerWorkerCmd("w-1", 500, deadline+90000))
	mustApply(t, s, applyPlanCmd("eval-3", []assign{{"task-1", "w-1", 2, deadline + 50000}}))
	if task, _ := s.Task("task-1"); task.State != TaskAssigned || task.NotBeforeMs != 0 {
		t.Errorf("state = %s notBefore = %d, want ASSIGNED and 0", task.State, task.NotBeforeMs)
	}
}

// The release is ONE-SHOT: a second tick must not re-emit the eval, or every
// tick after a backoff would manufacture scheduling work forever.
func TestBackoffReleaseIsOneShot(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s, failTaskCmd("task-1", "w-1", 1, true))
	task, _ := s.Task("task-1")

	mustApply(t, s, tickCmd(task.NotBeforeMs+1))
	res := mustApply(t, s, tickCmd(task.NotBeforeMs+2))
	for _, trig := range res.EvalTriggers {
		if trig.Reason == EvalBackoffElapsed {
			t.Error("BACKOFF_ELAPSED emitted twice for one backoff")
		}
	}
}

// Dedup must be bounded or the sessions map is a leak. The honest consequence
// is that dedup is bounded IN TIME: a retry after the TTL gets a new job.
func TestSessionsExpireOnTTL(t *testing.T) {
	s := New()
	mustApply(t, s, tickCmd(1000))
	first := mustApply(t, s, submitJobCmd("job-1", "client-a", "req-1", "task-1"))

	// Within the TTL, the retry still returns the original job.
	mustApply(t, s, tickCmd(1000+sessionTTLMs/2))
	retry := mustApply(t, s, submitJobCmd("job-X", "client-a", "req-1", "task-X"))
	if retry.JobID != first.JobID {
		t.Fatalf("retry returned %q, want the original %q", retry.JobID, first.JobID)
	}
	if s.JobCount() != 1 {
		t.Fatalf("job count = %d, want 1", s.JobCount())
	}

	// Past the TTL the session is swept, and the same request is a new job.
	mustApply(t, s, tickCmd(1000+sessionTTLMs+1))
	if n := s.SessionCount(); n != 0 {
		t.Errorf("session count = %d after the TTL, want 0 — the map would grow forever", n)
	}
	late := mustApply(t, s, submitJobCmd("job-2", "client-a", "req-1", "task-2"))
	if late.JobID == first.JobID {
		t.Error("session survived its TTL")
	}
	if s.JobCount() != 2 {
		t.Errorf("job count = %d, want 2", s.JobCount())
	}
}

// THE DAY 5 CHECKPOINT, state-machine half: the same request applied five
// times yields exactly one job. (The concurrent version lives in the server
// tests, where propose() is what serializes them.)
func TestRepeatedSubmitYieldsExactlyOneJob(t *testing.T) {
	s := New()
	mustApply(t, s, tickCmd(1000))
	ids := make(map[string]bool)
	for i := 0; i < 5; i++ {
		res := mustApply(t, s, submitJobCmd(fmt.Sprintf("job-%d", i), "client-a", "req-1", fmt.Sprintf("task-%d", i)))
		ids[res.JobID] = true
	}
	if len(ids) != 1 {
		t.Errorf("got %d distinct job ids, want 1", len(ids))
	}
	if s.JobCount() != 1 || s.TaskCount() != 1 {
		t.Errorf("jobs = %d tasks = %d, want 1 and 1", s.JobCount(), s.TaskCount())
	}
}

// THE DAY 5 CHECKPOINT, the effect half: a worker that does the work and dies
// before reporting leaves exactly ONE committed result, not two.
//
// Both attempts genuinely ran — that is at-least-once delivery, and the
// per-attempt ledger records both. What makes the EFFECT singular is that only
// one attempt is ever the live one, so only one result_ref is committed.
func TestWorkDoneButNotReportedYieldsOneCommittedResult(t *testing.T) {
	s := seedCluster(t, 1)
	mustApply(t, s, registerWorkerCmd("w-2", 500, 90000))

	// Attempt 1: w-1 takes it, runs it, and dies before ReportResult lands.
	mustApply(t, s, applyPlanCmd("eval-1", []assign{{"task-1", "w-1", 1, 5000}}))
	mustApply(t, s, tickCmd(5001)) // lease lapses, task reclaimed

	// Attempt 2: w-2 takes it and reports success.
	mustApply(t, s, registerWorkerCmd("w-2", 500, 20000))
	mustApply(t, s, applyPlanCmd("eval-2", []assign{{"task-1", "w-2", 2, 15000}}))
	mustApply(t, s, completeTaskCmd("task-1", "w-2", 2, "/data/w-2/task-1/2"))

	// Attempt 1's report finally arrives. It names a dead attempt.
	mustApply(t, s, completeTaskCmd("task-1", "w-1", 1, "/data/w-1/task-1/1"))

	task, _ := s.Task("task-1")
	if task.State != TaskComplete {
		t.Fatalf("state = %s, want COMPLETE", task.State)
	}
	if task.ResultRef != "/data/w-2/task-1/2" {
		t.Errorf("result_ref = %q, want w-2's attempt 2 — the late report must not overwrite it", task.ResultRef)
	}
	if task.Attempt != 2 {
		t.Errorf("attempt = %d, want 2", task.Attempt)
	}
}
