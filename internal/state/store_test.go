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



// Determinism — the property the entire replicated design rests on

func TestApplyIsDeterministicAcrossReplicas(t *testing.T) {
	log := []*commandpb.Command{
		submitJobCmd("job-1", "client-a", "req-1", "task-1", "task-2"),
		submitJobCmd("job-2", "client-b", "req-1", "task-3"),
		submitJobCmd("job-3", "client-a", "req-2", "task-4", "task-5", "task-6"),
		submitJobCmd("job-9", "client-a", "req-1", "task-9"), // dedup: no-op
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

func TestUnimplementedCommandsReturnErrors(t *testing.T) {
	s := New()
	for _, cmd := range []*commandpb.Command{
		{Kind: &commandpb.Command_CreateEval{CreateEval: &commandpb.CreateEval{}}},
		{Kind: &commandpb.Command_ApplyPlan{ApplyPlan: &commandpb.ApplyPlan{}}},
		{Kind: &commandpb.Command_CompleteTask{CompleteTask: &commandpb.CompleteTask{}}},
		{Kind: &commandpb.Command_FailTask{FailTask: &commandpb.FailTask{}}},
		{Kind: &commandpb.Command_RegisterWorker{RegisterWorker: &commandpb.RegisterWorker{}}},
		{Kind: &commandpb.Command_Tick{Tick: &commandpb.Tick{}}},
	} {
		if _, err := s.Apply(cmd); err == nil {
			t.Errorf("Apply(%T) = nil error, want 'not implemented'", cmd.GetKind())
		}
	}
	if _, err := s.Apply(nil); err == nil {
		t.Error("Apply(nil) = nil error, want error")
	}
	if _, err := s.Apply(&commandpb.Command{}); err == nil {
		t.Error("Apply(empty command) = nil error, want error")
	}
}
