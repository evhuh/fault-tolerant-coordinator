package sched

import (
	"context"
	"testing"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

func eval(id string, priority int32) Eval {
	return Eval{ID: id, JobID: "job-1", Priority: priority}
}

func mustDequeue(t *testing.T, b *Broker) Eval {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	e, ok := b.Dequeue(ctx)
	if !ok {
		t.Fatal("Dequeue returned nothing")
	}
	return e
}

func TestBrokerOrdersByPriorityThenFIFO(t *testing.T) {
	b := NewBroker()
	b.Enqueue(eval("low", 1))
	b.Enqueue(eval("high-first", 9))
	b.Enqueue(eval("high-second", 9))
	b.Enqueue(eval("mid", 5))

	want := []string{"high-first", "high-second", "mid", "low"}
	for _, id := range want {
		if got := mustDequeue(t, b).ID; got != id {
			t.Fatalf("dequeued %q, want %q", got, id)
		}
	}
}

// A repeated id must not enqueue twice. The leader can legitimately re-derive
// the same eval from a replayed command, and two copies would mean two plans
// racing for the same work.
func TestBrokerIgnoresDuplicateIDs(t *testing.T) {
	b := NewBroker()
	b.Enqueue(eval("e-1", 5))
	b.Enqueue(eval("e-1", 5))
	if ready, _ := b.Stats(); ready != 1 {
		t.Fatalf("ready = %d, want 1", ready)
	}
}

// Blocked evals wait for capacity rather than vanishing.
func TestBrokerBlockAndUnblock(t *testing.T) {
	b := NewBroker()
	e := eval("e-1", 5)
	b.Enqueue(e)
	mustDequeue(t, b)

	b.Block(e)
	if ready, blocked := b.Stats(); ready != 0 || blocked != 1 {
		t.Fatalf("ready = %d blocked = %d, want 0 and 1", ready, blocked)
	}

	b.Unblock()
	if ready, blocked := b.Stats(); ready != 1 || blocked != 0 {
		t.Fatalf("ready = %d blocked = %d, want 1 and 0", ready, blocked)
	}
	if got := mustDequeue(t, b).ID; got != "e-1" {
		t.Errorf("dequeued %q, want e-1", got)
	}
}

func TestBrokerDequeueBlocksUntilEnqueue(t *testing.T) {
	b := NewBroker()
	go func() {
		time.Sleep(20 * time.Millisecond)
		b.Enqueue(eval("late", 1))
	}()
	if got := mustDequeue(t, b).ID; got != "late" {
		t.Errorf("dequeued %q, want late", got)
	}
}

func TestBrokerDequeueUnblocksOnClose(t *testing.T) {
	b := NewBroker()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, ok := b.Dequeue(context.Background()); ok {
			t.Error("Dequeue returned an eval after Close")
		}
	}()
	time.Sleep(10 * time.Millisecond)
	b.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Dequeue did not return after Close; a scheduler worker would leak")
	}
}

// Rebuild is what makes derived-not-replicated correct: a new leader
// reconstructs the queue from committed state with none of the old leader's
// memory, by scanning for PENDING TASKS rather than for Eval objects.
func TestBrokerRebuildsOneEvalPerJobWithPendingWork(t *testing.T) {
	st := state.New()
	apply := func(cmd *commandpb.Command) {
		t.Helper()
		if _, err := st.Apply(cmd); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	mkJob := func(jobID string, n int, priority int32) {
		entries := make([]*commandpb.TaskEntry, 0, n)
		for i := 0; i < n; i++ {
			entries = append(entries, &commandpb.TaskEntry{
				TaskId: jobID + "/" + string(rune('a'+i)),
				Spec:   &schedulerpb.TaskSpec{Command: "/bin/true", MaxAttempts: 3},
			})
		}
		apply(&commandpb.Command{Kind: &commandpb.Command_SubmitJob{
			SubmitJob: &commandpb.SubmitJob{JobId: jobID, Tasks: entries, Priority: priority},
		}})
	}
	mkJob("job-a", 2, 1)
	mkJob("job-b", 3, 9)

	b := NewBroker()
	n := b.Rebuild(st.Snapshot(), func(jobID string) string { return "eval/" + jobID })

	if n != 2 {
		t.Fatalf("rebuilt %d evals, want one per job with pending work (2)", n)
	}
	// The rebuilt evals carry their job's priority, so the busier, more
	// important job is considered first.
	if got := mustDequeue(t, b).JobID; got != "job-b" {
		t.Errorf("first eval is for %q, want job-b (priority 9)", got)
	}
	if got := mustDequeue(t, b).JobID; got != "job-a" {
		t.Errorf("second eval is for %q, want job-a", got)
	}
}

// A job whose tasks are all terminal contributes nothing to a rebuild.
func TestBrokerRebuildSkipsFinishedWork(t *testing.T) {
	st := state.New()
	_, _ = st.Apply(&commandpb.Command{Kind: &commandpb.Command_SubmitJob{
		SubmitJob: &commandpb.SubmitJob{
			JobId: "job-a",
			Tasks: []*commandpb.TaskEntry{{
				TaskId: "job-a/0000",
				Spec:   &schedulerpb.TaskSpec{Command: "/bin/true", MaxAttempts: 3},
			}},
		},
	}})
	_, _ = st.Apply(&commandpb.Command{Kind: &commandpb.Command_RegisterWorker{
		RegisterWorker: &commandpb.RegisterWorker{WorkerId: "w-1", LeaseExpiryMs: 10_000, Capacity: 1},
	}})
	_, _ = st.Apply(&commandpb.Command{Kind: &commandpb.Command_ApplyPlan{
		ApplyPlan: &commandpb.ApplyPlan{EvalId: "e", Assignments: []*commandpb.Assignment{{
			TaskId: "job-a/0000", WorkerId: "w-1", Attempt: 1, LeaseExpiryMs: 10_000,
		}}},
	}})
	_, _ = st.Apply(&commandpb.Command{Kind: &commandpb.Command_CompleteTask{
		CompleteTask: &commandpb.CompleteTask{TaskId: "job-a/0000", Attempt: 1, WorkerId: "w-1"},
	}})

	b := NewBroker()
	if n := b.Rebuild(st.Snapshot(), func(jobID string) string { return "eval/" + jobID }); n != 0 {
		t.Errorf("rebuilt %d evals for a finished job, want 0", n)
	}
}
