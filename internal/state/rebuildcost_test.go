package state

import (
	"fmt"
	"testing"
	"time"

	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// What the eval-broker rebuild costs, measured at several state sizes.
//
// The rebuild runs on the READY LOOP goroutine — StartScheduling is called
// from the leadership-change handler — so whatever it costs is time the node
// processes no raft messages. Day 10's 600-step calibration recorded 131ms
// and 187ms stalls in that phase on a healthy disk, which raised the question
// this answers: does the rebuild's cost grow with state, and which part of it
// is expensive?
//
// Three candidates are timed separately, because "the rebuild is slow" is not
// a diagnosis:
//
//   - Snapshot(), the deep COPY of every job, task and worker plus three
//     sorts. Not a serialization — that is Encode(), which is what costs
//     32-74ms in maybeSnapshot and which this path does not call.
//   - PendingTaskIDs, the scan the rebuild actually drives.
//   - Encode(), included only as the yardstick the other two are being
//     confused with.
func TestRebuildCostVsStateSize(t *testing.T) {
	for _, jobs := range []int{250, 1000, 4000} {
		s := storeWithJobs(t, jobs)

		const reps = 5
		var snapTotal, pendingTotal, encodeTotal time.Duration
		var pending, bytes int
		for i := 0; i < reps; i++ {
			start := time.Now()
			sn := s.Snapshot()
			snapTotal += time.Since(start)

			start = time.Now()
			ids := sn.PendingTaskIDs("")
			pendingTotal += time.Since(start)
			pending = len(ids)

			start = time.Now()
			enc, err := s.Encode()
			encodeTotal += time.Since(start)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			bytes = len(enc)
		}

		t.Logf("jobs=%-5d tasks=%-5d pending=%-5d | Snapshot() copy %-9s | PendingTaskIDs %-9s | Encode() %-9s (%d bytes)",
			jobs, jobs*2, pending,
			(snapTotal / reps).Round(time.Microsecond),
			(pendingTotal / reps).Round(time.Microsecond),
			(encodeTotal / reps).Round(time.Microsecond), bytes)
	}
}

// storeWithJobs builds a store holding n jobs of two tasks each, half of them
// still pending, which is the shape a long run accumulates.
func storeWithJobs(t *testing.T, n int) *StateStore {
	t.Helper()
	s := New()
	if _, err := s.Apply(&commandpb.Command{
		Version: CommandVersionCurrent,
		Kind:    &commandpb.Command_Tick{Tick: &commandpb.Tick{NowMs: 1000}},
	}); err != nil {
		t.Fatalf("tick: %v", err)
	}
	for i := 0; i < n; i++ {
		jobID := fmt.Sprintf("job-%06d", i)
		tasks := []*commandpb.TaskEntry{
			{TaskId: jobID + "/0", Spec: &schedulerpb.TaskSpec{Command: "/usr/bin/true", MaxAttempts: 3}},
			{TaskId: jobID + "/1", Spec: &schedulerpb.TaskSpec{Command: "/usr/bin/true", MaxAttempts: 3}},
		}
		cmd := &commandpb.Command{
			Version: CommandVersionCurrent,
			Kind: &commandpb.Command_SubmitJob{SubmitJob: &commandpb.SubmitJob{
				JobId: jobID, ClientId: "c", RequestId: fmt.Sprintf("r-%06d", i),
				Tasks: tasks, CreatedMs: 1000,
			}},
		}
		if _, err := s.Apply(cmd); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		// Half the jobs complete, so the pending set is realistic rather
		// than everything or nothing.
		if i%2 == 0 {
			for _, te := range tasks {
				if _, err := s.Apply(&commandpb.Command{
					Version: CommandVersionCurrent,
					Kind: &commandpb.Command_CompleteTask{CompleteTask: &commandpb.CompleteTask{
						TaskId: te.TaskId, WorkerId: "w-1", Attempt: 0, ResultRef: "/tmp/x", CompletedMs: 1001,
					}},
				}); err != nil {
					t.Fatalf("complete: %v", err)
				}
			}
		}
	}
	return s
}
