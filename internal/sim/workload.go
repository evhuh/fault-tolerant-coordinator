package sim

import (
	"context"
	"fmt"

	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// The workload: enough traffic that faults land on a system that is actually
// doing something. A cluster attacked while idle proves very little.
//
// Every call here is BEST EFFORT. Mid-partition there may be no leader, and
// an RPC failing is the expected outcome rather than a test failure — the
// invariants, not the RPCs, are what decide whether the run passed.

func (s *Sim) registerWorkers() {
	lead := s.leader()
	if lead == nil {
		return
	}
	for i := 1; i <= s.cfg.WorkerCount; i++ {
		_, _ = lead.svc.RegisterWorker(context.Background(), &schedulerpb.RegisterWorkerRequest{
			WorkerId: fmt.Sprintf("w-%d", i),
			Capacity: s.cfg.WorkerSlots,
		})
	}
}

// offerWork submits jobs and renews worker leases.
//
// The heartbeats matter as much as the submissions: without them every
// worker's lease lapses, every task is reclaimed, and the run degenerates
// into testing lease expiry alone.
func (s *Sim) offerWork(step int) {
	lead := s.leader()
	if lead == nil {
		return // no leader right now; that is a normal state mid-chaos
	}

	for i := 1; i <= s.cfg.WorkerCount; i++ {
		_, _ = lead.svc.Heartbeat(context.Background(), &schedulerpb.HeartbeatRequest{
			WorkerId: fmt.Sprintf("w-%d", i),
		})
	}

	for j := 0; j < s.cfg.JobsPerStep; j++ {
		specs := make([]*schedulerpb.TaskSpec, 0, s.cfg.TasksPerJob)
		for t := 0; t < s.cfg.TasksPerJob; t++ {
			specs = append(specs, &schedulerpb.TaskSpec{Command: "/bin/true", MaxAttempts: 5})
		}
		// A stable request id per (step, index) so a retry across a failover
		// dedups rather than creating a second job — which is also what makes
		// the job count predictable enough to assert on.
		_, _ = lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
			ClientId:  "sim",
			RequestId: fmt.Sprintf("step-%d-%d", step, j),
			Tasks:     specs,
		})
	}
}

// drainSomeWork plays the worker pool: poll, report success, repeat.
//
// Deliberately partial — it drains a bounded number per step, so there is
// always work in flight when the next fault lands. A fault that only ever
// hits an idle cluster is a fault that tests nothing.
func (s *Sim) drainSomeWork() {
	lead := s.leader()
	if lead == nil {
		return
	}
	const maxPerStep = 12
	for i := 0; i < maxPerStep; i++ {
		progressed := false
		for w := 1; w <= s.cfg.WorkerCount; w++ {
			workerID := fmt.Sprintf("w-%d", w)
			resp, err := lead.svc.RequestTask(context.Background(), &schedulerpb.RequestTaskRequest{
				WorkerId: workerID,
			})
			if err != nil || resp.GetTask() == nil {
				continue
			}
			task := resp.GetTask()
			// The worker claims it, which is what promotes ASSIGNED→RUNNING,
			// then reports. Both can fail mid-chaos; neither is an error.
			_, _ = lead.svc.Heartbeat(context.Background(), &schedulerpb.HeartbeatRequest{
				WorkerId: workerID, RunningTaskIds: []string{task.GetTaskId()},
			})
			_, _ = lead.svc.ReportResult(context.Background(), &schedulerpb.ReportResultRequest{
				WorkerId: workerID, TaskId: task.GetTaskId(), Attempt: task.GetAttempt(),
				ExitCode: 0, ResultRef: "/sim/" + workerID,
			})
			progressed = true
		}
		if !progressed {
			return
		}
	}
}
