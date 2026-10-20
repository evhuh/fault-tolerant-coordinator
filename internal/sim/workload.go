package sim

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"

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
			_, _ = lead.svc.ReportResult(context.Background(), s.outcomeFor(task, workerID))
			progressed = true
		}
		if !progressed {
			return
		}
	}
}

// outcomeFor decides whether this execution succeeded, by HASHING
// (seed, taskID, attempt) rather than drawing from the run's RNG.
//
// Drawing would have been the obvious thing and it is wrong. The number of
// draws depends on how many tasks happened to be drained in a step, which
// depends on goroutine timing — so every workload draw shifts the fault RNG's
// sequence and the FAULT SCHEDULE stops being reproducible. That is the one
// property the whole harness is built to provide, and adding failures to the
// workload broke it until this was derived instead.
//
// Hashing gives the same answer for the same task on the same seed, costs the
// fault sequence nothing, and is the same trick the state machine already
// uses for retry jitter.
//
// The split between retryable and terminal matters: a retryable failure sends
// the task back to PENDING with a backoff deadline and must later be woken by
// a BACKOFF_ELAPSED eval, while a non-retryable one goes straight to FAILED.
// Those are different branches, and exercising one proves nothing about the
// other.
func (s *Sim) outcomeFor(task *schedulerpb.Task, workerID string) *schedulerpb.ReportResultRequest {
	req := &schedulerpb.ReportResultRequest{
		WorkerId: workerID,
		TaskId:   task.GetTaskId(),
		Attempt:  task.GetAttempt(),
	}
	roll, split := s.outcomeRolls(task.GetTaskId(), task.GetAttempt())
	if s.cfg.FailureRate <= 0 || roll >= s.cfg.FailureRate {
		req.ExitCode = 0
		req.ResultRef = "/sim/" + workerID
		s.result.Coverage.ReportedOK++
		return req
	}
	if split < s.cfg.RetryableFraction {
		req.ExitCode = 1
		req.Error = "simulated transient failure"
		req.Retryable = true
		s.result.Coverage.ReportedRetryable++
		return req
	}
	req.ExitCode = 2
	req.Error = "simulated permanent failure"
	req.Retryable = false
	s.result.Coverage.ReportedTerminal++
	return req
}

// outcomeRolls derives two independent [0,1) values from (seed, taskID,
// attempt): one for whether the execution fails, one for which kind.
//
// Two separate hashes rather than one split in half, so the failure decision
// and the retryable/terminal decision cannot correlate — a task that fails is
// no more likely to fail terminally than any other.
func (s *Sim) outcomeRolls(taskID string, attempt int32) (roll, split float64) {
	return s.hashUnit(taskID, attempt, "fail"), s.hashUnit(taskID, attempt, "kind")
}

func (s *Sim) hashUnit(taskID string, attempt int32, salt string) float64 {
	h := fnv.New64a()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(s.cfg.Seed))
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(taskID))
	binary.BigEndian.PutUint32(b[:4], uint32(attempt))
	_, _ = h.Write(b[:4])
	_, _ = h.Write([]byte(salt))
	// Top 53 bits, the same way a float64 in [0,1) is normally built.
	return float64(h.Sum64()>>11) / float64(1<<53)
}
