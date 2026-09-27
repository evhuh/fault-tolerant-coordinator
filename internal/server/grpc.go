package server

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// pollInterval is how often a long-polling RequestTask re-checks for work.
//
// It exists because the poll must NEVER hold c.mu while it waits. Blocking a
// long-poll under the apply lock would stop every other RPC, the ticker
// included, and deadlock the coordinator within one poll. So the shape is:
// take the lock, look, release, sleep, repeat.
//
// Day 4 makes this mostly moot — the planner assigns proactively, so the
// first look usually succeeds.
const pollInterval = 50 * time.Millisecond

// maxWait caps how long a worker can park a request, so a shutdown is never
// waiting on a client-chosen duration.
const maxWait = 30 * time.Second

type Service struct {
	schedulerpb.UnimplementedSchedulerServer
	c *Coordinator
}

func NewService(c *Coordinator) *Service { return &Service{c: c} }

// Coordinator exposes the Service's owner so cmd/coordinator can run the
// ticker alongside it.
func (s *Service) Coordinator() *Coordinator { return s.c }

// SubmitJob assigns every id, reads the clock once, and proposes a command in
// which nothing is left to compute. That is the RPC/command distinction: a
// request is an ask, a command is a decided fact.
func (s *Service) SubmitJob(ctx context.Context, req *schedulerpb.SubmitJobRequest) (*schedulerpb.SubmitJobResponse, error) {
	if len(req.GetTasks()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "job has no tasks")
	}
	for i, spec := range req.GetTasks() {
		if spec.GetCommand() == "" {
			return nil, status.Errorf(codes.InvalidArgument, "task %d has empty command", i)
		}
	}

	jobID := newJobID()
	entries := make([]*commandpb.TaskEntry, 0, len(req.GetTasks()))
	for i, spec := range req.GetTasks() {
		entries = append(entries, &commandpb.TaskEntry{
			TaskId: taskID(jobID, i),
			Spec:   spec,
		})
	}

	res, err := s.c.propose(&commandpb.Command{
		Kind: &commandpb.Command_SubmitJob{
			SubmitJob: &commandpb.SubmitJob{
				ClientId:  req.GetClientId(),
				RequestId: req.GetRequestId(),
				JobId:     jobID,
				Tasks:     entries,
				Priority:  req.GetPriority(),
				CreatedMs: s.c.nowMs(),
			},
		},
	})
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "submit job: %v", err)
	}
	// On a retry the state machine returns the ORIGINAL job id and the jobID
	// minted above is simply discarded, never having entered state. That is
	// (clientID, requestID) dedup doing its job.
	return &schedulerpb.SubmitJobResponse{JobId: res.JobID}, nil
}

func (s *Service) GetJob(ctx context.Context, req *schedulerpb.GetJobRequest) (*schedulerpb.GetJobResponse, error) {
	var out *schedulerpb.Job
	s.c.read(func(st *state.StateStore) {
		if j, ok := st.Job(req.GetJobId()); ok {
			out = jobToProto(st, j)
		}
	})
	if out == nil {
		return nil, status.Errorf(codes.NotFound, "unknown job %q", req.GetJobId())
	}
	return &schedulerpb.GetJobResponse{Job: out}, nil
}

func (s *Service) ListJobs(ctx context.Context, req *schedulerpb.ListJobsRequest) (*schedulerpb.ListJobsResponse, error) {
	out := &schedulerpb.ListJobsResponse{}
	s.c.read(func(st *state.StateStore) {
		for _, id := range st.JobIDs() {
			if req.GetLimit() > 0 && int32(len(out.Jobs)) >= req.GetLimit() {
				return
			}
			if j, ok := st.Job(id); ok {
				out.Jobs = append(out.Jobs, jobToProto(st, j))
			}
		}
	})
	return out, nil
}

func (s *Service) RegisterWorker(ctx context.Context, req *schedulerpb.RegisterWorkerRequest) (*schedulerpb.RegisterWorkerResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty worker_id")
	}
	if _, err := s.c.propose(s.c.registerWorkerCmd(req.GetWorkerId(), nil)); err != nil {
		return nil, status.Errorf(codes.Internal, "register worker: %v", err)
	}
	return &schedulerpb.RegisterWorkerResponse{LeaseTtlMs: s.c.cfg.LeaseTTL.Milliseconds()}, nil
}

// Heartbeat is renewal AND acknowledgement, proposed as a RegisterWorker.
// running_task_ids is what promotes ASSIGNED -> RUNNING; there is no eighth
// command, because keeping the set closed is what makes the log's shape
// checkable by the compiler rather than by discipline.
func (s *Service) Heartbeat(ctx context.Context, req *schedulerpb.HeartbeatRequest) (*schedulerpb.HeartbeatResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty worker_id")
	}
	if _, err := s.c.propose(s.c.registerWorkerCmd(req.GetWorkerId(), req.GetRunningTaskIds())); err != nil {
		return nil, status.Errorf(codes.Internal, "heartbeat: %v", err)
	}

	var expiry int64
	s.c.read(func(st *state.StateStore) {
		if w, ok := st.Worker(req.GetWorkerId()); ok {
			expiry = w.LeaseExpiryMs
		}
	})
	return &schedulerpb.HeartbeatResponse{LeaseExpiryMs: expiry}, nil
}

// RequestTask has two clauses, and the split is the Day 4 seam.
//
//	(1) return a task already assigned to this worker — a PURE READ, because
//	    the assignment was decided and committed by an ApplyPlan already;
//	    delivering a decision is not a mutation.
//	(2) only if there is none, run the placeholder assigner.
//
// On Day 4 clause (2) is DELETED: the planner produces ASSIGNED tasks
// proactively and this handler keeps only its read half. The seam is a branch
// removed, not a handler rewritten.
func (s *Service) RequestTask(ctx context.Context, req *schedulerpb.RequestTaskRequest) (*schedulerpb.RequestTaskResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty worker_id")
	}

	wait := time.Duration(req.GetWaitMs()) * time.Millisecond
	if wait > maxWait {
		wait = maxWait
	}
	deadline := time.Now().Add(wait)

	for {
		// Clause (1) and (2) share one lock acquisition, so a task assigned
		// to this worker by a concurrent poll cannot be missed between them.
		task, found := s.tryDispatch(req.GetWorkerId())
		if found {
			return &schedulerpb.RequestTaskResponse{Task: taskToProto(task)}, nil
		}
		if !time.Now().Before(deadline) {
			// Nothing available within wait_ms. An empty response is normal,
			// not an error: the worker simply polls again.
			return &schedulerpb.RequestTaskResponse{}, nil
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-time.After(pollInterval):
			// Lock released for the whole sleep. See pollInterval.
		}
	}
}

// tryDispatch runs both RequestTask clauses under one lock.
func (s *Service) tryDispatch(workerID string) (state.Task, bool) {
	s.c.mu.Lock()
	// (1) Already mine? Sorted scan, so the answer does not depend on map
	// iteration order.
	for _, id := range s.c.store.TaskIDs() {
		t, ok := s.c.store.Task(id)
		if !ok || t.WorkerID != workerID {
			continue
		}
		if t.State == state.TaskAssigned || t.State == state.TaskRunning {
			s.c.mu.Unlock()
			return t, true
		}
	}
	// (2) Otherwise, the placeholder. DELETE ON DAY 4.
	plan, ok := s.c.assign(workerID)
	s.c.mu.Unlock()
	if !ok {
		return state.Task{}, false
	}

	if _, err := s.c.propose(plan); err != nil {
		// A rejected plan is not an error worth reporting. Two workers racing
		// for the same task is exactly what applyPlan's PENDING check is for:
		// the loser just polls again. The placeholder is allowed to be naive
		// because the state machine is already the referee.
		return state.Task{}, false
	}

	assigned := plan.GetApplyPlan().GetAssignments()[0].GetTaskId()
	var out state.Task
	var found bool
	s.c.read(func(st *state.StateStore) {
		out, found = st.Task(assigned)
	})
	return out, found
}

// ReportResult is the at-least-once delivery point. It proposes CompleteTask
// or FailTask by exit code; the state machine decides everything else —
// whether the report is stale (its attempt is no longer the live one),
// whether the task is already terminal, and whether a failure retries or
// exhausts. None of that is decided here, because all of it must be decided
// identically on every replica.
func (s *Service) ReportResult(ctx context.Context, req *schedulerpb.ReportResultRequest) (*schedulerpb.ReportResultResponse, error) {
	now := s.c.nowMs()

	var cmd *commandpb.Command
	if req.GetExitCode() == 0 && req.GetError() == "" {
		cmd = &commandpb.Command{
			Kind: &commandpb.Command_CompleteTask{
				CompleteTask: &commandpb.CompleteTask{
					TaskId:      req.GetTaskId(),
					Attempt:     req.GetAttempt(),
					WorkerId:    req.GetWorkerId(),
					ResultRef:   req.GetResultRef(),
					ExitCode:    req.GetExitCode(),
					CompletedMs: now,
				},
			},
		}
	} else {
		cmd = &commandpb.Command{
			Kind: &commandpb.Command_FailTask{
				FailTask: &commandpb.FailTask{
					TaskId:    req.GetTaskId(),
					Attempt:   req.GetAttempt(),
					WorkerId:  req.GetWorkerId(),
					Error:     req.GetError(),
					Retryable: req.GetRetryable(),
					ExitCode:  req.GetExitCode(),
					FailedMs:  now,
				},
			},
		}
	}

	if _, err := s.c.propose(cmd); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "report result: %v", err)
	}
	return &schedulerpb.ReportResultResponse{}, nil
}
