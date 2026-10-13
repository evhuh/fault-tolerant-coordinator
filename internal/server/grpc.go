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

// notLeader is the redirect. Returning FAILED_PRECONDITION with the leader's
// address in the message lets a client retry against the right node without
// a discovery mechanism.
//
// Reads go through this too. A follower's state is a prefix of the leader's,
// so serving a read from one would be stale by construction — and the LEADER
// serving reads from local state is itself non-linearizable on the minority
// side of a partition. That is Day 9's bug to hunt, and the fix (a ReadIndex
// barrier) lands in Coordinator.read.
func (s *Service) notLeader() error {
	if addr, ok := s.c.LeaderAddr(); ok {
		return status.Errorf(codes.FailedPrecondition, "NOT_LEADER: leader is at %s", addr)
	}
	return status.Error(codes.Unavailable, "NOT_LEADER: no leader elected")
}

// requireLeader guards every RPC that must not be served by a follower.
func (s *Service) requireLeader() error {
	if !s.c.IsLeader() {
		return s.notLeader()
	}
	return nil
}

// Coordinator exposes the Service's owner so cmd/coordinator can run the
// ticker alongside it.
func (s *Service) Coordinator() *Coordinator { return s.c }

// SubmitJob assigns every id, reads the clock once, and proposes a command in
// which nothing is left to compute. That is the RPC/command distinction: a
// request is an ask, a command is a decided fact.
func (s *Service) SubmitJob(ctx context.Context, req *schedulerpb.SubmitJobRequest) (*schedulerpb.SubmitJobResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
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
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
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
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
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
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty worker_id")
	}
	// The worker declares how many tasks it runs at once. The planner needs
	// it to answer "does this worker have room" — Day 3 did not, because
	// workers pulled and self-limited.
	if _, err := s.c.propose(s.c.registerWorkerCmd(req.GetWorkerId(), nil, req.GetCapacity())); err != nil {
		return nil, status.Errorf(codes.Internal, "register worker: %v", err)
	}
	return &schedulerpb.RegisterWorkerResponse{LeaseTtlMs: s.c.cfg.LeaseTTL.Milliseconds()}, nil
}

// Heartbeat is renewal AND acknowledgement, proposed as a RegisterWorker.
// running_task_ids is what promotes ASSIGNED -> RUNNING; there is no eighth
// command, because keeping the set closed is what makes the log's shape
// checkable by the compiler rather than by discipline.
func (s *Service) Heartbeat(ctx context.Context, req *schedulerpb.HeartbeatRequest) (*schedulerpb.HeartbeatResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty worker_id")
	}
	// Capacity 0 means "unchanged": a heartbeat renews the lease and
	// republishes the running claim, it does not redeclare the worker.
	if _, err := s.c.propose(s.c.registerWorkerCmd(req.GetWorkerId(), req.GetRunningTaskIds(), 0)); err != nil {
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

// RequestTask is a PURE READ.
//
// It hands a worker a task the planner already decided on and committed via
// ApplyPlan. Delivering a decision is not making one, so this RPC mutates
// nothing — which is why there is no eighth command for it.
//
// This is what the Day 3 seam became. That version had a second clause that
// called assign() inline, scanning for a PENDING task and dispatching it on
// the worker's poll. Day 4 deleted it: the planner now produces ASSIGNED
// tasks proactively, so the handler keeps only its read half. The worker
// binary, the executor, and the ApplyPlan command did not move.
func (s *Service) RequestTask(ctx context.Context, req *schedulerpb.RequestTaskRequest) (*schedulerpb.RequestTaskResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "empty worker_id")
	}

	if err := s.requireLeader(); err != nil {
		return nil, err
	}

	wait := time.Duration(req.GetWaitMs()) * time.Millisecond
	if wait > maxWait {
		wait = maxWait
	}
	deadline := time.Now().Add(wait)

	for {
		// Re-checked every pass, not just on entry: a worker parked here for
		// seconds must be told promptly when this node is deposed, rather
		// than waiting out wait_ms against a node that can no longer assign
		// it anything.
		if !s.c.IsLeader() {
			return nil, s.notLeader()
		}
		task, found := s.heldTask(req.GetWorkerId())
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

// heldTask returns the task this worker currently holds, if any.
//
// The scan is over SORTED ids so the answer never depends on map iteration
// order. A worker holding several tasks (capacity > 1) gets the lowest-sorted
// one it has not yet claimed locally; the worker dedups its own claims.
func (s *Service) heldTask(workerID string) (state.Task, bool) {
	var out state.Task
	var found bool
	s.c.read(func(st *state.StateStore) {
		for _, id := range st.TaskIDs() {
			t, ok := st.Task(id)
			if !ok || t.WorkerID != workerID {
				continue
			}
			if t.State == state.TaskAssigned {
				out, found = t, true
				return
			}
		}
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
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
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
