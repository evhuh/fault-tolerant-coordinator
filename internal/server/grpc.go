package server

import (
	"context"
	"errors"
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

// proposeError maps a propose() failure onto a gRPC code the CLIENT can act
// on. Getting this wrong is silent: a client that is handed InvalidArgument
// for a lost proposal gives up instead of retrying, and the failure only
// shows during a failover.
//
// Three outcomes, three behaviours:
//
//	ErrNotLeader    → NOT_LEADER, so the pool re-aims at the real leader.
//	                  Leadership can be lost BETWEEN the guard and the commit.
//	ErrProposalLost → Unavailable, so the pool retries. The entry may still
//	                  commit, which is safe: every command is idempotent, and
//	                  that is exactly what the dedup key is for.
//	anything else   → InvalidArgument: the state machine rejected it, and
//	                  retrying would only be rejected again.
func (s *Service) proposeError(err error, what string) error {
	switch {
	case errors.Is(err, ErrNotLeader):
		return s.notLeader()
	case errors.Is(err, ErrProposalLost):
		return status.Errorf(codes.Unavailable, "%s: proposal lost, retry", what)
	case errors.Is(err, state.ErrRequestIDReused):
		// DEFINITIVE: nothing was created, and retrying cannot help because
		// the key itself is the problem.
		//
		// InvalidArgument deliberately, and listed explicitly rather than
		// left to the default below. AlreadyExists reads better in isolation
		// but would be WRONG here: the linearizability recorder classifies
		// nil and InvalidArgument as definitive and everything else as
		// UNKNOWN, so a new code would make a rejection that created nothing
		// look like an outcome the client cannot know — and client.Pool.Do
		// retries only NOT_LEADER and unreachable, so a definitive code is
		// also what stops a caller from retrying a key that will never work.
		return status.Errorf(codes.InvalidArgument, "%s: %v", what, err)
	default:
		return status.Errorf(codes.InvalidArgument, "%s: %v", what, err)
	}
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

	jobID := s.c.newJobID()
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
		return nil, s.proposeError(err, "submit job")
	}
	// On a retry the state machine returns the ORIGINAL job id and the jobID
	// minted above is simply discarded, never having entered state. That is
	// (clientID, requestID) dedup doing its job.
	return &schedulerpb.SubmitJobResponse{JobId: res.JobID}, nil
}

// GetJob reads through readClient, so under ReadIndexBarrier it is served
// only after a quorum confirmed this node still leads.
//
// The ORDER matters: the barrier runs before NOT_FOUND is decided. Deciding
// absence from local state and then confirming leadership would answer
// NOT_FOUND for a job the cluster had already committed — which is precisely
// the Day 9 violation, moved one line later.
func (s *Service) GetJob(ctx context.Context, req *schedulerpb.GetJobRequest) (*schedulerpb.GetJobResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	var out *schedulerpb.Job
	if err := s.c.readClient(ctx, func(st *state.StateStore) {
		if j, ok := st.Job(req.GetJobId()); ok {
			out = jobToProto(st, j)
		}
	}); err != nil {
		return nil, s.readError(err, "get job")
	}
	if out == nil {
		return nil, status.Errorf(codes.NotFound, "unknown job %q", req.GetJobId())
	}
	return &schedulerpb.GetJobResponse{Job: out}, nil
}

// readError maps a refused read onto a gRPC code.
//
// Unavailable, never NotFound and never an empty success. A read that could
// not be confirmed returned no value, and the one thing it must not look like
// is an answer — a client handed NOT_FOUND for an unconfirmed read draws
// exactly the false conclusion the barrier exists to prevent.
func (s *Service) readError(err error, what string) error {
	switch {
	case errors.Is(err, ErrNotLeader):
		return s.notLeader()
	case errors.Is(err, ErrReadBarrier):
		return status.Errorf(codes.Unavailable,
			"%s: read barrier not confirmed by a quorum, retry: %v", what, err)
	default:
		return status.Errorf(codes.Internal, "%s: %v", what, err)
	}
}

func (s *Service) ListJobs(ctx context.Context, req *schedulerpb.ListJobsRequest) (*schedulerpb.ListJobsResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	out := &schedulerpb.ListJobsResponse{}
	if err := s.c.readClient(ctx, func(st *state.StateStore) {
		for _, id := range st.JobIDs() {
			if req.GetLimit() > 0 && int32(len(out.Jobs)) >= req.GetLimit() {
				return
			}
			if j, ok := st.Job(id); ok {
				out.Jobs = append(out.Jobs, jobToProto(st, j))
			}
		}
	}); err != nil {
		return nil, s.readError(err, "list jobs")
	}
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
		return nil, s.proposeError(err, "register worker")
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
		return nil, s.proposeError(err, "heartbeat")
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
	deadline := s.c.clk.Now().Add(wait)

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
		if !s.c.clk.Now().Before(deadline) {
			// Nothing available within wait_ms. An empty response is normal,
			// not an error: the worker simply polls again.
			return &schedulerpb.RequestTaskResponse{}, nil
		}
		select {
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-s.c.clk.After(pollInterval):
			// Lock released for the whole sleep. See pollInterval.
		}
	}
}

// heldTask returns the task this worker currently holds, if any.
//
// Answered from the worker->tasks index, so the cost is bounded by this
// worker's capacity rather than by the size of the cluster's task map. It used
// to sort EVERY task id in the store, on every pass of the long poll, under
// the apply lock — 540-695ms Ready-loop stalls in apply at 10,000 tasks with
// 256 pollers. The rule it produced: no O(state) work under the apply lock on
// an RPC path.
//
// The index returns ids SORTED, so the answer never depends on map iteration
// order. A worker holding several tasks (capacity > 1) gets the lowest-sorted
// one it has not yet claimed locally; the worker dedups its own claims.
func (s *Service) heldTask(workerID string) (state.Task, bool) {
	var out state.Task
	var found bool
	s.c.read(func(st *state.StateStore) {
		out, found = st.AssignedTaskFor(workerID)
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
		return nil, s.proposeError(err, "report result")
	}
	return &schedulerpb.ReportResultResponse{}, nil
}
