// Package state implements the coordinator's replicated state machine

/*
StateStore is PURE. Apply is a deterministic function of (current state, command): it never reads the wall clock, never performs I/O, never consults randomness, and never lets Go's randomized map iteration order influence the resulting state. Two replicas that apply the same log prefix must reach identical state, and a replica replaying that log off disk days later mustreach the same state again.

Every mutation enters through Apply, and Apply accepts exactly the seven commands in commandpb.Command. There is no other way to change state. That property is what lets Day 6 swap direct calls for Raft proposals without a rewrite.
*/

package state

import (
	"fmt"

	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// defaultMaxAttempts applies when a TaskSpec omits it. Any defaulting done here must be a constant, never a configured or environment-derived value: a replica configured differently would diverge
const defaultMaxAttempts int32 = 3

// TaskState and JobState are the state machine's own types, deliberately not the generated API enums. The API schema evolves for clients; this one evolves for the log. Conversion happens at the server boundary.
type TaskState uint8

const (
	TaskPending  TaskState = iota + 1 // submitted, not yet assigned
	TaskAssigned                      // in a committed plan, worker hasn't picked it up
	TaskRunning                       // worker acknowledged, lease is live
	TaskComplete                      // terminal, succeeded
	TaskFailed                        // terminal, retries exhausted
)

func (s TaskState) String() string {
	switch s {
	case TaskPending:
		return "PENDING"
	case TaskAssigned:
		return "ASSIGNED"
	case TaskRunning:
		return "RUNNING"
	case TaskComplete:
		return "COMPLETE"
	case TaskFailed:
		return "FAILED"
	}
	return "UNKNOWN"
}

type JobState uint8

const (
	JobPending JobState = iota + 1
	JobRunning
	JobComplete
	JobFailed
)

func (s JobState) String() string {
	switch s {
	case JobPending:
		return "PENDING"
	case JobRunning:
		return "RUNNING"
	case JobComplete:
		return "COMPLETE"
	case JobFailed:
		return "FAILED"
	}
	return "UNKNOWN"
}

type TaskSpec struct {
	Command     string
	Args        []string
	TimeoutMs   int64
	MaxAttempts int32
}

type Task struct {
	ID            string
	JobID         string
	Spec          TaskSpec
	State         TaskState
	Attempt       int32 // increments per dispatch; (ID, Attempt) is the idempotency key
	WorkerID      string
	LeaseExpiryMs int64
	ResultRef     string // path on the worker's disk; the output never enters the log
	ExitCode      int32
	LastError     string
}

type Job struct {
	ID        string
	ClientID  string
	RequestID string
	State     JobState
	// TaskIDs is a slice, not a set: submission order is semantic, and a map would make iteration order (and therefore derived state) nondeterministic.
	TaskIDs   []string
	Priority  int32
	CreatedMs int64
}

// Result is what the leader rets its waiting client
// Followers compute the identical Result and discard it
type Result struct {
	JobID string
}

// sessionKey dedups client retries
// TOOD: TTL-based bounding
type sessionKey struct {
	clientID  string
	requestID string
}

type StateStore struct {
	jobs  map[string]*Job
	tasks map[string]*Task

	// nowMs is the state machine's ONLY notion of time, advanced solely by Tick. Nothing in this package may call time.Now().
	nowMs int64

	// sessions maps (clientID, requestID) to the Result first produced for it, so a retried SubmitJob returns the original response instead of creating a second job.

	// TOOD: bound this: expire sessions on a TTL carried by Tick, or it grows without lim
	sessions map[sessionKey]Result
}

func New() *StateStore {
	return &StateStore{
		jobs:     make(map[string]*Job),
		tasks:    make(map[string]*Task),
		sessions: make(map[sessionKey]Result), // for (clientID, requestID) dedup
	}
}

// Apply is the single entry point for mutation. Errors returned here must be deterministic: every replica must reject the same command for the same reason, or they disagree about whether it took effect
func (s *StateStore) Apply(cmd *commandpb.Command) (Result, error) {
	if cmd == nil {
		return Result{}, fmt.Errorf("apply: nil command")
	}
	switch k := cmd.GetKind().(type) {
	case *commandpb.Command_SubmitJob:
		return s.applySubmitJob(k.SubmitJob)
	case *commandpb.Command_CreateEval:
		return Result{}, fmt.Errorf("apply: CreateEval not implemented")
	case *commandpb.Command_ApplyPlan:
		return Result{}, fmt.Errorf("apply: ApplyPlan not implemented")
	case *commandpb.Command_CompleteTask:
		return Result{}, fmt.Errorf("apply: CompleteTask not implemented")
	case *commandpb.Command_FailTask:
		return Result{}, fmt.Errorf("apply: FailTask not implemented")
	case *commandpb.Command_RegisterWorker:
		return Result{}, fmt.Errorf("apply: RegisterWorker not implemented")
	case *commandpb.Command_Tick:
		return Result{}, fmt.Errorf("apply: Tick not implemented")
	default:
		return Result{}, fmt.Errorf("apply: unknown command kind %T", k)
	}
}

// 1. SubmitJob
/*
	(1) Dedup
	(2) Validate
	(3) Mutate: Create the job, create N PENDING tasks at Attemp: 0. record the session
*/
func (s *StateStore) applySubmitJob(c *commandpb.SubmitJob) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("submit job: nil payload")
	}
	if c.GetJobId() == "" {
		return Result{}, fmt.Errorf("submit job: empty job_id (the leader must assign it before proposing)")
	}

	// (1) Retry of an already-applied request: return the original response. Checked before validation so a retry stays idempotent even if the validation rules have since changed
	key := sessionKey{clientID: c.GetClientId(), requestID: c.GetRequestId()}
	dedupable := key.clientID != "" && key.requestID != ""
	if dedupable {
		if prev, ok := s.sessions[key]; ok {
			return prev, nil
		}
	}

	if _, exists := s.jobs[c.GetJobId()]; exists {
		return Result{}, fmt.Errorf("submit job: job %q already exists", c.GetJobId())
	}
	if len(c.GetTasks()) == 0 {
		return Result{}, fmt.Errorf("submit job: job %q has no tasks", c.GetJobId())
	}
	// (2) Validate every task before mutating anything, so a rejected cmd leaves no partial state behind
	for i, te := range c.GetTasks() {
		if te.GetTaskId() == "" {
			return Result{}, fmt.Errorf("submit job: task %d has empty task_id", i)
		}
		if _, exists := s.tasks[te.GetTaskId()]; exists {
			return Result{}, fmt.Errorf("submit job: task %q already exists", te.GetTaskId())
		}
		if te.GetSpec().GetCommand() == "" {
			return Result{}, fmt.Errorf("submit job: task %q has empty command", te.GetTaskId())
		}
	}

	// (3)
	job := &Job{
		ID:        c.GetJobId(),
		ClientID:  c.GetClientId(),
		RequestID: c.GetRequestId(),
		State:     JobPending,
		TaskIDs:   make([]string, 0, len(c.GetTasks())),
		Priority:  c.GetPriority(),
		CreatedMs: c.GetCreatedMs(),
	}
	for _, te := range c.GetTasks() {
		s.tasks[te.GetTaskId()] = &Task{
			ID:      te.GetTaskId(),
			JobID:   job.ID,
			Spec:    specFromProto(te.GetSpec()),
			State:   TaskPending,
			Attempt: 0,
		}
		job.TaskIDs = append(job.TaskIDs, te.GetTaskId())
	}
	s.jobs[job.ID] = job

	res := Result{JobID: job.ID}
	if dedupable {
		s.sessions[key] = res
	}
	return res, nil
}

func specFromProto(p *schedulerpb.TaskSpec) TaskSpec {
	spec := TaskSpec{
		Command:     p.GetCommand(),
		TimeoutMs:   p.GetTimeoutMs(),
		MaxAttempts: p.GetMaxAttempts(),
	}
	if args := p.GetArgs(); len(args) > 0 {
		spec.Args = append([]string(nil), args...)
	}
	if spec.MaxAttempts <= 0 {
		spec.MaxAttempts = defaultMaxAttempts
	}
	return spec
}

// Reads. All return copies: callers must never hold a pointer into live state, which is also the discipline scheduler workers (TOOD) depend on when they read a snapshot concurrently

func (s *StateStore) NowMs() int64 { return s.nowMs }

func (s *StateStore) Job(id string) (Job, bool) {
	j, ok := s.jobs[id]
	if !ok {
		return Job{}, false
	}
	out := *j
	out.TaskIDs = append([]string(nil), j.TaskIDs...)
	return out, true
}

func (s *StateStore) Task(id string) (Task, bool) {
	t, ok := s.tasks[id]
	if !ok {
		return Task{}, false
	}
	out := *t
	out.Spec.Args = append([]string(nil), t.Spec.Args...)
	return out, true
}

func (s *StateStore) JobCount() int  { return len(s.jobs) }
func (s *StateStore) TaskCount() int { return len(s.tasks) }
