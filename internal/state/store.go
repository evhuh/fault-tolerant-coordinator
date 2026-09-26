// Package state implements the coordinator's replicated state machine

/*
StateStore is PURE. Apply is a deterministic function of (current state, command): it never reads the wall clock, never performs I/O, never consults randomness, and never lets Go's randomized map iteration order influence the resulting state. Two replicas that apply the same log prefix must reach identical state, and a replica replaying that log off disk days later mustreach the same state again.

Every mutation enters through Apply, and Apply accepts exactly the seven commands in commandpb.Command. There is no other way to change state. That property is what lets Day 6 swap direct calls for Raft proposals without a rewrite.
*/

package state

import (
	"fmt"
	"sort"

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
	JobID    string
	WorkerID string
	EvalID   string
	// EvalTriggers are evals the leader should propose as a consequence of this
	// command. Deterministically computed on every replica; only the leader
	// acts on them.
	EvalTriggers []EvalTrigger
}

// sessionKey dedups client retries
// TOOD: TTL-based bounding
type WorkerState uint8

const (
	WorkerAlive WorkerState = iota + 1
	WorkerLost              // lease expired; the planner must not assign to it
)

func (s WorkerState) String() string {
	switch s {
	case WorkerAlive:
		return "ALIVE"
	case WorkerLost:
		return "LOST"
	}
	return "UNKNOWN"
}

type Worker struct {
	ID            string
	State         WorkerState
	RegisteredMs  int64
	LeaseExpiryMs int64
}

// EvalReason mirrors commandpb.EvalTrigger: why someone should re-think a job.
type EvalReason uint8

const (
	EvalJobSubmitted EvalReason = iota + 1
	EvalTaskFailed
	EvalWorkerLost
	EvalLeaseExpired
)

func (r EvalReason) String() string {
	switch r {
	case EvalJobSubmitted:
		return "JOB_SUBMITTED"
	case EvalTaskFailed:
		return "TASK_FAILED"
	case EvalWorkerLost:
		return "WORKER_LOST"
	case EvalLeaseExpired:
		return "LEASE_EXPIRED"
	}
	return "UNKNOWN"
}

// Eval is "something changed; someone should think about this job."
// Only CreateEval puts one here.
type Eval struct {
	ID          string
	JobID       string
	Priority    int32
	TriggeredBy EvalReason
	CreatedMs   int64
}

// EvalTrigger is a deterministic signal that an Eval *ought* to exist. Apply
// computes these on every replica; only the LEADER acts on them, by proposing
// a CreateEval. Followers discard the Result entirely.
//
// Losing one is survivable: the eval broker is rebuilt on leadership
// acquisition by scanning committed state, so pending work is rediscovered
// from task states rather than from Eval objects alone.
type EvalTrigger struct {
	JobID  string
	TaskID string // empty when the trigger is job-level
	Reason EvalReason
}

type sessionKey struct {
	clientID  string
	requestID string
}

type StateStore struct {
	jobs    map[string]*Job
	tasks   map[string]*Task
	workers map[string]*Worker
	evals   map[string]*Eval

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
		workers:  make(map[string]*Worker),
		evals:    make(map[string]*Eval),
		sessions: make(map[sessionKey]Result), // for (clientID, requestID) dedup
	}
}

// sortedTaskIDs / sortedWorkerIDs exist so that any scan over state is
// deterministic. Go randomizes map iteration on purpose; letting that order
// influence the resulting state would silently diverge replicas, and it would
// pass every single-node test.
func (s *StateStore) sortedTaskIDs() []string {
	ids := make([]string, 0, len(s.tasks))
	for id := range s.tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *StateStore) sortedWorkerIDs() []string {
	ids := make([]string, 0, len(s.workers))
	for id := range s.workers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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
		return s.applyCreateEval(k.CreateEval)
	case *commandpb.Command_ApplyPlan:
		return s.applyPlan(k.ApplyPlan)
	case *commandpb.Command_CompleteTask:
		return s.applyCompleteTask(k.CompleteTask)
	case *commandpb.Command_FailTask:
		return s.applyFailTask(k.FailTask)
	case *commandpb.Command_RegisterWorker:
		return s.applyRegisterWorker(k.RegisterWorker)
	case *commandpb.Command_Tick:
		return s.applyTick(k.Tick)
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
	// A new job is something someone should think about. The leader turns this into a CreateEval; followers discard it
	res.EvalTriggers = append(res.EvalTriggers, EvalTrigger{
		JobID:  job.ID,
		Reason: EvalJobSubmitted,
	})
	if dedupable {
		// Store the response WITHOUT triggers: a retry must return the original answer, but must not cause the eval to be created a second time
		s.sessions[key] = Result{JobID: job.ID}
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

// 2. RegisterWorker
/*
	Create-or-RENEW, and note WHY it does double duty: there is deliberately no Heartbeat command in the closed command set, so a worker's Heartbeat RPC is proposed as a RegisterWorker carrying a fresh lease_expiry_ms. Registration and renewal become one idempotent code path.

	Renewal also extends the leases of that worker's live tasks. Otherwise Tick
	would reclaim work out from under a perfectly healthy worker.

	(1) Validate
	(2) Create or revive the worker, set its lease
	(3) Extend leases on its ASSIGNED/RUNNING tasks (sorted scan)
*/
func (s *StateStore) applyRegisterWorker(c *commandpb.RegisterWorker) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("register worker: nil payload")
	}
	if c.GetWorkerId() == "" {
		return Result{}, fmt.Errorf("register worker: empty worker_id")
	}
	if c.GetLeaseExpiryMs() <= 0 {
		return Result{}, fmt.Errorf("register worker: worker %q has no lease_expiry_ms", c.GetWorkerId())
	}

	// (2) A worker that was marked LOST and comes back is revived, not rejected: a restarted worker re-registering is normal operation
	w, exists := s.workers[c.GetWorkerId()]
	if !exists {
		w = &Worker{ID: c.GetWorkerId(), RegisteredMs: c.GetRegisteredMs()}
		s.workers[w.ID] = w
	}
	w.State = WorkerAlive
	w.LeaseExpiryMs = c.GetLeaseExpiryMs()

	// (3) Sorted scan: map order must never influence resulting state
	for _, id := range s.sortedTaskIDs() {
		t := s.tasks[id]
		if t.WorkerID != w.ID {
			continue
		}
		if t.State == TaskAssigned || t.State == TaskRunning {
			t.LeaseExpiryMs = c.GetLeaseExpiryMs()
		}
	}
	return Result{WorkerID: w.ID}, nil
}


// 3. Tick
/*
	The state machine's ONLY clock. The leader proposes Tick{nowMs} every ~200ms and lease expiry reads the timestamp carried IN the log entry, so every replica expires the same leases at the same log position.

	nowMs is MONOTONIC NON-DECREASING: a Tick carrying an earlier timestamp (failover to a node with a skewed clock, an NTP step) is CLAMPED, not rejected. If time could move backward, an expired-and-reassigned task could become un-expired and you'd have two live assignments for one task.

	(1) Clamp nowMs forward
	(2) Expire worker leases:  ALIVE + lease < now  →  LOST
	(3) Expire task leases:    ASSIGNED/RUNNING + lease < now  →  PENDING
	        Attempt is NOT touched here — it is a dispatch counter, incremented only by ApplyPlan. That is what makes a slow worker's late report (carrying an older attempt) detectably stale rather than a duplicate.
	        BUT reclaim must respect the same attempt budget ApplyPlan enforces: a task already at MaxAttempts goes terminal FAILED instead of PENDING. Otherwise it would sit PENDING forever — every plan naming it asks for attempt MaxAttempts+1, which applyPlan rejects — and a scheduler that scans for PENDING work would pick it, propose, be rejected, and spin. Permanently-PENDING also breaks "every submitted task reaches a terminal state".
	Both scans are over SORTED ids
*/
func (s *StateStore) applyTick(c *commandpb.Tick) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("tick: nil payload")
	}

	// (1) Clamp. A backwards tick is a no-op, not an error.
	if c.GetNowMs() > s.nowMs {
		s.nowMs = c.GetNowMs()
	}
	now := s.nowMs

	var res Result

	// (2) Workers whose lease lapsed are LOST; the planner must stop assigning to them. Their tasks are reclaimed by (3) on the same tick
	for _, id := range s.sortedWorkerIDs() {
		w := s.workers[id]
		if w.State == WorkerAlive && w.LeaseExpiryMs < now {
			w.State = WorkerLost
		}
	}

	// (3) Reclaim tasks whose lease lapsed, or whose worker is gone
	for _, id := range s.sortedTaskIDs() {
		t := s.tasks[id]
		if t.State != TaskAssigned && t.State != TaskRunning {
			continue
		}
		workerLost := false
		if w, ok := s.workers[t.WorkerID]; ok && w.State == WorkerLost {
			workerLost = true
		}
		if t.LeaseExpiryMs >= now && !workerLost {
			continue
		}

		reason := EvalLeaseExpired
		if workerLost {
			reason = EvalWorkerLost
		}
		t.WorkerID = ""
		t.LeaseExpiryMs = 0

		// The attempt budget is shared by both failure paths. FailTask retries
		// while Attempt < MaxAttempts; reclaim must agree, or it hands the
		// scheduler a task no ApplyPlan can ever satisfy.
		if t.Attempt >= t.Spec.MaxAttempts {
			t.State = TaskFailed
			t.LastError = fmt.Sprintf("lease expired on attempt %d of %d (%s)",
				t.Attempt, t.Spec.MaxAttempts, reason)
			s.recomputeJobState(t.JobID)
			continue
		}

		t.State = TaskPending
		res.EvalTriggers = append(res.EvalTriggers, EvalTrigger{
			JobID:  t.JobID,
			TaskID: t.ID,
			Reason: reason,
		})
	}
	return res, nil
}


// 4. ApplyPlan
/*
	The dispatch command, and the ONLY place Attempt increments.

	One accepted plan commits as ONE Raft entry however many assignments it carries- that batching is what amortizes fsync across many scheduling decisions, and it is the throughput argument the whole design rests on.

	ALL-OR-NOTHING. The planner has already verified this plan against current state before proposing it (that is what optimistic concurrency means here), so by the time apply sees it, it should be valid. If any piece is not, the whole plan is rejected — a partially-applied plan would diverge replicas that gave up at different points.

	(1) Validate every assignment and stop
	(2) Apply stops:       ASSIGNED/RUNNING → PENDING, worker cleared
	(3) Apply assignments: PENDING → ASSIGNED, Attempt := assignment.Attempt
	(4) Promote the job to RUNNING on its first assignment
*/
func (s *StateStore) applyPlan(c *commandpb.ApplyPlan) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("apply plan: nil payload")
	}
	if c.GetEvalId() == "" {
		return Result{}, fmt.Errorf("apply plan: empty eval_id")
	}

	// (1) Validate everything first
	seen := make(map[string]bool, len(c.GetAssignments()))
	for _, a := range c.GetAssignments() {
		t, ok := s.tasks[a.GetTaskId()]
		if !ok {
			return Result{}, fmt.Errorf("apply plan: unknown task %q", a.GetTaskId())
		}
		if seen[a.GetTaskId()] {
			return Result{}, fmt.Errorf("apply plan: task %q assigned twice in one plan", a.GetTaskId())
		}
		seen[a.GetTaskId()] = true

		if t.State != TaskPending {
			return Result{}, fmt.Errorf("apply plan: task %q is %s, want PENDING", t.ID, t.State)
		}
		// Attempt is a strict dispatch counter. Anything else means the planner worked from a stale snapshot and the plan must not commit
		if a.GetAttempt() != t.Attempt+1 {
			return Result{}, fmt.Errorf("apply plan: task %q attempt %d, want %d (stale plan)",
				t.ID, a.GetAttempt(), t.Attempt+1)
		}
		if a.GetAttempt() > t.Spec.MaxAttempts {
			return Result{}, fmt.Errorf("apply plan: task %q attempt %d exceeds max_attempts %d",
				t.ID, a.GetAttempt(), t.Spec.MaxAttempts)
		}
		w, ok := s.workers[a.GetWorkerId()]
		if !ok {
			return Result{}, fmt.Errorf("apply plan: unknown worker %q", a.GetWorkerId())
		}
		if w.State != WorkerAlive {
			return Result{}, fmt.Errorf("apply plan: worker %q is %s, want ALIVE", w.ID, w.State)
		}
		if a.GetLeaseExpiryMs() <= s.nowMs {
			return Result{}, fmt.Errorf("apply plan: task %q lease %d already expired at now=%d",
				t.ID, a.GetLeaseExpiryMs(), s.nowMs)
		}
	}
	for _, st := range c.GetStops() {
		t, ok := s.tasks[st.GetTaskId()]
		if !ok {
			return Result{}, fmt.Errorf("apply plan: stop names unknown task %q", st.GetTaskId())
		}
		if t.State != TaskAssigned && t.State != TaskRunning {
			return Result{}, fmt.Errorf("apply plan: cannot stop task %q in state %s", t.ID, t.State)
		}
	}

	var res Result
	res.EvalID = c.GetEvalId()

	// (2) Stops first: a plan may stop a task on one worker and re-assign it
	for _, st := range c.GetStops() {
		t := s.tasks[st.GetTaskId()]
		t.State = TaskPending
		t.WorkerID = ""
		t.LeaseExpiryMs = 0
		t.LastError = st.GetReason()
	}

	// (3) Assignments
	for _, a := range c.GetAssignments() {
		t := s.tasks[a.GetTaskId()]
		t.State = TaskAssigned
		t.WorkerID = a.GetWorkerId()
		t.Attempt = a.GetAttempt()
		t.LeaseExpiryMs = a.GetLeaseExpiryMs()

		// (4)
		if j, ok := s.jobs[t.JobID]; ok && j.State == JobPending {
			j.State = JobRunning
		}
	}
	return res, nil
}


// staleReport reports whether a CompleteTask/FailTask names an attempt that is no longer the live one. This is the heart of effectively-once: 
// worker A gets attempt 1 -> goes slow (not dead) -> lease expires -> task reclaimed -> worker B gets attempt 2 → worker A finally reports A's report carries attempt 1 while the task is at 2. Ignoring it (silently, deterministically, on every replica) is what turns at-least-once DELIVERY into effectively-once EFFECTS
func staleReport(t *Task, attempt int32) bool {
	return attempt != t.Attempt
}

// terminal reports whether a task can no longer change state
func terminal(st TaskState) bool { return st == TaskComplete || st == TaskFailed }

// recomputeJobState derives a job's state from its tasks. Called after any task reaches a terminal state. Iterates job.TaskIDs- an ordered slice, so no map-order hazard
func (s *StateStore) recomputeJobState(jobID string) {
	j, ok := s.jobs[jobID]
	if !ok {
		return
	}
	allTerminal, anyFailed := true, false
	for _, id := range j.TaskIDs {
		t, ok := s.tasks[id]
		if !ok {
			continue
		}
		if !terminal(t.State) {
			allTerminal = false
		}
		if t.State == TaskFailed {
			anyFailed = true
		}
	}
	switch {
	case allTerminal && anyFailed:
		j.State = JobFailed
	case allTerminal:
		j.State = JobComplete
	}
}

// 5. CompleteTask
/*
	A worker finished attempt N successfully.

	result_ref is a PATH ON THE WORKER'S DISK. The output itself never enters the Raft log — only the reference and the exit code. Keeping the replicated log small is a deliberate production pattern.

	(1) Unknown task → error (a genuinely malformed command)
	(2) Stale attempt → silent no-op, NOT an error (see staleReport)
	(3) Already terminal → no-op, so a redelivered report is idempotent
	(4) Mark COMPLETE, clear the lease, recompute the job
*/
func (s *StateStore) applyCompleteTask(c *commandpb.CompleteTask) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("complete task: nil payload")
	}
	t, ok := s.tasks[c.GetTaskId()]
	if !ok {
		return Result{}, fmt.Errorf("complete task: unknown task %q", c.GetTaskId())
	}
	if staleReport(t, c.GetAttempt()) || terminal(t.State) {
		return Result{}, nil
	}

	t.State = TaskComplete
	t.ResultRef = c.GetResultRef()
	t.ExitCode = c.GetExitCode()
	t.WorkerID = c.GetWorkerId()
	t.LeaseExpiryMs = 0
	t.LastError = ""

	s.recomputeJobState(t.JobID)
	return Result{JobID: t.JobID}, nil
}


// 6. FailTask
/*
	A worker finished attempt N unsuccessfully — or the executor timed out.

	Two outcomes, decided entirely from state (never from config, never from a clock), so every replica reaches the same verdict:
	  retryable AND Attempt < MaxAttempts  →  back to PENDING, emit an eval
	  otherwise                            →  terminal FAILED

	Attempt is NOT incremented here. The next ApplyPlan does that when it dispatches attempt N+1. Backoff between attempts is a scheduling policy and lives in the planner, not in the state machine

	(1) Unknown task → error
	(2) Stale attempt or already terminal → no-op
	(3) Retry or fail terminally; recompute the job
*/
func (s *StateStore) applyFailTask(c *commandpb.FailTask) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("fail task: nil payload")
	}
	t, ok := s.tasks[c.GetTaskId()]
	if !ok {
		return Result{}, fmt.Errorf("fail task: unknown task %q", c.GetTaskId())
	}
	if staleReport(t, c.GetAttempt()) || terminal(t.State) {
		return Result{}, nil
	}

	res := Result{JobID: t.JobID}
	t.LastError = c.GetError()
	t.WorkerID = ""
	t.LeaseExpiryMs = 0

	if c.GetRetryable() && t.Attempt < t.Spec.MaxAttempts {
		t.State = TaskPending
		res.EvalTriggers = append(res.EvalTriggers, EvalTrigger{
			JobID:  t.JobID,
			TaskID: t.ID,
			Reason: EvalTaskFailed,
		})
	} else {
		t.State = TaskFailed
		t.ExitCode = c.GetExitCode()
	}

	s.recomputeJobState(t.JobID)
	return res, nil
}


// 7. CreateEval
/*
	"Something changed; someone should think about this job"

	The ONLY way an Eval enters state. The leader proposes it in response to an EvalTrigger that a previous command's apply returned- job submitted, lease expired, worker lost, task failed.

	Note what is NOT here: no priority queue, no ordering, no dequeue. That is the EvalBroker, and it is DERIVED, NOT REPLICATED — a leader-only in-memory structure rebuilt by scanning committed state on leadership acquisition (TOOD). Replicating a work queue would mean every pop is a consensus round-trip, and it would need rebuilding after failover anyway.

	Because the broker is derived, a CreateEval lost to a leader crash is survivable: the new leader rediscovers the work by scanning for PENDING tasks, not by reading Eval objects.

	(1) Validate; unknown job is an error
	(2) Duplicate eval_id → idempotent no-op (the leader may retry a propose)
	(3) Create
*/
func (s *StateStore) applyCreateEval(c *commandpb.CreateEval) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("create eval: nil payload")
	}
	if c.GetEvalId() == "" {
		return Result{}, fmt.Errorf("create eval: empty eval_id")
	}
	if _, ok := s.jobs[c.GetJobId()]; !ok {
		return Result{}, fmt.Errorf("create eval: unknown job %q", c.GetJobId())
	}
	// (2) A leader that crashed mid-propose may re-propose the same eval
	if _, exists := s.evals[c.GetEvalId()]; exists {
		return Result{EvalID: c.GetEvalId(), JobID: c.GetJobId()}, nil
	}

	s.evals[c.GetEvalId()] = &Eval{
		ID:          c.GetEvalId(),
		JobID:       c.GetJobId(),
		Priority:    c.GetPriority(),
		TriggeredBy: evalReasonFromProto(c.GetTriggeredBy()),
		CreatedMs:   c.GetCreatedMs(),
	}
	return Result{EvalID: c.GetEvalId(), JobID: c.GetJobId()}, nil
}

func evalReasonFromProto(t commandpb.EvalTrigger) EvalReason {
	switch t {
	case commandpb.EvalTrigger_EVAL_TRIGGER_JOB_SUBMITTED:
		return EvalJobSubmitted
	case commandpb.EvalTrigger_EVAL_TRIGGER_TASK_FAILED:
		return EvalTaskFailed
	case commandpb.EvalTrigger_EVAL_TRIGGER_WORKER_LOST:
		return EvalWorkerLost
	case commandpb.EvalTrigger_EVAL_TRIGGER_LEASE_EXPIRED:
		return EvalLeaseExpired
	}
	return EvalJobSubmitted
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

func (s *StateStore) Worker(id string) (Worker, bool) {
	w, ok := s.workers[id]
	if !ok {
		return Worker{}, false
	}
	return *w, true
}

func (s *StateStore) Eval(id string) (Eval, bool) {
	e, ok := s.evals[id]
	if !ok {
		return Eval{}, false
	}
	return *e, true
}

func (s *StateStore) JobCount() int    { return len(s.jobs) }
func (s *StateStore) TaskCount() int   { return len(s.tasks) }
func (s *StateStore) WorkerCount() int { return len(s.workers) }
func (s *StateStore) EvalCount() int   { return len(s.evals) }
