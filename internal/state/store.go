// Package state implements the coordinator's replicated state machine

/*
StateStore is PURE. Apply is a deterministic function of (current state, command): it never reads the wall clock, never performs I/O, never consults randomness, and never lets Go's randomized map iteration order influence the resulting state. Two replicas that apply the same log prefix must reach identical state, and a replica replaying that log off disk days later mustreach the same state again.

Every mutation enters through Apply, and Apply accepts exactly the seven commands in commandpb.Command. There is no other way to change state. That property is what lets Day 6 swap direct calls for Raft proposals without a rewrite.
*/

package state

import (
	"fmt"
	"hash/fnv"
	"sort"

	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// Command versions. These describe the MEANING of a command, not its wire
// format — protobuf already handles added fields, and we have added four
// without incident. What it cannot handle is a field or a command whose
// semantics changed, because the log holds yesterday's intent.
//
// This is not hypothetical here. ApplyPlan was ALL-OR-NOTHING until Raft was
// wired, and became deterministic partial apply because one stale assignment
// should not discard 99 good ones. A log written under the old rule, replayed
// by the new code, reaches different state than the cluster actually had: an
// old plan with one bad assignment used to apply nothing and would now apply
// the rest.
//
// So the old behaviour is kept, keyed on the version stamped into the entry.
// That is the whole point of the field, and the cost of having operated
// something.
const (
	// CommandVersionAllOrNothingPlan is the original semantics: an ApplyPlan
	// commits whole or is rejected whole.
	CommandVersionAllOrNothingPlan uint32 = 1
	// CommandVersionPartialPlan skips invalid assignments and applies the
	// rest. Current.
	CommandVersionPartialPlan uint32 = 2
)

// defaultMaxAttempts applies when a TaskSpec omits it. Any defaulting done here must be a constant, never a configured or environment-derived value: a replica configured differently would diverge
const defaultMaxAttempts int32 = 3

// defaultCapacity applies when a worker registers without declaring one. Like defaultMaxAttempts it must be a CONSTANT: a replica that defaulted differently would diverge
const defaultCapacity int32 = 1

// Policy holds the durations the state machine itself measures against its
// Tick-carried clock.
//
// READ THIS BEFORE CHANGING IT. These are not per-node tuning knobs. They are
// evaluated inside apply, so two replicas holding different values place tasks
// at different times and expire sessions at different log positions — they
// diverge, silently, and the divergence only becomes visible when a leader
// change makes one node's version authoritative.
//
// They are injectable for exactly one reason: a simulated run spans under a
// second of state-machine time, and at production values a 10-minute session
// TTL and a 30-second backoff cap mean those paths NEVER execute. A verifier
// that cannot reach a code path provides no evidence about it.
//
// The contract, therefore: ONE Policy per cluster, identical on every replica,
// chosen before the log has data. It is deliberately NOT exposed as a
// coordinator flag — that is where the footgun would live. Only programmatic
// callers (the simulator) set it.
//
// The harness enforces this by accident and on purpose: the convergence check
// compares encoded state across replicas, so a mismatched Policy shows up as
// a divergence rather than as a mystery.
type Policy struct {
	// BaseBackoffMs is the first retry delay; it doubles per attempt.
	BaseBackoffMs int64
	// MaxBackoffMs caps it.
	MaxBackoffMs int64
	// SessionTTLMs bounds (clientID, requestID) dedup. Without it the
	// sessions map grows forever, which is a real leak in an always-on
	// coordinator.
	//
	// The honest consequence: dedup is bounded IN TIME, not absolute. A
	// client retrying later than this gets a second job, so the TTL must
	// comfortably exceed any sane client retry window.
	SessionTTLMs int64
}

// DefaultPolicy is production. Changing these values changes the meaning of
// logs already on disk, so treat them like a schema.
func DefaultPolicy() Policy {
	return Policy{
		BaseBackoffMs: 200,
		MaxBackoffMs:  30_000,
		SessionTTLMs:  10 * 60 * 1000, // 10 minutes
	}
}

func (p Policy) withDefaults() Policy {
	d := DefaultPolicy()
	if p.BaseBackoffMs <= 0 {
		p.BaseBackoffMs = d.BaseBackoffMs
	}
	if p.MaxBackoffMs <= 0 {
		p.MaxBackoffMs = d.MaxBackoffMs
	}
	if p.SessionTTLMs <= 0 {
		p.SessionTTLMs = d.SessionTTLMs
	}
	return p
}

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
	// NotBeforeMs holds a retry back off: the task is PENDING but must not be
	// dispatched until the state machine's clock passes this. Zero means
	// dispatchable now.
	//
	// It lives on derived state rather than in a command because Task is not
	// log content — only commands are — so this costs no schema change. It is
	// computed deterministically (see backoffFor), never rolled by the leader.
	NotBeforeMs int64
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
	// PlanApplied and PlanSkipped report how much of an ApplyPlan took
	// effect. A skip is an optimistic-concurrency conflict: the plan was built
	// against state that moved before the entry committed. Computed
	// identically on every replica; only the leader meters it.
	PlanApplied int
	PlanSkipped int
	// WorkerAdmitted is true when a RegisterWorker CREATED or REVIVED a
	// worker, as opposed to merely renewing a live one's lease. That
	// distinction matters to the leader: new capacity should wake evals
	// blocked for want of a worker, but a routine heartbeat should not — at
	// H=500ms per worker, unblocking on every renewal would turn blocked
	// evals into a polling loop.
	WorkerAdmitted bool
	// EvalTriggers are evals the leader should propose as a consequence of this
	// command. Deterministically computed on every replica; only the leader
	// acts on them.
	EvalTriggers []EvalTrigger
}

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
	// Capacity is how many tasks this worker runs at once. The planner needs it to decide whether a worker has room; the state machine only stores it, because "is this worker full" is a scheduling POLICY question and policy lives in the planner, not in apply
	Capacity int32
}

// EvalReason mirrors commandpb.EvalTrigger: why someone should re-think a job.
type EvalReason uint8

const (
	EvalJobSubmitted EvalReason = iota + 1
	EvalTaskFailed
	EvalWorkerLost
	EvalLeaseExpired
	EvalBackoffElapsed
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
	case EvalBackoffElapsed:
		return "BACKOFF_ELAPSED"
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
	// Attempt is the task's dispatch counter at the moment the trigger fired. It exists to make the eval id DERIVABLE: applyCreateEval is idempotent only on a repeated eval_id, so a random id would create a duplicate whenever the leader re-proposes. (taskID, attempt, reason) is unique per occurrence, because a given attempt can expire once and fail once
	Attempt int32
}

type sessionKey struct {
	clientID  string
	requestID string
}

// session is a remembered response plus when it was remembered, so Tick can
// expire it. The timestamp comes from the log, never from a replica's clock.
type session struct {
	result    Result
	createdMs int64
}

type StateStore struct {
	jobs    map[string]*Job
	tasks   map[string]*Task
	workers map[string]*Worker
	evals   map[string]*Eval

	// nowMs is the state machine's ONLY notion of time, advanced solely by Tick. Nothing in this package may call time.Now().
	nowMs int64

	// sessions maps (clientID, requestID) to the Result first produced for it, so a retried SubmitJob returns the original response instead of creating a second job.
	//
	// BOUNDED by Tick, on Policy.SessionTTLMs. The honest consequence is that dedup is bounded IN TIME, not absolute: a client retrying later than the TTL gets a second job
	sessions map[sessionKey]session

	// policy is fixed at construction and never changes for the life of the
	// store. It must be identical on every replica — see Policy.
	policy Policy
}

// New builds a store with production policy.
func New() *StateStore { return NewWithPolicy(DefaultPolicy()) }

// NewWithPolicy builds a store with the given durations. Every replica in a
// cluster must use the same Policy.
func NewWithPolicy(p Policy) *StateStore {
	return &StateStore{
		policy:   p.withDefaults(),
		jobs:     make(map[string]*Job),
		tasks:    make(map[string]*Task),
		workers:  make(map[string]*Worker),
		evals:    make(map[string]*Eval),
		sessions: make(map[sessionKey]session), // for (clientID, requestID) dedup
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

func (s *StateStore) sortedJobIDs() []string {
	ids := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
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
		return s.applyPlan(k.ApplyPlan, cmd.GetVersion())
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
			return prev.result, nil
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
		s.sessions[key] = session{result: Result{JobID: job.ID}, createdMs: s.nowMs}
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

	It is ALSO the worker's acknowledgement. running_task_ids names the tasks
	the worker reports itself actively executing, and that is what promotes
	ASSIGNED -> RUNNING. There is deliberately no eighth command and no ack
	RPC: RequestTask only DELIVERS a decision ApplyPlan already committed, so
	it must stay a pure read. Folding the ack into renewal keeps the command
	set closed and makes registration, renewal, and acknowledgement one
	idempotent path.

	(1) Validate
	(2) Create or revive the worker, set its lease
	(3) Extend leases on its ASSIGNED/RUNNING tasks, and promote the ones it
	    claims to be running (sorted scan)
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
	admitted := !exists || w.State != WorkerAlive
	if !exists {
		w = &Worker{ID: c.GetWorkerId(), RegisteredMs: c.GetRegisteredMs()}
		s.workers[w.ID] = w
	}
	w.State = WorkerAlive
	w.LeaseExpiryMs = c.GetLeaseExpiryMs()
	// Capacity is re-read on every renewal, so a restarted worker running with
	// a different -concurrency takes effect without a separate command.
	if cap := c.GetCapacity(); cap > 0 {
		w.Capacity = cap
	} else if w.Capacity == 0 {
		w.Capacity = defaultCapacity
	}

	// Membership set for the worker's claim. Building it is deterministic; only the SCAN below may influence state, and that is over sorted ids
	claimed := make(map[string]bool, len(c.GetRunningTaskIds()))
	for _, id := range c.GetRunningTaskIds() {
		claimed[id] = true
	}

	// (3) Sorted scan: map order must never influence resulting state
	for _, id := range s.sortedTaskIDs() {
		t := s.tasks[id]
		// A worker may only speak for tasks it actually holds. A claim on someone else's task, or on a task already terminal, is ignored silently- deterministically, on every replica- because a stale heartbeat from a worker whose lease already lapsed is normal
		if t.WorkerID != w.ID {
			continue
		}
		if t.State != TaskAssigned && t.State != TaskRunning {
			continue
		}
		t.LeaseExpiryMs = c.GetLeaseExpiryMs()
		if t.State == TaskAssigned && claimed[t.ID] {
			t.State = TaskRunning
		}
	}
	return Result{WorkerID: w.ID, WorkerAdmitted: admitted}, nil
}

// 3. Tick
/*
	The state machine's ONLY clock. The leader proposes Tick{nowMs} every ~200ms and lease expiry reads the timestamp carried IN the log entry, so every replica expires the same leases at the same log position.

	nowMs is MONOTONIC NON-DECREASING: a Tick carrying an earlier timestamp (failover to a node with a skewed clock, an NTP step) is CLAMPED, not rejected. If time could move backward, an expired-and-reassigned task could become un-expired and you'd have two live assignments for one task.

	(1) Clamp nowMs forward
	(2) Expire worker leases:  ALIVE + lease < now  →  LOST
	(3) Elapsed retry backoffs  →  clear, and emit an eval so the scheduler looks again
	(4) Expired dedup sessions  →  delete
	(5) Expire task leases:    ASSIGNED/RUNNING + lease < now  →  PENDING
	        Attempt is NOT touched here — it is a dispatch counter, incremented only by ApplyPlan. That is what makes a slow worker's late report (carrying an older attempt) detectably stale rather than a duplicate.
	        BUT reclaim must respect the same attempt budget ApplyPlan enforces: a task already at MaxAttempts goes terminal FAILED instead of PENDING. Otherwise it would sit PENDING forever — every plan naming it asks for attempt MaxAttempts+1, which applyPlan rejects — and a scheduler that scans for PENDING work would pick it, propose, be rejected, and spin. Permanently-PENDING also breaks "every submitted task reaches a terminal state".
	Both scans are over SORTED ids
*/
func (s *StateStore) applyTick(c *commandpb.Tick) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("tick: nil payload")
	}

	// (1) Clamp. A backwards tick is a no-op, not an error.
	wasUnset := s.nowMs == 0
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

	// (3) Backoffs that have elapsed. Clearing NotBeforeMs makes this
	// one-shot, and the trigger is what wakes the scheduler: time passing is
	// otherwise invisible to it, so a task waiting out a retry would sit
	// PENDING with nothing coming to dispatch it.
	for _, id := range s.sortedTaskIDs() {
		t := s.tasks[id]
		if t.State != TaskPending || t.NotBeforeMs == 0 || t.NotBeforeMs > now {
			continue
		}
		t.NotBeforeMs = 0
		res.EvalTriggers = append(res.EvalTriggers, EvalTrigger{
			JobID:   t.JobID,
			TaskID:  t.ID,
			Reason:  EvalBackoffElapsed,
			Attempt: t.Attempt,
		})
	}

	// (4) Expire dedup sessions. Unbounded growth is a real leak in a
	// long-running coordinator, and the sweep must be deterministic: it reads
	// the Tick-carried clock, never time.Now().
	//
	// REBASE FIRST. The clock starts at zero, so anything recorded before the
	// first Tick carries createdMs = 0 — not "the epoch", but "no time yet".
	// Measured against a real clock that looks infinitely old, and the sweep
	// would destroy it on the very first tick. That is not a lost edge case:
	// it silently broke dedup for every request served in the window between
	// winning an election and the first Tick committing, and it looked
	// exactly like a snapshot bug.
	//
	// Treating those as created now is deterministic — same state, same log
	// position, same result on every replica.
	if wasUnset {
		for key, sess := range s.sessions {
			if sess.createdMs == 0 {
				sess.createdMs = now
				s.sessions[key] = sess
			}
		}
	}
	for key, sess := range s.sessions {
		if sess.createdMs+s.policy.SessionTTLMs < now {
			delete(s.sessions, key)
		}
	}

	// (5) Reclaim tasks whose lease lapsed, or whose worker is gone
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

		// NO BACKOFF HERE, deliberately. The worker died; the task never had
		// its chance, so delaying the retry only burns the reclaim budget.
		t.State = TaskPending
		t.NotBeforeMs = 0
		res.EvalTriggers = append(res.EvalTriggers, EvalTrigger{
			JobID:   t.JobID,
			TaskID:  t.ID,
			Reason:  reason,
			Attempt: t.Attempt,
		})
	}
	return res, nil
}

// 4. ApplyPlan
/*
	The dispatch command, and the ONLY place Attempt increments.

	One accepted plan commits as ONE Raft entry however many assignments it carries- that batching is what amortizes fsync across many scheduling decisions, and it is the throughput argument the whole design rests on.

	DETERMINISTIC PARTIAL APPLY. The planner verified this plan against current state before proposing it, but under Raft there is a real gap between that check and this apply: Tick, CompleteTask, or FailTask can commit in between and invalidate a single assignment. Rejecting the whole batch for one stale entry would throw away 99 good assignments because one worker's lease lapsed 2ms earlier — a throughput cliff under fault injection.

	So invalid assignments are SKIPPED and the rest apply. This is still deterministic, which is the only property that matters: every replica sees the identical state at the identical log position, so every replica skips exactly the same assignments. Determinism does not require all-or-nothing; it requires that the decision be a pure function of (state, command), and skipping is.

	Malformed commands are still errors — a nil payload or an empty eval_id is a bug in the proposer, not a race.

	(1) Apply stops:       ASSIGNED/RUNNING → PENDING, worker cleared
	(2) Apply assignments: PENDING → ASSIGNED, Attempt := assignment.Attempt
	(3) Promote the job to RUNNING on its first assignment
*/
func (s *StateStore) applyPlan(c *commandpb.ApplyPlan, version uint32) (Result, error) {
	if c == nil {
		return Result{}, fmt.Errorf("apply plan: nil payload")
	}
	if c.GetEvalId() == "" {
		return Result{}, fmt.Errorf("apply plan: empty eval_id")
	}

	// Entries written before the semantics changed must replay under the rule
	// they were written with, or a restart reaches state the cluster never
	// had. Version 0 means an entry predating the field entirely, which can
	// only be the older meaning.
	if version < CommandVersionPartialPlan {
		return s.applyPlanAllOrNothing(c)
	}

	var res Result
	res.EvalID = c.GetEvalId()

	// (1) Stops first: a plan may stop a task on one worker and re-assign it.
	for _, st := range c.GetStops() {
		t, ok := s.tasks[st.GetTaskId()]
		if !ok || (t.State != TaskAssigned && t.State != TaskRunning) {
			res.PlanSkipped++
			continue
		}
		t.State = TaskPending
		t.WorkerID = ""
		t.LeaseExpiryMs = 0
		t.LastError = st.GetReason()
		res.PlanApplied++
	}

	// (2) Assignments. Each is validated on its own and skipped if it no
	// longer holds; the rest still apply.
	seen := make(map[string]bool, len(c.GetAssignments()))
	for _, a := range c.GetAssignments() {
		if seen[a.GetTaskId()] {
			res.PlanSkipped++ // the same task twice in one plan is a proposer bug
			continue
		}
		t, ok := s.tasks[a.GetTaskId()]
		if !ok {
			res.PlanSkipped++
			continue
		}
		// Someone else got there first: already assigned, running, terminal.
		if t.State != TaskPending {
			res.PlanSkipped++
			continue
		}
		// Attempt is a strict dispatch counter. Anything else means the plan
		// was built against a snapshot that predates a reclaim.
		if a.GetAttempt() != t.Attempt+1 || a.GetAttempt() > t.Spec.MaxAttempts {
			res.PlanSkipped++
			continue
		}
		w, ok := s.workers[a.GetWorkerId()]
		if !ok || w.State != WorkerAlive {
			res.PlanSkipped++ // the worker died between verify and commit
			continue
		}
		if a.GetLeaseExpiryMs() <= s.nowMs {
			res.PlanSkipped++
			continue
		}
		// The scheduler filters these and the planner re-checks; this is the
		// backstop that makes the rule true on every replica regardless.
		if t.NotBeforeMs > s.nowMs {
			res.PlanSkipped++
			continue
		}

		seen[a.GetTaskId()] = true
		t.State = TaskAssigned
		t.NotBeforeMs = 0 // dispatched; the backoff is spent
		t.WorkerID = a.GetWorkerId()
		t.Attempt = a.GetAttempt()
		t.LeaseExpiryMs = a.GetLeaseExpiryMs()
		res.PlanApplied++

		// (3) First assignment promotes the job.
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
		// The task RAN and failed, which may be a crash loop — back off before
		// trying again. Lease expiry deliberately does NOT do this: there the
		// worker died, the task never got its chance, and the reclaim budget
		// ("within 3s of a missed heartbeat") has no room for a backoff.
		t.NotBeforeMs = s.nowMs + s.backoffFor(t.ID, t.Attempt)
		res.EvalTriggers = append(res.EvalTriggers, EvalTrigger{
			JobID:   t.JobID,
			TaskID:  t.ID,
			Reason:  EvalTaskFailed,
			Attempt: t.Attempt,
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

	Note what is NOT here: no priority queue, no ordering, no dequeue. That is the EvalBroker (internal/sched), and it is DERIVED, NOT REPLICATED — a leader-only in-memory structure rebuilt by scanning committed state on leadership acquisition. Replicating a work queue would mean every pop is a consensus round-trip, and it would need rebuilding after failover anyway.

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
	case commandpb.EvalTrigger_EVAL_TRIGGER_BACKOFF_ELAPSED:
		return EvalBackoffElapsed
	}
	return EvalJobSubmitted
}

// Reads. All return copies: callers must never hold a pointer into live state. That is the same discipline Snapshot relies on, and it is what lets the scheduler workers in internal/sched read state concurrently while the planner commits

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

// JobIDs, TaskIDs, and WorkerIDs enumerate state in sorted order, so a caller that scans them makes the same decision on every replica. Pair them with Job/Task/Worker, which return copies — never hold a pointer into live state
func (s *StateStore) JobIDs() []string    { return s.sortedJobIDs() }
func (s *StateStore) TaskIDs() []string   { return s.sortedTaskIDs() }
func (s *StateStore) WorkerIDs() []string { return s.sortedWorkerIDs() }

func (s *StateStore) JobCount() int    { return len(s.jobs) }
func (s *StateStore) TaskCount() int   { return len(s.tasks) }
func (s *StateStore) WorkerCount() int { return len(s.workers) }
func (s *StateStore) EvalCount() int   { return len(s.evals) }

// SessionCount reports how many dedup entries are held. It exists so a test
// can prove the TTL sweep actually bounds the map.
func (s *StateStore) SessionCount() int { return len(s.sessions) }

// backoffFor computes how long a task must wait before its next dispatch.
//
// THE JITTER IS DETERMINISTIC. apply runs on every replica, so a real random
// roll here would place the same task at different times on different nodes —
// the exact divergence the Tick design exists to prevent. Instead the spread
// is derived by hashing (taskID, attempt): identical everywhere, but
// decorrelated across tasks, so N tasks that fail together do not retry in
// lockstep. That is the property jitter is actually for.
//
// Exponential in the attempt number, capped, plus up to 50% spread.
func (s *StateStore) backoffFor(taskID string, attempt int32) int64 {
	if attempt < 1 {
		attempt = 1
	}
	base := s.policy.BaseBackoffMs
	for i := int32(1); i < attempt && base < s.policy.MaxBackoffMs; i++ {
		base *= 2
	}
	if base > s.policy.MaxBackoffMs {
		base = s.policy.MaxBackoffMs
	}

	h := fnv.New64a()
	_, _ = h.Write([]byte(taskID))
	_, _ = h.Write([]byte{byte(attempt), byte(attempt >> 8), byte(attempt >> 16), byte(attempt >> 24)})
	spread := int64(h.Sum64() % uint64(base/2+1))
	return base + spread
}

// applyPlanAllOrNothing is ApplyPlan's ORIGINAL semantics, kept for replay.
//
// It is dead code for anything this build proposes — every new entry is
// stamped CommandVersionPartialPlan — and it must stay anyway. A log written
// before the change still holds entries that meant this, and replaying them
// under the new rule would silently produce state the cluster never had.
//
// That is the whole problem schema versioning exists for, and it only ever
// appears once a system has a past.
func (s *StateStore) applyPlanAllOrNothing(c *commandpb.ApplyPlan) (Result, error) {
	// Validate EVERYTHING before mutating anything: under this rule a single
	// invalid entry rejects the batch, so a partial mutation would be a bug.
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

	res := Result{EvalID: c.GetEvalId()}
	for _, st := range c.GetStops() {
		t := s.tasks[st.GetTaskId()]
		t.State = TaskPending
		t.WorkerID = ""
		t.LeaseExpiryMs = 0
		t.LastError = st.GetReason()
		res.PlanApplied++
	}
	for _, a := range c.GetAssignments() {
		t := s.tasks[a.GetTaskId()]
		t.State = TaskAssigned
		t.WorkerID = a.GetWorkerId()
		t.Attempt = a.GetAttempt()
		t.LeaseExpiryMs = a.GetLeaseExpiryMs()
		res.PlanApplied++
		if j, ok := s.jobs[t.JobID]; ok && j.State == JobPending {
			j.State = JobRunning
		}
	}
	return res, nil
}

// CorruptForTesting puts a task into a state no command path can produce: it
// leaves a worker attached while marking the task PENDING.
//
// It exists so the simulator's invariant checker can be shown to FAIL. A
// checker that has never caught anything is indistinguishable from one that
// cannot, and "zero violations across N seeds" is only evidence if the
// detector demonstrably works.
func (s *StateStore) CorruptForTesting(taskID string) {
	if t, ok := s.tasks[taskID]; ok {
		t.State = TaskPending // worker and lease deliberately left behind
	}
}
