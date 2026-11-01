package state

import (
	"fmt"
	"sort"

	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
	snapshotpb "github.com/evazheng/fault-tolerant-coordinator/proto/snapshotpb"
	"google.golang.org/protobuf/proto"
)

// snapshotVersion is the encoding this build writes. Like Command.version it
// exists so a future build can change the MEANING of a field and still read
// what is already on disk.
const snapshotVersion uint32 = 1

// Encode serializes the whole state machine.
//
// DETERMINISTIC: every collection is emitted in sorted id order, so two
// replicas snapshotting identical state produce identical bytes. Raft does not
// require that, but it makes divergence testable — and a property you can
// assert is worth more than one you can only hope for.
//
// Everything mutable must be here. The easy omission is `sessions`: without
// it a restored node looks perfectly healthy and silently duplicates a job the
// first time a client retries across the restore.
func (s *StateStore) Encode() ([]byte, error) {
	snap := &snapshotpb.Snapshot{
		Version: snapshotVersion,
		NowMs:   s.nowMs,
	}

	for _, id := range s.sortedJobIDs() {
		j := s.jobs[id]
		snap.Jobs = append(snap.Jobs, &snapshotpb.Job{
			JobId:     j.ID,
			ClientId:  j.ClientID,
			RequestId: j.RequestID,
			State:     jobStateToSnap(j.State),
			TaskIds:   append([]string(nil), j.TaskIDs...),
			Priority:  j.Priority,
			CreatedMs: j.CreatedMs,
		})
	}
	for _, id := range s.sortedTaskIDs() {
		t := s.tasks[id]
		snap.Tasks = append(snap.Tasks, &snapshotpb.Task{
			TaskId:        t.ID,
			JobId:         t.JobID,
			Spec:          specToSnap(t.Spec),
			State:         taskStateToSnap(t.State),
			Attempt:       t.Attempt,
			WorkerId:      t.WorkerID,
			LeaseExpiryMs: t.LeaseExpiryMs,
			ResultRef:     t.ResultRef,
			ExitCode:      t.ExitCode,
			LastError:     t.LastError,
			NotBeforeMs:   t.NotBeforeMs,
		})
	}
	for _, id := range s.sortedWorkerIDs() {
		w := s.workers[id]
		snap.Workers = append(snap.Workers, &snapshotpb.Worker{
			WorkerId:      w.ID,
			State:         workerStateToSnap(w.State),
			RegisteredMs:  w.RegisteredMs,
			LeaseExpiryMs: w.LeaseExpiryMs,
			Capacity:      w.Capacity,
		})
	}

	evalIDs := make([]string, 0, len(s.evals))
	for id := range s.evals {
		evalIDs = append(evalIDs, id)
	}
	sort.Strings(evalIDs)
	for _, id := range evalIDs {
		e := s.evals[id]
		snap.Evals = append(snap.Evals, &snapshotpb.Eval{
			EvalId:      e.ID,
			JobId:       e.JobID,
			Priority:    e.Priority,
			TriggeredBy: evalReasonToSnap(e.TriggeredBy),
			CreatedMs:   e.CreatedMs,
		})
	}

	// sessionKey is a struct, so sort on the pair to keep the output stable.
	keys := make([]sessionKey, 0, len(s.sessions))
	for k := range s.sessions {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].clientID != keys[j].clientID {
			return keys[i].clientID < keys[j].clientID
		}
		return keys[i].requestID < keys[j].requestID
	})
	for _, k := range keys {
		sess := s.sessions[k]
		snap.Sessions = append(snap.Sessions, &snapshotpb.Session{
			ClientId:  k.clientID,
			RequestId: k.requestID,
			JobId:     sess.result.JobID,
			CreatedMs: sess.createdMs,
			// Empty for sessions created before the field existed, which is
			// exactly what the restore side reads as "do not judge".
			PayloadHash: sess.payloadHash,
		})
	}

	// Deterministic marshaling, not protobuf's default. The default is
	// explicitly allowed to vary between runs, which would make two replicas
	// with identical state produce different bytes.
	return proto.MarshalOptions{Deterministic: true}.Marshal(snap)
}

// Restore replaces the entire state machine from a snapshot.
//
// It REPLACES rather than merges: a snapshot is a complete state, and merging
// would leave behind anything the snapshot had already compacted away.
func (s *StateStore) Restore(data []byte) error {
	snap := &snapshotpb.Snapshot{}
	if err := proto.Unmarshal(data, snap); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	if v := snap.GetVersion(); v > snapshotVersion {
		// Refusing is correct: a newer encoding may mean something this build
		// would misread, and misreading state is worse than refusing to start.
		return fmt.Errorf("restore: snapshot version %d is newer than this build understands (%d)", v, snapshotVersion)
	}

	jobs := make(map[string]*Job, len(snap.GetJobs()))
	for _, j := range snap.GetJobs() {
		jobs[j.GetJobId()] = &Job{
			ID:        j.GetJobId(),
			ClientID:  j.GetClientId(),
			RequestID: j.GetRequestId(),
			State:     jobStateFromSnap(j.GetState()),
			TaskIDs:   append([]string(nil), j.GetTaskIds()...),
			Priority:  j.GetPriority(),
			CreatedMs: j.GetCreatedMs(),
		}
	}
	tasks := make(map[string]*Task, len(snap.GetTasks()))
	for _, t := range snap.GetTasks() {
		tasks[t.GetTaskId()] = &Task{
			ID:            t.GetTaskId(),
			JobID:         t.GetJobId(),
			Spec:          specFromSnap(t.GetSpec()),
			State:         taskStateFromSnap(t.GetState()),
			Attempt:       t.GetAttempt(),
			WorkerID:      t.GetWorkerId(),
			LeaseExpiryMs: t.GetLeaseExpiryMs(),
			ResultRef:     t.GetResultRef(),
			ExitCode:      t.GetExitCode(),
			LastError:     t.GetLastError(),
			NotBeforeMs:   t.GetNotBeforeMs(),
		}
	}
	workers := make(map[string]*Worker, len(snap.GetWorkers()))
	for _, w := range snap.GetWorkers() {
		workers[w.GetWorkerId()] = &Worker{
			ID:            w.GetWorkerId(),
			State:         workerStateFromSnap(w.GetState()),
			RegisteredMs:  w.GetRegisteredMs(),
			LeaseExpiryMs: w.GetLeaseExpiryMs(),
			Capacity:      w.GetCapacity(),
		}
	}
	evals := make(map[string]*Eval, len(snap.GetEvals()))
	for _, e := range snap.GetEvals() {
		evals[e.GetEvalId()] = &Eval{
			ID:          e.GetEvalId(),
			JobID:       e.GetJobId(),
			Priority:    e.GetPriority(),
			TriggeredBy: evalReasonFromSnap(e.GetTriggeredBy()),
			CreatedMs:   e.GetCreatedMs(),
		}
	}
	sessions := make(map[sessionKey]session, len(snap.GetSessions()))
	for _, sess := range snap.GetSessions() {
		sessions[sessionKey{clientID: sess.GetClientId(), requestID: sess.GetRequestId()}] = session{
			result:      Result{JobID: sess.GetJobId()},
			createdMs:   sess.GetCreatedMs(),
			payloadHash: sess.GetPayloadHash(),
		}
	}

	s.jobs, s.tasks, s.workers, s.evals, s.sessions = jobs, tasks, workers, evals, sessions
	s.nowMs = snap.GetNowMs()
	// DERIVED STATE IS REBUILT, not restored. tasksByWorker is not in the
	// snapshot — it carries no version and the wire format is unchanged — so
	// it has to be recomputed from the tasks just installed. Doing it here
	// rather than in the caller means no future restore path can forget.
	s.rebuildTasksByWorker()
	return nil
}

// ---------------------------------------------------------------------------
// Enum mapping. Explicit and one-way on purpose: the proto values are the
// durable encoding, and renumbering the Go iota constants must never silently
// change what is already on disk.
// ---------------------------------------------------------------------------

func taskStateToSnap(s TaskState) snapshotpb.TaskState {
	switch s {
	case TaskPending:
		return snapshotpb.TaskState_TASK_STATE_PENDING
	case TaskAssigned:
		return snapshotpb.TaskState_TASK_STATE_ASSIGNED
	case TaskRunning:
		return snapshotpb.TaskState_TASK_STATE_RUNNING
	case TaskComplete:
		return snapshotpb.TaskState_TASK_STATE_COMPLETE
	case TaskFailed:
		return snapshotpb.TaskState_TASK_STATE_FAILED
	}
	return snapshotpb.TaskState_TASK_STATE_UNSPECIFIED
}

func taskStateFromSnap(s snapshotpb.TaskState) TaskState {
	switch s {
	case snapshotpb.TaskState_TASK_STATE_PENDING:
		return TaskPending
	case snapshotpb.TaskState_TASK_STATE_ASSIGNED:
		return TaskAssigned
	case snapshotpb.TaskState_TASK_STATE_RUNNING:
		return TaskRunning
	case snapshotpb.TaskState_TASK_STATE_COMPLETE:
		return TaskComplete
	case snapshotpb.TaskState_TASK_STATE_FAILED:
		return TaskFailed
	}
	return TaskPending
}

func jobStateToSnap(s JobState) snapshotpb.JobState {
	switch s {
	case JobPending:
		return snapshotpb.JobState_JOB_STATE_PENDING
	case JobRunning:
		return snapshotpb.JobState_JOB_STATE_RUNNING
	case JobComplete:
		return snapshotpb.JobState_JOB_STATE_COMPLETE
	case JobFailed:
		return snapshotpb.JobState_JOB_STATE_FAILED
	}
	return snapshotpb.JobState_JOB_STATE_UNSPECIFIED
}

func jobStateFromSnap(s snapshotpb.JobState) JobState {
	switch s {
	case snapshotpb.JobState_JOB_STATE_PENDING:
		return JobPending
	case snapshotpb.JobState_JOB_STATE_RUNNING:
		return JobRunning
	case snapshotpb.JobState_JOB_STATE_COMPLETE:
		return JobComplete
	case snapshotpb.JobState_JOB_STATE_FAILED:
		return JobFailed
	}
	return JobPending
}

func workerStateToSnap(s WorkerState) snapshotpb.WorkerState {
	switch s {
	case WorkerAlive:
		return snapshotpb.WorkerState_WORKER_STATE_ALIVE
	case WorkerLost:
		return snapshotpb.WorkerState_WORKER_STATE_LOST
	}
	return snapshotpb.WorkerState_WORKER_STATE_UNSPECIFIED
}

func workerStateFromSnap(s snapshotpb.WorkerState) WorkerState {
	switch s {
	case snapshotpb.WorkerState_WORKER_STATE_ALIVE:
		return WorkerAlive
	case snapshotpb.WorkerState_WORKER_STATE_LOST:
		return WorkerLost
	}
	return WorkerLost
}

func evalReasonToSnap(r EvalReason) snapshotpb.EvalReason {
	switch r {
	case EvalJobSubmitted:
		return snapshotpb.EvalReason_EVAL_REASON_JOB_SUBMITTED
	case EvalTaskFailed:
		return snapshotpb.EvalReason_EVAL_REASON_TASK_FAILED
	case EvalWorkerLost:
		return snapshotpb.EvalReason_EVAL_REASON_WORKER_LOST
	case EvalLeaseExpired:
		return snapshotpb.EvalReason_EVAL_REASON_LEASE_EXPIRED
	case EvalBackoffElapsed:
		return snapshotpb.EvalReason_EVAL_REASON_BACKOFF_ELAPSED
	}
	return snapshotpb.EvalReason_EVAL_REASON_UNSPECIFIED
}

func evalReasonFromSnap(r snapshotpb.EvalReason) EvalReason {
	switch r {
	case snapshotpb.EvalReason_EVAL_REASON_JOB_SUBMITTED:
		return EvalJobSubmitted
	case snapshotpb.EvalReason_EVAL_REASON_TASK_FAILED:
		return EvalTaskFailed
	case snapshotpb.EvalReason_EVAL_REASON_WORKER_LOST:
		return EvalWorkerLost
	case snapshotpb.EvalReason_EVAL_REASON_LEASE_EXPIRED:
		return EvalLeaseExpired
	case snapshotpb.EvalReason_EVAL_REASON_BACKOFF_ELAPSED:
		return EvalBackoffElapsed
	}
	return EvalJobSubmitted
}

func specToSnap(s TaskSpec) *schedulerpb.TaskSpec {
	return &schedulerpb.TaskSpec{
		Command:     s.Command,
		Args:        append([]string(nil), s.Args...),
		TimeoutMs:   s.TimeoutMs,
		MaxAttempts: s.MaxAttempts,
	}
}

func specFromSnap(p *schedulerpb.TaskSpec) TaskSpec {
	return specFromProto(p)
}
