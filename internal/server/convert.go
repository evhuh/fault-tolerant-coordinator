package server

import (
	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// The state package deliberately keeps its own enums rather than reusing the
// generated API ones, because the two schemas evolve under different
// pressure: scheduler.proto breaks clients, command.proto breaks replay of
// data already on disk. This file is the boundary where they meet, and it is
// the only place that knows both.

func taskStateToProto(s state.TaskState) schedulerpb.TaskState {
	switch s {
	case state.TaskPending:
		return schedulerpb.TaskState_TASK_STATE_PENDING
	case state.TaskAssigned:
		return schedulerpb.TaskState_TASK_STATE_ASSIGNED
	case state.TaskRunning:
		return schedulerpb.TaskState_TASK_STATE_RUNNING
	case state.TaskComplete:
		return schedulerpb.TaskState_TASK_STATE_COMPLETE
	case state.TaskFailed:
		return schedulerpb.TaskState_TASK_STATE_FAILED
	}
	return schedulerpb.TaskState_TASK_STATE_UNSPECIFIED
}

func jobStateToProto(s state.JobState) schedulerpb.JobState {
	switch s {
	case state.JobPending:
		return schedulerpb.JobState_JOB_STATE_PENDING
	case state.JobRunning:
		return schedulerpb.JobState_JOB_STATE_RUNNING
	case state.JobComplete:
		return schedulerpb.JobState_JOB_STATE_COMPLETE
	case state.JobFailed:
		return schedulerpb.JobState_JOB_STATE_FAILED
	}
	return schedulerpb.JobState_JOB_STATE_UNSPECIFIED
}

func specToProto(s state.TaskSpec) *schedulerpb.TaskSpec {
	return &schedulerpb.TaskSpec{
		Command:     s.Command,
		Args:        append([]string(nil), s.Args...),
		TimeoutMs:   s.TimeoutMs,
		MaxAttempts: s.MaxAttempts,
	}
}

func taskToProto(t state.Task) *schedulerpb.Task {
	return &schedulerpb.Task{
		TaskId:        t.ID,
		JobId:         t.JobID,
		Spec:          specToProto(t.Spec),
		State:         taskStateToProto(t.State),
		Attempt:       t.Attempt,
		WorkerId:      t.WorkerID,
		LeaseExpiryMs: t.LeaseExpiryMs,
		ResultRef:     t.ResultRef,
		ExitCode:      t.ExitCode,
		LastError:     t.LastError,
	}
}

// jobToProto needs the store to resolve the job's tasks, so it is called
// under the read lock.
func jobToProto(s *state.StateStore, j state.Job) *schedulerpb.Job {
	out := &schedulerpb.Job{
		JobId:     j.ID,
		State:     jobStateToProto(j.State),
		CreatedMs: j.CreatedMs,
		Tasks:     make([]*schedulerpb.Task, 0, len(j.TaskIDs)),
	}
	// j.TaskIDs is an ordered slice, not a set — submission order is
	// semantic, so the response preserves it.
	for _, id := range j.TaskIDs {
		if t, ok := s.Task(id); ok {
			out.Tasks = append(out.Tasks, taskToProto(t))
		}
	}
	return out
}
