package server

import (
	"encoding/hex"
	"fmt"
	"io"

	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
)

// IDs are minted by the LEADER, before proposing, and baked into the command.
// They can never be generated inside Apply: that runs on every replica and
// must be deterministic, so a random ID there would diverge state instantly.
//
// Randomness is safe HERE precisely because the id becomes a decided fact the
// moment it enters the command. The hazard a random id would otherwise create
// — re-proposing after an ambiguous outcome and minting a second job — is
// covered for SubmitJob by (clientID, requestID) dedup in the state machine.
//
// Day 4 needs a different answer for eval ids, which have no such dedup key
// and must therefore be DERIVED from what triggered them.
//
// The SOURCE of that randomness is injectable (Config.Rand) for one reason:
// under a seeded simulator run, ids drawn from crypto/rand differ every time,
// so two runs of the same seed cannot be diffed to find where they diverged.
// The seed reproduces the fault schedule; readable ids are what make that
// reproduction usable.

func (c *Coordinator) newJobID() string {
	var b [8]byte
	if _, err := io.ReadFull(c.rand, b[:]); err != nil {
		panic("server: id randomness unavailable: " + err.Error())
	}
	return "job-" + hex.EncodeToString(b[:])
}

// taskID is derived from the job id and the task's index within the job, so
// task ids sort in submission order. The padding is load-bearing: assign()
// scans SORTED ids, and unpadded indices would put task 10 ahead of task 2.
func taskID(jobID string, index int) string {
	return fmt.Sprintf("%s/%04d", jobID, index)
}

// evalIDFor derives an eval id from the trigger that caused it.
//
// It must be DETERMINISTIC and unique per occurrence. Deterministic because
// applyCreateEval is an idempotent no-op on a repeated id, so a leader that
// re-proposes after an ambiguous outcome must land on the same id rather than
// creating a second eval. Unique per occurrence because the reverse failure —
// two genuinely different events colliding on one id — would silently drop
// the second one.
//
// (taskID, attempt, reason) satisfies both: a given dispatch attempt can have
// its lease expire once and fail once, and those are different reasons.
func evalIDFor(t state.EvalTrigger) string {
	if t.TaskID == "" {
		// Job-level: a job is submitted exactly once.
		return fmt.Sprintf("eval/%s/%s", t.JobID, reasonSlug(t.Reason))
	}
	return fmt.Sprintf("eval/%s/%d/%s", t.TaskID, t.Attempt, reasonSlug(t.Reason))
}

func reasonSlug(r state.EvalReason) string {
	switch r {
	case state.EvalJobSubmitted:
		return "submitted"
	case state.EvalTaskFailed:
		return "failed"
	case state.EvalWorkerLost:
		return "worker-lost"
	case state.EvalLeaseExpired:
		return "lease-expired"
	case state.EvalBackoffElapsed:
		return "backoff-elapsed"
	}
	return "unknown"
}
