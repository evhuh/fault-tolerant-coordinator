package state

import (
	"errors"
	"testing"

	commandpb "github.com/evazheng/fault-tolerant-coordinator/proto/commandpb"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// submitWithKey builds a SubmitJob carrying a specific idempotency key, job
// id and task count, at a given command version.
//
// The job id varies with the call because the LEADER assigns it before
// proposing: a retry of the same logical request arrives with a fresh job id,
// which is exactly why the payload hash must exclude it.
func submitWithKey(version uint32, clientID, requestID, jobID string, tasks int, cmd string) *commandpb.Command {
	te := make([]*commandpb.TaskEntry, tasks)
	for i := range te {
		te[i] = &commandpb.TaskEntry{
			TaskId: jobID + "-t" + string(rune('a'+i)),
			Spec: &schedulerpb.TaskSpec{
				Command: cmd, TimeoutMs: 1000, MaxAttempts: 3,
			},
		}
	}
	return &commandpb.Command{
		Version: version,
		Kind: &commandpb.Command_SubmitJob{SubmitJob: &commandpb.SubmitJob{
			ClientId: clientID, RequestId: requestID, JobId: jobID,
			CreatedMs: 1000, Tasks: te,
		}},
	}
}

// (1) A RETRY — same key, same payload — still replays the original response.
//
// This is the behaviour the guard must not break. It is what makes SubmitJob
// safe to retry through a failover, and the effect ledger depends on it.
func TestReusedKeyWithSamePayloadReplaysTheOriginalResponse(t *testing.T) {
	s := New()
	first, err := s.Apply(submitWithKey(CommandVersionCurrent, "c-1", "r-1", "job-A", 2, "/bin/noop"))
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if first.JobID != "job-A" {
		t.Fatalf("first submit returned job %q", first.JobID)
	}

	// The retry carries a DIFFERENT job id, as a real retry would: the leader
	// assigns one per proposal.
	again, err := s.Apply(submitWithKey(CommandVersionCurrent, "c-1", "r-1", "job-B", 2, "/bin/noop"))
	if err != nil {
		t.Fatalf("retry of an identical payload was rejected: %v", err)
	}
	if again.JobID != "job-A" {
		t.Errorf("retry returned job %q, want the original %q", again.JobID, "job-A")
	}
	if n := len(s.jobs); n != 1 {
		t.Errorf("retry created a second job: %d jobs exist", n)
	}
}

// (2) A REUSED KEY carrying different work is REJECTED, with an error that
// says what happened.
func TestReusedKeyWithDifferentPayloadIsRejected(t *testing.T) {
	s := New()
	if _, err := s.Apply(submitWithKey(CommandVersionCurrent, "c-1", "r-1", "job-A", 1, "/bin/noop")); err != nil {
		t.Fatalf("first submit: %v", err)
	}

	// Same key, more tasks. This is the shape that cost 138 benchmark trials:
	// a harness reused its key and the cluster replayed an answer for work it
	// never created.
	_, err := s.Apply(submitWithKey(CommandVersionCurrent, "c-1", "r-1", "job-B", 6, "/bin/noop"))
	if err == nil {
		t.Fatal("a reused key with a different payload was accepted; the original job was " +
			"replayed and the new work silently not created")
	}
	if !errors.Is(err, ErrRequestIDReused) {
		t.Errorf("error = %v, want it to wrap ErrRequestIDReused so callers can classify it", err)
	}

	// A different COMMAND is also different work, not just a different count.
	if _, err := s.Apply(submitWithKey(CommandVersionCurrent, "c-2", "r-2", "job-C", 1, "/bin/noop")); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := s.Apply(submitWithKey(CommandVersionCurrent, "c-2", "r-2", "job-D", 1, "/bin/other")); !errors.Is(err, ErrRequestIDReused) {
		t.Errorf("a reused key with a different command gave %v, want ErrRequestIDReused", err)
	}
}

// (3) A PRE-UPGRADE LOG REPLAYS WITH THE OLD BEHAVIOUR.
//
// The gate is on the ENTRY's version, never the build's. A log containing a
// version-3 SubmitJob that reused a key must still replay as "returned the
// original job", or restoring from that log diverges from the cluster that
// wrote it — and divergence is the one failure this system cannot recover
// from.
func TestPreV4LogReplaysWithThePermissiveBehaviour(t *testing.T) {
	s := New()
	old := CommandVersionClaimedRenewal // 3: before the guard existed
	if _, err := s.Apply(submitWithKey(old, "c-1", "r-1", "job-A", 1, "/bin/noop")); err != nil {
		t.Fatalf("first submit: %v", err)
	}

	// Same key, DIFFERENT payload, at the old version. The cluster that wrote
	// this log returned the original job, so replay must do the same.
	res, err := s.Apply(submitWithKey(old, "c-1", "r-1", "job-B", 9, "/bin/noop"))
	if err != nil {
		t.Fatalf("a version-%d entry was rejected by a rule that did not exist when it was "+
			"written: %v", old, err)
	}
	if res.JobID != "job-A" {
		t.Errorf("replay returned job %q, want the original %q", res.JobID, "job-A")
	}

	// And a v4 entry arriving later still gets the guard, since the gate is
	// per entry. The v3 session carries no hash, so this one must REPLAY
	// rather than reject — "no hash" means do not judge.
	res, err = s.Apply(submitWithKey(CommandVersionPayloadHashedSession, "c-1", "r-1", "job-C", 9, "/bin/noop"))
	if err != nil {
		t.Errorf("a v4 retry against a hashless v3 session was rejected (%v); that breaks a "+
			"legitimate retry across the upgrade", err)
	}
	if res.JobID != "job-A" {
		t.Errorf("returned job %q, want %q", res.JobID, "job-A")
	}
}

// (4) THE VERSION REFUSAL STILL HOLDS. An entry above what this build
// understands halts rather than being guessed at, and bumping the current
// version must not weaken that.
func TestVersionRefusalStillHoldsAboveTheNewCurrent(t *testing.T) {
	s := New()
	_, err := s.Apply(submitWithKey(CommandVersionCurrent+1, "c-1", "r-1", "job-A", 1, "/bin/noop"))
	if !errors.Is(err, ErrCommandVersionTooNew) {
		t.Errorf("an entry at version %d gave %v, want ErrCommandVersionTooNew",
			CommandVersionCurrent+1, err)
	}
	if n := len(s.jobs); n != 0 {
		t.Errorf("a refused entry still created %d job(s); the refusal must happen before apply", n)
	}
}

// The hash must survive a snapshot, or the guard evaporates on every restore.
func TestPayloadHashSurvivesSnapshotRestore(t *testing.T) {
	s := New()
	if _, err := s.Apply(submitWithKey(CommandVersionCurrent, "c-1", "r-1", "job-A", 2, "/bin/noop")); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	blob, err := s.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	restored := New()
	if err := restored.Restore(blob); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Still rejects different work...
	if _, err := restored.Apply(submitWithKey(CommandVersionCurrent, "c-1", "r-1", "job-B", 5, "/bin/noop")); !errors.Is(err, ErrRequestIDReused) {
		t.Errorf("after restore, a reused key with a different payload gave %v, want "+
			"ErrRequestIDReused — the hash did not survive the snapshot", err)
	}
	// ...and still replays a genuine retry.
	res, err := restored.Apply(submitWithKey(CommandVersionCurrent, "c-1", "r-1", "job-C", 2, "/bin/noop"))
	if err != nil {
		t.Errorf("after restore, an identical retry was rejected: %v", err)
	}
	if res.JobID != "job-A" {
		t.Errorf("after restore, retry returned %q, want %q", res.JobID, "job-A")
	}
}
