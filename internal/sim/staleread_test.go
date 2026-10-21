package sim

// A deterministic reproduction of the bug Day 9 exists to find, built by hand
// rather than waited for.
//
// The seeded schedule CAN produce this, but relying on it is a mistake: a
// 25-step run reached a cut-off leader 68 times and still came back Ok,
// because the violation needs four things to line up in order — the leader
// cut off, the majority electing a successor, that successor committing a job
// the client is told about, and a read of that job landing on the old leader.
// A random schedule hits that conjunction rarely, and a test that depends on
// a rare conjunction is a flaky test that also proves nothing when it passes.
//
// So this constructs the sequence directly, and it is the artifact that
// matters: it either demonstrates the stale read or it demonstrates that
// reads are safe, every time, in under a second.

import (
	"context"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc/codes"

	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// awaitLeaderExcept waits for a node OTHER than excluded to claim leadership.
func (s *Sim) awaitLeaderExcept(excluded uint64, within time.Duration) *node {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		for _, id := range s.liveIDs() {
			if id == excluded {
				continue
			}
			if n, ok := s.nodeAt(id); ok && n.coord.IsLeader() {
				return n
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return nil
}

// TestPartitionedLeaderServesStaleReads is the Day 9 hunt, made explicit.
//
// It does not assert that the read is stale. It records what happens and
// checks the history, so the test states the TRUTH about the system rather
// than the hope: if reads are linearizable it passes quietly, and if they are
// not it fails with the history that proves it.
//
// IT CURRENTLY FAILS, and that failure is the finding. It stays until the
// read path is decided — through the log, behind a ReadIndex barrier, or
// documented as explicitly non-linearizable.
func TestPartitionedLeaderServesStaleReads(t *testing.T) {
	if res := runStaleReadScenario(t, 20261020); res != porcupine.Ok {
		t.Fatalf("NOT LINEARIZABLE: a leader on the minority side of a partition answered a read "+
			"from its own stale state. Verdict %s.", res)
	}
}

func runStaleReadScenario(t *testing.T, seed int64) porcupine.CheckResult {
	cfg := DefaultConfig()
	cfg.Seed = seed
	cfg.Steps = 0
	// No fault schedule and no recorded clients: this test IS the schedule.
	cfg.LinClients = 0
	cfg.Faults.DropRate = 0
	cfg.Faults.CorruptRate = 0

	s := New(cfg, dirsFor(t, cfg.Nodes))
	defer s.shutdown()
	for i := 1; i <= cfg.Nodes; i++ {
		if err := s.start(uint64(i)); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	old := s.awaitLeader(10 * time.Second)
	if old == nil {
		t.Fatal("no leader came up")
	}

	h := newHistory(0)
	ctx := context.Background()

	// A job submitted and acknowledged BEFORE the partition. Both sides have
	// it, so a read of it is never stale and it is here as a control: if this
	// one goes missing the problem is not a partitioned read.
	before := mustSubmit(t, ctx, h, old, "pre", 0)

	// Cut the leader off. It keeps its term and its belief, because raft here
	// runs without CheckQuorum and without PreVote.
	s.net.Partition(old.id)

	fresh := s.awaitLeaderExcept(old.id, 10*time.Second)
	if fresh == nil {
		t.Fatal("the majority never elected a successor; without that there is nothing to be stale about")
	}
	if !old.coord.IsLeader() {
		// Only reachable once CheckQuorum lands. Not a pass: the scenario
		// did not happen, so the verdict would be about nothing.
		t.Skip("the cut-off leader stepped down on its own; this scenario needs it to keep believing it leads")
	}
	t.Logf("split brain: node %d (cut off) and node %d both claim leadership", old.id, fresh.id)

	// Commit a job on the MAJORITY side. The cut-off leader cannot have it:
	// it is not receiving the entries.
	during := mustSubmit(t, ctx, h, fresh, "during", 1)

	// Now ask the cut-off leader about both.
	recordGet(ctx, h, old, 2, before)
	recordGet(ctx, h, old, 2, during)

	// And ask the MAJORITY leader about the job it just committed.
	//
	// This read is why the test is not green for the wrong reason. Under
	// ReadIndexBarrier the cut-off leader refuses both reads above, and a
	// history of nothing but refusals is trivially linearizable — a barrier
	// that refused EVERY read, everywhere, would produce the same Ok while
	// making the system useless. This one must come back FOUND, which it can
	// only do by obtaining a real quorum confirmation.
	if !recordGet(ctx, h, fresh, 3, during) {
		t.Fatal("the majority leader could not serve a read of the job it had just committed; " +
			"a verdict over a history of nothing but refusals would mean nothing")
	}

	ops := h.snapshot()
	for _, op := range ops {
		t.Logf("  %s", describeLinOp(op.Input, op.Output))
	}

	return porcupine.CheckOperationsTimeout(jobsModel(), ops, 30*time.Second)
}

func mustSubmit(t *testing.T, ctx context.Context, h *history, n *node, key string, client int) string {
	t.Helper()
	in := linInput{Kind: opSubmit, Key: key}
	op := h.begin(client, in, false, false)
	resp, err := n.svc.SubmitJob(ctx, &schedulerpb.SubmitJobRequest{
		ClientId:  key,
		RequestId: "1",
		Tasks:     []*schedulerpb.TaskSpec{{Command: "/bin/true", MaxAttempts: 1}},
	})
	if err != nil {
		t.Fatalf("submit %q to node %d: %v", key, n.id, err)
	}
	op.done(linOutput{JobID: resp.GetJobId()})
	return resp.GetJobId()
}

// recordGet files a read into the history and reports whether it found the
// job. A refused read reports false, same as a definitive absence — the
// caller distinguishes them only where it matters.
func recordGet(ctx context.Context, h *history, n *node, client int, jobID string) bool {
	op := h.begin(client, linInput{Kind: opGet, JobID: jobID}, true, true)
	_, err := n.svc.GetJob(ctx, &schedulerpb.GetJobRequest{JobId: jobID})
	switch {
	case err == nil:
		op.done(linOutput{Found: true})
		return true
	case codeOf(err) == codes.NotFound:
		op.done(linOutput{Found: false})
	default:
		op.done(linOutput{Unknown: true})
	}
	return false
}
