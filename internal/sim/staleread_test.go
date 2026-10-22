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

	"fmt"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc/codes"

	"github.com/evazheng/fault-tolerant-coordinator/internal/server"

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

// TestPartitionedLeaderServesStaleReads_Local is the NEGATIVE CONTROL, and it
// is permanent.
//
// It runs ReadLocal and asserts the history is ILLEGAL. That is not a test of
// a bug left in; it is a test of the CHECKER. A linearizability harness that
// only ever runs against the fixed configuration cannot tell "the system is
// correct" from "the checker stopped looking", and those two produce
// identical green output. This fails if the model, the recorder, the
// classification of NOT_FOUND as definitive, or the scenario itself ever
// stops being able to see a stale read.
//
// If this starts passing, do not delete it — find out what broke.
func TestPartitionedLeaderServesStaleReads_Local(t *testing.T) {
	res := runStaleReadScenario(t, server.ReadLocal, 20261020)
	if res != porcupine.Illegal {
		t.Fatalf("expected the checker to find a stale read under ReadLocal, got %s. "+
			"Either the coordinator stopped serving local reads, or the harness stopped "+
			"being able to detect one — the second is the dangerous case.", res)
	}
}

// TestPartitionedLeaderReadIndexIsLinearizable is the same scenario with the
// barrier on, across several seeds.
//
// Under ReadIndexBarrier the cut-off leader cannot get a quorum to confirm it
// still leads, so the read is REFUSED rather than answered. A refused read
// returns no value, makes no claim, and is recorded ambiguous — legal,
// because the client learned nothing. The violation is gone because the wrong
// answer is gone, not because the question stopped being asked, which is what
// the majority-side read inside the scenario pins down.
func TestPartitionedLeaderReadIndexIsLinearizable(t *testing.T) {
	for _, seed := range []int64{20261020, 20261021, 20261022, 20261023} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			if res := runStaleReadScenario(t, server.ReadIndexBarrier, seed); res != porcupine.Ok {
				t.Fatalf("ReadIndexBarrier should make this history linearizable, got %s", res)
			}
		})
	}
}

// runStaleReadScenario drives the sequence by hand and returns the verdict,
// so the two tests above differ only in the read mode.
//
// It does not assert staleness itself. It records what happened and checks
// the history, so each test states the TRUTH about its configuration rather
// than the hope.
func runStaleReadScenario(t *testing.T, mode server.ReadMode, seed int64) porcupine.CheckResult {
	cfg := DefaultConfig()
	cfg.Seed = seed
	cfg.ReadMode = mode
	cfg.Steps = 0
	// PIN THE ADVERSARY: no CheckQuorum, so the cut-off leader keeps its
	// term and keeps believing it leads for as long as the scenario needs.
	//
	// This is the scenario's PREMISE, not a convenience. With CheckQuorum on
	// (the production default since the liveness change) the old leader
	// steps down about one election timeout after being cut off, while the
	// majority needs its own randomized timeout to elect a successor — so
	// the window in which both believe they lead is narrow and sometimes
	// absent. Racing it was tried and is flaky: the control passed, then
	// failed within twenty runs with "got Ok", which is the most dangerous
	// possible failure here because it reads as "the bug is fixed".
	//
	// Both modes face the same pinned adversary, which is what makes the two
	// results comparable: ReadLocal answers from stale state and the history
	// is Illegal; ReadIndexBarrier refuses the read and it is Ok. CheckQuorum
	// narrows this window in production but does not close it — reads served
	// inside the step-down timeout are still stale — which is precisely why
	// the barrier, not CheckQuorum, is the fix.
	cfg.DisableCheckQuorum = true
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
		t.Fatalf("the majority leader could not serve a read of the job it had just committed; "+
			"the barrier is refusing reads it should allow, and a passing verdict here would "+
			"mean nothing (mode=%s)", mode)
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
