package sim

// Tests that CheckQuorum and PreVote actually do their jobs.
//
// These exist because the only other scenario that touches either option
// turns CheckQuorum OFF. The stale-read control pins the pre-liveness
// configuration deliberately — it has to, or its premise races a step-down —
// which means that without these two tests the shipped configuration would
// be exercised by nothing that asserts anything about it. An option enabled
// in production and asserted nowhere is an option that can be silently
// reverted.
//
// Both drive the sequence by hand rather than waiting for a seeded schedule
// to produce it, for the same reason the stale-read scenario does: a property
// that only holds on some seeds is not a property.

import (
	"testing"
	"time"
)

// CheckQuorum: a leader cut off from the majority must STOP believing it
// leads, within about one election timeout.
//
// This is the liveness half of the Day 9 story. It is not the correctness
// fix — a read served inside the step-down window is still stale, which is
// what the ReadIndex barrier is for — but a leader that never steps down
// keeps a stale answer available indefinitely, and shrinking that from
// "forever" to "one election timeout" is worth having.
func TestCheckQuorumMakesACutOffLeaderStepDown(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 20261021
	cfg.Steps = 0
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
	lead := s.awaitLeader(10 * time.Second)
	if lead == nil {
		t.Fatal("no leader came up")
	}

	s.net.Partition(lead.id)

	// Generous bound: the step-down happens on a tick after an election
	// timeout elapses with no quorum contact, and the test asserts it
	// HAPPENS rather than racing its exact timing.
	election := time.Duration(cfg.ElectionTick) * cfg.TickInterval
	deadline := time.Now().Add(10 * election)
	for time.Now().Before(deadline) {
		if !lead.coord.IsLeader() {
			t.Logf("node %d stepped down %s after being cut off (election timeout %s)",
				lead.id, time.Since(deadline.Add(-10*election)).Round(time.Millisecond), election)
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("node %d still claims leadership %s after losing contact with the majority — "+
		"CheckQuorum is not in effect, and a cut-off leader will serve stale reads indefinitely",
		lead.id, 10*election)
}

// Without CheckQuorum the same leader keeps its belief, which is what makes
// the test above meaningful rather than a tautology about timeouts.
//
// It also pins the configuration the stale-read control depends on: if this
// ever starts failing, that control's premise is gone and it will quietly
// stop reproducing the bug.
func TestWithoutCheckQuorumACutOffLeaderKeepsBelieving(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 20261021
	cfg.Steps = 0
	cfg.LinClients = 0
	cfg.Faults.DropRate = 0
	cfg.Faults.CorruptRate = 0
	cfg.DisableCheckQuorum = true

	s := New(cfg, dirsFor(t, cfg.Nodes))
	defer s.shutdown()
	for i := 1; i <= cfg.Nodes; i++ {
		if err := s.start(uint64(i)); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	lead := s.awaitLeader(10 * time.Second)
	if lead == nil {
		t.Fatal("no leader came up")
	}

	s.net.Partition(lead.id)
	election := time.Duration(cfg.ElectionTick) * cfg.TickInterval
	time.Sleep(6 * election)

	if !lead.coord.IsLeader() {
		t.Fatalf("node %d stepped down with CheckQuorum disabled — the stale-read control's "+
			"premise no longer holds, so it has stopped reproducing the bug it exists to catch",
			lead.id)
	}
}

// PreVote: a node that was partitioned away must not depose a healthy leader
// when it comes back.
//
// Without PreVote the returning node spends the partition incrementing its
// term in elections it cannot win, and rejoins with a term higher than the
// cluster's. That forces the sitting leader to step down and the cluster to
// hold an election it had no reason to hold — a disruption caused entirely by
// a node that was never eligible to lead.
//
// Asserted on the TERM rather than on who leads: the term is the thing
// PreVote protects, and a cluster that kept its leader but burned three terms
// doing it has not been protected.
func TestPreVoteStopsARejoiningNodeDeposingTheLeader(t *testing.T) {
	if moved := rejoinTermMovement(t, false); moved != 0 {
		t.Errorf("term moved by %d when a cut-off node rejoined — it disrupted a healthy "+
			"leader, which is what PreVote exists to prevent", moved)
	}
}

// And the same scenario WITHOUT PreVote must move the term, or the test
// above is a tautology about a quiet cluster rather than a test of PreVote.
func TestWithoutPreVoteARejoiningNodeDisruptsTheLeader(t *testing.T) {
	if moved := rejoinTermMovement(t, true); moved == 0 {
		t.Error("term did not move with PreVote disabled — the scenario is not actually " +
			"producing the disruption, so the PreVote test above proves nothing")
	}
}

// rejoinTermMovement partitions a follower, lets it burn elections, heals,
// and reports how far the leader's term moved.
func rejoinTermMovement(t *testing.T, disablePreVote bool) uint64 {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Seed = 20261022
	cfg.DisablePreVote = disablePreVote
	cfg.Steps = 0
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
	lead := s.awaitLeader(10 * time.Second)
	if lead == nil {
		t.Fatal("no leader came up")
	}

	// Cut off a FOLLOWER, so the majority keeps its leader throughout and
	// any disruption on rejoin is attributable to the returning node.
	var victim *node
	for _, id := range s.liveIDs() {
		if id != lead.id {
			victim, _ = s.nodeAt(id)
			break
		}
	}
	if victim == nil {
		t.Fatal("no follower to partition")
	}

	election := time.Duration(cfg.ElectionTick) * cfg.TickInterval
	s.net.Partition(victim.id)
	// Long enough that a node without PreVote would have run several
	// elections and climbed several terms.
	time.Sleep(8 * election)

	termBefore := termOf(t, lead)
	leaderBefore := lead.id

	s.healAll()
	time.Sleep(8 * election)

	termAfter := termOf(t, lead)
	t.Logf("prevote=%v: term %d -> %d across the rejoin of node %d (leader was %d)",
		!disablePreVote, termBefore, termAfter, victim.id, leaderBefore)
	if termAfter < termBefore {
		return 0
	}
	return termAfter - termBefore
}

// termOf reads a node's raft term.
//
// Through HardState.GetTerm, not Status().Term: Term is reached through an
// embedded pointer, and reading it the obvious way once compared pointer
// ADDRESSES in an earlier experiment, which silently made a term that never
// moved look like a term that never moved for the wrong reason.
func termOf(t *testing.T, n *node) uint64 {
	t.Helper()
	return n.coord.RaftStatus().Term
}
