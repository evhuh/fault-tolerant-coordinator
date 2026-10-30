package sim

import (
	"io"
	"slices"
	"testing"

	"github.com/evazheng/fault-tolerant-coordinator/internal/simnet"
)

// holdHarness is the fault schedule's isolation bookkeeping with no cluster
// under it. The semantics under test are decided per-step and per-node, so
// they can be driven directly instead of hoped for out of a 600-step run.
func holdHarness(t *testing.T, holdSteps int) *Sim {
	t.Helper()
	return &Sim{
		cfg: Config{Out: io.Discard, LeaderPartitionSteps: holdSteps},
		net: simnet.New(1, simnet.Faults{}),
	}
}

// A held partition SURVIVES a later partition of somebody else, and survives
// the heals the schedule rolls while it runs. Under the replace semantics
// this supersedes, the partition of node 3 silently released node 2 while the
// hold went on suppressing heals — the simulator believed it held a partition
// it had let go.
func TestHeldPartitionSurvivesLaterPartitionsAndHeals(t *testing.T) {
	s := holdHarness(t, 3)

	s.net.Partition(2)
	s.hold(2, 1) // held through steps 1, 2, 3

	// A later partition event, of a different node.
	s.net.Partition(3)
	if !s.net.IsPartitioned(2) {
		t.Fatal("partitioning node 3 released the held partition of node 2")
	}

	// A heal rolled by the schedule mid-hold: node 3 is loose and goes, node
	// 2 is held and stays.
	if !s.heal(2) {
		t.Error("heal released nothing; node 3 was not held and should have gone")
	}
	if s.net.IsPartitioned(3) {
		t.Error("node 3 was not held but survived the heal")
	}
	if !s.net.IsPartitioned(2) {
		t.Error("the heal ended a hold that still had steps to run")
	}
}

// The hold ends at its OWN scheduled step, whether or not the schedule
// happens to roll a heal. A countdown that only advanced on heal rolls made
// "held for 3 steps" mean three heal rolls, which is about five steps and
// varies with the seed.
func TestHoldEndsAtItsScheduledStep(t *testing.T) {
	s := holdHarness(t, 3)
	s.net.Partition(2)
	s.hold(2, 1)

	for step := 2; step <= 3; step++ {
		s.releaseExpiredHolds(step)
		if !s.net.IsPartitioned(2) {
			t.Fatalf("hold ended at step %d, before its deadline of step 4", step)
		}
	}

	s.releaseExpiredHolds(4)
	if s.net.IsPartitioned(2) {
		t.Error("node 2 is still partitioned at step 4, after its hold expired")
	}
	if s.result.Faults.Heals != 1 {
		t.Errorf("expiring a hold counted %d heals, want 1", s.result.Faults.Heals)
	}
	if len(s.heldUntil) != 0 {
		t.Errorf("expired hold left bookkeeping behind: %v", s.heldUntil)
	}
}

// Isolation accumulates across steps, and a heal releases every node that is
// not held — not just the one most recently partitioned.
func TestHealReleasesEveryUnheldNode(t *testing.T) {
	s := holdHarness(t, 5)
	s.net.Partition(1)
	s.net.Partition(2)
	s.net.Partition(3)
	s.hold(3, 1)

	if !s.heal(2) {
		t.Fatal("heal released nothing")
	}
	if got, want := s.net.PartitionedIDs(), []uint64{3}; !slices.Equal(got, want) {
		t.Errorf("after the heal, partitioned = %v, want %v", got, want)
	}

	// Nothing left to release but a held node: the heal reports that it did
	// nothing, which is what keeps the schedule's heal COUNT honest.
	if s.heal(2) {
		t.Error("heal reported releasing something when only a held node remained")
	}
}

// healAll is the explicit whole-network heal, and it abandons holds. A hold
// that survived quiescence would make the drain bound a lie.
func TestHealAllAbandonsHolds(t *testing.T) {
	s := holdHarness(t, 50)
	s.net.Partition(2)
	s.hold(2, 1)

	s.healAll()
	if ids := s.net.PartitionedIDs(); len(ids) != 0 {
		t.Errorf("healAll left %v partitioned", ids)
	}
	if len(s.heldUntil) != 0 {
		t.Errorf("healAll left holds behind: %v", s.heldUntil)
	}
}
