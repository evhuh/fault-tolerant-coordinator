package sim

// The shipped configuration must have the liveness options ON.
//
// This is the containment check for two knobs that exist only so a test can
// turn them off. The risk they carry is not that they are wrong but that
// they are reachable: an option disabled by accident in a default, or
// exposed as a flag someone sets in production, would silently restore the
// behaviour the Day 9 work removed. Nothing in cmd/ exposes them, and this
// pins the defaults so a future edit cannot quietly flip one.

import (
	"testing"

	"github.com/evazheng/fault-tolerant-coordinator/internal/raftnode"
)

func TestShippedConfigEnablesCheckQuorumAndPreVote(t *testing.T) {
	if r := raftnode.DefaultConfig(); r.DisableCheckQuorum || r.DisablePreVote {
		t.Errorf("raftnode.DefaultConfig ships with CheckQuorum=%v PreVote=%v; both must be on",
			!r.DisableCheckQuorum, !r.DisablePreVote)
	}
	// The simulator's default is what every sweep and every gate runs, so a
	// linearizability claim is only about the shipped configuration if this
	// holds too.
	if s := DefaultConfig(); s.DisableCheckQuorum || s.DisablePreVote {
		t.Errorf("sim.DefaultConfig ships with CheckQuorum=%v PreVote=%v; the gate would then "+
			"not be testing what production runs",
			!s.DisableCheckQuorum, !s.DisablePreVote)
	}
}
