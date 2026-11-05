package sim

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/simnet"
	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// A DROPPED MsgSnap PARKS A FOLLOWER FOREVER, and the leader then cannot
// commit although it holds its term and believes it has a quorum.
//
// THE DEFECT. etcd/raft puts a follower in StateSnapshot when it sends it a
// snapshot and pauses all log probes to it — Progress.IsPaused() returns true
// unconditionally in that state — until the APPLICATION reports the
// snapshot's outcome via Node.ReportSnapshot. Nothing in this build ever
// called it, in either transport. raft's own documentation names the
// consequence: the follower "could end up in a limbo, never getting any
// updates from the leader".
//
// Meanwhile that follower's heartbeat responses keep Progress.RecentActive
// true, so CheckQuorum is satisfied and the leader is never deposed. With the
// other follower down, the result is a leader in one term, with a nominal
// majority, committing nothing.
//
// That MATCHES the shape of the five frozen-clock windows the chaos checkpoint
// recorded — same leader, same term, zero leadership transitions, 2.6 seconds
// of "proposal lost" — but a matching shape is not an attribution. Those
// windows were captured before the raft progress table was instrumented, so
// none of them shows a follower in StateSnapshot. What this test proves is
// the mechanism; what would attribute the windows to it is a post-fix
// checkpoint over the same adversary coming back with no windows at all.
//
// WHY THIS IS DETERMINISTIC AND THE SWEEP IS NOT. The sweep reaches this
// roughly 6% of runs, so a 24-seed hunt for it found nothing — a one-in-four
// outcome that proves neither direction. The mechanism needs one specific
// message lost, so the test loses exactly that message and nothing else.
func TestDroppedSnapshotDoesNotParkAFollower(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 7
	cfg.Steps = 0 // driven by hand; the fault schedule would add noise
	cfg.Nodes = 3
	cfg.StepTime = 20 * time.Millisecond
	// Snapshot often, so a restarted follower genuinely needs one rather
	// than being repairable from the log tail.
	cfg.SnapshotEvery = 30
	cfg.LinClients = 0
	cfg.Faults = simnet.Faults{} // no random drops: one loss, chosen
	cfg.ProposalTimeout = 300 * time.Millisecond

	s := New(cfg, dirsFor(t, cfg.Nodes))
	defer s.shutdown()

	for i := 1; i <= cfg.Nodes; i++ {
		if err := s.start(uint64(i)); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	lead := s.awaitLeader(10 * time.Second)
	if lead == nil {
		t.Fatal("no leader")
	}

	// Pick the two followers.
	var followers []uint64
	for _, id := range s.liveIDs() {
		if id != lead.id {
			followers = append(followers, id)
		}
	}
	parked, down := followers[0], followers[1]
	t.Logf("leader=node%d, follower to park=node%d, follower to take down=node%d", lead.id, parked, down)

	// 1. Take the follower-to-park down, and keep committing until the
	//    leader has compacted past its log. Now it can only be repaired by a
	//    snapshot, which is the state this defect lives in.
	s.stop(parked)
	for i := 0; i < 160; i++ {
		_ = submitOne(lead, fmt.Sprintf("fill-%d", i))
	}

	// 2. Lose exactly the snapshot that repairs it, and bring it back. The
	//    leader will mark it StateSnapshot and, with nothing reporting the
	//    outcome, never probe it again.
	s.net.DropSnapshotsTo(parked, 1)
	if err := s.start(parked); err != nil {
		t.Fatalf("restart node %d: %v", parked, err)
	}

	// Let the leader notice it is behind and send the snapshot that will be
	// swallowed.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, drops := s.net.SnapTraffic(); len(drops) > 0 {
			break
		}
		_ = submitOne(lead, fmt.Sprintf("probe-%d", time.Now().UnixNano()))
		time.Sleep(20 * time.Millisecond)
	}
	_, drops := s.net.SnapTraffic()
	if len(drops) == 0 {
		t.Skip("the leader never sent a snapshot to the restarted follower, so the condition " +
			"under test was not set up; raise SnapshotEvery pressure")
	}
	t.Logf("dropped: %+v", drops)

	// 3. Take the OTHER follower down. The leader now needs the parked one
	//    to make a majority — and a parked follower acks nothing.
	s.stop(down)

	// 4. Can the leader still commit? It should: raft is supposed to resume
	//    probing a follower once the snapshot's outcome is reported, and the
	//    follower is up and reachable.
	var lastErr error
	committed := false
	deadline = time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if err := submitOne(lead, fmt.Sprintf("after-%d", time.Now().UnixNano())); err == nil {
			committed = true
			break
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}

	info := lead.coord.RaftStatus()
	t.Logf("leader raft view: term=%d ackingQuorum=%v commit=%d", info.Term, info.AckingQuorum, info.Commit)
	for _, f := range info.Followers {
		t.Logf("  follower node%d: state=%s match=%d pendingSnapshot=%d recentActive=%v paused=%v",
			f.ID, f.State, f.Match, f.PendingSnapshot, f.RecentActive, f.Paused)
	}

	if !committed {
		for _, f := range info.Followers {
			if f.ID == parked && f.State == "StateSnapshot" {
				t.Fatalf("node%d is parked in StateSnapshot with pendingSnapshot=%d and recentActive=%v, "+
					"so the leader probes it never and commits nothing while keeping term %d: "+
					"the dropped MsgSnap was never reported to raft (last propose error: %v)",
					f.ID, f.PendingSnapshot, f.RecentActive, info.Term, lastErr)
			}
		}
		t.Fatalf("the leader could not commit for 6s after a dropped snapshot (last error: %v)", lastErr)
	}
}

// submitOne drives one committable operation through the real client path.
//
// A client submit rather than a hand-built command, because `propose` is
// unexported and because this is the path a user would notice frozen: it
// proposes and waits for the entry to commit, so its error is exactly what a
// caller sees when a leader cannot make progress.
func submitOne(lead *node, requestID string) error {
	_, err := lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
		ClientId:  "limbo-test",
		RequestId: requestID,
		Tasks:     []*schedulerpb.TaskSpec{{Command: "/usr/bin/true", MaxAttempts: 1}},
	})
	return err
}
