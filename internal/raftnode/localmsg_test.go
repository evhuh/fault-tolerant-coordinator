package raftnode

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// DOES etcd/raft ALREADY IGNORE MsgHup ARRIVING OVER THE NETWORK?
//
// This question decides whether the transport's plausibility guard is a fix
// for a real hole or defense-in-depth, and the honest answer matters more
// than the flattering one. An empty HTTP body unmarshals cleanly into
// Message{Type: MsgHup, From: 0, To: 0}, and MsgHup is what makes a node
// campaign — so if raft Stepped it, one empty POST would force an election.
//
// It does not. node.Step drops local message types unless they came from one
// of raft's internal sentinel sources:
//
//	if IsLocalMsg(m.GetType()) && !IsLocalMsgTarget(m.GetFrom()) { return nil }
//
// This test asserts that empirically rather than taking the source's word,
// by driving a real node and watching its term.
func TestRaftIgnoresLocalMessagesArrivingFromTheNetwork(t *testing.T) {
	if !raft.IsLocalMsg(raftpb.MsgHup) {
		t.Fatal("precondition: MsgHup is no longer classified as a local message")
	}
	if raft.IsLocalMsgTarget(0) {
		t.Fatal("precondition: From=0 now counts as a local-message target")
	}

	storage := raft.NewMemoryStorage()
	cfg := &raft.Config{
		ID: 1, ElectionTick: 10, HeartbeatTick: 1,
		Storage: storage, MaxSizePerMsg: 1024, MaxInflightMsgs: 16,
	}
	node := raft.StartNode(cfg, []raft.Peer{{ID: 1}})
	defer node.Stop()

	// Minimal Ready loop: a single-node cluster elects itself promptly.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				node.Tick()
			case rd := <-node.Ready():
				if rd.HardState != nil && !raft.IsEmptyHardState(rd.HardState) {
					_ = storage.SetHardState(rd.HardState)
				}
				_ = storage.Append(rd.Entries)
				for _, ent := range rd.CommittedEntries {
					if ent.GetType() == raftpb.EntryConfChange {
						cc := &raftpb.ConfChange{}
						if proto.Unmarshal(ent.GetData(), cc) == nil {
							node.ApplyConfChange(cc)
						}
					}
				}
				node.Advance()
			}
		}
	}()

	// Wait for it to lead, so the term has settled.
	deadline := time.Now().Add(5 * time.Second)
	for node.Status().RaftState != raft.StateLeader {
		if time.Now().After(deadline) {
			t.Fatal("single-node cluster never became leader")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// GetTerm(), not .Term: BasicStatus embeds *pb.HardState, whose Term is a
	// *uint64 — so .Term is a POINTER, and comparing two of those compares
	// allocations rather than terms. The first version of this test did
	// exactly that and "detected" a term change on every run.
	before := node.Status().HardState.GetTerm()
	t.Logf("settled as leader at term %d", before)

	// THE EXPERIMENT: feed it exactly what an empty HTTP body decodes to,
	// repeatedly, with no guard in the way.
	empty := &raftpb.Message{}
	if err := proto.Unmarshal(nil, empty); err != nil {
		t.Fatalf("empty body no longer parses: %v", err)
	}
	for i := 0; i < 50; i++ {
		if err := node.Step(context.Background(), empty); err != nil {
			t.Fatalf("Step returned %v", err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	after := node.Status().HardState.GetTerm()
	if after != before {
		t.Errorf("term moved %d -> %d: raft ACTED on a network-delivered MsgHup, "+
			"so the transport guard is fixing a real hole", before, after)
	} else {
		t.Logf("term unchanged at %d after 50 network MsgHup messages — "+
			"raft already drops them; the transport guard is defense-in-depth", after)
	}
}
