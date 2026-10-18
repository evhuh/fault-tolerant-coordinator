package simnet

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
)

// recorder collects what a node received.
type recorder struct {
	mu   sync.Mutex
	msgs []*raftpb.Message
}

func (r *recorder) step(_ context.Context, m *raftpb.Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m)
	return nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs)
}

func wire(t *testing.T, n *Network, ids ...uint64) map[uint64]*recorder {
	t.Helper()
	out := map[uint64]*recorder{}
	for _, id := range ids {
		r := &recorder{}
		out[id] = r
		if err := n.Transport(id).Listen("", r.step); err != nil {
			t.Fatalf("listen %d: %v", id, err)
		}
	}
	t.Cleanup(n.Close)
	return out
}

func msg(from, to uint64) *raftpb.Message {
	return &raftpb.Message{From: &from, To: &to}
}

// awaitCount waits for a recorder to reach n, so the test does not depend on
// delivery goroutines having been scheduled yet.
func awaitCount(t *testing.T, r *recorder, want int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if r.count() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("got %d messages, want %d within %s", r.count(), want, within)
}

func TestDeliversWhenHealthy(t *testing.T) {
	n := New(1, Faults{})
	rs := wire(t, n, 1, 2)
	for i := 0; i < 10; i++ {
		n.Transport(1).Send(msg(1, 2))
	}
	awaitCount(t, rs[2], 10, 2*time.Second)
}

// A partition drops messages that CROSS it, and only those. Nodes on the same
// side must keep talking, which is what lets a majority carry on while a
// minority is cut off.
func TestPartitionIsOneSided(t *testing.T) {
	n := New(1, Faults{})
	rs := wire(t, n, 1, 2, 3)
	n.Partition(3) // 3 alone; 1 and 2 together

	for i := 0; i < 5; i++ {
		n.Transport(1).Send(msg(1, 2)) // same side → delivered
		n.Transport(1).Send(msg(1, 3)) // crosses → dropped
		n.Transport(3).Send(msg(3, 1)) // crosses → dropped
	}
	awaitCount(t, rs[2], 5, 2*time.Second)

	time.Sleep(50 * time.Millisecond)
	if got := rs[3].count(); got != 0 {
		t.Errorf("node 3 received %d messages across a partition, want 0", got)
	}
	if st := n.Stats(); st.DroppedPartition != 10 {
		t.Errorf("partition drops = %d, want 10", st.DroppedPartition)
	}
}

func TestHealRestoresDelivery(t *testing.T) {
	n := New(1, Faults{})
	rs := wire(t, n, 1, 2)
	n.Partition(2)
	n.Transport(1).Send(msg(1, 2))
	time.Sleep(30 * time.Millisecond)
	if rs[2].count() != 0 {
		t.Fatal("delivered across a partition")
	}

	n.Heal()
	n.Transport(1).Send(msg(1, 2))
	awaitCount(t, rs[2], 1, 2*time.Second)
}

// A node that is DOWN is not the same as one that is partitioned: it is not
// there at all, in either direction.
func TestDownNodeNeitherSendsNorReceives(t *testing.T) {
	n := New(1, Faults{})
	rs := wire(t, n, 1, 2)
	n.SetDown(2, true)

	n.Transport(1).Send(msg(1, 2))
	n.Transport(2).Send(msg(2, 1))
	time.Sleep(50 * time.Millisecond)
	if rs[2].count() != 0 || rs[1].count() != 0 {
		t.Errorf("down node exchanged messages: to=%d from=%d", rs[2].count(), rs[1].count())
	}

	n.SetDown(2, false)
	n.Transport(1).Send(msg(1, 2))
	awaitCount(t, rs[2], 1, 2*time.Second)
}

// A message in flight when a partition opens must still be swallowed —
// otherwise a delayed message tunnels through a partition that exists, and
// the partition is a lie.
func TestPartitionOpeningMidFlightSwallowsTheMessage(t *testing.T) {
	n := New(1, Faults{MinDelay: 80 * time.Millisecond, MaxDelay: 100 * time.Millisecond})
	rs := wire(t, n, 1, 2)

	n.Transport(1).Send(msg(1, 2))
	time.Sleep(10 * time.Millisecond) // still in flight
	n.Partition(2)

	time.Sleep(300 * time.Millisecond)
	if got := rs[2].count(); got != 0 {
		t.Errorf("an in-flight message tunnelled through a partition (%d delivered)", got)
	}
}

// THE CHECKPOINT PROPERTY: one seed, one fault schedule.
//
// Two networks built from the same seed and fed the same sends must drop
// exactly the same messages. Without this a failing seed is not reproducible
// and the whole harness is decoration.
func TestSameSeedProducesSameDropSchedule(t *testing.T) {
	run := func(seed int64) []int {
		n := New(seed, Faults{DropRate: 0.5})
		r := &recorder{}
		if err := n.Transport(2).Listen("", r.step); err != nil {
			t.Fatal(err)
		}
		var fate []int
		for i := 0; i < 200; i++ {
			before := n.Stats().DroppedRandom
			n.Transport(1).Send(msg(1, 2))
			after := n.Stats().DroppedRandom
			fate = append(fate, after-before) // 1 = dropped, 0 = sent on
		}
		n.Close()
		return fate
	}

	a, b := run(42), run(42)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("seed 42 diverged at message %d: %d vs %d", i, a[i], b[i])
		}
	}

	// And a different seed must actually differ, or the test above proves
	// nothing but that both runs dropped everything.
	c := run(43)
	same := true
	for i := range a {
		if a[i] != c[i] {
			same = false
			break
		}
	}
	if same {
		t.Error("seeds 42 and 43 produced identical schedules; the seed is not being used")
	}
}

func TestDropRateIsRoughlyHonoured(t *testing.T) {
	n := New(7, Faults{DropRate: 0.3})
	wire(t, n, 1, 2)
	const total = 2000
	for i := 0; i < total; i++ {
		n.Transport(1).Send(msg(1, 2))
	}
	ratio := float64(n.Stats().DroppedRandom) / total
	if ratio < 0.25 || ratio > 0.35 {
		t.Errorf("drop ratio = %.3f, want ~0.30", ratio)
	}
}

// Each drop reason is counted on its own. Folding them together produces a
// "drop rate" that is mostly messages to a switched-off node, which is not a
// network fault — and a metric reporting 33% when 2% was configured is worse
// than no metric.
func TestDropReasonsAreCountedSeparately(t *testing.T) {
	n := New(1, Faults{}) // no random drops at all
	wire(t, n, 1, 2, 3)

	n.SetDown(2, true)
	for i := 0; i < 4; i++ {
		n.Transport(1).Send(msg(1, 2))
	}
	n.SetDown(2, false)

	n.Partition(3)
	for i := 0; i < 6; i++ {
		n.Transport(1).Send(msg(1, 3))
	}

	st := n.Stats()
	if st.DroppedDown != 4 {
		t.Errorf("node-down drops = %d, want 4", st.DroppedDown)
	}
	if st.DroppedPartition != 6 {
		t.Errorf("partition drops = %d, want 6", st.DroppedPartition)
	}
	if st.DroppedRandom != 0 {
		t.Errorf("random drops = %d, want 0 — DropRate was zero", st.DroppedRandom)
	}
}
