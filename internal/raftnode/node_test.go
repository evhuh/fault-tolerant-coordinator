package raftnode

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/clock"
)

// THE ORDERING THE REVOCATION-DEADLOCK FIX DEPENDS ON.
//
// onLeadershipChanged runs on the Ready-loop goroutine and must not block on
// anything that goroutine serves. The coordinator's teardown is safe only
// because a scheduler worker reaching proposeRaft *after* revocation is
// turned away by the IsLeader guard, so it never registers a waiter nobody
// will release.
//
// That depends entirely on setLeadership publishing isLeader=false BEFORE
// invoking the callback. Nothing else pins it, and a refactor that moves the
// callback above the assignment would reintroduce a deadlock that self-heals
// after a timeout — the hardest kind to notice, because the symptom is an
// unexplained election rather than a hang.
func TestLeadershipIsPublishedBeforeTheCallbackFires(t *testing.T) {
	var observed atomic.Bool
	var sawLeader atomic.Bool
	n := &Node{}
	n.h = Handler{
		LeadershipChanged: func(isLeader bool) {
			// What a callback observes about the node it is being told about.
			observed.Store(true)
			sawLeader.Store(n.IsLeader())
		},
	}

	// Gain leadership: the callback must already see IsLeader() == true.
	n.setLeadership(1, true)
	if !observed.Load() {
		t.Fatal("callback never fired on gaining leadership")
	}
	if !sawLeader.Load() {
		t.Error("callback saw IsLeader()==false while being told leadership was GAINED")
	}

	// Lose it: the callback must already see IsLeader() == false. This is the
	// direction the deadlock fix relies on.
	observed.Store(false)
	n.setLeadership(0, false)
	if !observed.Load() {
		t.Fatal("callback never fired on losing leadership")
	}
	if sawLeader.Load() {
		t.Error("callback saw IsLeader()==true while being told leadership was LOST — " +
			"a scheduler worker proposing here would register a waiter nobody releases")
	}
}

// The callback fires only on a CHANGE, not on every Ready batch. Firing it
// repeatedly would tear down and rebuild the leader-only components on every
// loop iteration.
func TestLeadershipCallbackFiresOnlyOnChange(t *testing.T) {
	var calls atomic.Int64
	n := &Node{}
	n.h = Handler{LeadershipChanged: func(bool) { calls.Add(1) }}

	n.setLeadership(1, true)
	n.setLeadership(1, true)
	n.setLeadership(1, true)
	if got := calls.Load(); got != 1 {
		t.Errorf("callback fired %d times for one transition, want 1", got)
	}

	n.setLeadership(2, false)
	if got := calls.Load(); got != 2 {
		t.Errorf("callback fired %d times across two transitions, want 2", got)
	}
}

// The stall threshold must be STRICTLY BELOW the election timeout, so it
// fires before the symptom it exists to catch.
//
// A loop stalled past the election timeout stops ticking and stops sending
// heartbeats, so the cluster elects around it — and then the stall self-heals
// when whatever it waited on times out, leaving an unexplained election and
// no evidence. A detector that only fires after that point reports the
// aftermath.
func TestStallThresholdIsBelowTheElectionTimeout(t *testing.T) {
	for _, tc := range []struct {
		name         string
		tickInterval time.Duration
		electionTick int
	}{
		{"production", 100 * time.Millisecond, 10},
		{"simulator", 10 * time.Millisecond, 10},
		{"slow ticks", time.Second, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := &Node{cfg: Config{TickInterval: tc.tickInterval, ElectionTick: tc.electionTick}}
			election := time.Duration(tc.electionTick) * tc.tickInterval
			got := n.stallThreshold()
			if got <= 0 {
				t.Fatalf("threshold = %s, want positive", got)
			}
			if got >= election {
				t.Errorf("threshold %s >= election timeout %s — it would fire after the election it should precede",
					got, election)
			}
		})
	}
}

// An explicit threshold wins, so a caller can tighten it.
func TestStallThresholdHonoursConfig(t *testing.T) {
	n := &Node{cfg: Config{
		TickInterval: 100 * time.Millisecond, ElectionTick: 10,
		StallThreshold: 7 * time.Millisecond,
	}}
	if got := n.stallThreshold(); got != 7*time.Millisecond {
		t.Errorf("threshold = %s, want the configured 7ms", got)
	}
}

// IDLE IS NOT STALLED. The Ready loop spends most of its life parked waiting
// for work, and a detector that could not tell the two apart would fire
// constantly and be turned off.
func TestIdleLoopIsNotReportedAsStalled(t *testing.T) {
	var fired atomic.Int64
	n := &Node{
		cfg:   Config{TickInterval: 2 * time.Millisecond, ElectionTick: 10},
		clk:   clock.Real{},
		stopc: make(chan struct{}),
		done:  make(chan struct{}),
	}
	n.h = Handler{Stalled: func(time.Duration) { fired.Add(1) }}

	go n.watchStalls()
	defer close(n.stopc)

	// processingSince stays 0: the loop is idle, not working.
	time.Sleep(150 * time.Millisecond)
	if got := fired.Load(); got != 0 {
		t.Errorf("stall reported %d times while the loop was idle", got)
	}
}

// A loop that is genuinely inside a batch too long IS reported — once.
func TestStalledLoopIsReportedOnce(t *testing.T) {
	var fired atomic.Int64
	n := &Node{
		cfg:   Config{TickInterval: 2 * time.Millisecond, ElectionTick: 10}, // threshold 10ms
		clk:   clock.Real{},
		stopc: make(chan struct{}),
		done:  make(chan struct{}),
	}
	n.h = Handler{Stalled: func(time.Duration) { fired.Add(1) }}

	go n.watchStalls()
	defer close(n.stopc)

	// Enter a batch and stay there.
	n.processingSince.Store(time.Now().UnixNano())
	time.Sleep(200 * time.Millisecond)

	got := fired.Load()
	if got == 0 {
		t.Fatal("a loop stuck inside a batch was never reported")
	}
	// Reported once per stall, not once per poll — otherwise a long stall
	// buries its own first report.
	if got > 1 {
		t.Errorf("one stall reported %d times", got)
	}

	// Leaving the batch clears it, and a new stall reports again.
	n.processingSince.Store(0)
	time.Sleep(30 * time.Millisecond)
	n.processingSince.Store(time.Now().UnixNano())
	time.Sleep(200 * time.Millisecond)
	if fired.Load() != 2 {
		t.Errorf("second stall reported %d times total, want 2", fired.Load())
	}
}
