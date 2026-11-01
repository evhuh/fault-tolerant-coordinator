package raftnode

import (
	"strings"
	"sync"
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
	n.h = Handler{Stalled: func(Stall) { fired.Add(1) }}

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
	n.h = Handler{Stalled: func(Stall) { fired.Add(1) }}

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

// A stall must say WHICH phase blocked. Without it the report names a class
// of bug rather than a line of code, and a real stall that does not reproduce
// gets filed as flakiness — which is exactly what happened to a 176ms stall
// seen on one simulator run in twenty-four.
func TestStallReportsThePhaseItBlockedIn(t *testing.T) {
	var got Stall
	var fired atomic.Int64
	n := &Node{
		cfg:   Config{TickInterval: 2 * time.Millisecond, ElectionTick: 10}, // threshold 10ms
		clk:   clock.Real{},
		stopc: make(chan struct{}),
		done:  make(chan struct{}),
	}
	n.h = Handler{Stalled: func(s Stall) {
		got = s
		fired.Add(1)
	}}

	go n.watchStalls()
	defer close(n.stopc)

	// Stall inside the compaction phase specifically.
	n.phase.Store(phaseCompact)
	n.processingSince.Store(time.Now().UnixNano())
	time.Sleep(100 * time.Millisecond)

	if fired.Load() == 0 {
		t.Fatal("no stall reported")
	}
	if got.Phase != "snapshot+compact" {
		t.Errorf("stall phase = %q, want %q", got.Phase, "snapshot+compact")
	}
	if got.Duration <= 0 {
		t.Errorf("stall duration = %s, want positive", got.Duration)
	}
	// The String form is what lands in the simulator's violation message.
	if !strings.Contains(got.String(), "snapshot+compact") {
		t.Errorf("stall string %q does not name the phase", got.String())
	}
}

// The two causes a stall can have must be distinguishable, because they have
// opposite meanings: an fsync is the host being slow, and an apply that blocks
// is this program deadlocking on its own goroutine. Reporting them the same
// way meant a 3.42s fsync was announced as "a blocking call on the loop's own
// goroutine", and would equally have let a real deadlock be dismissed as a
// slow disk.
func TestStallDistinguishesDiskFromBlockingCall(t *testing.T) {
	for _, tc := range []struct {
		phase   int32
		name    string
		ioBound bool
	}{
		{phasePersist, "persist", true},
		{phaseCompact, "snapshot+compact", true},
		{phaseSnapshotIn, "restore-incoming-snapshot", true},
		{phaseApply, "apply", false},
		{phaseSoftState, "leadership-change", false},
		{phaseSend, "send", false},
	} {
		if got := phaseName(tc.phase); got != tc.name {
			t.Errorf("phaseName(%d) = %q, want %q", tc.phase, got, tc.name)
		}
		if got := ioBoundPhase(tc.phase); got != tc.ioBound {
			t.Errorf("ioBoundPhase(%s) = %v, want %v", tc.name, got, tc.ioBound)
		}
	}

	// The string form carries the distinction too, since that is what lands
	// in the simulator's message.
	disk := Stall{Duration: 3420 * time.Millisecond, Phase: "persist", IOBound: true}
	if !strings.Contains(disk.String(), "host I/O") {
		t.Errorf("disk stall %q does not name its cause", disk.String())
	}
	logic := Stall{Duration: 200 * time.Millisecond, Phase: "apply"}
	if !strings.Contains(logic.String(), "blocking call") {
		t.Errorf("logic stall %q does not name its cause", logic.String())
	}
}

// A STALL REPORTS ITS COMPLETED DURATION, not just the censored floor the
// watchdog saw when it fired.
//
// The detector polls at threshold/2 and reports once per stall, so its first
// report lands between threshold and 1.5x threshold however long the stall
// actually runs. Measured on a real cluster at a 500ms threshold: every
// reported duration fell in 500-748ms with a 501ms median, which is the poll
// interval showing through — a six-second stall was reported as roughly
// 500ms, and raft_ready_stall_seconds could never reach its upper buckets.
//
// So there are two reports, and this pins the second one.
func TestStallReportsItsCompletedDuration(t *testing.T) {
	const threshold = 20 * time.Millisecond
	const blocked = 10 * threshold // comfortably past 1.5x, which is the censor

	var mu sync.Mutex
	var got []Stall
	n := &Node{
		cfg:   Config{TickInterval: 2 * time.Millisecond, ElectionTick: 10},
		clk:   clock.Real{},
		stopc: make(chan struct{}),
		done:  make(chan struct{}),
	}
	n.h = Handler{Stalled: func(s Stall) {
		mu.Lock()
		got = append(got, s)
		mu.Unlock()
	}}

	go n.watchStalls()
	defer close(n.stopc)

	// Drive one batch by hand, the way handleReady does: mark the start,
	// block past the threshold, then run the completion report.
	startedAt := time.Now()
	since := startedAt.UnixNano()
	n.processingSince.Store(since)
	n.phase.Store(phaseApply)
	time.Sleep(blocked)
	// What handleReady's defer does.
	n.processingSince.Store(0)
	if n.reportedStall.CompareAndSwap(since, 0) {
		n.h.Stalled(Stall{
			ID: since, Duration: time.Since(startedAt),
			Phase: phaseName(n.reportedPhase.Load()), Completed: true,
		})
	}

	mu.Lock()
	defer mu.Unlock()
	var early, complete []Stall
	for _, s := range got {
		if s.Completed {
			complete = append(complete, s)
		} else {
			early = append(early, s)
		}
	}
	if len(early) != 1 {
		t.Fatalf("expected exactly 1 early-warning report, got %d (%v)", len(early), got)
	}
	if len(complete) != 1 {
		t.Fatalf("expected exactly 1 completed report, got %d — without it every quoted "+
			"stall duration is a floor and the latency histogram cannot reach its upper "+
			"buckets", len(complete))
	}
	// The early warning is censored by construction: it cannot exceed 1.5x
	// the threshold however long the stall ran.
	if early[0].Duration > threshold*2 {
		t.Errorf("early report %v exceeded the poll censor; the test is not measuring what it thinks",
			early[0].Duration)
	}
	// The completed report must carry the REAL blocked time.
	if complete[0].Duration < blocked {
		t.Errorf("completed report says %v, but the batch was blocked for at least %v",
			complete[0].Duration, blocked)
	}
	if complete[0].ID != early[0].ID {
		t.Errorf("the two reports carry different ids (%d vs %d), so a consumer cannot "+
			"correlate them", early[0].ID, complete[0].ID)
	}
	// The phase must survive to the completed report. handleReady resets the
	// phase to idle in a defer registered after the reporting one, so reading
	// n.phase at completion time would always say "idle".
	if complete[0].Phase != "apply" {
		t.Errorf("completed report lost the stalled phase: got %q, want %q",
			complete[0].Phase, "apply")
	}
}
