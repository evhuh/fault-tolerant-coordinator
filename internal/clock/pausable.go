package clock

// A clock whose tickers can be FROZEN, for modelling a paused process.
//
// # What is being modelled
//
// A GC pause, a VM stall, a `SIGSTOP`. The process is not dead and it is not
// partitioned — it is simply not running, and the distinction matters because
// raft defends against the other two and not against this one.
//
// CheckQuorum is the specific thing a pause defeats. It makes a leader step
// down when an election timeout elapses without hearing from a majority, and
// it is driven by TICKS. A leader that is not ticking never runs the check,
// so it is still convinced it leads when it resumes — which is how a lease
// read goes stale on a cluster that is otherwise correctly configured.
//
// # Why only the tickers
//
// Freezing Now() as well would be more faithful and much more dangerous: the
// coordinator's propose timeouts and long-poll deadlines read Now(), and
// stopping it would wedge callers rather than pause a node. Ticks are what
// drive raft's internal timers, which is the whole mechanism under test.
// A paused node therefore still measures elapsed time correctly; it just
// stops advancing raft's clock, which is what a stalled process does to its
// own scheduled work.

import (
	"sync"
	"time"
)

// Pausable wraps a Clock and can freeze the tickers it hands out.
//
// Safe for concurrent use, because Pause and Resume are called from the fault
// schedule while the node reads ticks on its own goroutines.
type Pausable struct {
	inner Clock

	mu      sync.Mutex
	paused  bool
	tickers []*pausableTicker
}

// NewPausable wraps a clock. A nil inner clock means the real one.
func NewPausable(inner Clock) *Pausable {
	return &Pausable{inner: OrReal(inner)}
}

func (p *Pausable) Now() time.Time                         { return p.inner.Now() }
func (p *Pausable) Since(t time.Time) time.Duration        { return p.inner.Since(t) }
func (p *Pausable) After(d time.Duration) <-chan time.Time { return p.inner.After(d) }
func (p *Pausable) Sleep(d time.Duration)                  { p.inner.Sleep(d) }

func (p *Pausable) NewTicker(d time.Duration) Ticker {
	t := &pausableTicker{inner: p.inner.NewTicker(d), out: make(chan time.Time, 1), done: make(chan struct{})}
	p.mu.Lock()
	t.paused = p.paused
	p.tickers = append(p.tickers, t)
	p.mu.Unlock()
	go t.pump()
	return t
}

// Pause freezes every ticker this clock has handed out, and any it hands out
// later, until Resume.
func (p *Pausable) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = true
	for _, t := range p.tickers {
		t.setPaused(true)
	}
}

// Resume unfreezes them.
//
// Deliberately does NOT deliver the ticks that were swallowed. A process
// coming back from a stall does not get to catch up on the timers it missed;
// it finds out what happened by doing its ordinary work, which here means
// draining the messages that queued while it was away.
func (p *Pausable) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.paused = false
	for _, t := range p.tickers {
		t.setPaused(false)
	}
}

func (p *Pausable) Paused() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}

// pausableTicker forwards ticks from an inner ticker, dropping them while
// paused.
//
// A pump goroutine rather than a filtered channel read, because the consumer
// holds the channel and must simply see nothing arrive. Dropping rather than
// buffering is the point: a paused process misses its timers, it does not
// queue them.
type pausableTicker struct {
	inner Ticker
	out   chan time.Time
	done  chan struct{}

	mu     sync.Mutex
	paused bool
}

func (t *pausableTicker) C() <-chan time.Time { return t.out }

func (t *pausableTicker) Stop() {
	t.inner.Stop()
	select {
	case <-t.done:
	default:
		close(t.done)
	}
}

func (t *pausableTicker) setPaused(v bool) {
	t.mu.Lock()
	t.paused = v
	t.mu.Unlock()
}

func (t *pausableTicker) isPaused() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.paused
}

func (t *pausableTicker) pump() {
	for {
		select {
		case <-t.done:
			return
		case now, ok := <-t.inner.C():
			if !ok {
				return
			}
			if t.isPaused() {
				continue // the process is not running; the tick is lost
			}
			// Non-blocking, exactly as time.Ticker behaves: a consumer that
			// is behind misses ticks rather than slowing the ticker.
			select {
			case t.out <- now:
			default:
			}
		}
	}
}
