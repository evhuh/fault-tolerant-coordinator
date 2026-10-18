// Package clock is the one place the system reads wall time.
//
// Note carefully what this is NOT for. The apply path already has no clock at
// all — lease expiry reads the timestamp carried in a committed Tick, which is
// what makes every replica expire the same leases at the same log position.
// That property predates this package and does not depend on it.
//
// This is for the DRIVERS: the raft ticker, the Tick proposer, long-poll
// deadlines, worker heartbeats. Those read real time, and until Day 8 they
// read it directly, which left the simulator no way to compress a run or to
// know what time the system thought it was.
//
// # What a Clock does and does not buy
//
// It makes timeouts injectable and compressible, so a simulated cluster can
// run leases of 50ms instead of 2s and cover hours of scheduled faults in
// seconds.
//
// It does NOT make execution deterministic. Go's goroutine scheduling stays
// nondeterministic no matter what the clock says, so two runs of one seed
// reproduce the same FAULT SCHEDULE, not the same interleaving. Pretending
// otherwise would be the overclaim the whole verification phase is meant to
// avoid.
package clock

import "time"

// Ticker is the subset of time.Ticker the system uses. An interface because
// time.Ticker is a concrete struct with an exported channel field, which
// cannot be substituted.
type Ticker interface {
	// C is the tick channel. A method rather than a field, which is the only
	// reason this interface exists at all.
	C() <-chan time.Time
	Stop()
}

// Clock is the system's view of wall time.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
	NewTicker(d time.Duration) Ticker
}

// Real is the ordinary system clock. It is the default everywhere; the
// simulator is what supplies anything else.
type Real struct{}

func (Real) Now() time.Time                         { return time.Now() }
func (Real) Since(t time.Time) time.Duration        { return time.Since(t) }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (Real) Sleep(d time.Duration)                  { time.Sleep(d) }
func (Real) NewTicker(d time.Duration) Ticker       { return realTicker{time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// OrReal returns c, or the real clock when c is nil.
//
// Every Config holds a nil Clock by default, so callers that never heard of
// this package keep working unchanged. That matters: the alternative is
// threading a clock through every constructor in the codebase on the day the
// simulator lands.
func OrReal(c Clock) Clock {
	if c == nil {
		return Real{}
	}
	return c
}
