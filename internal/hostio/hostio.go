// Package hostio measures whether this host can support the simulator's
// compressed timings.
//
// It lives in a package rather than inside cmd/fsyncprobe because two
// different callers need it: the release gate, which refuses to assert
// anything on a host that cannot keep up, and the long chaos run, which
// records the disk's state BEFORE AND AFTER every seed so a result carries
// the conditions it was measured in. A number from a run whose disk degraded
// halfway through is not a number, and the only way to know is to have asked
// at both ends.
package hostio

import (
	"fmt"
	"os"
	"time"
)

// Probe is one measurement of fsync latency.
type Probe struct {
	Dir   string
	N     int
	Mean  time.Duration
	Worst time.Duration
}

func (p Probe) String() string {
	return fmt.Sprintf("fsync x%d in %s: mean %s, worst %s",
		p.N, p.Dir, p.Mean.Round(time.Microsecond), p.Worst.Round(time.Microsecond))
}

// Exceeds reports whether this measurement is outside the budget, and why.
func (p Probe) Exceeds(maxMean, maxWorst time.Duration) (bool, string) {
	switch {
	case p.Mean > maxMean:
		return true, fmt.Sprintf("mean fsync %s exceeds %s", p.Mean.Round(time.Microsecond), maxMean)
	case p.Worst > maxWorst:
		return true, fmt.Sprintf("worst fsync %s exceeds %s", p.Worst.Round(time.Microsecond), maxWorst)
	}
	return false, ""
}

// Fsync times n write+fsync pairs in dir, the shape the WAL actually does:
// one append and one Sync per batch, the same as etcd.
func Fsync(dir string, n int) (Probe, error) {
	if n <= 0 {
		return Probe{}, fmt.Errorf("hostio: n must be positive")
	}
	f, err := os.CreateTemp(dir, "fsyncprobe-")
	if err != nil {
		return Probe{}, err
	}
	name := f.Name()
	defer func() {
		f.Close()
		os.Remove(name)
	}()

	// 4KiB, one page, the order of a raft entry batch here. The point is the
	// SYNC, not the write.
	buf := make([]byte, 4096)
	p := Probe{Dir: dir, N: n}
	var total time.Duration
	for i := 0; i < n; i++ {
		start := time.Now()
		if _, err := f.Write(buf); err != nil {
			return Probe{}, err
		}
		if err := f.Sync(); err != nil {
			return Probe{}, err
		}
		d := time.Since(start)
		total += d
		if d > p.Worst {
			p.Worst = d
		}
	}
	p.Mean = total / time.Duration(n)
	return p, nil
}
