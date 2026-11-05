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
	"runtime"
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

// CPU is one measurement of whether this host is still giving the process the
// compute and scheduling it had at the start of a long run.
//
// THE FSYNC PROBE ONLY MEASURES DISK, and the chaos checkpoint produced four
// frozen-clock findings that clustered in the last fifth of a 2.5-hour run
// and did not reproduce when their seeds ran alone. That pattern points at
// run-duration effects — thermal throttling, GC pressure, memory the process
// has accumulated — none of which an fsync probe can see. A finding that is
// really the host degrading must be attributable as such, exactly as a slow
// disk already is, rather than being reported as a defect in the system.
type CPU struct {
	// Compute is how long a FIXED amount of arithmetic took. It measures the
	// cycles this process is actually getting: thermal throttling and noisy
	// neighbours both show up here and nowhere else.
	Compute time.Duration
	// SchedLatency is the mean overshoot of a 1ms sleep over several
	// attempts — how late the runtime hands the goroutine back. GC pauses
	// and runnable-queue depth show up here.
	SchedLatency time.Duration
	// HeapMB and SysMB are what the process is holding, and NumGC how many
	// collections it has done. Growth across a long run is the memory
	// pressure the sleep overshoot would reflect.
	HeapMB float64
	SysMB  float64
	NumGC  uint32
}

func (c CPU) String() string {
	return fmt.Sprintf("compute %s, sched latency %s, heap %.0fMB, sys %.0fMB, %d GCs",
		c.Compute.Round(time.Microsecond), c.SchedLatency.Round(time.Microsecond),
		c.HeapMB, c.SysMB, c.NumGC)
}

// ProbeCPU runs the fixed workload and the scheduler measurement.
//
// Deliberately short — a few milliseconds — because it runs between every
// seed of a long run and a probe that perturbs what it measures is worse than
// no probe.
func ProbeCPU() CPU {
	var out CPU

	// A fixed arithmetic loop. Not a benchmark of anything real: the point is
	// that the WORK is constant, so the only variable is the host.
	start := time.Now()
	var acc uint64 = 1
	for i := 0; i < 4_000_000; i++ {
		acc = acc*6364136223846793005 + 1442695040888963407
		acc ^= acc >> 33
	}
	out.Compute = time.Since(start)
	// Keep the compiler from eliminating the loop.
	if acc == 0 {
		out.Compute = 0
	}

	const sleeps = 20
	const want = time.Millisecond
	var overshoot time.Duration
	for i := 0; i < sleeps; i++ {
		t0 := time.Now()
		time.Sleep(want)
		if d := time.Since(t0) - want; d > 0 {
			overshoot += d
		}
	}
	out.SchedLatency = overshoot / sleeps

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	out.HeapMB = float64(ms.HeapInuse) / (1 << 20)
	out.SysMB = float64(ms.Sys) / (1 << 20)
	out.NumGC = ms.NumGC
	return out
}
