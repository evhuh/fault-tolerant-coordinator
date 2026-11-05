package sim

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/hostio"
	"github.com/evazheng/fault-tolerant-coordinator/internal/raftnode"
	"github.com/evazheng/fault-tolerant-coordinator/internal/server"
)

// longRunConfig is the configuration proposed for the chaos checkpoint, and
// the one the calibration sweep measures at a tenth of the length.
//
// THE WORKLOAD HAS TO BE SUSTAINABLE, which the sweep configuration is not.
// At quickConfig's rates — 3 tasks per step, 60ms steps, a third of them
// sleeping six leases — the run generates about 16.7 slow tasks a second
// against six worker slots that can retire roughly 3.3. The queue grows
// without bound, which is why 40-step runs needed ten to twenty seconds of
// draining. At 150ms steps with two tasks per job and a fifth of them slow,
// generation is about 2.7 a second against the same capacity, and a measured
// 150-step run drained in 4s of a 36s budget with nothing outstanding.
func longRunConfig(seed int64, steps int) Config {
	cfg := DefaultConfig()
	cfg.Seed = seed
	cfg.Steps = steps
	// The shipped step time, not the sweeps' compressed one.
	cfg.StepTime = 150 * time.Millisecond
	cfg.TasksPerJob = 2
	cfg.SlowTaskFraction = 0.2
	// MORE SLOTS, measured rather than assumed, after the recorded clients
	// started executing for real.
	//
	// Fixing them routed roughly 1,300 tasks a run through fork/exec instead
	// of failing to start, which changed the load: measured at 120 steps,
	// 24-31 tasks/sec are now generated. The fast ones are not the
	// constraint (a /usr/bin/true attempt is ~10ms, so six slots retire
	// hundreds a second); the SLOW ones are, because each holds a slot for
	// six leases. At two tasks per job and a fifth of them slow that is
	// 2.67 slow tasks/sec against 3.33 that six slots can retire — 80%
	// utilization, which is too tight for a 600-step run where any
	// slowdown compounds.
	//
	// Three slots per worker gives nine, so 5.0/sec of slow capacity against
	// the same 2.67 generated: 53%. The alternative was cutting
	// SlowTaskFraction, which would have bought headroom by blunting the
	// ledger's only instrument for overlap.
	cfg.WorkerSlots = 3
	cfg.RealWorkers = true
	cfg.PauseWorker = true
	cfg.ReadMode = server.ReadIndexBarrier
	// The shipped liveness configuration: CheckQuorum and PreVote on, which
	// is what the checkpoint is a claim about.
	cfg.LinClients = 3
	// A long history needs a cap that does not truncate it, and partitioning
	// is what makes checking one affordable. Reported either way: a verdict
	// over a prefix is not a verdict over the run.
	cfg.LinMaxOps = 60000
	cfg.LinCheckTimeout = 120 * time.Second
	cfg.Faults = DefaultConfig().Faults
	return cfg
}

// THE CALIBRATION SWEEP for the long chaos run.
//
// It exists to answer three questions that cannot be answered by
// extrapolating from the 40-step sweeps, and to answer them BEFORE hours are
// spent:
//
//  1. Does porcupine finish? The search is exponential in overlap, so a
//     6,000-step history is not a longer 600-step one — it is a search that
//     reports Unknown, and the headline property then comes back
//     INCONCLUSIVE exactly when the run got interesting. Check time is
//     reported per run so it can be projected.
//  2. Does the snapshot cost climb toward the stall threshold? It grows with
//     state and blocks the Ready loop for all of it.
//  3. What are the overlap and stale-report aggregates at this width, so a
//     FLOOR can be calibrated rather than guessed?
//
// The disk is probed before and after every seed, because a result from a
// run whose host degraded halfway through is not a result.
func TestLongRunCalibration(t *testing.T) {
	requireSweeps(t)
	steps := 600
	if v := os.Getenv("FTC_CALIBRATION_STEPS"); v != "" {
		if _, err := fmt.Sscanf(v, "%d", &steps); err != nil {
			t.Fatalf("FTC_CALIBRATION_STEPS=%q: %v", v, err)
		}
	}

	var (
		totalOverlaps, totalStale, totalEffects, totalExecs int
		overlapPerRun, stalePerRun                          []int
		checkTimes                                          []time.Duration
		worstSnapshot                                       time.Duration
		drained, quiesced, runs                             int
		faults                                              FaultCounts
		acquisitions                                        []server.LeadershipChange
	)

	for seed := int64(1); seed <= 8; seed++ {
		before, err := hostio.Fsync(os.TempDir(), 40)
		if err != nil {
			t.Fatalf("fsync probe: %v", err)
		}

		cfg := longRunConfig(seed, steps)
		cfg.WorkerDir = t.TempDir()
		start := time.Now()
		res, runErr := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
		wall := time.Since(start)

		after, err := hostio.Fsync(os.TempDir(), 40)
		if err != nil {
			t.Fatalf("fsync probe: %v", err)
		}
		runs++

		l := res.Ledger
		t.Logf("seed %d: wall=%s steps=%d | jobs=%d tasks=%d | effects=%d executions=%d overlaps=%d stale=%d dup=%d budget-failed=%d",
			seed, wall.Round(time.Second), res.Steps, res.Jobs, res.Tasks,
			l.CommittedEffects, l.Executions, l.Overlaps, l.StaleRejected,
			l.DuplicateIgnored, l.NoEffectByBudget)
		t.Logf("seed %d: lin verdict=%s ops=%d ambiguous=%d (%.0f%%) covered=%v CHECK TIME=%s capped=%v",
			seed, string(res.Lin.Verdict), res.Lin.Ops, res.Lin.Ambiguous,
			100*res.Lin.AmbiguousShare(), res.Lin.Covered, res.Lin.Elapsed.Round(time.Millisecond),
			res.Lin.Ops >= cfg.LinMaxOps)
		t.Logf("seed %d: snapshots=%s", seed, describeSnapshots(res.Snapshots))
		t.Logf("seed %d: leadership GAINED (Ready-loop cost vs state): %s",
			seed, describeTransitions(res.LeadershipChanges, true))
		t.Logf("seed %d: leadership LOST    (Ready-loop cost vs state): %s",
			seed, describeTransitions(res.LeadershipChanges, false))
		acquisitions = append(acquisitions, res.LeadershipChanges...)
		// QUIESCED IS THE COLUMN THAT RECONCILES drained. A run that ended
		// early never reaches the heal-and-drain phase, so drained=false
		// there means "not asked", not "did not finish" — and without this
		// column a sweep reporting 6 of 8 drained looks like two failures
		// when it is two runs that stopped before the question.
		t.Logf("seed %d: quiesced=%v drained=%v undrained=%d in %s of %s | stalls host-io=%d unattributable=%d",
			seed, l.Quiesced, l.Drained, l.Undrained, l.DrainWaited.Round(time.Second), l.DrainBound,
			res.HostIOStalls, res.DowngradedStalls)
		t.Logf("seed %d: fsync before %s | after %s", seed, before, after)
		t.Logf("seed %d: faults kills=%d restarts=%d partitions=%d leader-partitions=%d pauses=%d heals=%d | outcome: %v",
			seed, res.Faults.NodeKills, res.Faults.NodeRestarts, res.Faults.Partitions,
			res.Faults.LeaderPartitions, res.Faults.Pauses, res.Faults.Heals, runErr)

		for _, v := range res.Violations {
			if !v.Inconclusive {
				t.Errorf("seed %d: %v", seed, v)
			}
		}

		totalOverlaps += l.Overlaps
		totalStale += l.StaleRejected
		totalEffects += l.CommittedEffects
		totalExecs += l.Executions
		overlapPerRun = append(overlapPerRun, l.Overlaps)
		stalePerRun = append(stalePerRun, l.StaleRejected)
		checkTimes = append(checkTimes, res.Lin.Elapsed)
		if w := worstSnapshotDuration(res.Snapshots); w > worstSnapshot {
			worstSnapshot = w
		}
		if l.Quiesced {
			quiesced++
		}
		if l.Drained {
			drained++
		}
		faults.NodeKills += res.Faults.NodeKills
		faults.NodeRestarts += res.Faults.NodeRestarts
		faults.Partitions += res.Faults.Partitions
		faults.LeaderPartitions += res.Faults.LeaderPartitions
		faults.Pauses += res.Faults.Pauses
		faults.Heals += res.Faults.Heals
	}

	sort.Slice(checkTimes, func(i, j int) bool { return checkTimes[i] < checkTimes[j] })
	t.Logf("AGGREGATE over %d runs at %d steps: overlaps=%d stale=%d effects=%d executions=%d",
		runs, steps, totalOverlaps, totalStale, totalEffects, totalExecs)
	// Drained is reported against the runs that REACHED quiescence, with the
	// early exits named separately, because dividing by the wrong
	// denominator is how "6 of 8 drained" reads as two liveness failures.
	t.Logf("DRAIN: %d of %d runs that reached quiescence drained; %d run(s) ended before it",
		drained, quiesced, runs-quiesced)
	t.Logf("DISTRIBUTION overlaps per run: %v", overlapPerRun)
	t.Logf("DISTRIBUTION stale per run: %v", stalePerRun)
	t.Logf("CHECK TIME median=%s worst=%s — projection to 6000 steps assumes the partitioned "+
		"search stays near-linear in operations; verify on the first long run",
		checkTimes[len(checkTimes)/2].Round(time.Millisecond),
		checkTimes[len(checkTimes)-1].Round(time.Millisecond))
	t.Logf("WORST SNAPSHOT across the sweep: %s (stall threshold is half the election timeout, %s)",
		worstSnapshot.Round(time.Millisecond),
		time.Duration(DefaultConfig().ElectionTick)*DefaultConfig().TickInterval/2)
	t.Logf("FAULTS induced in total: %+v", faults)
	t.Logf("LEADERSHIP GAINED, cost vs state across the sweep:\n%s", transitionTable(acquisitions, true))
	t.Logf("LEADERSHIP LOST, cost vs state across the sweep:\n%s", transitionTable(acquisitions, false))

	if totalExecs == 0 {
		t.Fatal("no attempt executed; the calibration measured nothing")
	}
}

// describeSnapshots renders the trend: first, median and last cost with
// sizes, which is what shows a climb.
func describeSnapshots(ss []raftnode.SnapshotStat) string {
	if len(ss) == 0 {
		return "none taken"
	}
	byDur := append([]raftnode.SnapshotStat(nil), ss...)
	sort.Slice(byDur, func(i, j int) bool { return byDur[i].Duration < byDur[j].Duration })
	return fmt.Sprintf("n=%d first=%s/%dB median=%s/%dB worst=%s/%dB last=%s/%dB",
		len(ss),
		ss[0].Duration.Round(time.Millisecond), ss[0].Bytes,
		byDur[len(byDur)/2].Duration.Round(time.Millisecond), byDur[len(byDur)/2].Bytes,
		byDur[len(byDur)-1].Duration.Round(time.Millisecond), byDur[len(byDur)-1].Bytes,
		ss[len(ss)-1].Duration.Round(time.Millisecond), ss[len(ss)-1].Bytes)
}

func worstSnapshotDuration(ss []raftnode.SnapshotStat) time.Duration {
	var worst time.Duration
	for _, s := range ss {
		if s.Duration > worst {
			worst = s.Duration
		}
	}
	return worst
}

// describeTransitions renders the typical and worst cost of a leadership
// transition, SPLIT BY DIRECTION.
//
// The split is the whole measurement. Acquisition runs StartScheduling, whose
// cost was assumed to be the eval-broker rebuild and measured at 1-4ms at
// 1,400 jobs — the rebuild's input is a 0.5ms deep copy, so it cannot be the
// 131-187ms stalls seen in this phase. Revocation runs StopScheduling, which
// WAITS for the scheduler workers, and a worker blocked inside node.Propose
// is waiting on the Ready loop itself. Printing one number for both hid which
// side was expensive.
func describeTransitions(cs []server.LeadershipChange, isLeader bool) string {
	var sel []server.LeadershipChange
	for _, c := range cs {
		if c.IsLeader == isLeader {
			sel = append(sel, c)
		}
	}
	if len(sel) == 0 {
		return "none"
	}
	byDur := append([]server.LeadershipChange(nil), sel...)
	sort.Slice(byDur, func(i, j int) bool { return byDur[i].Duration < byDur[j].Duration })
	return fmt.Sprintf("n=%d median=%s@%d jobs worst=%s@%d jobs",
		len(sel),
		byDur[len(byDur)/2].Duration.Round(time.Millisecond), byDur[len(byDur)/2].Jobs,
		byDur[len(byDur)-1].Duration.Round(time.Millisecond), byDur[len(byDur)-1].Jobs)
}

// acquisitionTable buckets acquisitions by job count, which is the only form
// that answers "does it scale".
//
// A median per bucket rather than a mean: one acquisition that landed behind
// an fsync should not decide the shape of the curve.
func transitionTable(cs []server.LeadershipChange, isLeader bool) string {
	buckets := map[int][]time.Duration{}
	for _, c := range cs {
		if c.IsLeader != isLeader {
			continue
		}
		b := c.Jobs / 250 * 250
		buckets[b] = append(buckets[b], c.Duration)
	}
	keys := make([]int, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	var b strings.Builder
	fmt.Fprintf(&b, "      jobs      n    median      worst\n")
	for _, k := range keys {
		ds := buckets[k]
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		fmt.Fprintf(&b, "  %4d-%4d  %5d  %8s  %9s\n", k, k+249, len(ds),
			ds[len(ds)/2].Round(time.Millisecond), ds[len(ds)-1].Round(time.Millisecond))
	}
	return b.String()
}
