package sim

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/evazheng/fault-tolerant-coordinator/internal/hostio"
)

// THE DAY 10 CHECKPOINT: hours of seeded chaos, with the ledger watching.
//
// BREADTH, NOT DEPTH, and that is a measured decision rather than a
// preference. A single multi-hour run is not available: maybeSnapshot
// serializes, fsyncs and compacts inside the Ready loop, Encode is linear at
// about 6.5ms per MB, and a 6,000-step run projects to 10-13MB of state —
// 65-85ms of serialization plus fsync against a 125ms stall threshold. Such a
// run would start truncating on host-io stalls exactly where it got
// interesting. Decompressing the clock to buy headroom is not available
// either: an election must resolve inside a lease, and scaling the whole
// timing set together voids the worker-slot arithmetic the overlap floor is
// calibrated against. So the hours are spent across many seeds at the
// calibrated 600-step width.
//
// EVERY SEED'S ROW IS WRITTEN AS IT COMPLETES, to FTC_CHECKPOINT_OUT, and the
// report is built from that file rather than from memory. Three hours is long
// enough that something will interrupt it, and a run that has to start over
// from zero is a run nobody finishes. A restart skips the seeds already in
// the file.
//
// The floors are calibrated, not chosen: two 8-seed sweeps at this exact
// configuration measured 41 and 41 overlaps and 62 and 51 stale rejections,
// which is 0.85 overlaps and 1.17 stale per 100 run-steps. Scaled to 64 seeds
// that projects to about 328 and 452; the floors sit near half of that.
func TestDay10Checkpoint(t *testing.T) {
	if os.Getenv("FTC_CHECKPOINT") != "1" {
		t.Skip("the Day 10 checkpoint: set FTC_CHECKPOINT=1 (hours, and it must have a quiet host)")
	}
	out := os.Getenv("FTC_CHECKPOINT_OUT")
	if out == "" {
		t.Fatal("FTC_CHECKPOINT_OUT must name the file the per-seed rows are appended to")
	}
	// A RANGE, not just a count, so one seed can be re-run on its own. The
	// checkpoint's findings land on particular seeds, and re-running 1..59
	// to look at 59 costs two hours for no information.
	firstSeed := envInt(t, "FTC_CHECKPOINT_SEED_FROM", 1)
	seeds := envInt(t, "FTC_CHECKPOINT_SEEDS", 64)
	steps := envInt(t, "FTC_CHECKPOINT_STEPS", 600)
	minOverlaps := envInt(t, "FTC_CHECKPOINT_MIN_OVERLAPS", 160)
	minStale := envInt(t, "FTC_CHECKPOINT_MIN_STALE", 200)

	// THE HOST GATE, before anything is claimed. A disk that cannot sustain
	// the run's compressed timings produces runs that truncate and
	// checkpoints that skip, both of which exit 0.
	probe, err := hostio.Fsync(os.TempDir(), 40)
	if err != nil {
		t.Fatalf("fsync probe: %v", err)
	}
	if bad, why := probe.Exceeds(12*time.Millisecond, 100*time.Millisecond); bad {
		t.Fatalf("host gate: %s (%s) — wait for the machine to go quiet; a checkpoint measured "+
			"on this disk would describe the disk", why, probe)
	}
	t.Logf("host gate passed: %s", probe)

	runStart := time.Now()
	done, err := readRows(out)
	if err != nil {
		t.Fatalf("reading %s: %v", out, err)
	}
	if len(done) > 0 {
		t.Logf("resuming: %d seed(s) already recorded in %s", len(done), out)
	}

	for seed := int64(firstSeed); seed <= int64(seeds); seed++ {
		if _, ok := done[seed]; ok {
			continue
		}
		row := runCheckpointSeed(t, seed, steps, runStart, filepath.Dir(out))
		if err := appendRow(out, row); err != nil {
			t.Fatalf("appending seed %d: %v", seed, err)
		}
		done[seed] = row
		t.Logf("seed %d/%d: %s", seed, seeds, row.oneLine())
	}

	reportCheckpoint(t, done, steps, minOverlaps, minStale)
}

// checkpointRow is one seed's result, as written to the file.
type checkpointRow struct {
	Seed       int64 `json:"seed"`
	Steps      int   `json:"steps"`
	StepsRun   int   `json:"steps_run"`
	WallMs     int64 `json:"wall_ms"`
	Jobs       int   `json:"jobs"`
	Tasks      int   `json:"tasks"`
	Effects    int   `json:"effects"`
	Executions int   `json:"executions"`
	Overlaps   int   `json:"overlaps"`
	Stale      int   `json:"stale"`
	Duplicates int   `json:"duplicates"`
	ByBudget   int   `json:"no_effect_by_budget"`
	Drained    bool  `json:"drained"`
	Quiesced   bool  `json:"quiesced"`
	Undrained  int   `json:"undrained"`

	LinVerdict   string `json:"lin_verdict"`
	LinOps       int    `json:"lin_ops"`
	LinAmbiguous int    `json:"lin_ambiguous"`
	LinCovered   bool   `json:"lin_covered"`
	LinCheckMs   int64  `json:"lin_check_ms"`

	HostIOStalls      int   `json:"host_io_stalls"`
	Unattributable    int   `json:"unattributable_stalls"`
	WorstSnapshotMs   int64 `json:"worst_snapshot_ms"`
	WorstSnapshotKB   int   `json:"worst_snapshot_kb"`
	FsyncBeforeMeanUs int64 `json:"fsync_before_mean_us"`
	FsyncAfterMeanUs  int64 `json:"fsync_after_mean_us"`
	FsyncAfterWorstUs int64 `json:"fsync_after_worst_us"`

	// THE HOST, NOT JUST ITS DISK. The fsync probe cannot see thermal
	// throttling, GC pressure or a process that has grown, and Day 10's
	// frozen-clock findings clustered in the last fifth of a 2.5-hour run
	// while not reproducing per-seed — which is what run-duration effects
	// look like. StartedAtMs gives position in the run, so a correlation can
	// be measured rather than guessed at.
	StartedAtMs    int64   `json:"started_at_ms"`
	ElapsedInRunMs int64   `json:"elapsed_in_run_ms"`
	CPUBeforeUs    int64   `json:"cpu_before_us"`
	CPUAfterUs     int64   `json:"cpu_after_us"`
	SchedBeforeUs  int64   `json:"sched_before_us"`
	SchedAfterUs   int64   `json:"sched_after_us"`
	HeapMB         float64 `json:"heap_mb"`
	SysMB          float64 `json:"sys_mb"`
	NumGC          uint32  `json:"num_gc"`
	FaultLogPath   string  `json:"fault_log_path,omitempty"`

	Kills            int `json:"kills"`
	Restarts         int `json:"restarts"`
	Partitions       int `json:"partitions"`
	LeaderPartitions int `json:"leader_partitions"`
	Pauses           int `json:"pauses"`
	Heals            int `json:"heals"`

	// Violations holds every finding, so a partial file is still a complete
	// account of what was found.
	Violations []string `json:"violations,omitempty"`
	// FrozenClock is the self-reported timeline of a frozen-clock window,
	// present only on seeds where that invariant fired. In the row file so
	// the diagnosis survives the run.
	FrozenClock []string `json:"frozen_clock,omitempty"`
}

func (r checkpointRow) oneLine() string {
	return fmt.Sprintf("steps=%d/%d wall=%s effects=%d execs=%d overlaps=%d stale=%d drained=%v lin=%s(covered=%v) stalls=%d/%d violations=%d",
		r.StepsRun, r.Steps, time.Duration(r.WallMs)*time.Millisecond, r.Effects, r.Executions,
		r.Overlaps, r.Stale, r.Drained, r.LinVerdict, r.LinCovered,
		r.HostIOStalls, r.Unattributable, len(r.Violations))
}

func runCheckpointSeed(t *testing.T, seed int64, steps int, runStart time.Time, outDir string) checkpointRow {
	t.Helper()
	before, err := hostio.Fsync(os.TempDir(), 40)
	if err != nil {
		t.Fatalf("seed %d: fsync probe: %v", seed, err)
	}
	cpuBefore := hostio.ProbeCPU()

	cfg := longRunConfig(seed, steps)
	cfg.WorkerDir = t.TempDir()

	// THE FAULT LOG IS PERMANENT FOR A CHECKPOINT, never io.Discard.
	//
	// The first 64-seed run sent it nowhere, so when four seeds failed the
	// schedule that produced them was gone: the diagnosis could not say
	// which injected fault explained any leadership transition without
	// re-running, and the finding does not reproduce per-seed. A checkpoint
	// failure has to be diagnosable from what the checkpoint already wrote.
	faultLogPath := filepath.Join(outDir, fmt.Sprintf("faults-seed-%03d.log", seed))
	faultLog, err := os.Create(faultLogPath)
	if err != nil {
		t.Fatalf("seed %d: fault log: %v", seed, err)
	}
	defer faultLog.Close()
	buffered := bufio.NewWriterSize(faultLog, 64<<10)
	defer buffered.Flush()
	cfg.Out = buffered

	start := time.Now()
	res, runErr := New(cfg, dirsFor(t, cfg.Nodes)).Run(context.Background())
	wall := time.Since(start)
	_ = runErr // every finding is recorded below; the error is the first of them

	after, err := hostio.Fsync(os.TempDir(), 40)
	if err != nil {
		t.Fatalf("seed %d: fsync probe: %v", seed, err)
	}
	cpuAfter := hostio.ProbeCPU()

	l := res.Ledger
	row := checkpointRow{
		Seed: seed, Steps: steps, StepsRun: res.Steps, WallMs: wall.Milliseconds(),
		Jobs: res.Jobs, Tasks: res.Tasks,
		Effects: l.CommittedEffects, Executions: l.Executions,
		Overlaps: l.Overlaps, Stale: l.StaleRejected, Duplicates: l.DuplicateIgnored,
		ByBudget: l.NoEffectByBudget,
		Drained:  l.Drained, Quiesced: l.Quiesced, Undrained: l.Undrained,

		LinVerdict: string(res.Lin.Verdict), LinOps: res.Lin.Ops,
		LinAmbiguous: res.Lin.Ambiguous, LinCovered: res.Lin.Covered,
		LinCheckMs: res.Lin.Elapsed.Milliseconds(),

		HostIOStalls: res.HostIOStalls, Unattributable: res.DowngradedStalls,
		FsyncBeforeMeanUs: before.Mean.Microseconds(),
		FsyncAfterMeanUs:  after.Mean.Microseconds(),
		FsyncAfterWorstUs: after.Worst.Microseconds(),

		Kills: res.Faults.NodeKills, Restarts: res.Faults.NodeRestarts,
		Partitions: res.Faults.Partitions, LeaderPartitions: res.Faults.LeaderPartitions,
		Pauses: res.Faults.Pauses, Heals: res.Faults.Heals,

		StartedAtMs:    start.UnixMilli(),
		ElapsedInRunMs: start.Sub(runStart).Milliseconds(),
		CPUBeforeUs:    cpuBefore.Compute.Microseconds(),
		CPUAfterUs:     cpuAfter.Compute.Microseconds(),
		SchedBeforeUs:  cpuBefore.SchedLatency.Microseconds(),
		SchedAfterUs:   cpuAfter.SchedLatency.Microseconds(),
		HeapMB:         cpuAfter.HeapMB,
		SysMB:          cpuAfter.SysMB,
		NumGC:          cpuAfter.NumGC,
		FaultLogPath:   faultLogPath,
	}
	for _, s := range res.Snapshots {
		if s.Duration.Milliseconds() > row.WorstSnapshotMs {
			row.WorstSnapshotMs = s.Duration.Milliseconds()
		}
		if kb := s.Bytes / 1024; kb > row.WorstSnapshotKB {
			row.WorstSnapshotKB = kb
		}
	}
	for _, v := range res.Violations {
		row.Violations = append(row.Violations, v.Error())
	}
	row.FrozenClock = res.FrozenClock
	return row
}

// reportCheckpoint builds the checkpoint from the FILE, so a resumed run reports on
// everything that was ever measured rather than on this process's share.
func reportCheckpoint(t *testing.T, rows map[int64]checkpointRow, steps, minOverlaps, minStale int) {
	t.Helper()
	seeds := make([]int64, 0, len(rows))
	for s := range rows {
		seeds = append(seeds, s)
	}
	sort.Slice(seeds, func(i, j int) bool { return seeds[i] < seeds[j] })

	var (
		effects, execs, overlaps, stale, dups, byBudget      int
		kills, restarts, parts, leaderParts, pauses, heals   int
		drained, quiesced, complete, covered, illegal        int
		hostIO, unattributable                               int
		worstSnapMs, worstSnapKB, worstCheckMs, worstFsyncUs int64
		ambiguous, linOps                                    int
		wallMs                                               int64
		findings                                             []string
		overlapPerRun, stalePerRun                           []int
	)
	for _, s := range seeds {
		r := rows[s]
		effects += r.Effects
		execs += r.Executions
		overlaps += r.Overlaps
		stale += r.Stale
		dups += r.Duplicates
		byBudget += r.ByBudget
		kills += r.Kills
		restarts += r.Restarts
		parts += r.Partitions
		leaderParts += r.LeaderPartitions
		pauses += r.Pauses
		heals += r.Heals
		hostIO += r.HostIOStalls
		unattributable += r.Unattributable
		ambiguous += r.LinAmbiguous
		linOps += r.LinOps
		wallMs += r.WallMs
		overlapPerRun = append(overlapPerRun, r.Overlaps)
		stalePerRun = append(stalePerRun, r.Stale)
		if r.Drained {
			drained++
		}
		if r.Quiesced {
			quiesced++
		}
		if r.StepsRun >= r.Steps {
			complete++
		}
		if r.LinCovered {
			covered++
		}
		if r.LinVerdict == "Illegal" {
			illegal++
		}
		if r.WorstSnapshotMs > worstSnapMs {
			worstSnapMs = r.WorstSnapshotMs
		}
		if int64(r.WorstSnapshotKB) > worstSnapKB {
			worstSnapKB = int64(r.WorstSnapshotKB)
		}
		if r.LinCheckMs > worstCheckMs {
			worstCheckMs = r.LinCheckMs
		}
		if r.FsyncAfterWorstUs > worstFsyncUs {
			worstFsyncUs = r.FsyncAfterWorstUs
		}
		for _, v := range r.Violations {
			findings = append(findings, fmt.Sprintf("seed %d: %s", s, v))
		}
	}

	n := len(seeds)
	t.Logf("=== DAY 10 CHECKPOINT: %d seeds x %d steps, %s of cumulative run time ===",
		n, steps, (time.Duration(wallMs) * time.Millisecond).Round(time.Second))
	t.Logf("FAULTS INDUCED: %d kills, %d restarts, %d partitions, %d leader partitions, %d pauses, %d heals",
		kills, restarts, parts, leaderParts, pauses, heals)
	t.Logf("EFFECTS: %d committed over %d executions (%d duplicate reports ignored, %d terminal by attempt budget)",
		effects, execs, dups, byBudget)
	t.Logf("LEDGER COVERAGE: %d overlapping executions (floor %d), %d late reports rejected as stale (floor %d)",
		overlaps, minOverlaps, stale, minStale)
	t.Logf("  overlaps per run: %v", overlapPerRun)
	t.Logf("  stale per run:    %v", stalePerRun)
	t.Logf("DRAIN: %d of %d runs reached quiescence, %d of those drained; %d run(s) completed all %d steps",
		quiesced, n, drained, complete, steps)
	t.Logf("STALLS: %d host-io, %d unattributable%s", hostIO, unattributable,
		quotableNote(hostIO, unattributable))
	t.Logf("SNAPSHOTS: worst %dms at %dKB (stall threshold 125ms)", worstSnapMs, worstSnapKB)
	t.Logf("LINEARIZABILITY: %d histories, %d Illegal, %d ops checked, %d ambiguous (%.0f%%), worst check %dms",
		n, illegal, linOps, ambiguous, 100*float64(ambiguous)/float64(max(linOps, 1)), worstCheckMs)
	t.Logf("  COVERED: %d of %d. The shipped configuration closes the split-brain read window by "+
		"construction — CheckQuorum steps a cut-off leader down in about one election timeout — so "+
		"these histories could not have exposed a stale read. Zero Illegal here is NOT linearizability "+
		"evidence; that evidence is the targeted sweeps, which are covered 8/8 and report 8/8 Illegal "+
		"under local reads against 0/8 under the ReadIndex barrier.", covered, n)
	t.Logf("RACE DETECTOR: deliberately NOT run here. Race instrumentation slows the apply path enough " +
		"that this run's compressed timings stop holding — measured as 8 to 31 second Ready-loop stalls " +
		"— so -race coverage lives in `make test` and `make repro` instead.")
	t.Logf("FSYNC: worst bracketing probe %dus (budget 100ms)", worstFsyncUs)
	t.Logf("HOST OVER THE RUN (does position in the run predict trouble?):\n%s",
		hostDriftTable(rows, seeds))

	for _, f := range findings {
		t.Logf("FINDING: %s", f)
	}
	for _, s := range seeds {
		for _, line := range rows[s].FrozenClock {
			t.Logf("seed %d | %s", s, line)
		}
	}

	// The assertions.
	for _, s := range seeds {
		for _, v := range rows[s].Violations {
			// Inconclusive findings are reported above and do not fail: the
			// linearizability-coverage one is expected on every seed here.
			if isConclusiveViolation(v) {
				t.Errorf("seed %d: %s", s, v)
			}
		}
	}
	if overlaps < minOverlaps {
		t.Errorf("overlapping executions %d below the floor of %d — the idempotency was exercised "+
			"less than the calibration measured, so effectively-once is weaker evidence here than "+
			"the number of seeds suggests", overlaps, minOverlaps)
	}
	if stale < minStale {
		t.Errorf("late reports rejected as stale %d below the floor of %d — the (taskID, attempt) "+
			"rejection ran less than the calibration measured", stale, minStale)
	}
	if effects > execs {
		t.Errorf("committed effects %d exceed executions %d, which is impossible: an effect is an "+
			"accepted terminal report for an attempt that ran", effects, execs)
	}
}

// isConclusiveViolation reports whether a recorded finding is a defect rather
// than a run that could not look.
func isConclusiveViolation(v string) bool {
	return len(v) >= len("VIOLATION") && v[:len("VIOLATION")] == "VIOLATION"
}

func appendRow(path string, row checkpointRow) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(row)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	// Durable before the next seed starts: the point of the file is that an
	// interrupted run keeps what it measured.
	return f.Sync()
}

func readRows(path string) (map[int64]checkpointRow, error) {
	out := map[int64]checkpointRow{}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var r checkpointRow
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("malformed row %q: %w", sc.Text(), err)
		}
		out[r.Seed] = r
	}
	return out, sc.Err()
}

func envInt(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		t.Fatalf("%s=%q: %v", name, v, err)
	}
	return n
}

// hostDriftTable shows the host probes against position in the run, in
// quarters, with the findings counted per quarter.
//
// It exists because Day 10's four frozen-clock findings landed at seeds 23,
// 50, 58 and 59 — three of them in the last fifth — and did not reproduce
// when those seeds ran alone. That is the shape of a host degrading over
// hours rather than a schedule finding a bug, and the two have opposite
// conclusions: one is attributable to the environment like a slow disk, the
// other is a defect. Position in the run is the discriminator, so it is
// measured.
func hostDriftTable(rows map[int64]checkpointRow, seeds []int64) string {
	if len(seeds) == 0 {
		return "  (no rows)"
	}
	type bucket struct {
		n        int
		compute  int64
		sched    int64
		heap     float64
		findings int
	}
	var qs [4]bucket
	for i, s := range seeds {
		q := i * 4 / len(seeds)
		if q > 3 {
			q = 3
		}
		r := rows[s]
		qs[q].n++
		qs[q].compute += r.CPUAfterUs
		qs[q].sched += r.SchedAfterUs
		qs[q].heap += r.HeapMB
		for _, v := range r.Violations {
			if isConclusiveViolation(v) {
				qs[q].findings++
			}
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "  quarter   seeds   compute(us)   sched(us)   heap(MB)   conclusive findings\n")
	for i, q := range qs {
		if q.n == 0 {
			continue
		}
		fmt.Fprintf(&b, "  %7d   %5d   %11d   %9d   %8.0f   %d\n",
			i+1, q.n, q.compute/int64(q.n), q.sched/int64(q.n), q.heap/float64(q.n), q.findings)
	}
	return b.String()
}
