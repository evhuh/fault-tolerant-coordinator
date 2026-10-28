package sim

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"os"
	"time"

	schedulerpb "github.com/evazheng/fault-tolerant-coordinator/proto/schedulerpb"
)

// The workload: enough traffic that faults land on a system that is actually
// doing something. A cluster attacked while idle proves very little.
//
// Every call here is BEST EFFORT. Mid-partition there may be no leader, and
// an RPC failing is the expected outcome rather than a test failure — the
// invariants, not the RPCs, are what decide whether the run passed.

func (s *Sim) registerWorkers() {
	lead := s.leader()
	if lead == nil {
		return
	}
	for i := 1; i <= s.cfg.WorkerCount; i++ {
		_, _ = lead.svc.RegisterWorker(context.Background(), &schedulerpb.RegisterWorkerRequest{
			WorkerId: fmt.Sprintf("w-%d", i),
			Capacity: s.cfg.WorkerSlots,
		})
	}
}

// offerWork submits jobs and renews worker leases.
//
// The heartbeats matter as much as the submissions: without them every
// worker's lease lapses, every task is reclaimed, and the run degenerates
// into testing lease expiry alone.
func (s *Sim) offerWork(step int) {
	lead := s.leader()
	if lead == nil {
		return // no leader right now; that is a normal state mid-chaos
	}

	// Real workers heartbeat for themselves, on their own frozen-or-not
	// clocks. Renewing their leases from here would hide exactly the lease
	// expiry a paused worker is supposed to suffer.
	if !s.cfg.RealWorkers {
		for i := 1; i <= s.cfg.WorkerCount; i++ {
			_, _ = lead.svc.Heartbeat(context.Background(), &schedulerpb.HeartbeatRequest{
				WorkerId: fmt.Sprintf("w-%d", i),
			})
		}
	}

	for j := 0; j < s.cfg.JobsPerStep; j++ {
		specs := make([]*schedulerpb.TaskSpec, 0, s.cfg.TasksPerJob)
		for t := 0; t < s.cfg.TasksPerJob; t++ {
			specs = append(specs, s.specFor(step, j, t))
		}
		// A stable request id per (step, index) so a retry across a failover
		// dedups rather than creating a second job — which is also what makes
		// the job count predictable enough to assert on.
		_, _ = lead.svc.SubmitJob(context.Background(), &schedulerpb.SubmitJobRequest{
			ClientId:  "sim",
			RequestId: fmt.Sprintf("step-%d-%d", step, j),
			Tasks:     specs,
		})
	}
}

// drainSomeWork plays the worker pool: poll, report success, repeat.
//
// Deliberately partial — it drains a bounded number per step, so there is
// always work in flight when the next fault lands. A fault that only ever
// hits an idle cluster is a fault that tests nothing.
func (s *Sim) drainSomeWork() {
	lead := s.leader()
	if lead == nil {
		return
	}
	const maxPerStep = 12
	for i := 0; i < maxPerStep; i++ {
		progressed := false
		for w := 1; w <= s.cfg.WorkerCount; w++ {
			workerID := fmt.Sprintf("w-%d", w)
			resp, err := lead.svc.RequestTask(context.Background(), &schedulerpb.RequestTaskRequest{
				WorkerId: workerID,
			})
			if err != nil || resp.GetTask() == nil {
				continue
			}
			task := resp.GetTask()
			// The worker claims it, which is what promotes ASSIGNED→RUNNING,
			// then reports. Both can fail mid-chaos; neither is an error.
			_, _ = lead.svc.Heartbeat(context.Background(), &schedulerpb.HeartbeatRequest{
				WorkerId: workerID, RunningTaskIds: []string{task.GetTaskId()},
			})
			_, _ = lead.svc.ReportResult(context.Background(), s.outcomeFor(task, workerID))
			progressed = true
		}
		if !progressed {
			return
		}
	}
}

// specFor builds one task's spec.
//
// With the simulated pool it is a command string nobody runs. With real
// workers it is a SHELL SCRIPT that appends to a shared log, because the
// ledger needs to see the executions themselves:
//
//	START <taskID> <attempt>
//	END <taskID> <attempt>
//
// Two lines rather than one, and that is what makes OVERLAP visible at all. A
// single line per execution shows that two attempts ran; it cannot show they
// ran AT THE SAME TIME, which is what at-least-once permits and what
// idempotency exists to survive. With a start and an end, overlap is the
// log's own order: a START for a task that already has an open attempt.
//
// ORDER, NOT TIMESTAMPS, deliberately. BSD date has no %N, so a portable
// shell cannot stamp sub-second time, and a one-second resolution is useless
// against a 300ms lease. The log file is itself a serialization of the
// events, which is all overlap needs.
//
// The task learns its own identity from the environment the executor now
// passes, so the script is identical for every task and the log is keyed by
// what actually ran rather than by what the simulator believed it submitted.
//
// Appends are O_APPEND writes of well under PIPE_BUF from separate processes,
// which do not interleave on a local filesystem.
func (s *Sim) specFor(step, job, task int) *schedulerpb.TaskSpec {
	id := fmt.Sprintf("step-%d-%d-%d", step, job, task)
	// SOME TASKS ARE SLOW, and they have to be, or overlap stays at zero: a
	// task that finishes inside its lease is never reclaimed mid-execution,
	// so the only attempt that exists is the one that completed. A slow task
	// outlives its lease, is reclaimed, and keeps running — exactly the
	// at-least-once case the ledger exists to measure.
	//
	// Hashed, like every other workload decision, so the slow set is the same
	// on a replay and the fault RNG is untouched.
	slow := s.cfg.RealWorkers && s.hashUnit(id, 0, "slow") < s.cfg.SlowTaskFraction
	return s.wrapSpec(s.commandFor(step, job, task), slow, 5)
}

// linSpec is the spec a RECORDED CLIENT's submit carries.
//
// IT GOES THROUGH THE SAME BUILDER, and it did not before — which produced a
// measurement impossible on its face. The recorded clients submitted
// `{Command: "/bin/true", MaxAttempts: 1}` directly: a path that does not
// exist on this host, so every one of those tasks failed to START, was
// reported non-retryable, and became a COMMITTED EFFECT that never executed.
// They also bypassed the wrapper, so they wrote nothing to the payload log
// and were invisible to the execution count.
//
// With roughly 1,300 such jobs in a 600-step run against about 770 real
// executions, the calibration reported 10,585 effects over 5,022 executions —
// more effects than executions, which cannot happen when an effect is an
// accepted terminal report for an attempt that ran. Not a double count: two
// different populations compared as though they were one.
//
// Never slow, and one attempt. These exist to make job existence observable
// and a recorded client submits far more often than the workload does;
// letting a fifth of them sleep six leases would hand the ledger's instrument
// the job of flooding the worker slots.
func (s *Sim) linSpec() *schedulerpb.TaskSpec {
	return s.wrapSpec(s.cmdOK, false, 1)
}

// wrapSpec turns a command into a spec, wrapped in the payload log when real
// workers are running.
func (s *Sim) wrapSpec(cmd string, slow bool, maxAttempts int32) *schedulerpb.TaskSpec {
	spec := &schedulerpb.TaskSpec{Command: cmd, MaxAttempts: maxAttempts}
	if !s.cfg.RealWorkers || s.overlapLog == "" {
		return spec
	}
	if cmd == cmdCannotStart {
		// NOT WRAPPED, on purpose. This task's whole point is that the
		// command cannot be started, which is the executor's non-retryable
		// branch; wrapping it in a shell would make the shell start fine and
		// exit 127, turning a terminal failure into a retryable one and
		// taking that branch out of the run's coverage. It also has no
		// execution to log — nothing ever runs.
		return spec
	}

	sleep := ""
	if slow {
		// SIX LEASES, measured rather than guessed. 2.5 was the first value
		// and it produced a reclaim with no overlap: the lease takes a full
		// TTL to lapse after the worker freezes, the planner then has to
		// redispatch, and by the time the second attempt starts a 2.5-lease
		// sleep has long since written its END line. The window the first
		// attempt has to still be running spans "pause lands" plus "lease
		// lapses" plus "redispatch", so the sleep has to cover all three
		// with room, not just the lease.
		sleep = fmt.Sprintf("sleep %.3f; ", slowTaskLeases*s.cfg.LeaseTTL.Seconds())
	}

	// Every piece substituted here is a path or an env var name this process
	// chose; nothing from the cluster reaches the script.
	script := fmt.Sprintf(
		`printf 'START %%s %%s
' "$FTC_TASK_ID" "$FTC_ATTEMPT" >> %[1]s; `+
			`%[2]s%[3]s; rc=$?; `+
			`printf 'END %%s %%s
' "$FTC_TASK_ID" "$FTC_ATTEMPT" >> %[1]s; `+
			`exit $rc`,
		s.overlapLog, sleep, cmd)

	spec.Command = s.cmdShell
	spec.Args = []string{"-c", script}
	// The timeout must outlast the sleep, or the executor kills the task
	// before it can write its END line and every slow attempt looks like a
	// crash. Zero would mean the executor's own one-minute default.
	spec.TimeoutMs = int64(10*s.cfg.LeaseTTL/time.Millisecond) + 5000
	return spec
}

// commandFor picks the command a task will run.
//
// With the simulated worker pool the outcome is decided by hashing at REPORT
// time, which can vary by attempt — a retry of a failed task may succeed. A
// real worker has no such freedom: the outcome is whatever the command does,
// so the choice moves to SUBMIT time and a failing task fails on every
// attempt until it exhausts its budget.
//
// The two failure kinds have to be separate binaries because they are
// separate branches in the executor and the state machine. /bin/false RAN and
// returned nonzero — the program's own verdict, retryable, so the task goes
// back to PENDING with a backoff deadline. A command that does not exist
// never starts, which no number of attempts fixes, so it is terminal. A
// workload that only produced one of them would say nothing about the other.
//
// Hashed on (seed, step, job, task) and never drawn from the fault RNG, per
// the Day 8 rule: a timing-dependent number of draws would destroy schedule
// reproducibility.
func (s *Sim) commandFor(step, job, task int) string {
	if !s.cfg.RealWorkers || s.cfg.FailureRate <= 0 {
		return s.cmdOK
	}
	id := fmt.Sprintf("step-%d-%d-%d", step, job, task)
	if s.hashUnit(id, 0, "fail") >= s.cfg.FailureRate {
		return s.cmdOK
	}
	if s.hashUnit(id, 0, "kind") < s.cfg.RetryableFraction {
		return s.cmdFail
	}
	return cmdCannotStart
}

// validateCommands refuses to start a run whose tasks cannot be executed.
//
// /bin/true HAS NOW BITTEN TWICE, through two different spec builders, and
// both times it was silent in the same direction. The workload shipped it
// from Day 3 and nothing ran it until real workers arrived, at which point
// every task reported "fork/exec /bin/true: no such file or directory" —
// there is no /bin/true on this host, only /usr/bin/true. Then the recorded
// clients turned out to carry their own copy of the same literal, which made
// roughly 1,300 tasks per run fail to start while still being counted as
// committed effects, and produced a calibration with more effects than
// executions.
//
// Neither failure announced itself. A task that cannot start is reported as a
// non-retryable failure, which is a perfectly ordinary outcome the state
// machine handles correctly, so the run stays green and the workload quietly
// stops being a workload. The only defence is to check the commands against
// the host BEFORE the run, where "this cannot work here" is loud.
//
// cmdCannotStart is checked in the OPPOSITE direction, deliberately: the
// terminal-failure branch depends on it not existing, so a host that happens
// to have that path would silently take that coverage away.
func (s *Sim) validateCommands() error {
	if !s.cfg.RealWorkers {
		// Nothing executes the commands, so a bad path cannot mislead a run
		// — and demanding a shell on a host that only ever simulates workers
		// would refuse runs that are fine.
		return nil
	}
	for _, c := range []struct {
		what string
		path string
	}{
		{"success command", s.cmdOK},
		{"retryable-failure command", s.cmdFail},
		{"shell for the payload wrapper", s.cmdShell},
	} {
		info, err := os.Stat(c.path)
		if err != nil {
			return fmt.Errorf("%s %q does not resolve on this host: %w — every task using it would "+
				"fail to START, which the state machine reports as an ordinary non-retryable "+
				"failure, so the run would stay green while executing nothing",
				c.what, c.path, err)
		}
		if info.IsDir() || info.Mode()&0o111 == 0 {
			return fmt.Errorf("%s %q is not executable (mode %s)", c.what, c.path, info.Mode())
		}
	}
	if _, err := os.Stat(cmdCannotStart); err == nil {
		return fmt.Errorf("%q EXISTS on this host, so the terminal-failure branch would never be "+
			"reached: tasks meant to fail to start would run instead", cmdCannotStart)
	}
	return nil
}

// slowTaskLeases is how many lease TTLs a slow task's payload sleeps for.
const slowTaskLeases = 6

// cmdCannotStart is a path that does not exist, which is the executor's
// non-retryable branch: a command that never starts is not made startable by
// another attempt.
const cmdCannotStart = "/sim/no-such-command"

// outcomeFor decides whether this execution succeeded, by HASHING
// (seed, taskID, attempt) rather than drawing from the run's RNG.
//
// Drawing would have been the obvious thing and it is wrong. The number of
// draws depends on how many tasks happened to be drained in a step, which
// depends on goroutine timing — so every workload draw shifts the fault RNG's
// sequence and the FAULT SCHEDULE stops being reproducible. That is the one
// property the whole harness is built to provide, and adding failures to the
// workload broke it until this was derived instead.
//
// Hashing gives the same answer for the same task on the same seed, costs the
// fault sequence nothing, and is the same trick the state machine already
// uses for retry jitter.
//
// The split between retryable and terminal matters: a retryable failure sends
// the task back to PENDING with a backoff deadline and must later be woken by
// a BACKOFF_ELAPSED eval, while a non-retryable one goes straight to FAILED.
// Those are different branches, and exercising one proves nothing about the
// other.
func (s *Sim) outcomeFor(task *schedulerpb.Task, workerID string) *schedulerpb.ReportResultRequest {
	req := &schedulerpb.ReportResultRequest{
		WorkerId: workerID,
		TaskId:   task.GetTaskId(),
		Attempt:  task.GetAttempt(),
	}
	roll, split := s.outcomeRolls(task.GetTaskId(), task.GetAttempt())
	if s.cfg.FailureRate <= 0 || roll >= s.cfg.FailureRate {
		req.ExitCode = 0
		req.ResultRef = "/sim/" + workerID
		s.result.Coverage.ReportedOK++
		return req
	}
	if split < s.cfg.RetryableFraction {
		req.ExitCode = 1
		req.Error = "simulated transient failure"
		req.Retryable = true
		s.result.Coverage.ReportedRetryable++
		return req
	}
	req.ExitCode = 2
	req.Error = "simulated permanent failure"
	req.Retryable = false
	s.result.Coverage.ReportedTerminal++
	return req
}

// outcomeRolls derives two independent [0,1) values from (seed, taskID,
// attempt): one for whether the execution fails, one for which kind.
//
// Two separate hashes rather than one split in half, so the failure decision
// and the retryable/terminal decision cannot correlate — a task that fails is
// no more likely to fail terminally than any other.
func (s *Sim) outcomeRolls(taskID string, attempt int32) (roll, split float64) {
	return s.hashUnit(taskID, attempt, "fail"), s.hashUnit(taskID, attempt, "kind")
}

func (s *Sim) hashUnit(taskID string, attempt int32, salt string) float64 {
	h := fnv.New64a()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(s.cfg.Seed))
	_, _ = h.Write(b[:])
	_, _ = h.Write([]byte(taskID))
	binary.BigEndian.PutUint32(b[:4], uint32(attempt))
	_, _ = h.Write(b[:4])
	_, _ = h.Write([]byte(salt))
	// Top 53 bits, the same way a float64 in [0,1) is normally built.
	return float64(h.Sum64()>>11) / float64(1<<53)
}
