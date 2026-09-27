package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestExecutorCapturesOutputAndExitCode(t *testing.T) {
	e := &Executor{DataDir: t.TempDir()}
	res := e.Run(context.Background(), "job-1/0000", 1, "/bin/sh", []string{"-c", "echo out; echo err >&2"}, 5*time.Second)
	if res.ExitCode != 0 || res.Err != "" {
		t.Fatalf("exit = %d err = %q, want 0 and empty", res.ExitCode, res.Err)
	}
	for name, want := range map[string]string{"stdout": "out\n", "stderr": "err\n"} {
		got, err := os.ReadFile(filepath.Join(res.ResultRef, name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

// A nonzero exit is the program's own verdict: the run happened, so another
// attempt might survive. A command that cannot START is terminal, because no
// number of attempts fixes a missing binary.
func TestExecutorClassifiesRetryability(t *testing.T) {
	e := &Executor{DataDir: t.TempDir()}

	failed := e.Run(context.Background(), "t-exit", 1, "/bin/sh", []string{"-c", "exit 3"}, 5*time.Second)
	if failed.ExitCode != 3 || !failed.Retryable {
		t.Errorf("nonzero exit: got exit=%d retryable=%v, want 3 and true", failed.ExitCode, failed.Retryable)
	}

	missing := e.Run(context.Background(), "t-missing", 1, "/definitely/not/a/binary", nil, 5*time.Second)
	if missing.Retryable {
		t.Error("a missing binary must not be retryable")
	}

	slow := e.Run(context.Background(), "t-slow", 1, "/bin/sh", []string{"-c", "sleep 10"}, 100*time.Millisecond)
	if !slow.Retryable || slow.Err == "" {
		t.Errorf("timeout: got retryable=%v err=%q, want true and a reason", slow.Retryable, slow.Err)
	}
}

// Results are keyed on (taskID, attempt), so re-running the SAME attempt
// overwrites rather than appends. That is what makes a redelivered dispatch a
// no-op on disk instead of a second observable effect.
func TestExecutorRerunOfSameAttemptOverwrites(t *testing.T) {
	e := &Executor{DataDir: t.TempDir()}
	first := e.Run(context.Background(), "job-1/0000", 1, "/bin/sh", []string{"-c", "echo one"}, 5*time.Second)
	second := e.Run(context.Background(), "job-1/0000", 1, "/bin/sh", []string{"-c", "echo two"}, 5*time.Second)
	if first.ResultRef != second.ResultRef {
		t.Fatalf("result refs differ: %q vs %q", first.ResultRef, second.ResultRef)
	}
	got, err := os.ReadFile(filepath.Join(second.ResultRef, "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "two\n" {
		t.Errorf("stdout = %q, want %q — a re-run must overwrite, not append", got, "two\n")
	}
}

// Different attempts of one task keep separate directories, so a slow
// attempt 1 writing its output cannot clobber attempt 2's.
func TestExecutorAttemptsAreIsolated(t *testing.T) {
	e := &Executor{DataDir: t.TempDir()}
	a1 := e.Run(context.Background(), "job-1/0000", 1, "/bin/sh", []string{"-c", "echo first"}, 5*time.Second)
	a2 := e.Run(context.Background(), "job-1/0000", 2, "/bin/sh", []string{"-c", "echo second"}, 5*time.Second)
	if a1.ResultRef == a2.ResultRef {
		t.Fatalf("attempts share a directory: %q", a1.ResultRef)
	}
	got, _ := os.ReadFile(filepath.Join(a1.ResultRef, "stdout"))
	if string(got) != "first\n" {
		t.Errorf("attempt 1 stdout = %q, want it untouched by attempt 2", got)
	}
}

// Distinct tasks running side by side must not interfere.
func TestExecutorConcurrentDistinctTasks(t *testing.T) {
	e := &Executor{DataDir: t.TempDir()}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var failures []string
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			id := fmt.Sprintf("job-1/%04d", g)
			res := e.Run(context.Background(), id, 1, "/bin/sh", []string{"-c", "echo ok"}, 5*time.Second)
			if res.ExitCode != 0 {
				mu.Lock()
				failures = append(failures, fmt.Sprintf("%s: exit=%d err=%s", id, res.ExitCode, res.Err))
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	for _, f := range failures {
		t.Error(f)
	}
}

// The regression that the 100-task demo surfaced.
//
// RequestTask's first clause hands back ANY task already assigned to this
// worker, which is correct — a restarted worker must get its assignment back.
// But every poll goroutine in one process sees the same task, so without a
// local claim, concurrency > 1 runs the same (taskID, attempt) twice into the
// same directory: one goroutine's RemoveAll races the other's Create.
func TestClaimPreventsTwoGoroutinesRunningOneTask(t *testing.T) {
	w := &Worker{cfg: DefaultConfig("w-1", t.TempDir()), running: map[string]bool{}}

	if !w.claim("task-1") {
		t.Fatal("first claim must succeed")
	}
	if w.claim("task-1") {
		t.Error("second claim on a held task must fail")
	}
	if !w.claim("task-2") {
		t.Error("a different task must still be claimable")
	}

	// The claim is what the heartbeat publishes as running_task_ids, which is
	// what promotes ASSIGNED -> RUNNING.
	if got := len(w.runningIDs()); got != 2 {
		t.Errorf("runningIDs = %d, want 2", got)
	}

	// Releasing makes it claimable again, so a retry after a completed run works.
	w.mu.Lock()
	delete(w.running, "task-1")
	w.mu.Unlock()
	if !w.claim("task-1") {
		t.Error("a released task must be claimable again")
	}
}

// Under contention the claim must admit exactly one winner.
func TestClaimIsAtomicUnderContention(t *testing.T) {
	w := &Worker{cfg: DefaultConfig("w-1", t.TempDir()), running: map[string]bool{}}
	var wins int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w.claim("hot") {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Errorf("%d goroutines claimed the same task, want exactly 1", wins)
	}
}
