package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// Executor runs a task's command as a subprocess.
//
// THE OUTPUT NEVER ENTERS THE RAFT LOG. stdout and stderr are written to the
// worker's own disk and what travels back to the coordinator is a reference
// plus an exit code. Replicating references rather than payloads is what
// keeps the log small enough to snapshot and replay, and it is a deliberate
// production pattern rather than a shortcut.
type Executor struct {
	// DataDir roots the result tree: <DataDir>/<taskID>/<attempt>/.
	DataDir string
}

// Result is what the worker reports back.
type Result struct {
	ResultRef string // directory on THIS worker's disk holding stdout/stderr
	ExitCode  int32
	Err       string
	// Retryable distinguishes "this task's command failed" from "this
	// execution could not be carried out". A nonzero exit is the program's
	// own verdict and is worth retrying; a malformed spec is not.
	Retryable bool
}

// Run executes one attempt. It is keyed on (taskID, attempt), and the result
// directory is keyed the same way — so a re-run OVERWRITES rather than
// appends, and a duplicate delivery of the same attempt is a no-op on disk
// rather than a second observable effect.
func (e *Executor) Run(ctx context.Context, taskID string, attempt int32, command string, args []string, timeout time.Duration) Result {
	dir := filepath.Join(e.DataDir, taskID, fmt.Sprintf("%d", attempt))
	// RemoveAll first: a retry of the SAME attempt must land on a clean
	// directory, or a partial previous run leaves output nobody wrote.
	_ = os.RemoveAll(dir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return Result{Err: fmt.Sprintf("mkdir %s: %v", dir, err), ExitCode: -1, Retryable: false}
	}

	if timeout <= 0 {
		timeout = time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		return Result{ResultRef: dir, Err: err.Error(), ExitCode: -1, Retryable: false}
	}
	defer stdout.Close()
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		return Result{ResultRef: dir, Err: err.Error(), ExitCode: -1, Retryable: false}
	}
	defer stderr.Close()

	cmd := exec.CommandContext(runCtx, command, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	runErr := cmd.Run()

	res := Result{ResultRef: dir}
	switch {
	case runErr == nil:
		res.ExitCode = 0

	case errors.Is(runCtx.Err(), context.DeadlineExceeded):
		// A timeout is a failure of this ATTEMPT, not of the task — a slow
		// or wedged run is exactly what another attempt might survive.
		res.ExitCode = -1
		res.Err = fmt.Sprintf("timed out after %s", timeout)
		res.Retryable = true

	default:
		var ee *exec.ExitError
		if errors.As(runErr, &ee) {
			// The program ran and decided it failed. Its verdict, retryable.
			res.ExitCode = int32(ee.ExitCode())
			res.Err = runErr.Error()
			res.Retryable = true
		} else {
			// Could not start at all: missing binary, bad permissions. No
			// number of attempts fixes that, so it is terminal.
			res.ExitCode = -1
			res.Err = runErr.Error()
			res.Retryable = false
		}
	}
	return res
}
