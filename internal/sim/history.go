package sim

// The history recorder.
//
// A linearizability check is only as good as the history, and the history has
// exactly two jobs: record when each operation was INVOKED and when it
// RETURNED, in real time, and record what the client actually observed —
// including when that is nothing.
//
// # Timestamps
//
// Nanoseconds since the run started, from one monotonic clock read per
// boundary. porcupine treats [Call, Return] as a CLOSED interval, so two
// operations that read the same nanosecond are concurrent, which is the
// conservative direction: it widens the set of legal orderings rather than
// inventing an ordering the clients never established.
//
// # Ambiguity
//
// Rule 6 of the harness: an ambiguous outcome is recorded, never dropped. It
// goes in with Unknown set, which the model accepts against any state — so
// it constrains nothing and masks nothing. The count is reported, because a
// run whose history is mostly ambiguous is a run that checked very little,
// and that is the same failure mode MinComparisons exists to catch on the
// convergence side.
//
// # The cap
//
// porcupine's search is exponential in CONCURRENCY, not in length, but a long
// history under partition has long concurrent stretches and the two multiply.
// MaxOps bounds what is recorded; a history that hits the cap is still
// checked, and the cap is reported so a green verdict is never mistaken for a
// green verdict over the whole run.

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
)

// history collects client operations across all simulated clients.
type history struct {
	mu     sync.Mutex
	start  time.Time
	ops    []porcupine.Operation
	maxOps int

	// Counts reported alongside the verdict.
	ambiguous int
	// Coverage: how many recorded operations landed somewhere that could
	// actually produce a violation.
	partitioned int
	splitBrain  int
	// splitBrainReads counts reads served by a node that believed it led
	// while another node also did.
	splitBrainReads int
	// TWO counters, because "covered" means different things to the two read
	// modes and collapsing them is wrong in both directions.
	//
	// targetedReads is the OPPORTUNITY: a read from the cut-off node, of a
	// job the majority committed during that same partition, was issued at
	// all. This is the right notion for the barrier, where the CORRECT
	// outcome is a refusal — demand an answer there and correct behaviour
	// becomes indistinguishable from never reaching the window. Measured:
	// under ReadIndexBarrier every targeted read is refused, so an
	// answered-only definition reported 0 of 8 covered on a working system.
	//
	// targetedAnswered is the narrower COULD-HAVE-FAILED: the read came back
	// found or not-found rather than refused. This is the right notion for
	// validating the DETECTOR under local reads, where a refusal constrains
	// nothing and must not count — the pause adversary read 3 covered
	// against 2 Illegal until this was separated out.
	//
	// A split-brain read of a PRE-partition job is excluded from both: both
	// sides hold that job, so finding it is correct and proves nothing.
	// Counting those was the first version of this counter, and it let a run
	// bank coverage on reads that could never have been stale.
	targetedReads    int
	targetedAnswered int
	dropped          int // operations not recorded because the cap was reached
	rejected         int // definitive InvalidArgument: a malformed request we never sent
}

func newHistory(maxOps int) *history {
	return &history{start: time.Now(), maxOps: maxOps}
}

// now is the elapsed-nanosecond timestamp porcupine orders by.
func (h *history) now() int64 { return int64(time.Since(h.start)) }

// linOp is an operation in flight. The call timestamp is taken before the
// RPC and held here, so the recorder never has to guess it afterwards.
type linOp struct {
	h      *history
	client int
	in     linInput
	call   int64
	// Where this operation landed, sampled at invocation. Coverage only:
	// nothing in the model reads it, because the model must not be told
	// anything the client could not observe.
	partitioned bool
	splitBrain  bool
	// targeted marks a read issued by the targeted workload against the
	// cut-off node, for a job the majority committed during the partition.
	targeted bool
}

// begin stamps an invocation. It does NOT take the history lock — the whole
// point of stamping here is that the timestamp is the moment just before the
// call, and blocking on a shared mutex first would skew every interval by
// however long the other clients hold it.
func (h *history) begin(client int, in linInput, partitioned, splitBrain bool) linOp {
	return linOp{
		h: h, client: client, in: in, call: h.now(),
		partitioned: partitioned, splitBrain: splitBrain,
	}
}

// beginTargeted stamps a targeted read: the one shape that can be stale.
func (h *history) beginTargeted(client int, in linInput, partitioned bool) linOp {
	op := h.begin(client, in, partitioned, true)
	op.targeted = true
	return op
}

// done stamps the response and files the operation.
func (op linOp) done(out linOutput) {
	ret := op.h.now()
	h := op.h
	h.mu.Lock()
	defer h.mu.Unlock()
	// Counted only if it is RECORDED. Counting before the cap check reports
	// an ambiguity total against an operation total it does not belong to —
	// the first run printed "1500 ops, 6406 ambiguous", which is not a
	// confusing number so much as a wrong one.
	if h.maxOps > 0 && len(h.ops) >= h.maxOps {
		h.dropped++
		return
	}
	if out.Unknown {
		h.ambiguous++
	}
	if op.partitioned {
		h.partitioned++
	}
	if op.splitBrain {
		h.splitBrain++
		if op.in.Kind == opGet {
			h.splitBrainReads++
			if op.targeted {
				h.targetedReads++
				if !out.Unknown {
					h.targetedAnswered++
				}
			}
		}
	}
	h.ops = append(h.ops, porcupine.Operation{
		ClientId: op.client,
		Input:    op.in,
		Output:   out,
		Call:     op.call,
		Return:   ret,
	})
}

func (h *history) noteRejected() {
	h.mu.Lock()
	h.rejected++
	h.mu.Unlock()
}

func (h *history) snapshot() []porcupine.Operation {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]porcupine.Operation, len(h.ops))
	copy(out, h.ops)
	return out
}

// LinResult is what the linearizability check produced.
type LinResult struct {
	Checked bool
	// Verdict is porcupine's: Ok, Illegal, or Unknown (the search timed out).
	Verdict porcupine.CheckResult
	Ops     int
	Submits int
	Gets    int
	// Ambiguous is how many operations returned without telling the client
	// whether they took effect.
	Ambiguous int
	// Dropped is how many were not recorded because MaxOps was reached.
	Dropped int
	// Partitioned is how many recorded operations were issued to a node
	// the network had cut off, and SplitBrain how many were issued while
	// more than one node claimed leadership.
	//
	// These are the numbers that make the verdict mean something. Ok over a
	// history where both are zero says only that a healthy cluster is
	// linearizable, which needs no checker to establish.
	Partitioned int
	SplitBrain  int
	// SplitBrainReads is how many reads were served while two nodes claimed
	// leadership, including reads of jobs that predate the partition.
	SplitBrainReads int
	// TargetedReads is how many reads of a job the majority committed DURING
	// the partition were ISSUED against the cut-off node. The opportunity,
	// and what Covered rests on — under the barrier a refusal is the right
	// answer, so reaching the window is the most that can be asked.
	TargetedReads int
	// TargetedReadsAnswered is how many of those came back with a value
	// rather than a refusal: the reads that COULD have exposed staleness,
	// and what the detector validation measures.
	TargetedReadsAnswered int
	// Covered reports whether the history contained an operation that could
	// actually have exposed a stale read. False means the verdict proves
	// nothing about linearizability.
	Covered bool
	// Rejected counts definitive InvalidArgument responses. This workload
	// only ever sends valid requests, so a nonzero count is itself a finding.
	Rejected int
	// Empty means no operations were recorded at all, so there was nothing
	// to check. Distinct from a search that timed out.
	Empty bool
	// VizPath is the HTML timeline, written only on a failure.
	VizPath string
	Elapsed time.Duration
}

// check runs porcupine over the recorded history.
//
// On anything other than Ok it writes the HTML timeline, because a
// linearizability failure is almost unreadable as text and completely
// readable as a picture: the violating operation is the one whose interval
// cannot be slid anywhere the model accepts.
func (h *history) check(timeout time.Duration, vizDir string) LinResult {
	ops := h.snapshot()
	h.mu.Lock()
	res := LinResult{
		Checked:               true,
		Ops:                   len(ops),
		Ambiguous:             h.ambiguous,
		Dropped:               h.dropped,
		Rejected:              h.rejected,
		Partitioned:           h.partitioned,
		SplitBrain:            h.splitBrain,
		SplitBrainReads:       h.splitBrainReads,
		TargetedReads:         h.targetedReads,
		TargetedReadsAnswered: h.targetedAnswered,
	}
	h.mu.Unlock()
	for _, o := range ops {
		if o.Input.(linInput).Kind == opSubmit {
			res.Submits++
		} else {
			res.Gets++
		}
	}
	// An empty history is not a timed-out search, and the first version
	// reported it as one: "search did not finish in 20s over 0 operations",
	// a sentence describing a search that never ran. Both are inconclusive,
	// but they have different causes and different fixes.
	if len(ops) == 0 {
		res.Verdict = porcupine.Unknown
		res.Empty = true
		return res
	}

	model := jobsModel()
	start := time.Now()
	verdict, info := porcupine.CheckOperationsVerbose(model, ops, timeout)
	res.Elapsed = time.Since(start)
	res.Verdict = verdict

	if verdict != porcupine.Ok && vizDir != "" {
		if path, err := writeViz(model, info, vizDir); err == nil {
			res.VizPath = path
		}
	}
	return res
}

// AmbiguousShare is the fraction of recorded operations whose outcome the
// client never learned.
//
// Reported on every run, not just failing ones, because it is the single
// number that says how much a verdict is worth. An ambiguous operation
// constrains nothing — the model accepts it against any state — so a history
// that is 90% ambiguous is a history in which almost nothing was checked, and
// its Ok looks identical to a real one. Measured runs under the default fault
// schedule sit near a third, which is the cost of asking a cluster that is
// being actively partitioned.
func (l LinResult) AmbiguousShare() float64 {
	if l.Ops == 0 {
		return 0
	}
	return float64(l.Ambiguous) / float64(l.Ops)
}

func writeViz(model porcupine.Model, info porcupine.LinearizationInfo, dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, fmt.Sprintf("linearizability-%d.html", time.Now().UnixNano()))
	if err := porcupine.VisualizePath(model, info, path); err != nil {
		return "", err
	}
	return path, nil
}
