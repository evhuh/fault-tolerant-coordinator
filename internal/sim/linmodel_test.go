package sim

import (
	"fmt"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func submitOp(client int, key, jobID string) porcupine.Operation {
	out := linOutput{JobID: jobID}
	if jobID == "" {
		out = linOutput{Unknown: true}
	}
	return porcupine.Operation{
		ClientId: client,
		Input:    linInput{Kind: opSubmit, Key: key},
		Output:   out,
	}
}

func getOp(client int, jobID string, found bool) porcupine.Operation {
	return porcupine.Operation{
		ClientId: client,
		Input:    linInput{Kind: opGet, JobID: jobID},
		Output:   linOutput{Found: found},
	}
}

// Partitioning must not separate operations that constrain each other.
//
// A partition function is a claim that the history is linearizable IF AND
// ONLY IF each part is, and the "only if" is where it goes wrong: split two
// coupled operations and the violation between them becomes invisible —
// green for the worst possible reason. So this checks the couplings the spec
// actually expresses rather than checking that jobs end up apart.
func TestLinPartitionsKeepsCoupledOperationsTogether(t *testing.T) {
	partOf := func(parts [][]porcupine.Operation, op porcupine.Operation) int {
		for i, p := range parts {
			for _, candidate := range p {
				if candidate.Input == op.Input && candidate.Output == op.Output {
					return i
				}
			}
		}
		return -1
	}

	t.Run("a get travels with the submit that minted its job", func(t *testing.T) {
		submit := submitOp(1, "c/r1", "job-a")
		get := getOp(2, "job-a", true)
		other := submitOp(1, "c/r2", "job-b")
		parts := linPartitions([]porcupine.Operation{submit, get, other})

		if partOf(parts, submit) != partOf(parts, get) {
			t.Error("the get of job-a was separated from its submit; the stale-read violation " +
				"this whole harness hunts is exactly a get that disagrees with its submit")
		}
		if partOf(parts, other) == partOf(parts, submit) {
			t.Error("job-b was grouped with job-a, so partitioning bought nothing")
		}
	})

	t.Run("two submits sharing a dedup key travel together even when dedup failed", func(t *testing.T) {
		// THE CASE JOB-ID KEYING WOULD LOSE. A correct system returns one id
		// for one key; a broken one returns two, and keying by job id alone
		// would file these in different parts where neither could see the
		// other — hiding the violation precisely when it occurred.
		first := submitOp(1, "c/r1", "job-a")
		second := submitOp(2, "c/r1", "job-b")
		parts := linPartitions([]porcupine.Operation{first, second})

		if partOf(parts, first) != partOf(parts, second) {
			t.Fatal("two submits of one dedup key were separated, so a dedup failure would be unverifiable")
		}
		// And the model must actually reject that pair once they are checked
		// together, or the grouping protects nothing.
		if res := porcupine.CheckOperations(jobsModel(), []porcupine.Operation{first, second}); res {
			t.Error("the model accepted one dedup key minting two jobs")
		}
	})

	t.Run("two keys colliding on one job travel together", func(t *testing.T) {
		a := submitOp(1, "c/r1", "job-a")
		b := submitOp(2, "c/r2", "job-a")
		parts := linPartitions([]porcupine.Operation{a, b})
		if partOf(parts, a) != partOf(parts, b) {
			t.Fatal("two distinct requests that returned the same job were separated")
		}
		if res := porcupine.CheckOperations(jobsModel(), []porcupine.Operation{a, b}); res {
			t.Error("the model accepted two distinct requests colliding on one job id")
		}
	})

	t.Run("an ambiguous submit travels with a later retry of its key", func(t *testing.T) {
		// An ambiguous submit is identity in the spec, and the only way it
		// ever becomes observable is a retry of the same key, so the two
		// have to be checked together.
		ambiguous := submitOp(1, "c/r1", "")
		retry := submitOp(1, "c/r1", "job-a")
		get := getOp(2, "job-a", true)
		parts := linPartitions([]porcupine.Operation{ambiguous, retry, get})
		if partOf(parts, ambiguous) != partOf(parts, retry) {
			t.Error("an ambiguous submit was separated from the retry that makes it observable")
		}
		if partOf(parts, retry) != partOf(parts, get) {
			t.Error("the retry was separated from the get of the job it returned")
		}
	})

	t.Run("a stale read is still caught after partitioning", func(t *testing.T) {
		// The end-to-end claim: partitioning must not launder the bug the
		// harness exists to find.
		submit := submitOp(1, "c/r1", "job-a")
		stale := getOp(2, "job-a", false)
		noise := submitOp(3, "c/r9", "job-z")
		ops := []porcupine.Operation{
			{ClientId: 1, Input: submit.Input, Output: submit.Output, Call: 0, Return: 1},
			{ClientId: 2, Input: stale.Input, Output: stale.Output, Call: 2, Return: 3},
			{ClientId: 3, Input: noise.Input, Output: noise.Output, Call: 4, Return: 5},
		}
		if porcupine.CheckOperations(jobsModel(), ops) {
			t.Error("a NOT_FOUND after a completed submit was accepted; partitioning laundered the violation")
		}
	})
}

// What partitioning actually buys, measured rather than asserted.
//
// The first version of this comment claimed an unpartitioned 6,000-step
// history would TIME OUT. Measured, that is not true, and the real numbers
// are the point — synthetic histories of independent jobs, every operation
// overlapping every other, 10% ambiguous where noted:
//
//	   ops   partitioned   unpartitioned
//	   240          ~0ms             2ms
//	 1,000           2ms            24ms
//	 4,000           4ms           195ms
//	12,000           6ms          1,410ms
//
// Partitioned is near-linear in operations; unpartitioned is roughly
// quadratic. A 6,000-step run records about 72,000 operations, which projects
// to tens of milliseconds partitioned and to roughly 50 SECONDS
// unpartitioned — inside the 120s budget, but on a quadratic curve measured
// against a UNIFORM synthetic history, where real interleavings are deeper.
//
// So the honest justification is margin, not necessity: partitioning turns
// the headline property from something that depends on the search budget into
// something that does not. Overclaiming it would have been easy here and the
// measurement was cheap.
func TestPartitioningIsWhatMakesALongHistoryCheckable(t *testing.T) {
	// 12,000 operations: the largest size measured above, and the one the
	// projection is anchored on.
	const jobs = 6000

	var ops []porcupine.Operation
	span := int64(2*jobs + 10)
	for i := 0; i < jobs; i++ {
		jobID := fmt.Sprintf("job-%04d", i)
		key := fmt.Sprintf("c/r%04d", i)
		out := linOutput{JobID: jobID}
		if i%10 == 0 {
			// Ambiguity is what multiplies branching in a real history, so a
			// measurement without any is measuring the easy case.
			out = linOutput{Unknown: true}
		}
		// EVERYTHING OVERLAPS: every operation is open across the whole
		// history, the worst case for the search and a fair model of three
		// clients running concurrently for thousands of steps.
		ops = append(ops,
			porcupine.Operation{ClientId: i % 3, Input: linInput{Kind: opSubmit, Key: key},
				Output: out, Call: 0, Return: span},
			porcupine.Operation{ClientId: (i + 1) % 3, Input: linInput{Kind: opGet, JobID: jobID},
				Output: linOutput{Found: out.JobID != ""}, Call: 1, Return: span + 1},
		)
	}

	// Generous: the measurement is 6ms and the assertion is about the
	// property the long run depends on, not about this machine's speed.
	const budget = 20 * time.Second

	start := time.Now()
	partitioned, _ := porcupine.CheckOperationsVerbose(jobsModel(), ops, budget)
	withPart := time.Since(start)

	flat := jobsModel()
	flat.Partition = nil
	start = time.Now()
	unpartitioned, _ := porcupine.CheckOperationsVerbose(flat, ops, budget)
	withoutPart := time.Since(start)

	t.Logf("%d operations over %d jobs, all overlapping, 10%% ambiguous:", len(ops), jobs)
	t.Logf("  partitioned:   %-7s in %s", partitioned, withPart.Round(time.Millisecond))
	t.Logf("  unpartitioned: %-7s in %s", unpartitioned, withoutPart.Round(time.Millisecond))

	if partitioned != porcupine.Ok {
		t.Errorf("the partitioned check did not return Ok over a legal history: %s — the long "+
			"run's headline property depends on this finishing", partitioned)
	}
	if withPart > 2*time.Second {
		t.Errorf("the partitioned check took %s over %d operations; a 6,000-step run records "+
			"about six times this many and must not be racing the search budget", withPart, len(ops))
	}
}
