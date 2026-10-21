package sim

// The sequential specification the recorded history is checked against.
//
// # What is modelled, and why only this
//
// Two client operations: SubmitJob and GetJob.
// Those are the whole client-visible surface that touches job existence.
// RequestTask delivers a decision the log already committed, and the worker
// RPCs (RegisterWorker, Heartbeat, ReportResult) are not client operations at
// all — they are the execution substrate reporting in.
// Modelling them would mean modelling the scheduler, which is a concurrent
// actor inside the system rather than a caller of it, and a sequential spec
// cannot express "and then, at some point, a worker reported".
//
// State is the jobs map, exactly as the roadmap says, plus the dedup
// bindings: SubmitJob is idempotent on (clientID, requestID), and a spec that
// ignores that would call every correct retry a violation.
//
// # What a state is
//
// Two sorted, delimited strings rather than two maps, which buys three
// things that matter more here than elegance.
// The state is comparable, so porcupine's default equality and the
// `interface{}` boxing both work with no Equal or Hash function to get
// subtly wrong.
// It is immutable, which porcupine REQUIRES — Step is called during a
// backtracking search and a state it mutates corrupts every branch that
// shares it.
// And it is cheap to copy, which is the operation the search performs most.
// Job ids and request keys contain neither '|' nor '=', so the encoding is
// unambiguous.

import (
	"fmt"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

type linOpKind string

const (
	opSubmit linOpKind = "submit"
	opGet    linOpKind = "get"
)

// linInput is one client operation's arguments.
type linInput struct {
	Kind linOpKind
	// Key is "clientID/requestID" — the dedup key, for a submit.
	Key string
	// JobID is the id being read, for a get.
	JobID string
}

// linOutput is what the client observed.
//
// Unknown is the honest third answer and the reason this type is not a bare
// (string, bool).
// A client that sees a timeout, NOT_LEADER after proposing, or a lost
// proposal learns NOTHING about whether the operation took effect, and the
// entry may still commit after the RPC has returned.
// Recording that as "it failed" is how a failing history turns into a passing
// one, because the checker is then told something the client never observed.
type linOutput struct {
	JobID   string
	Found   bool
	Unknown bool
}

// linState is the sequential state: which jobs exist, and which dedup key
// minted each one.
type linState struct {
	// jobs is the sorted set of job ids that exist.
	jobs string
	// keys is the sorted set of "clientID/requestID=jobID" bindings.
	keys string
}

// jobsModel is the model handed to porcupine.
func jobsModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} { return linState{} },
		Step: func(st, in, out interface{}) (bool, interface{}) {
			return linStep(st.(linState), in.(linInput), out.(linOutput))
		},
		DescribeOperation: describeLinOp,
		DescribeState: func(st interface{}) string {
			s := st.(linState)
			n := 0
			if s.jobs != "" {
				n = strings.Count(s.jobs, "|") + 1
			}
			return fmt.Sprintf("%d jobs {%s}", n, s.jobs)
		},
	}
}

// linStep is the spec itself.
//
// # The ambiguous submit
//
// An ambiguous SubmitJob steps the state nowhere, and that is sound here for
// a specific reason rather than for convenience.
// The job id is minted by the SERVER, so a client whose submit returned
// ambiguously never learns the id of the job it may have created — which
// means it can never pass that id to GetJob, and no operation in this model
// can observe the job's existence.
// If the commit did happen, the only way it ever becomes visible is a retry
// of the same (clientID, requestID), and dedup makes that retry return the
// original id, which the model then treats as the submit that created it.
// Both branches — committed or not — end in the same observable state, so
// splitting them (a porcupine NondeterministicModel) would double the search
// space to distinguish two things no operation can tell apart.
//
// THIS STOPS BEING SOUND the moment an operation enumerates jobs without
// naming one. Add ListJobs to the history and the branches diverge
// immediately: the committed branch has a job the unchanged branch does not,
// and a list would see it. At that point this must become a
// NondeterministicModel. That trigger is the thing to remember, not the
// shortcut.
func linStep(s linState, in linInput, out linOutput) (bool, interface{}) {
	switch in.Kind {
	case opSubmit:
		if out.Unknown {
			return true, s
		}
		// A repeat of a key already bound MUST return the same job. A
		// different id means dedup failed and there are now two jobs where
		// the client asked for one.
		if bound, ok := linLookup(s.keys, in.Key); ok {
			return bound == out.JobID, s
		}
		// A fresh key that returns an id already in use means two distinct
		// requests collided on one job.
		if linHas(s.jobs, out.JobID) {
			return false, s
		}
		return true, linState{
			jobs: linAdd(s.jobs, out.JobID),
			keys: linAdd(s.keys, in.Key+"="+out.JobID),
		}

	case opGet:
		if out.Unknown {
			return true, s
		}
		// The whole hunt lands here. A job the history has already seen
		// submitted must be found; a job that does not exist must not be.
		// A partitioned leader answering from its own stale state returns
		// NOT_FOUND for a job the client was told was created, and no
		// ordering of these operations makes that legal.
		return out.Found == linHas(s.jobs, in.JobID), s
	}
	return false, s
}

func describeLinOp(in, out interface{}) string {
	i := in.(linInput)
	o := out.(linOutput)
	switch i.Kind {
	case opSubmit:
		if o.Unknown {
			return fmt.Sprintf("Submit(%s) -> AMBIGUOUS", i.Key)
		}
		return fmt.Sprintf("Submit(%s) -> %s", i.Key, o.JobID)
	case opGet:
		switch {
		case o.Unknown:
			return fmt.Sprintf("Get(%s) -> AMBIGUOUS", i.JobID)
		case o.Found:
			return fmt.Sprintf("Get(%s) -> found", i.JobID)
		default:
			return fmt.Sprintf("Get(%s) -> NOT_FOUND", i.JobID)
		}
	}
	return "?"
}

// Set helpers over the delimited encoding. Sorted on insert, so two states
// that hold the same elements are the same string and compare equal.

func linHas(set, elem string) bool {
	if set == "" {
		return false
	}
	for _, e := range strings.Split(set, "|") {
		if e == elem {
			return true
		}
	}
	return false
}

func linAdd(set, elem string) string {
	if set == "" {
		return elem
	}
	parts := append(strings.Split(set, "|"), elem)
	sort.Strings(parts)
	return strings.Join(parts, "|")
}

// linLookup finds a "key=value" binding.
func linLookup(set, key string) (string, bool) {
	if set == "" {
		return "", false
	}
	prefix := key + "="
	for _, e := range strings.Split(set, "|") {
		if strings.HasPrefix(e, prefix) {
			return e[len(prefix):], true
		}
	}
	return "", false
}
