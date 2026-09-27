package server

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// IDs are minted by the LEADER, before proposing, and baked into the command.
// They can never be generated inside Apply: that runs on every replica and
// must be deterministic, so a random ID there would diverge state instantly.
//
// Randomness is safe HERE precisely because the id becomes a decided fact the
// moment it enters the command. The hazard a random id would otherwise create
// — re-proposing after an ambiguous outcome and minting a second job — is
// covered for SubmitJob by (clientID, requestID) dedup in the state machine.
//
// Day 4 needs a different answer for eval ids, which have no such dedup key
// and must therefore be DERIVED from what triggered them.

func newJobID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("server: crypto/rand unavailable: " + err.Error())
	}
	return "job-" + hex.EncodeToString(b[:])
}

// taskID is derived from the job id and the task's index within the job, so
// task ids sort in submission order. The padding is load-bearing: assign()
// scans SORTED ids, and unpadded indices would put task 10 ahead of task 2.
func taskID(jobID string, index int) string {
	return fmt.Sprintf("%s/%04d", jobID, index)
}

// placeholderEvalID exists only because applyPlan requires a non-empty
// eval_id and Day 3 has no evaluations yet. Day 4 deletes it along with
// assign(): every plan will then carry the id of the eval it came from.
func placeholderEvalID(taskID string, attempt int32) string {
	return fmt.Sprintf("placeholder/%s/%d", taskID, attempt)
}
