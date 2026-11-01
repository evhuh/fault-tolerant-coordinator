package server

import (
	"testing"

	"github.com/evazheng/fault-tolerant-coordinator/internal/state"
)

// THE SHIPPED CONFIG MUST PROPOSE THE NEWEST COMMAND VERSION.
//
// Every version-gated rule in the state machine reads the ENTRY's version, so
// a build whose DefaultConfig still proposes an older one applies the older
// rules and the new rule is inert. That is not hypothetical: the payload-hash
// guard was written, reviewed and covered by five state-machine tests while
// doing nothing over gRPC, because DefaultConfig still proposed version 3. An
// end-to-end test caught it; this one makes the next bump unable to land
// inert.
//
// Deliberately an equality check rather than ">=". A default ABOVE
// CommandVersionCurrent would propose entries this build's own state machine
// refuses, which is worse than lagging.
func TestDefaultConfigProposesTheCurrentCommandVersion(t *testing.T) {
	if got := DefaultConfig().CommandVersion; got != state.CommandVersionCurrent {
		t.Errorf("DefaultConfig proposes command version %d, but this build understands %d. "+
			"Every version-gated rule reads the entry's version, so a lagging default makes "+
			"the newest rules dead code — and a default above current proposes entries this "+
			"build would refuse.", got, state.CommandVersionCurrent)
	}
}
