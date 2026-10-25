.PHONY: proto test race vet build sweeps release-check

# Regenerate both schemas. Run from proto/ so `source:` in the generated headers stays stable; --go_opt=module strips the package prefix so files land in proto/commandpb and proto/schedulerpb rather than a nested tree
PROTO_MODULE := github.com/evazheng/fault-tolerant-coordinator/proto

proto:
	cd proto && protoc \
		--go_out=. --go_opt=module=$(PROTO_MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(PROTO_MODULE) \
		command.proto scheduler.proto snapshot.proto

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

# The adversary sweeps: targeted stale-read validation against each adversary.
#
# NOT part of `make test`. They take minutes, and they are VALIDATION of the
# targeted workload — proof that it can still catch a bug that is definitely
# present — rather than per-commit regression guards. What guards the
# behaviour on every commit is the sub-second deterministic stale-read
# scenario under both read modes, plus the liveness tests, all in `make test`.
#
# REQUIRED BEFORE:
#   - tagging a release, since the tag asserts the claim these measure;
#   - quoting any catch rate, coverage figure or "zero violations" number in
#     the README, the roadmap or anywhere else.
#
# EVERY QUOTED RATE CARRIES ITS n AND SEED RANGE. "8/8 Illegal" means nothing
# on its own; "8/8 Illegal over seeds 1-8 at 7 steps" can be reproduced and
# argued with. The tests print exactly that, and a number copied out of them
# without its n is not a result.
#
# REPRODUCIBILITY IS NARROWER HERE THAN IN `make test`. These sweeps DRIVE
# their window — they inject a leader pause or partition — and the injection
# has to find the current leader, deferring when there is none. So the step it
# lands on and the node it hits depend on timing, not on the seed: a sweep
# seed reproduces the adversary's INTENT, not its realization. Each run
# reports the realization (Result.Window) and Config.ReplayWindow feeds it
# back to force the same step and victim.
#
# Consequence for anything quoted: a failure found here MUST be reduced to a
# deterministic scenario test before it is called fixed. Ordinary runs do
# reproduce their schedule from the seed — that claim is unaffected and is
# pinned by TestSameSeedReproducesTheFaultSchedule.
#
# -timeout is explicit because the default 10m fails the whole package with no
# test failing, which is the least informative failure Go can produce.
sweeps:
	FTC_SWEEPS=1 go test -timeout 30m -v ./internal/sim/ \
		-run 'TestTargetedWorkload|TestPauseDefeats|TestShippedConfigWithoutPause|TestReadIndexFindsNoStale|TestEffectLedger'

# Everything a tag asserts: the default suite, the race detector, and the
# sweeps whose numbers the claim rests on.
release-check: vet test race sweeps

vet:
	go vet ./...
