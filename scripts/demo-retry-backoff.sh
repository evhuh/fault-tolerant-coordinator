#!/bin/bash
# A flaky task retries with exponential backoff and deterministic
# jitter, and a task that keeps failing goes terminally FAILED.
source "$(dirname "$0")/common.sh"
build
start_coordinator
start_workers 2 1

# Exits nonzero every time: retryable, so it burns its whole attempt budget.
JOB=$(ctl submit -n 3 -max-attempts 3 -- /bin/sh -c 'echo trying >&2; exit 7')
echo "JOB=$JOB   (base backoff 200ms, doubling, +<=50% deterministic jitter)"
for i in $(seq 1 "${WAIT:-20}"); do
  line=$(ctl status "$JOB" | head -1)
  echo "t+${i}s  $line"
  case "$line" in *"FAILED  tasks=3"*) break;; esac
  sleep 1
done
echo
echo "--- final: every task exhausted its attempts and went terminal ---"
ctl status "$JOB" | tail -n +2
echo
echo "--- the per-attempt ledger on disk: one directory per attempt ---"
find data-w-* -name stdout 2>/dev/null | sort | head -10
echo "total attempt records: $(find data-w-* -name stdout 2>/dev/null | wc -l | tr -d ' ')"
