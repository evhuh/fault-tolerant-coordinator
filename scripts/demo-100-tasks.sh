#!/bin/bash
# Day 3 checkpoint, part 1: 100 tasks across 3 workers all complete.
source "$(dirname "$0")/common.sh"
build
start_coordinator
start_workers 3 2

JOB=$(ctl submit -n "${N:-100}" -- /bin/sh -c 'echo task ok')
echo "JOB=$JOB   ($RUN_DIR)"
for i in $(seq 1 "${WAIT:-20}"); do
  line=$(ctl status "$JOB" | head -1)
  echo "t+${i}s  $line"
  case "$line" in *"COMPLETE  tasks="*) break;; esac
  sleep 1
done
echo
echo "--- tasks not COMPLETE ---"
ctl status "$JOB" | tail -n +2 | grep -v COMPLETE || echo "(none)"
echo "--- result dirs written to worker disks ---"
find data-w-* -name stdout 2>/dev/null | wc -l
