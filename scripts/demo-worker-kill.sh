#!/bin/bash
# Single-node checkpoint, part 2: SIGKILL a worker mid-task; its task's lease
# expires, Tick reclaims it, and another worker finishes it as attempt 2.
source "$(dirname "$0")/common.sh"
build
start_coordinator
start_workers 3 1
set -- $WORKER_PIDS
VICTIM=$2   # w-2

JOB=$(ctl submit -n 6 -max-attempts 3 -timeout 30s -- /bin/sh -c 'sleep 4; echo done')
echo "JOB=$JOB   (lease TTL 2s, heartbeat 500ms)"
sleep 1.5
echo; echo "--- before the kill ---"; ctl status "$JOB" | tail -n +2
echo; echo ">>> SIGKILL w-2 (pid $VICTIM) mid-task"
kill -9 "$VICTIM" 2>/dev/null; wait "$VICTIM" 2>/dev/null

for i in $(seq 1 "${WAIT:-18}"); do
  line=$(ctl status "$JOB" | head -1)
  echo "t+$((i+1))s  $line"
  case "$line" in *"COMPLETE  tasks=6"*) break;; esac
  sleep 1
done
echo; echo "--- final ---"; ctl status "$JOB" | tail -n +2
echo; echo "--- redispatched (attempt > 1) ---"
ctl status "$JOB" | tail -n +2 | grep -v 'attempt=1' || echo "(none — the kill landed between tasks)"
