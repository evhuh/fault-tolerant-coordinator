#!/bin/bash
# Day 6: a real 3-node Raft cluster. SIGKILL the leader mid-flight; a new one
# rebuilds its eval broker from committed state and keeps going.
source "$(dirname "$0")/common.sh"
build

RAFT="1=http://127.0.0.1:19021,2=http://127.0.0.1:19022,3=http://127.0.0.1:19023"
API="1=127.0.0.1:18081,2=127.0.0.1:18082,3=127.0.0.1:18083"

pkill -x coordinator 2>/dev/null; pkill -x worker 2>/dev/null; sleep 0.5
rm -rf raft-* data-w-* ./*.log

declare -a PIDS
for i in 1 2 3; do
  ./coordinator -id $i -addr ":1808$i" -peers "$RAFT" -api-peers "$API" \
    -data-dir "./raft-$i" > "coord$i.log" 2>&1 &
  PIDS[$i]=$!
done
sleep 3

find_leader() {
  for i in 1 2 3; do
    if grep -q "became leader" "coord$i.log" 2>/dev/null; then :; fi
  done
  # Ask each node: the leader is the one that accepts a write.
  for i in 1 2 3; do
    if ./schedctl -coordinator "127.0.0.1:1808$i" list >/dev/null 2>&1; then
      echo $i; return
    fi
  done
  echo ""
}

LEADER=$(find_leader)
[ -z "$LEADER" ] && { echo "FATAL: no leader"; tail -5 coord1.log; exit 1; }
echo ">>> leader is node $LEADER (pid ${PIDS[$LEADER]})"

# Seeded with ALL THREE addresses. The worker follows NOT_LEADER on its own —
# it is never told about the failover.
ALL_API="127.0.0.1:18081,127.0.0.1:18082,127.0.0.1:18083"
./worker -id w-1 -coordinator "$ALL_API" -concurrency 1 > w1.log 2>&1 &
sleep 1

JOB=$(./schedctl -coordinator "$ALL_API" submit -n 8 -max-attempts 5 -- /bin/sh -c 'sleep 1; echo ok')
echo "JOB=$JOB"
sleep 2
echo
echo "--- before the kill (on node $LEADER) ---"
./schedctl -coordinator "127.0.0.1:1808$LEADER" status "$JOB" | head -1

echo
echo ">>> SIGKILL node $LEADER mid-flight"
kill -9 "${PIDS[$LEADER]}" 2>/dev/null
sleep 3

NEW=$(find_leader)
[ -z "$NEW" ] && { echo "FATAL: no new leader elected"; exit 1; }
echo ">>> new leader is node $NEW"
echo
echo "--- the job survived in the log ---"
./schedctl -coordinator "127.0.0.1:1808$NEW" status "$JOB" | head -1

# The worker is NOT restarted. It discovers the new leader by itself.
for i in $(seq 1 25); do
  line=$(./schedctl -coordinator "127.0.0.1:1808$NEW" status "$JOB" | head -1)
  echo "t+${i}s  $line"
  case "$line" in *"COMPLETE  tasks=8"*) break;; esac
  sleep 1
done
echo
echo "--- final ---"
./schedctl -coordinator "127.0.0.1:1808$NEW" status "$JOB" | tail -n +2
echo
echo "--- the worker process never restarted ---"
echo "worker pid now:  $(pgrep -x worker | tr '\n' ' ')"
echo "worker started:  $(head -1 w1.log)"
echo "(one process, one log, spanning the failover; the pool re-aimed itself)"
