#!/usr/bin/env bash
# Start a 3-node coordinator cluster and N workers for a demo.
#
#   ./demos/up.sh [worker-count] [concurrency]
#
# Idempotent only in the sense that down.sh precedes it: a demo that starts on
# top of a previous run's WAL is measuring the previous run.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

WORKERS=${1:-2}
CONCURRENCY=${2:-2}

require_bin
mkdir -p "$RUN"

say "starting 3 coordinators"
for i in 0 1 2; do
  id=$((i + 1))
  "$BIN/coordinator" -id "$id" \
    -peers "$PEERS" -api-peers "$API_PEERS" \
    -addr ":${GRPC_PORTS[$i]}" \
    -metrics-addr "127.0.0.1:${METRICS_PORTS[$i]}" \
    -data-dir "$RUN/raft-$id" \
    >"$RUN/coord-$id.log" 2>&1 &
  echo $! >"$RUN/coord-$id.pid"
done

if ! wait_for_leader 25; then
  bad "no leader after 25s — see $RUN/coord-*.log"
  exit 1
fi
ok "leader elected: node $(leader_node)"

say "starting $WORKERS worker(s), concurrency $CONCURRENCY"
for n in $(seq 1 "$WORKERS"); do
  "$BIN/worker" -id "w-$n" -coordinator "$COORDS" \
    -concurrency "$CONCURRENCY" -data-dir "$RUN/data-w-$n" \
    >"$RUN/worker-$n.log" 2>&1 &
  echo $! >"$RUN/worker-$n.pid"
done

# Registration is a committed command, so it is worth waiting for rather than
# sleeping on: a demo that submits before any worker is registered measures
# the broker blocking, not dispatch.
deadline=$(( $(date +%s) + 15 ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  n=$(jsonget "http://127.0.0.1:${METRICS_PORTS[0]}/debug/workers" "len(d['workers'])")
  [ "${n:-0}" -ge "$WORKERS" ] && break
  sleep 0.2
done
ok "$(jsonget "http://127.0.0.1:${METRICS_PORTS[0]}/debug/workers" "len(d['workers'])") worker(s) registered"
