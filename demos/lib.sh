#!/usr/bin/env bash
# Shared plumbing for the two demos.
#
# Everything a demo writes goes under demos/run/, which is gitignored and
# deleted by down.sh. Nothing here writes outside the repository.
#
# No jq: the debug endpoints are JSON, and python3 ships with macOS while jq
# does not. A demo that needs a package installed is a demo that does not run.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="$ROOT/demos/run"
BIN="$ROOT/bin"

GRPC_PORTS=(8081 8082 8083)
RAFT_PORTS=(7001 7002 7003)
METRICS_PORTS=(9091 9092 9093)

PEERS="1=http://127.0.0.1:7001,2=http://127.0.0.1:7002,3=http://127.0.0.1:7003"
API_PEERS="1=127.0.0.1:8081,2=127.0.0.1:8082,3=127.0.0.1:8083"
COORDS="127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083"

# Colours, but only onto a terminal: a redirected log should stay readable.
if [ -t 1 ]; then
  B=$'\033[1m'; DIM=$'\033[2m'; G=$'\033[32m'; Y=$'\033[33m'; R=$'\033[31m'; Z=$'\033[0m'
else
  B=''; DIM=''; G=''; Y=''; R=''; Z=''
fi

say()  { printf '%s\n' "${B}$*${Z}"; }
note() { printf '%s\n' "${DIM}$*${Z}"; }
ok()   { printf '%s\n' "${G}$*${Z}"; }
warn() { printf '%s\n' "${Y}$*${Z}"; }
bad()  { printf '%s\n' "${R}$*${Z}"; }

# jsonget <url> <python expression over `d`>
#
# Prints the expression's value, or nothing if the endpoint is unreachable.
# Runs python with -I so nothing is imported from the working directory.
jsonget() {
  local url="$1" expr="$2"
  curl -s --max-time 2 "$url" 2>/dev/null | python3 -I -c "
import json,sys
raw = sys.stdin.read()
if not raw.strip():
    sys.exit(0)
d = json.loads(raw)
print($expr)
" 2>/dev/null
}

# leader_node -> prints the node id (1..3) that believes it is leader, or nothing.
#
# Asks every node rather than trusting one: during an election two nodes can
# disagree, and the demo should show that rather than hide it.
leader_node() {
  local i
  for i in 0 1 2; do
    if [ "$(jsonget "http://127.0.0.1:${METRICS_PORTS[$i]}/debug/workers" "d['is_leader']")" = "True" ]; then
      printf '%s\n' "$((i + 1))"
      return 0
    fi
  done
  return 1
}

# wait_for_leader <seconds>
wait_for_leader() {
  local deadline=$(( $(date +%s) + ${1:-20} ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if leader_node >/dev/null; then return 0; fi
    sleep 0.2
  done
  return 1
}

coord_pid() { cat "$RUN/coord-$1.pid" 2>/dev/null; }
alive()     { [ -n "${1:-}" ] && kill -0 "$1" 2>/dev/null; }

# live_metrics -> the base URL of a node that answers, or nothing.
#
# Never hardcode node 1's port after a demo that kills a coordinator: node 1
# may BE the corpse, every read silently returns empty, and the demo reports
# "0 tasks terminal" about a cluster that finished the job correctly.
live_metrics() {
  local i url
  for i in 0 1 2; do
    url="http://127.0.0.1:${METRICS_PORTS[$i]}"
    if curl -s --max-time 1 -o /dev/null "$url/debug/workers" 2>/dev/null; then
      printf '%s\n' "$url"
      return 0
    fi
  done
  return 1
}

# assignments_on <node id 1..3> -> that node's sched_assignments_total.
#
# This counter is LEADER-ONLY: a follower never increments it, and a new
# leader starts its own at zero because the counter lives in the process, not
# in the log. So it must be read from a NAMED node, and after a failover the
# meaningful question is "has the new leader dispatched anything yet", not
# "has the number gone up" — the old leader's count died with it.
#
# Reading it from "any live node" silently answers 0 forever, which is how the
# first version of this demo reported no dispatch for twenty seconds while the
# survivor was dispatching fine.
assignments_on() {
  local idx=$(( $1 - 1 )) v
  v=$(curl -s --max-time 2 "http://127.0.0.1:${METRICS_PORTS[$idx]}/metrics" 2>/dev/null \
    | awk '/^sched_assignments_total /{print int($2); exit}')
  printf '%s\n' "${v:-0}"
}

require_bin() {
  local b
  for b in coordinator worker schedctl loadgen; do
    if [ ! -x "$BIN/$b" ]; then
      bad "missing $BIN/$b — run: make bin"
      exit 1
    fi
  done
}
