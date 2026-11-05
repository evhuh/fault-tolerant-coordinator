# Shared setup for the demo scripts. Builds the three binaries into a scratch
# directory, starts a coordinator and N workers there, and cleans up on exit.
set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${RUN_DIR:-$(mktemp -d)}"
ADDR="${ADDR:-localhost:18080}"
PORT="${ADDR##*:}"

cleanup() { pkill -x coordinator 2>/dev/null; pkill -x worker 2>/dev/null; }
trap cleanup EXIT

build() {
  ( cd "$REPO" && go build -o "$RUN_DIR/coordinator" ./cmd/coordinator \
      && go build -o "$RUN_DIR/worker" ./cmd/worker \
      && go build -o "$RUN_DIR/schedctl" ./cmd/schedctl ) || exit 1
  cd "$RUN_DIR"
}

start_coordinator() {
  cleanup; sleep 0.5
  ./coordinator -addr ":$PORT" > coord.log 2>&1 &
  sleep 0.8
  # Fail loudly: without this, workers silently attach to a stale coordinator
  # still holding the port, and every result afterwards is meaningless.
  grep -q "listening on" coord.log || { echo "FATAL: coordinator did not start:"; cat coord.log; exit 1; }
}

# start_workers N [concurrency]
start_workers() {
  local n=$1 conc=${2:-1}
  WORKER_PIDS=""
  for i in $(seq 1 "$n"); do
    ./worker -id "w-$i" -coordinator "$ADDR" -concurrency "$conc" > "w$i.log" 2>&1 &
    WORKER_PIDS="$WORKER_PIDS $!"
  done
  sleep 0.8
}

ctl() { ./schedctl -coordinator "$ADDR" "$@"; }
