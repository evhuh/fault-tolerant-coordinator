#!/usr/bin/env bash
# Stop everything a demo started and delete its state.
#
# Kills by RECORDED PID, never by name. `pkill -f` cost this project a cycle
# once already: the pattern did not match how the harness launched the
# process, so stale workers kept heartbeating into the next measurement and
# the register_worker count climbed after "killing all workers".

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

shopt -s nullglob
for f in "$RUN"/*.pid; do
  pid=$(cat "$f" 2>/dev/null)
  if alive "$pid"; then
    kill -9 "$pid" 2>/dev/null
  fi
  rm -f "$f"
done

# Report leaks rather than assuming none: anything still bound to a demo port
# will make the next run look broken for no visible reason.
leaked=0
for p in "${GRPC_PORTS[@]}" "${METRICS_PORTS[@]}" "${RAFT_PORTS[@]}"; do
  if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then
    warn "port $p still has a listener"
    leaked=1
  fi
done

if [ "${1:-}" = "--keep-logs" ]; then
  rm -rf "$RUN"/raft-* "$RUN"/data-*
  note "logs kept in $RUN"
else
  rm -rf "$RUN"
fi

[ "$leaked" -eq 0 ] && ok "stopped, nothing left listening" || bad "stopped, but see the warnings above"
