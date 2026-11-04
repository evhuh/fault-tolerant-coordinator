#!/usr/bin/env python3
"""Analyse the reclaim and failover trials, with validity decided in code.

RETROSPECTIVE VALIDITY. The 100-trial runs predate the explicit
kill-took-effect checks, so validity is re-derived from what the rows record:

  (a) The reclaim must have happened at all. A trial whose task was never
      released timed out, which is exactly the symptom of a stale worker
      renewing the lease.
  (b) reclaim_from_last_heartbeat_ms must be at least the lease TTL. The
      lease cannot lapse before it expires, so a value below the TTL means
      t=0 was wrong.
  (c) It must not exceed the TTL by more than a generous allowance. If a
      process other than the one killed had renewed the lease, the true last
      heartbeat would be LATER than the recorded t=0, inflating this figure.
      The pkill bug would have produced timeouts rather than large values,
      so this is the weaker of the three, and it is applied as INVALID
      rather than quietly dropped.

Host validity is separate: a trial whose bracketing probes left the budget is
EXCLUDED, with the count reported.
"""
import json, os, sys, statistics as st

LEGACY = False
LEASE_TTL_MS = 2000
TICK_MS = 200
BUDGET_MS = LEASE_TTL_MS + TICK_MS + 50   # lease + one tick + commit
HB_MS = 500

def pct(xs, p):
    if not xs: return None
    xs = sorted(xs)
    if len(xs) == 1: return xs[0]
    k = (len(xs) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)

def host_ok(row):
    for key in ("probe_before", "probe_after"):
        p = row.get(key) or {}
        if p.get("within_budget") is False:
            return False, f"{key}: {p.get('budget_violation')}"
    return True, ""

def load(path):
    """Read a run's rows, REFUSING inputs that do not carry the run's id.

    An analyzer that reads a shared directory cannot tell which run it is
    looking at, and neither can its reader: one stall analysis divided a
    four-run count by a one-run denominator because the coordinator logs were
    append-mode in a shared path. So the run directory is the scope, and an
    input from outside it is refused rather than silently included.
    """
    d = os.path.dirname(os.path.abspath(path))
    marker = os.path.join(d, "RUN_ID")
    if os.path.exists(marker):
        want = ""
        for line in open(marker):
            if line.startswith("run_id="):
                want = line.strip().split("=", 1)[1]
        print(f"[run {want}] reading {os.path.basename(path)} from {d}")
    elif "--allow-legacy" in sys.argv:
        # EXPLICIT, and the output says so. A flat file cannot be attributed
        # to a run, and an analysis that cannot name its run has already
        # reported a four-run stall count against a one-run denominator.
        global LEGACY
        LEGACY = True
        print(f"[LEGACY, UNSCOPED] {path} has no RUN_ID; read only because --allow-legacy "
              f"was passed. Figures below cannot be attributed to a single run.")
    else:
        print(f"REFUSED: {path} has no RUN_ID beside it, so this analysis cannot say which "
              f"run produced it. Move the artifacts into a run-stamped directory, or pass "
              f"--allow-legacy to read it anyway (the output will be stamped unscoped).",
              file=sys.stderr)
        sys.exit(2)
    return [json.loads(l) for l in open(path) if l.strip()]

def reclaim(path):
    rows = load(path)
    invalid, excluded, good = [], [], []
    for r in rows:
        if r.get("invalid"):
            invalid.append((r["trial"], r["invalid"])); continue
        if r.get("excluded"):
            invalid.append((r["trial"], "setup: " + r["excluded"])); continue
        fk, fh = r.get("reclaim_from_kill_ms"), r.get("reclaim_from_last_heartbeat_ms")
        if fk is None or fh is None:
            invalid.append((r["trial"], "task never released; the kill may not have taken effect"))
            continue
        if fh < LEASE_TTL_MS:
            invalid.append((r["trial"], f"reclaim {fh:.0f}ms from last heartbeat is inside the "
                                        f"{LEASE_TTL_MS}ms lease; t=0 is wrong")); continue
        if fh > LEASE_TTL_MS + 4 * HB_MS:
            invalid.append((r["trial"], f"reclaim {fh:.0f}ms from last heartbeat exceeds the TTL by "
                                        f"more than 4 heartbeats; another process may have renewed")); continue
        ok, why = host_ok(r)
        if not ok:
            excluded.append((r["trial"], why)); continue
        # A reclaim with no reassignment is LAWFUL when the task went
        # terminal on its attempt budget: there was nothing left to dispatch.
        # Distinguished from a reassignment that never came, because calling
        # the first a failure would understate the system.
        if r.get("reassign_from_kill_ms") is None:
            m = (r.get("timeout_state") or {}).get("metrics", "")
            if 'sched_queue_depth{state="pending"} 0' in m:
                r["_reassign_na"] = "task terminal by attempt budget"
        good.append(r)

    print(f"=== WORKER RECLAIM LATENCY ===")
    print(f"trials: {len(rows)} run, {len(good)} valid, {len(invalid)} INVALID, {len(excluded)} excluded on host probes")
    for t, why in invalid[:8]: print(f"   INVALID trial {t}: {why}")
    if len(invalid) > 8: print(f"   ... and {len(invalid)-8} more")
    for t, why in excluded[:5]: print(f"   excluded trial {t}: {why}")
    if not good: return
    for label, key in (("from LAST HEARTBEAT (budget ~%dms)" % BUDGET_MS, "reclaim_from_last_heartbeat_ms"),
                       ("from the KILL (user-visible)", "reclaim_from_kill_ms"),
                       ("reassigned, from LAST HEARTBEAT", "reassign_from_last_heartbeat_ms"),
                       ("reassigned, from the KILL", "reassign_from_kill_ms")):
        xs = [r[key] for r in good if r.get(key) is not None]
        if not xs: continue
        print(f"  {label:42s} n={len(xs):3d} p50={pct(xs,50):7.0f}ms p95={pct(xs,95):7.0f}ms "
              f"max={max(xs):7.0f}ms (observed)")
    na = [r["trial"] for r in good if r.get("_reassign_na")]
    if na:
        print(f"  reassignment not applicable on {len(na)} trial(s) (task terminal by attempt "
              f"budget, nothing left to dispatch): trials {na[:10]}")
    bud = [r["reclaim_from_last_heartbeat_ms"] for r in good]
    over = [x for x in bud if x > BUDGET_MS]
    # NOT "N/N within budget". The worst case sits at the budget's edge and
    # the measurement carries +/-10ms of poll error, so a pass/fail count
    # claims a precision the method does not have.
    print(f"  consistent with the ~{BUDGET_MS}ms budget (lease {LEASE_TTL_MS} + tick {TICK_MS} "
          f"+ commit); worst case {max(bud):.0f}ms at its edge, +/-10ms poll error"
          + (f"; {len(over)} trial(s) above it" if over else ""))
    slots = [r.get("free_slots_at_release") for r in good if r.get("free_slots_at_release") is not None]
    print(f"  free slots at release: min {min(slots)}, median {st.median(slots):.0f} "
          f"({sum(1 for s in slots if s == 0)} trial(s) had none)")
    half = len(good) // 2
    a = [r["reclaim_from_kill_ms"] for r in good[:half]]
    b = [r["reclaim_from_kill_ms"] for r in good[half:]]
    print(f"  trial-index drift: first half p50 {pct(a,50):.0f}ms, second half p50 {pct(b,50):.0f}ms")
    print(f"  measurement error: +/-10ms, the state poll interval")

def failover(path):
    rows = load(path)
    invalid, excluded, good = [], [], []
    for r in rows:
        if r.get("invalid"): invalid.append((r["trial"], r["invalid"])); continue
        if r.get("excluded"): invalid.append((r["trial"], "setup: " + r["excluded"])); continue
        if r.get("failover_to_first_dispatch_ms") is None:
            invalid.append((r["trial"], "no dispatch observed within 30s")); continue
        ok, why = host_ok(r)
        if not ok: excluded.append((r["trial"], why)); continue
        good.append(r)
    print(f"\n=== LEADER FAILOVER LATENCY ===")
    print(f"trials: {len(rows)} run, {len(good)} valid, {len(invalid)} INVALID, {len(excluded)} excluded on host probes")
    for t, why in invalid[:8]: print(f"   INVALID trial {t}: {why}")
    if not good: return
    xs = [r["failover_to_first_dispatch_ms"] for r in good]
    print(f"  kill -> first task dispatched        n={len(xs):3d} p50={pct(xs,50):7.0f}ms "
          f"p95={pct(xs,95):7.0f}ms max={max(xs):7.0f}ms (observed)")
    rb = [r["broker_rebuild_ms"] for r in good if r.get("broker_rebuild_ms") is not None]
    if rb:
        print(f"  of which BROKER REBUILD              n={len(rb):3d} p50={pct(rb,50):7.2f}ms "
              f"p95={pct(rb,95):7.2f}ms max={max(rb):7.2f}ms (observed)")
        print(f"  so election + first plan commit      p50 ~{pct(xs,50)-pct(rb,50):.0f}ms")
    half = len(good) // 2
    print(f"  trial-index drift: first half p50 {pct(xs[:half],50):.0f}ms, "
          f"second half p50 {pct(xs[half:],50):.0f}ms")
    nl = {}
    for r in good: nl[r.get("new_leader")] = nl.get(r.get("new_leader"), 0) + 1
    print(f"  new leader distribution: {nl}")
    print(f"  measurement error: +/-10ms, the metrics poll interval")

def stamp():
    if LEGACY:
        print("\n*** LEGACY, UNSCOPED: at least one input carried no run id, so nothing "
              "above can be attributed to a single run. ***")


if __name__ == "__main__":
    # Flags are not paths. Passing --allow-legacy used to be read as the
    # reclaim file's name.
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    reclaim(args[0] if len(args) > 0 else "/tmp/ftc-bench/reclaim-trials.jsonl")
    failover(args[1] if len(args) > 1 else "/tmp/ftc-bench/failover-trials.jsonl")
    stamp()
