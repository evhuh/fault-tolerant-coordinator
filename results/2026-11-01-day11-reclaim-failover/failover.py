#!/usr/bin/env python3
"""Leader failover latency: SIGKILL the leader, 100 trials.

t=0 is the kill. t=1 is the FIRST TASK DISPATCHED by the new leader, i.e. the
first assignment committed after the kill, read from sched_assignments_total
across the survivors.

The workload is deliberately NOT the reclaim experiment's. Fast tasks and a
continuously refilled backlog, so the first post-kill dispatch is bounded by
election + broker rebuild + one plan commit, and never by waiting for a slot
to free. With sleep-30 tasks and busy workers the number would measure slot
availability instead.

Broker rebuild is reported SEPARATELY from sched_failover_latency_seconds,
which is LeadershipChange.Duration: the time inside establishLeadership, the
rebuild included. The breakdown is election + rebuild + first plan commit,
with the middle term measured and the others by subtraction.
"""
import json, os, signal, subprocess, time, urllib.request, re

D = "/tmp/ftc-bench"
NODES = [1, 2, 3]
ADDRS = ",".join(f"127.0.0.1:808{i}" for i in NODES)
TRIALS = int(os.environ.get("TRIALS", "100"))
POLL = 0.01
# A stated minimum, asserted per trial rather than assumed.
MIN_BACKLOG = 50
# Backlog ceiling, and a cluster reset cadence.
#
# Terminal tasks are never collected from committed state, so state grows with
# THROUGHPUT and not merely with backlog: 100 trials at 360 tasks each put
# ~36,000 task records in the store, and failover degraded monotonically with
# it — dispatch p50 1602ms to 17060ms, broker rebuild 1.3ms to 29.4ms, since
# the rebuild scans that state. Measuring 100 trials against one ever-growing
# cluster measures state size, not failover.
#
# So: bound the backlog, and rebuild the cluster from empty every RESET_EVERY
# trials so each block is measured at a comparable state size. The trial index
# is recorded either way, so any residual drift stays visible.
BACKLOG_CEILING = 200
RESET_EVERY = int(os.environ.get("RESET_EVERY","10"))

def sh(cmd): return subprocess.run(cmd, shell=True, capture_output=True, text=True)

def scrape(i, timeout=2):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:909{i}/metrics", timeout=timeout) as r:
            return r.read().decode()
    except Exception:
        return ""

def metric(text, name):
    tot = 0.0
    for line in text.splitlines():
        m = re.match(rf'^{name}(?:\{{[^}}]*\}})?\s+([0-9.e+-]+)$', line)
        if m: tot += float(m.group(1))
    return tot

def leader():
    for i in NODES:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:909{i}/debug/workers", timeout=2) as r:
                if json.load(r).get("is_leader"): return i
        except Exception:
            pass
    return None

def probe():
    out = subprocess.run([f"{D}/fsyncprobe", "-json", "-n", "20", "-dir", D],
                         capture_output=True, text=True)
    try: return json.loads(out.stdout)
    except Exception: return {"error": out.stderr.strip()[:200]}

def start_node(i):
    peers = ",".join(f"{n}=http://127.0.0.1:700{n}" for n in NODES)
    apis = ",".join(f"{n}=127.0.0.1:808{n}" for n in NODES)
    return subprocess.Popen(
        [f"{D}/coordinator", "-id", str(i), "-addr", f"127.0.0.1:808{i}",
         "-peers", peers, "-api-peers", apis, "-data-dir", f"{D}/raft-{i}",
         "-metrics-addr", f"127.0.0.1:909{i}", "-max-plan-size", "100",
         "-snapshot-entries", os.environ.get("SNAPENTRIES","500000")],
        stdout=open(f"{D}/fo-coord-{i}.log", "a"), stderr=subprocess.STDOUT)

def backlog():
    """Pending tasks across all jobs on the leader, or -1 if unreadable."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:909{leader() or 1}/debug/tasks",
                                    timeout=3) as r:
            d = json.load(r)
            return sum(j.get("pending", 0) for j in (d.get("jobs") or []))
    except Exception:
        return -1


def total_tasks():
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:909{leader() or 1}/debug/tasks",
                                    timeout=3) as r:
            d = json.load(r)
            return sum(j.get("pending", 0) + j.get("assigned", 0) + j.get("running", 0)
                       + j.get("complete", 0) + j.get("failed", 0)
                       for j in (d.get("jobs") or []))
    except Exception:
        return -1


def main():
    # See reclaim.py: pkill -f on a path does not match ./worker, so stale
    # workers from an earlier run survive and keep heartbeating.
    leaked = sh('pgrep -x worker').stdout.split()
    if leaked:
        print(f"WARNING: {len(leaked)} worker(s) leaked by this run: {leaked}", flush=True)
    sh('pkill -x worker')
    sh(f"{D}/down.sh")
    time.sleep(1)
    for i in NODES: sh(f"rm -rf {D}/raft-{i}")
    nodes = {i: start_node(i) for i in NODES}
    time.sleep(6)
    true_bin = sh("command -v true").stdout.strip() or "/usr/bin/true"
    workers = [subprocess.Popen(
        [f"{D}/worker", "-id", f"fw-{k}", "-coordinator", ADDRS, "-concurrency", "8",
         "-data-dir", f"{D}/fw-{k}"],
        stdout=open(f"{D}/fo-worker-{k}.log", "a"), stderr=subprocess.STDOUT)
        for k in range(4)]
    time.sleep(3)

    for trial in range(1, TRIALS + 1):
        if trial > 1 and (trial - 1) % RESET_EVERY == 0:
            # FRESH CLUSTER. Not a tidiness step: without it the later trials
            # measure a store holding tens of thousands of terminal tasks.
            for p in workers:
                try:
                    p.send_signal(signal.SIGKILL)
                except Exception:
                    pass
            sh('pkill -x worker')
            for i in NODES:
                try:
                    nodes[i].send_signal(signal.SIGKILL)
                    nodes[i].wait()
                except Exception:
                    pass
                sh(f"rm -rf {D}/raft-{i}")
            nodes = {i: start_node(i) for i in NODES}
            time.sleep(6)
            workers = [subprocess.Popen(
                [f"{D}/worker", "-id", f"fw-{k}", "-coordinator", ADDRS,
                 "-concurrency", "8", "-data-dir", f"{D}/fw-{k}"],
                stdout=open(f"{D}/fo-worker-{k}.log", "a"), stderr=subprocess.STDOUT)
                for k in range(4)]
            time.sleep(3)
            print(f"  [reset cluster before trial {trial}]", flush=True)
        # CONTINUOUS BACKLOG of fast tasks, topped up every trial so the new
        # leader always has something to dispatch immediately.
        # UNBOUNDED=1 restores the original, degrading workload: submit a
        # fixed batch every trial regardless of backlog. Kept so the finding
        # can be reproduced rather than only described.
        pending_now = backlog()
        if os.environ.get("UNBOUNDED") == "1":
            sh(f"{D}/loadgen -addrs {ADDRS} -prefill 6 -tasks-per-job 60 "
               f"-command {true_bin} -submitters 8 >/dev/null 2>&1")
        elif pending_now < MIN_BACKLOG:
            need = max(1, (BACKLOG_CEILING - pending_now) // 60)
            sh(f"{D}/loadgen -addrs {ADDRS} -prefill {need} -tasks-per-job 60 "
               f"-command {true_bin} -submitters 8 >/dev/null 2>&1")
        time.sleep(1.0)
        # PRECONDITION: a real backlog. The new leader can only be timed to
        # its first dispatch if there is something to dispatch, and the
        # earlier 100-trial run was lost to a backlog that had drained to
        # nothing while every top-up reported success.
        pending = 0
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:909{leader() or 1}/debug/tasks",
                                        timeout=3) as r:
                for j in (json.load(r).get("jobs") or []):
                    pending += j.get("pending", 0)
        except Exception:
            pending = -1
        if pending < MIN_BACKLOG:
            row = {"trial": trial, "pending_before": pending,
                   "invalid": f"precondition: pending backlog {pending} is below the "
                              f"{MIN_BACKLOG} minimum, so there may be nothing to dispatch"}
            with open(f"{D}/failover-trials.jsonl", "a") as f:
                f.write(json.dumps(row) + "\n")
            print(f"trial {trial}/{TRIALS}: INVALID — backlog {pending} < {MIN_BACKLOG}", flush=True)
            continue

        old = leader()
        if old is None:
            with open(f"{D}/failover-trials.jsonl", "a") as f:
                f.write(json.dumps({"trial": trial, "excluded": "no leader before the kill"}) + "\n")
            continue
        before = probe()
        survivors = [i for i in NODES if i != old]
        base = {i: metric(scrape(i), "sched_assignments_total") for i in survivors}
        base_fo = {i: metric(scrape(i), "sched_failover_latency_seconds_sum") for i in survivors}
        base_fc = {i: metric(scrape(i), "sched_failover_latency_seconds_count") for i in survivors}

        killed_pid = nodes[old].pid
        nodes[old].send_signal(signal.SIGKILL)
        nodes[old].wait()
        t0 = time.time() * 1000

        # PROVE THE KILL. A coordinator still serving its gRPC port would
        # make "failover" a measurement of nothing, and the reclaim arm's
        # pkill bug is the reason this is checked rather than assumed.
        # NOT named `survivors`: that is the list of surviving NODES, and
        # shadowing it with this pid list made the detection loop iterate an
        # empty sequence, so 100 trials reported no dispatch while the new
        # leader was in fact assigning hundreds of tasks.
        leftover_pids = sh(f'pgrep -f "[-]id {old} -addr"').stdout.split()
        if leftover_pids:
            row = {"trial": trial, "killed_node": old, "killed_pid": killed_pid,
               "pending_before": pending,
                   "invalid": f"a coordinator with -id {old} survived the kill: pids {leftover_pids}"}
            with open(f"{D}/failover-trials.jsonl", "a") as f:
                f.write(json.dumps(row) + "\n")
            print(f"trial {trial}/{TRIALS}: INVALID — node {old} survived the kill", flush=True)
            nodes[old] = start_node(old)
            time.sleep(2)
            continue

        t1 = None
        new_leader = None
        deadline = time.time() + 30
        while time.time() < deadline:
            for i in survivors:
                txt = scrape(i, timeout=1)
                if not txt: continue
                if metric(txt, "sched_assignments_total") > base[i]:
                    t1 = time.time() * 1000
                    new_leader = i
                    fo_sum = metric(txt, "sched_failover_latency_seconds_sum") - base_fo[i]
                    fo_cnt = metric(txt, "sched_failover_latency_seconds_count") - base_fc[i]
                    break
            if t1: break
            time.sleep(POLL)

        after = probe()
        # A TIMEOUT MUST EXPLAIN ITSELF, same rule as the reclaim harness.
        timeout_state = None
        if t1 is None:
            alive, who_leads, counters = [], None, {}
            for i in NODES:
                txt = scrape(i, timeout=2)
                if not txt:
                    continue
                alive.append(i)
                counters[i] = {
                    "assignments": metric(txt, "sched_assignments_total"),
                    "pending": metric(txt, "sched_queue_depth"),
                    "plans": metric(txt, "sched_plan_size_count"),
                    "elections": metric(txt, "raft_leader_elections_total"),
                }
            who_leads = leader()
            timeout_state = {"alive_nodes": alive, "leader_now": who_leads,
                             "baseline_assignments": base, "counters": counters}
        row = {"trial": trial, "killed_node": old, "killed_pid": killed_pid,
               "pending_before": pending, "tasks_in_store": total_tasks(),
               "new_leader": new_leader, "timeout_state": timeout_state,
               "failover_to_first_dispatch_ms": (t1 - t0) if t1 else None,
               "broker_rebuild_ms": (1000 * fo_sum / fo_cnt) if t1 and fo_cnt else None,
               "probe_before": before, "probe_after": after}
        with open(f"{D}/failover-trials.jsonl", "a") as f:
            f.write(json.dumps(row) + "\n")
        print(f"trial {trial}/{TRIALS}: killed={old} new={new_leader} "
              f"dispatch={row['failover_to_first_dispatch_ms']} "
              f"rebuild={row['broker_rebuild_ms']}", flush=True)

        nodes[old] = start_node(old)
        # Wait for 3-node health before the next trial, so each trial starts
        # from the same cluster rather than from the last one's damage.
        hdeadline = time.time() + 30
        while time.time() < hdeadline:
            if sum(1 for i in NODES if scrape(i, timeout=1)) == 3: break
            time.sleep(0.2)
        time.sleep(1.0)

    for p in workers:
        try: p.send_signal(signal.SIGKILL)
        except Exception: pass
    sh('pkill -x worker')
    sh(f"{D}/down.sh")
    for i in NODES:
        try: nodes[i].send_signal(signal.SIGKILL)
        except Exception: pass
    print(f"wrote {D}/failover-trials.jsonl")

main()
