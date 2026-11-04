#!/usr/bin/env python3
"""Profile a failover at LARGE state, to name the degradation mechanism.

The unbounded run showed dispatch flat to ~12,600 task records and then
rising to 17s by ~34,200. Broker rebuild grew too, but only 28ms of a 15,400ms
increase — 0.18% — so it is evidence that state grew, not the cause.

Both survivors are profiled from just BEFORE the kill, because the expensive
work may be in the election rather than in anything the new leader does after
it, and we do not know in advance which node wins.
"""
import json, signal, subprocess, time, urllib.request

D = "/tmp/ftc-bench"
NODES = [1, 2, 3]
ADDRS = ",".join(f"127.0.0.1:808{i}" for i in NODES)
TARGET = 30000

def sh(c): return subprocess.run(c, shell=True, capture_output=True, text=True)

def leader():
    for i in NODES:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:909{i}/debug/workers", timeout=2) as r:
                if json.load(r).get("is_leader"): return i
        except Exception: pass
    return None

def tasks():
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:909{leader() or 1}/debug/tasks", timeout=5) as r:
            d = json.load(r)
            return sum(j.get("pending",0)+j.get("assigned",0)+j.get("running",0)
                       +j.get("complete",0)+j.get("failed",0) for j in (d.get("jobs") or []))
    except Exception: return -1

def start_node(i):
    peers = ",".join(f"{n}=http://127.0.0.1:700{n}" for n in NODES)
    apis = ",".join(f"{n}=127.0.0.1:808{n}" for n in NODES)
    return subprocess.Popen(
        [f"{D}/coordinator","-id",str(i),"-addr",f"127.0.0.1:808{i}","-peers",peers,
         "-api-peers",apis,"-data-dir",f"{D}/raft-{i}","-metrics-addr",f"127.0.0.1:909{i}",
         "-max-plan-size","100","-snapshot-entries","500000"],
        stdout=open(f"{D}/pf-coord-{i}.log","a"), stderr=subprocess.STDOUT)

sh('pkill -x worker'); sh('pkill -x coordinator'); sh(f"{D}/down.sh")
for i in NODES: sh(f"rm -rf {D}/raft-{i}")
time.sleep(1)
nodes = {i: start_node(i) for i in NODES}
time.sleep(6)
true_bin = sh("command -v true").stdout.strip() or "/usr/bin/true"
workers = [subprocess.Popen(
    [f"{D}/worker","-id",f"pw-{k}","-coordinator",ADDRS,"-concurrency","8","-data-dir",f"{D}/pw-{k}"],
    stdout=open(f"{D}/pf-worker-{k}.log","a"), stderr=subprocess.STDOUT) for k in range(4)]
time.sleep(3)

print(f"building state toward {TARGET} task records...", flush=True)
while True:
    n = tasks()
    print(f"  tasks in store: {n}", flush=True)
    if n >= TARGET or n < 0: break
    sh(f"{D}/loadgen -addrs {ADDRS} -prefill 50 -tasks-per-job 60 -command {true_bin} "
       f"-submitters 8 >/dev/null 2>&1")
    time.sleep(2)
state = tasks()
print(f"state built: {state} task records", flush=True)

old = leader()
survivors = [i for i in NODES if i != old]
print(f"leader is {old}; profiling survivors {survivors} for 25s across the kill", flush=True)
profs = [subprocess.Popen(
    ["curl","-s",f"127.0.0.1:909{i}/debug/pprof/profile?seconds=25",
     "-o",f"{D}/cpu-failover-{state}-node{i}.pprof"]) for i in survivors]
time.sleep(1)
nodes[old].send_signal(signal.SIGKILL); nodes[old].wait()
t0 = time.time()
base = {}
for i in survivors:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:909{i}/metrics", timeout=2) as r:
            base[i] = sum(float(l.split()[-1]) for l in r.read().decode().splitlines()
                          if l.startswith("sched_assignments_total "))
    except Exception: base[i] = 0.0
newlead = None
while time.time() - t0 < 60 and newlead is None:
    for i in survivors:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:909{i}/metrics", timeout=2) as r:
                v = sum(float(l.split()[-1]) for l in r.read().decode().splitlines()
                        if l.startswith("sched_assignments_total "))
            if v > base.get(i, 0): newlead = i; break
        except Exception: pass
    time.sleep(0.05)
print(f"first dispatch after {1000*(time.time()-t0):.0f}ms, new leader {newlead}", flush=True)
for p in profs: p.wait()
for p in workers:
    try: p.send_signal(signal.SIGKILL)
    except Exception: pass
sh('pkill -x worker')
for i in NODES:
    try: nodes[i].send_signal(signal.SIGKILL)
    except Exception: pass
sh(f"{D}/down.sh")
print(f"profiles: {D}/cpu-failover-{state}-node*.pprof  (state={state})", flush=True)
