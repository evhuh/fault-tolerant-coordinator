# Fault-tolerant Coordinator
Task scheduler

### spike/counter module on 3 nodes to test this loop:
```
for {
    select {
    case <-ticker.C:
        node.Tick()                       // drives election + heartbeat timeouts
    case rd := <-node.Ready():
        saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)  // 1. PERSIST
        send(rd.Messages)                                     // 2. then send
        applyToStateMachine(rd.CommittedEntries)              // 3. then apply
        node.Advance()                                        // 4. ask for more
    }
}
```

Want to kill leaders and watch new one continue from commmitted prefix
1. Start 3 nodes
2. Check they agree
3. Find a kill the leader
4. Watch survivors recover
5. Resurrect dead node from its disk (WAL)
6. Check it caught up

```
for i in 1 2 3; do ./counter -id $i > node$i.log 2>&1 & done
jobs -l

tail -3 node1.log node2.log node3.log   # check they're the same

grep -h "became leader" node*.log       # highest term wins if there are several lines
kill -9 <that node's PID from jobs -l>

grep "became leader" node*.log          # a new line, higher term, from a survivor
tail -3 <the two surviving logs>

./counter -id <N> >> node<N>.log 2>&1 &

tail -5 node<N>.log

pkill -9 -f './counter -id'
```

## Delivery semantics: what "effectively-once" covers, and what it does not

Delivery is **at-least-once**.
A task may be executed more than once, and under fault injection it regularly is: a worker that goes slow or is descheduled loses its lease, the coordinator reclaims the task and dispatches the next attempt, and the first attempt keeps running.
Two workers executing one task at the same time is this system working as designed, not a defect.

Effects are **effectively-once, for effects routed through the result tree**.
Each attempt writes to its own directory, `<workerDir>/<taskID>/<attempt>/`, and reports `(taskID, attempt)` back.
The state machine accepts a report only when the attempt it names is still the live one, so exactly one attempt's result is ever committed, and a late report naming a dead attempt is a silent no-op on every replica.
That is what makes the *committed* effect unique even though the *executions* are not.

**External side effects run at-least-once.**
If a task's payload posts to a third-party API, sends an email, or writes to a database the coordinator knows nothing about, that happens once per execution — so twice if two attempts execute.
Nothing here can prevent it: the payload's writes are outside the result tree, and the coordinator learns about an attempt only when it reports.
Preventing it would require **fencing** — the payload presenting a monotonic token that the downstream system checks and rejects when stale — which this system does not implement and does not claim.
A payload that needs single-execution semantics must be idempotent on `FTC_TASK_ID`, which the executor passes in its environment alongside `FTC_ATTEMPT`.

The simulator measures both halves of this and reports them, because a run that never overlapped did not test the claim at all: see `Ledger.Overlaps` (executions that ran simultaneously) and `Ledger.StaleRejected` (late reports that reached a coordinator and were discarded).
A zero in either is reported as INCONCLUSIVE rather than as a pass.
