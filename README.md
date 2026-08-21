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
