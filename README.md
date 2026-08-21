# fault-tolerant-coordinator
Task scheduler

spike/counter module to test...
1. Start 3 nodes
2. Check they agree
3. Find a kill the leader
4. Watch survivors recover
5. Resurrect dead node from its disk (WAL)
6. Check it caught up

```