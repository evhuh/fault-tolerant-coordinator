# spike/counter — superseded, kept as history

**This is not live code.** It is a separate Go module (`module counter`), so
`go build ./...` and `go test ./...` at the repo root do not compile or test
it. Nothing in the coordinator imports it.

It was the first exercise: a three-node replicated counter, built to get the
`etcd/raft` `Ready` contract right before a real state machine could make
failures ambiguous.

When Raft was wired into the coordinator, its three pieces were promoted into
[`internal/raftnode`](../../internal/raftnode/) essentially unchanged — the
contract etcd/raft requires did not care that the state machine grew from an
integer into a coordinator:

| Here | Promoted to |
|---|---|
| `openWAL` / `Save`, framed records, torn-tail truncation | `internal/raftnode/wal.go` |
| `startReceiver` / `send`, drop-on-corrupt | `internal/raftnode/transport.go` |
| the `Ready` loop: persist → send → apply → Advance | `internal/raftnode/node.go` |

Two things changed in the promotion, both because a cluster now runs inside a
single process (the tests, and the simulator):

- `log.Fatalf` on a WAL or storage error became `Handler.Fatal`, so one node's
  failure halts that node rather than the whole process.
- Leadership transitions are surfaced to the host, which is what drives
  `establishLeadership` / `revokeLeadership`.

Delete it whenever the history stops being useful.
