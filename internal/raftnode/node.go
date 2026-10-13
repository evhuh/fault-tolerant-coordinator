// Package raftnode hosts etcd/raft: it implements the storage, transport, and
// apply contract the library requires, and nothing else.
//
// This is the Day 1 spike promoted into the product. The library implements
// the algorithm and hands back a Ready struct; everything in this package is
// the host side of that contract:
//
//	persist → send → apply → Advance
//
// The ordering IS the durability guarantee. A message implying "I have this
// entry" must never precede the entry hitting disk.
package raftnode

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Config describes one node and its fixed cluster. Membership changes are out
// of scope, so the peer set is configured at startup and never moves.
type Config struct {
	ID      uint64
	Peers   map[uint64]string // id -> raft transport URL, including self
	DataDir string

	// TickInterval defines the TIME UNIT for ElectionTick and HeartbeatTick,
	// which etcd/raft counts in ticks rather than durations. At 100ms and
	// ElectionTick 10, an election timeout is ~1s.
	TickInterval  time.Duration
	ElectionTick  int
	HeartbeatTick int
}

func DefaultConfig() Config {
	return Config{
		TickInterval:  100 * time.Millisecond,
		ElectionTick:  10,
		HeartbeatTick: 1,
	}
}

// Handler is what the coordinator plugs into the Ready loop.
type Handler struct {
	// Apply is called for every committed entry, on EVERY node, in log order.
	// It must be deterministic: this is the apply path, so no wall clock, no
	// randomness, no map-order dependence.
	Apply func(data []byte)

	// LeadershipChanged fires when this node gains or loses leadership. The
	// coordinator uses it to start and stop the leader-only components —
	// stopping cleanly is what prevents split-brain scheduling.
	LeadershipChanged func(isLeader bool)

	// Fatal reports an unrecoverable node error: a WAL write that failed, or
	// storage that rejected an entry raft already durably logged. This node
	// can no longer honour the contract, so it STOPS.
	//
	// It is a callback rather than a log.Fatal because the right response
	// depends on who is hosting. A standalone binary should exit. But the
	// cluster tests and Day 8's simulator run every node in ONE PROCESS, and
	// there one node's disk error must not take down its peers and the
	// harness with it — you would end up debugging the wrong failure.
	//
	// If nil, the node stops silently; the host notices via Done.
	Fatal func(error)
}

type Node struct {
	cfg     Config
	h       Handler
	node    raft.Node
	storage *raft.MemoryStorage
	wal     *wal
	tp      *Transport

	mu       sync.RWMutex
	leaderID uint64
	isLeader bool

	stopc chan struct{}
	done  chan struct{}

	fatalOnce sync.Once
	fatalErr  error
}

// Start brings up the node and runs the Ready loop in the background.
func Start(cfg Config, h Handler) (*Node, error) {
	selfURL, ok := cfg.Peers[cfg.ID]
	if !ok {
		return nil, fmt.Errorf("raftnode: node id %d is not in the peer set", cfg.ID)
	}

	// Bring-up order matters: the WAL opens FIRST, and the replayed state
	// seeds MemoryStorage, which is raft's in-process read-back copy of the
	// log. Raft calls back into storage mid-decision (to re-send entries to a
	// lagging follower), and it cannot wait on a disk read.
	w, hs, ents, hadState, err := openWAL(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("raftnode: open wal: %w", err)
	}

	storage := raft.NewMemoryStorage()
	if hadState {
		if hs != nil && !raft.IsEmptyHardState(hs) {
			if err := storage.SetHardState(hs); err != nil {
				return nil, err
			}
		}
		if err := storage.Append(ents); err != nil {
			return nil, err
		}
	}

	rc := &raft.Config{
		ID:              cfg.ID,
		ElectionTick:    cfg.ElectionTick,
		HeartbeatTick:   cfg.HeartbeatTick,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          &raft.DefaultLogger{Logger: log.New(log.Writer(), "raft ", log.LstdFlags)},
	}

	n := &Node{
		cfg: cfg, h: h, storage: storage, wal: w,
		tp:    newTransport(cfg.Peers),
		stopc: make(chan struct{}),
		done:  make(chan struct{}),
	}

	// Fork on whether this node has a past.
	//
	//	restart      → RestartNode, no peers: the membership is already in the log
	//	fresh start  → StartNode with bootstrap entries recording membership
	if hadState {
		n.node = raft.RestartNode(rc)
	} else {
		bootstrap := make([]raft.Peer, 0, len(cfg.Peers))
		for pid := range cfg.Peers {
			bootstrap = append(bootstrap, raft.Peer{ID: pid})
		}
		n.node = raft.StartNode(rc, bootstrap)
	}

	if err := n.tp.listen(selfURL, n.node.Step); err != nil {
		n.node.Stop()
		_ = w.Close()
		return nil, fmt.Errorf("raftnode: listen %s: %w", selfURL, err)
	}

	go n.run()
	return n, nil
}

// Propose submits a command. Returning nil means ACCEPTED INTO THE PIPELINE,
// not committed — commitment is only observable when the entry re-emerges
// through CommittedEntries and reaches Handler.Apply.
//
// A follower's proposal is forwarded to the leader automatically.
func (n *Node) Propose(ctx context.Context, data []byte) error {
	return n.node.Propose(ctx, data)
}

// Leader reports the current leader's id, and whether one is known. During an
// election there is none, and callers must say so rather than guess.
func (n *Node) Leader() (uint64, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID, n.leaderID != raft.None
}

// LeaderAddr maps the leader's id through the peer set.
func (n *Node) LeaderAddr() (string, bool) {
	id, ok := n.Leader()
	if !ok {
		return "", false
	}
	addr, ok := n.cfg.Peers[id]
	return addr, ok
}

func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.isLeader
}

// fatal records an unrecoverable error and stops THIS node. It never exits
// the process: see Handler.Fatal.
func (n *Node) fatal(err error) {
	n.fatalOnce.Do(func() {
		n.fatalErr = err
		log.Printf("raftnode: node %d fatal: %v", n.cfg.ID, err)
		if n.h.Fatal != nil {
			n.h.Fatal(err)
		}
	})
}

// Err reports the error that stopped this node, if any.
func (n *Node) Err() error { return n.fatalErr }

// Done closes when the Ready loop has exited, for whatever reason.
func (n *Node) Done() <-chan struct{} { return n.done }

func (n *Node) Stop() {
	select {
	case <-n.stopc:
		return // already stopping
	default:
		close(n.stopc)
	}
	<-n.done
}

// run is the Ready loop: the whole contract this package exists to implement.
func (n *Node) run() {
	defer close(n.done)
	defer n.tp.close()
	defer n.node.Stop()
	defer func() { _ = n.wal.Close() }()

	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopc:
			// Leadership is given up explicitly on the way out, so the
			// coordinator tears down its leader-only components rather than
			// leaving a scheduler running against a dead node.
			n.setLeadership(raft.None, false)
			return

		case <-ticker.C:
			n.node.Tick() // feeds raft its time units: elections and heartbeats

		case rd := <-n.node.Ready():
			// Leadership first, so the coordinator has started or stopped its
			// leader-only parts before any of this batch is applied.
			if rd.SoftState != nil {
				n.setLeadership(rd.SoftState.Lead, rd.SoftState.RaftState == raft.StateLeader)
			}

			// 1. PERSIST — WAL (fsync'd) before MemoryStorage, and before any
			// message goes out.
			// A failure here means this node cannot keep its durability
			// promise. It stops rather than continuing as a liar — but it
			// stops ALONE, so a peer sharing the process survives.
			if err := n.wal.Save(rd.HardState, rd.Entries); err != nil {
				n.fatal(fmt.Errorf("wal save: %w", err))
				return
			}
			if rd.HardState != nil && !raft.IsEmptyHardState(rd.HardState) {
				if err := n.storage.SetHardState(rd.HardState); err != nil {
					n.fatal(fmt.Errorf("set hardstate: %w", err))
					return
				}
			}
			if err := n.storage.Append(rd.Entries); err != nil {
				n.fatal(fmt.Errorf("append: %w", err))
				return
			}

			// 2. SEND — only now, because the entries these messages claim are
			// durable actually are.
			for _, m := range rd.Messages {
				go n.tp.send(m)
			}

			// 3. APPLY
			for _, ent := range rd.CommittedEntries {
				switch ent.GetType() {
				case raftpb.EntryConfChange:
					cc := &raftpb.ConfChange{}
					if err := proto.Unmarshal(ent.GetData(), cc); err != nil {
						n.fatal(fmt.Errorf("unmarshal conf change: %w", err))
						return
					}
					n.node.ApplyConfChange(cc)
				case raftpb.EntryNormal:
					if len(ent.GetData()) == 0 {
						continue // new-leader no-op, committing the leader's term
					}
					n.h.Apply(ent.GetData())
				}
			}

			// 4. ADVANCE — raft holds the next Ready until this call.
			n.node.Advance()
		}
	}
}

func (n *Node) setLeadership(leaderID uint64, isLeader bool) {
	n.mu.Lock()
	changed := n.isLeader != isLeader
	n.leaderID = leaderID
	n.isLeader = isLeader
	n.mu.Unlock()

	if changed && n.h.LeadershipChanged != nil {
		n.h.LeadershipChanged(isLeader)
	}
}
