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
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/evazheng/fault-tolerant-coordinator/internal/clock"
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

	// SnapshotEntries is how many applied entries trigger a snapshot. Without
	// one the log grows forever and restart replay grows with uptime, which
	// is the thing that makes snapshots non-optional for an always-on
	// service rather than a nicety.
	SnapshotEntries uint64
	// SnapshotCatchUpEntries are kept after compaction so a slightly lagging
	// follower can still be caught up by shipping entries rather than a whole
	// snapshot.
	SnapshotCatchUpEntries uint64

	// StallThreshold is how long the Ready loop may spend processing one
	// batch before it is reported as stalled. Zero derives one from the
	// election timeout.
	//
	// It MUST be strictly below ElectionTick × TickInterval, so it fires
	// before the symptom it exists to catch. A loop that stalls past the
	// election timeout stops ticking AND stops sending heartbeats, so the
	// cluster elects around it — and the stall self-heals once whatever it
	// was waiting on times out, leaving an unexplained election and no
	// evidence.
	StallThreshold time.Duration

	// Clock reads wall time. Nil means the real one.
	Clock clock.Clock
	// Transport carries raft messages between nodes. Nil means HTTP, which is
	// what every real deployment uses; the simulator supplies an in-memory
	// one it can drop, delay and partition.
	Transport Transport
}

func DefaultConfig() Config {
	return Config{
		TickInterval:           100 * time.Millisecond,
		ElectionTick:           10,
		HeartbeatTick:          1,
		SnapshotEntries:        10_000,
		SnapshotCatchUpEntries: 1_000,
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

	// Stalled fires when the Ready loop spends longer than StallThreshold
	// inside one batch.
	//
	// This is a CONTRACT GUARD, not a recovery mechanism. onLeadershipChanged
	// and Apply both run on that goroutine, and neither may block on anything
	// the goroutine itself serves — waiting on a proposal to commit, or on a
	// scheduler worker that is waiting on a proposal. Such a cycle self-heals
	// after a timeout, which is exactly what makes it hard to find: the
	// symptom is an unexplained election, not a hang.
	//
	// The simulator treats a stall as an invariant violation and fails the
	// run with its seed.
	Stalled func(time.Duration)

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

	// Snapshot serializes the state machine. Called when enough entries have
	// been applied to be worth compacting.
	Snapshot func() ([]byte, error)

	// Restore REPLACES the state machine from a snapshot. Called on startup
	// when a snapshot exists on disk, and when the leader ships one to a
	// follower that fell too far behind to catch up from the log.
	//
	// Without it, compaction is a correctness bug rather than a feature: a
	// lagging follower would receive a snapshot it silently dropped and stay
	// stuck forever.
	Restore func([]byte) error
}

type Node struct {
	cfg     Config
	h       Handler
	node    raft.Node
	storage *raft.MemoryStorage
	wal     *wal
	tp      Transport
	clk     clock.Clock

	mu       sync.RWMutex
	leaderID uint64
	isLeader bool

	stopc chan struct{}
	done  chan struct{}

	// Indices driving compaction. Only the Ready loop WRITES these.
	// appliedIndex is mirrored into appliedAtomic for readers outside the
	// loop — the invariant checker needs it to compare two replicas at the
	// SAME log position, which is the only comparison that means anything.
	appliedIndex  uint64
	snapshotIndex uint64
	confState     *raftpb.ConfState

	appliedAtomic atomic.Uint64

	// processingSince is the unix-nano instant the Ready loop began handling
	// the current batch, or 0 when idle.
	//
	// IDLE IS NOT STALLED. The loop spends most of its life parked in a
	// select waiting for work, and a watchdog that could not tell the two
	// apart would fire constantly. Only time spent INSIDE a batch counts.
	processingSince atomic.Int64

	fatalOnce sync.Once
	fatalErr  error
}

// AppliedIndex is the highest log index this node has applied.
func (n *Node) AppliedIndex() uint64 { return n.appliedAtomic.Load() }

// Start brings up the node and runs the Ready loop in the background.
//
// Prefer New + Run when the caller has fields to publish before the loop can
// observe them: Start launches the goroutine immediately, so anything the
// handler touches must already be in place.
func Start(cfg Config, h Handler) (*Node, error) {
	n, err := New(cfg, h)
	if err != nil {
		return nil, err
	}
	n.Run()
	return n, nil
}

// New brings up the node WITHOUT starting the Ready loop.
//
// The split exists because of a real race: the Ready loop calls back into the
// handler immediately, and a caller that assigns `node` to a field *after*
// Start returns has already lost — the loop can be applying entries and
// reading that still-nil field. Construct, publish, then Run.
func New(cfg Config, h Handler) (*Node, error) {
	selfURL, ok := cfg.Peers[cfg.ID]
	if !ok {
		return nil, fmt.Errorf("raftnode: node id %d is not in the peer set", cfg.ID)
	}

	// Bring-up order matters: the WAL opens FIRST, and the replayed state
	// seeds MemoryStorage, which is raft's in-process read-back copy of the
	// log. Raft calls back into storage mid-decision (to re-send entries to a
	// lagging follower), and it cannot wait on a disk read.
	// The snapshot comes FIRST and sets the base the WAL continues from.
	// Reading them in the other order means not knowing which entries the
	// snapshot already covers.
	snap, err := loadSnapshot(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("raftnode: load snapshot: %w", err)
	}
	var baseIndex uint64
	if snap != nil && snap.GetMetadata() != nil {
		baseIndex = snap.GetMetadata().GetIndex()
	}

	w, hs, ents, hadState, err := openWAL(cfg.DataDir, baseIndex)
	if err != nil {
		return nil, fmt.Errorf("raftnode: open wal: %w", err)
	}

	storage := raft.NewMemoryStorage()
	if snap != nil {
		if err := storage.ApplySnapshot(snap); err != nil {
			return nil, fmt.Errorf("raftnode: apply snapshot: %w", err)
		}
		if h.Restore != nil {
			if err := h.Restore(snap.GetData()); err != nil {
				return nil, fmt.Errorf("raftnode: restore state machine: %w", err)
			}
		}
		hadState = true // a snapshot IS a past: restart, never bootstrap
	}
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

	tp := cfg.Transport
	if tp == nil {
		tp = newHTTPTransport(cfg.ID, cfg.Peers)
	}
	n := &Node{
		cfg: cfg, h: h, storage: storage, wal: w,
		tp:            tp,
		clk:           clock.OrReal(cfg.Clock),
		stopc:         make(chan struct{}),
		done:          make(chan struct{}),
		appliedIndex:  baseIndex,
		snapshotIndex: baseIndex,
	}
	n.appliedAtomic.Store(baseIndex)

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

	if err := n.tp.Listen(selfURL, n.node.Step); err != nil {
		n.node.Stop()
		_ = w.Close()
		return nil, fmt.Errorf("raftnode: listen %s: %w", selfURL, err)
	}

	return n, nil
}

// Run starts the Ready loop and its stall watchdog. Call it once, after the
// caller has published whatever the handler needs.
func (n *Node) Run() {
	go n.run()
	go n.watchStalls()
}

// stallThreshold is the configured value, or a derived one strictly below the
// election timeout.
//
// Half the election timeout: late enough that ordinary batch work never trips
// it, early enough to fire before the cluster starts electing around this
// node.
//
// THERE IS A FLOOR THIS CANNOT GO BELOW, and it is not a time the config
// controls. A batch does real disk I/O — a WAL fsync, and periodically a
// snapshot plus log compaction, measured at 23-40ms on an SSD. Disk I/O does
// NOT compress when the rest of the timing budget does, so an election
// timeout squeezed below roughly 4x the snapshot cost derives a threshold
// that the node trips just by doing its job.
//
// The simulator hit exactly this: at a 100ms election timeout the derived 50ms
// threshold sat under the fsync floor, and legitimate snapshots looked like
// stalls. The budget has to stay above the hardware, not just internally
// consistent.
func (n *Node) stallThreshold() time.Duration {
	if n.cfg.StallThreshold > 0 {
		return n.cfg.StallThreshold
	}
	election := time.Duration(n.cfg.ElectionTick) * n.cfg.TickInterval
	if election <= 0 {
		return time.Second
	}
	return election / 2
}

// watchStalls reports a Ready loop that has been inside one batch too long.
//
// It reports ONCE per stall rather than every poll: a stall that lasts
// seconds would otherwise bury its own first report.
func (n *Node) watchStalls() {
	threshold := n.stallThreshold()
	ticker := n.clk.NewTicker(threshold / 2)
	defer ticker.Stop()

	var reportedFor int64
	for {
		select {
		case <-n.stopc:
			return
		case <-n.done:
			return
		case <-ticker.C():
			since := n.processingSince.Load()
			if since == 0 {
				reportedFor = 0 // idle, not stalled
				continue
			}
			if since == reportedFor {
				continue // already reported this one
			}
			if elapsed := time.Since(time.Unix(0, since)); elapsed > threshold {
				reportedFor = since
				log.Printf("raftnode: node %d READY LOOP STALLED for %s (threshold %s) — "+
					"something on that goroutine is blocking on work the goroutine itself serves",
					n.cfg.ID, elapsed.Round(time.Millisecond), threshold)
				if n.h.Stalled != nil {
					n.h.Stalled(elapsed)
				}
			}
		}
	}
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
	defer n.tp.Close()
	defer n.node.Stop()
	defer func() { _ = n.wal.Close() }()

	ticker := n.clk.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopc:
			// Leadership is given up explicitly on the way out, so the
			// coordinator tears down its leader-only components rather than
			// leaving a scheduler running against a dead node.
			n.setLeadership(raft.None, false)
			return

		case <-ticker.C():
			n.node.Tick() // feeds raft its time units: elections and heartbeats

		case rd := <-n.node.Ready():
			if !n.handleReady(rd) {
				return
			}
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

// maybeSnapshot takes a snapshot and compacts the log once enough entries
// have been applied.
//
// Order is load-bearing throughout: serialize, make the snapshot durable,
// and only then drop the entries it covers. Compacting first would leave a
// window where a crash loses entries nothing else holds.
func (n *Node) maybeSnapshot() error {
	if n.h.Snapshot == nil || n.cfg.SnapshotEntries == 0 {
		return nil
	}
	if n.appliedIndex < n.snapshotIndex+n.cfg.SnapshotEntries {
		return nil
	}

	data, err := n.h.Snapshot()
	if err != nil {
		return fmt.Errorf("serialize state machine: %w", err)
	}

	// CreateSnapshot records the membership in effect at this index, so a
	// node restored from it knows who its peers are.
	snap, err := n.storage.CreateSnapshot(n.appliedIndex, n.confState, data)
	if err != nil {
		if err == raft.ErrSnapOutOfDate {
			return nil // a newer snapshot already covers this; nothing to do
		}
		return fmt.Errorf("create snapshot: %w", err)
	}
	if err := saveSnapshot(n.cfg.DataDir, snap); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}

	// Keep a tail of entries so a slightly lagging follower can still be
	// repaired by shipping entries, which is far cheaper than shipping the
	// whole state machine.
	compactIndex := n.appliedIndex
	if compactIndex > n.cfg.SnapshotCatchUpEntries {
		compactIndex -= n.cfg.SnapshotCatchUpEntries
	} else {
		compactIndex = 0
	}
	if compactIndex > 0 {
		if err := n.storage.Compact(compactIndex); err != nil && err != raft.ErrCompacted {
			return fmt.Errorf("compact: %w", err)
		}
		if err := n.wal.compact(n.cfg.DataDir, compactIndex); err != nil {
			return fmt.Errorf("compact wal: %w", err)
		}
	}

	n.snapshotIndex = n.appliedIndex
	log.Printf("raftnode: node %d snapshotted at index %d (%d bytes), log compacted to %d",
		n.cfg.ID, n.appliedIndex, len(data), compactIndex)
	return nil
}

// handleReady processes one Ready batch. It returns false when the node must
// stop.
//
// Split out of the select purely so processingSince is cleared by a DEFER.
// The watchdog reads that field to tell "inside a batch" from "idle", and
// every fatal path in here returns early — so clearing it only at the bottom
// left it set forever on a halted node, and the watchdog then reported an
// ever-growing stall for a loop that was not running at all. A phantom
// violation is worse than none: it trains you to ignore the detector.
func (n *Node) handleReady(rd raft.Ready) bool {
	n.processingSince.Store(time.Now().UnixNano())
	defer n.processingSince.Store(0)

	// Leadership first, so the coordinator has started or stopped its
	// leader-only parts before any of this batch is applied.
	if rd.SoftState != nil {
		n.setLeadership(rd.SoftState.Lead, rd.SoftState.RaftState == raft.StateLeader)
	}

	// 0. INCOMING SNAPSHOT. The leader ships one when this node has
	// fallen further behind than its log can repair. Dropping it —
	// which is what ignoring rd.Snapshot does — leaves the node stuck
	// forever, so this must exist before compaction is ever enabled.
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := saveSnapshot(n.cfg.DataDir, rd.Snapshot); err != nil {
			n.fatal(fmt.Errorf("save incoming snapshot: %w", err))
			return false
		}
		if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
			n.fatal(fmt.Errorf("apply incoming snapshot: %w", err))
			return false
		}
		if n.h.Restore != nil {
			if err := n.h.Restore(rd.Snapshot.GetData()); err != nil {
				n.fatal(fmt.Errorf("restore from incoming snapshot: %w", err))
				return false
			}
		}
		idx := rd.Snapshot.GetMetadata().GetIndex()
		n.appliedIndex, n.snapshotIndex = idx, idx
		n.appliedAtomic.Store(idx)
		log.Printf("raftnode: node %d restored from leader snapshot at index %d", n.cfg.ID, idx)
	}

	// 1. PERSIST — WAL (fsync'd) before MemoryStorage, and before any
	// message goes out.
	// A failure here means this node cannot keep its durability
	// promise. It stops rather than continuing as a liar — but it
	// stops ALONE, so a peer sharing the process survives.
	if err := n.wal.Save(rd.HardState, rd.Entries); err != nil {
		n.fatal(fmt.Errorf("wal save: %w", err))
		return false
	}
	if rd.HardState != nil && !raft.IsEmptyHardState(rd.HardState) {
		if err := n.storage.SetHardState(rd.HardState); err != nil {
			n.fatal(fmt.Errorf("set hardstate: %w", err))
			return false
		}
	}
	if err := n.storage.Append(rd.Entries); err != nil {
		n.fatal(fmt.Errorf("append: %w", err))
		return false
	}

	// 2. SEND — only now, because the entries these messages claim are
	// durable actually are.
	for _, m := range rd.Messages {
		go n.tp.Send(m)
	}

	// 3. APPLY
	for _, ent := range rd.CommittedEntries {
		switch ent.GetType() {
		case raftpb.EntryConfChange:
			cc := &raftpb.ConfChange{}
			if err := proto.Unmarshal(ent.GetData(), cc); err != nil {
				n.fatal(fmt.Errorf("unmarshal conf change: %w", err))
				return false
			}
			// Kept because CreateSnapshot needs the membership that
			// was in effect at the snapshot index.
			n.confState = n.node.ApplyConfChange(cc)
		case raftpb.EntryNormal:
			if len(ent.GetData()) > 0 {
				n.h.Apply(ent.GetData())
			}
			// An empty entry is a new leader's no-op. It still
			// ADVANCES the index: skipping it here would make the
			// snapshot claim coverage it does not have.
		}
		if idx := ent.GetIndex(); idx > n.appliedIndex {
			n.appliedIndex = idx
			n.appliedAtomic.Store(idx)
		}
	}

	// 3b. COMPACT once enough has been applied to be worth it.
	if err := n.maybeSnapshot(); err != nil {
		n.fatal(err)
		return false
	}

	// 4. ADVANCE — raft holds the next Ready until this call.
	n.node.Advance()
	return true
}
