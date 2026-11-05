package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// tickInterval : defines the time unit for ElectionTick/HeartbeatTick
const tickInterval = 100 * time.Millisecond // one tick = 100ms of wall time




// WAL: an append-only file of length-prefixed records
// Record framing:  [4-byte big-endian length][1 type byte][proto payload]
// Two record types: entry, and hardstate
const (
	recEntry     byte = 1
	recHardState byte = 2
)

type wal struct {
	f *os.File
}

func openWAL(dir string) (w *wal, hs *raftpb.HardState, ents []*raftpb.Entry, hadState bool, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, nil, false, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, nil, false, err
	}

	// Replay. ents[i] holds the entry at raft index i+1 (no compaction in this spike, so the log always starts at index 1)
	var validEnd int64 // file offset after the last complete, parseable record
	r := &countingReader{r: f}
replay:
	for {
		payload, typ, rerr := readRecord(r)
		if rerr != nil {
			// io.EOF is a clean end; anything else is a torn final record
			// from a crash mid-write. Either way: stop, truncate the tail.
			break
		}
		switch typ {
		case recEntry:
			e := &raftpb.Entry{}
			if proto.Unmarshal(payload, e) != nil {
				break replay
			}
			// A later record at an index we already hold is a leader-forced overwrite: drop the old suffix, then append
			if n := int(e.GetIndex()) - 1; n < len(ents) {
				ents = ents[:n]
			}
			ents = append(ents, e)
		case recHardState:
			h := &raftpb.HardState{}
			if proto.Unmarshal(payload, h) != nil {
				break replay
			}
			hs = h
		}
		hadState = true
		validEnd = r.n
	}
	// Discard any torn tail so future appends start at a record boundary
	if err := f.Truncate(validEnd); err != nil {
		return nil, nil, nil, false, err
	}
	if _, err := f.Seek(validEnd, io.SeekStart); err != nil {
		return nil, nil, nil, false, err
	}
	return &wal{f: f}, hs, ents, hadState, nil
}

// Save persists a Ready's HardState and Entries
// It MUST fsync before returning: the Ready loop sends messages immediately after this call, and "persist before send" is only true if persist means durable
func (w *wal) Save(hs *raftpb.HardState, ents []*raftpb.Entry) error {
	var buf bytes.Buffer
	for _, e := range ents {
		b, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		writeRecord(&buf, recEntry, b)
	}
	if hs != nil && !raft.IsEmptyHardState(hs) {
		b, err := proto.Marshal(hs)
		if err != nil {
			return err
		}
		writeRecord(&buf, recHardState, b)
	}
	if buf.Len() == 0 {
		return nil
	}
	if _, err := w.f.Write(buf.Bytes()); err != nil {
		return err
	}
	return w.f.Sync()
}

func writeRecord(buf *bytes.Buffer, typ byte, payload []byte) {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)+1))
	buf.Write(hdr[:])
	buf.WriteByte(typ)
	buf.Write(payload)
}

func readRecord(r io.Reader) (payload []byte, typ byte, err error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, 0, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return nil, 0, fmt.Errorf("zero-length record")
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, 0, err
	}
	return body[1:], body[0], nil
}

// countingReader tracks how many bytes have been consumed, so replay knows
// where the last complete record ended.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}



// Transport: fire-and-forget HTTP POSTs of marshaled raftpb.Messages
// No retries, no acks, no ordering (Raft tolerates drop, reorder, and duplication), BUT never Step() a corrupt message
func startReceiver(node raft.Node, selfURL string) {
	// Dedicated mux + server, NOT http.HandleFunc/DefaultServeMux: the global mux would panic on double registration if two nodes ever share a process (the coordinator's simulator runs the whole cluster in one binary).
	mux := http.NewServeMux()
	mux.HandleFunc("/raft", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		m := &raftpb.Message{}
		if proto.Unmarshal(body, m) != nil {
			return // corrupt → drop, never Step
		}
		node.Step(r.Context(), m)
	})
	u, err := url.Parse(selfURL)
	if err != nil {
		log.Fatalf("bad self url %q: %v", selfURL, err)
	}
	srv := &http.Server{Addr: u.Host, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("listen %s: %v", u.Host, err)
		}
	}()
}



func send(peerURL string, m *raftpb.Message) {
	b, err := proto.Marshal(m)
	if err != nil {
		return
	}
	resp, err := http.Post(peerURL+"/raft", "application/octet-stream", bytes.NewReader(b))
	if err != nil {
		return // peer down or partitioned; raft's own protocol retries
	}
	resp.Body.Close()
}

func parsePeers(s string) map[uint64]string {
	out := map[uint64]string{}
	for _, kv := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			log.Fatalf("bad peer spec %q, want id=url", kv)
		}
		pid, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			log.Fatalf("bad peer id %q: %v", k, err)
		}
		out[pid] = v
	}
	return out
}



// Main: bring-up, a proposal source, and the Ready loop.
func main() {
	// Mock
	id := flag.Uint64("id", 1, "this node's raft ID")
	peerSpec := flag.String("peers",
		"1=http://127.0.0.1:9021,2=http://127.0.0.1:9022,3=http://127.0.0.1:9023",
		"comma-separated id=url list, including this node")
	dir := flag.String("dir", "", "data directory for the WAL (default: ./data-<id>)")
	flag.Parse()

	peers := parsePeers(*peerSpec)
	if _, ok := peers[*id]; !ok {
		log.Fatalf("node id %d not present in -peers", *id)
	}
	dataDir := *dir
	if dataDir == "" {
		dataDir = "data-" + strconv.FormatUint(*id, 10)
	}

	w, hs, ents, hadState, err := openWAL(dataDir)
	if err != nil {
		log.Fatalf("open wal: %v", err)
	}

	// Seed raft's in-memory read-back copy of the log from the durable WAL
	storage := raft.NewMemoryStorage()
	if hadState {
		if hs != nil && !raft.IsEmptyHardState(hs) {
			storage.SetHardState(hs)
		}
		storage.Append(ents)
	}

	cfg := &raft.Config{
		ID:            *id,
		ElectionTick:  10, // ~1s at tickInterval (100ms ticker == ~1s election timeout)
		HeartbeatTick: 1,  // ~100ms; must be well below ElectionTick
		Storage:       storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		Logger:          &raft.DefaultLogger{Logger: log.Default()},
	}

	var node raft.Node
	// (Case A) Restart: the peer set is already recorded in the persisted log, so this constructor takes no peers (use WAL)
	if hadState {
		node = raft.RestartNode(cfg)
	// (Case B) Fresh cluster: bootstrap entries record the membership
	} else {
		bootstrap := make([]raft.Peer, 0, len(peers))
		for pid := range peers {
			bootstrap = append(bootstrap, raft.Peer{ID: pid})
		}
		node = raft.StartNode(cfg, bootstrap)
	}
	defer node.Stop()

	startReceiver(node, peers[*id])

	// SIMULATE WORK
	// Proposal source: every node proposes +1 once a second (followers forward proposals to the leader automatically)
	// NOTE: propose returning nil means "accepted into the pipeline", NOT "committed" (commitment is only observable when the entry comes back out via CommittedEntries)
	go func() {
		delta := make([]byte, 8)
		binary.BigEndian.PutUint64(delta, 1)
		for range time.Tick(1 * time.Second) {
			node.Propose(context.TODO(), delta)
		}
	}()

	// The Ready loop
	// Contract: persist → send → apply → advance
	var counter uint64
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			node.Tick() // feed raft its time units (drives election and heartbeats)

		case rd := <-node.Ready():
			// 1. PERSIST to WAL (fsync'd), then MemoryStorage
			if err := w.Save(rd.HardState, rd.Entries); err != nil {
				log.Fatalf("wal save: %v", err)
			}
			if rd.HardState != nil && !raft.IsEmptyHardState(rd.HardState) {
				storage.SetHardState(rd.HardState)
			}
			storage.Append(rd.Entries)

			// 2. SEND
			for _, m := range rd.Messages {
				go send(peers[m.GetTo()], m)
			}

			// 3. APPLY (entries raft has decided are committed)
			for _, ent := range rd.CommittedEntries {
				switch ent.GetType() {
				case raftpb.EntryConfChange:
					// Bootstrap membership entries from StartNode. Raft does not consider the change in effect until the state machine confirms it applied
					cc := &raftpb.ConfChange{}
					if err := proto.Unmarshal(ent.GetData(), cc); err != nil {
						log.Fatalf("unmarshal conf change: %v", err)
					}
					node.ApplyConfChange(cc)
				case raftpb.EntryNormal:
					if len(ent.GetData()) == 0 {
						continue // new-leader no-op; commits the leader's term
					}
					counter += binary.BigEndian.Uint64(ent.GetData())
					fmt.Printf("node %d: counter = %d (index %d)\n", *id, counter, ent.GetIndex())
				}
			}

			// 4. ADVANCE (done with this batch, Raft holds next Ready until this call)
			node.Advance()
		}
	}
}

