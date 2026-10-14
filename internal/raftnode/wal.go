package raftnode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// The WAL is an append-only file of length-prefixed records.
//
//	[4-byte big-endian length][1 type byte][proto payload]
//
// Two record types: a log entry, and a HardState. Promoted unchanged from the
// Day 1 spike — the storage contract etcd/raft requires did not need to
// change to host a coordinator rather than a counter.
const (
	recEntry     byte = 1
	recHardState byte = 2
)

type wal struct {
	f *os.File
}

// openWAL replays the file into memory and truncates any torn tail.
//
// baseIndex is the snapshot index this log continues from: everything at or
// below it lives in the snapshot, not here. Entries at or below it are
// dropped, so ents[0] is always index baseIndex+1.
//
// Before compaction existed this was implicitly 0 and ents[i] was index i+1.
// Hardcoding that assumption is exactly what breaks the first time a log is
// truncated, which is why the base is a parameter rather than a constant.
func openWAL(dir string, baseIndex uint64) (w *wal, hs *raftpb.HardState, ents []*raftpb.Entry, hadState bool, err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, nil, false, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, nil, nil, false, err
	}

	var validEnd int64 // offset after the last complete, parseable record
	r := &countingReader{r: f}
replay:
	for {
		payload, typ, rerr := readRecord(r)
		if rerr != nil {
			// io.EOF is a clean end; anything else is a torn final record from
			// a crash mid-write. Either way: stop, and truncate the tail.
			break
		}
		switch typ {
		case recEntry:
			e := &raftpb.Entry{}
			if proto.Unmarshal(payload, e) != nil {
				break replay
			}
			hadState = true
			validEnd = r.n
			// Already covered by the snapshot; the state machine has it.
			if e.GetIndex() <= baseIndex {
				continue
			}
			// A later record at an index we already hold is a leader-forced
			// overwrite: drop the old suffix, then append.
			if n := int(e.GetIndex()-baseIndex) - 1; n >= 0 && n < len(ents) {
				ents = ents[:n]
			}
			ents = append(ents, e)
			continue
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
	if err := f.Truncate(validEnd); err != nil {
		return nil, nil, nil, false, err
	}
	if _, err := f.Seek(validEnd, io.SeekStart); err != nil {
		return nil, nil, nil, false, err
	}
	return &wal{f: f}, hs, ents, hadState, nil
}

// Save persists a Ready's HardState and Entries.
//
// It MUST fsync before returning. The Ready loop sends messages immediately
// after this call, and "persist before send" is only a durability guarantee if
// persist means DURABLE: a message implying "I have this entry" must never
// precede the entry hitting disk, or a crash makes this node a liar and Raft's
// safety argument collapses.
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

func (w *wal) Close() error { return w.f.Close() }

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

// countingReader tracks bytes consumed, so replay knows where the last
// complete record ended.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// compact rewrites the log to hold only entries ABOVE baseIndex.
//
// Called after a snapshot is durable, never before: the entries being dropped
// are only safe to drop because the snapshot that covers them is already on
// disk. A crash between the two costs replay time, not correctness.
//
// Written to a temp file and renamed, so a crash mid-rewrite leaves the old
// complete log rather than a truncated one.
func (w *wal) compact(dir string, baseIndex uint64) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var keep []*raftpb.Entry
	var hs *raftpb.HardState
	r := &countingReader{r: w.f}
	for {
		payload, typ, err := readRecord(r)
		if err != nil {
			break
		}
		switch typ {
		case recEntry:
			e := &raftpb.Entry{}
			if proto.Unmarshal(payload, e) != nil {
				break
			}
			if e.GetIndex() > baseIndex {
				keep = append(keep, e)
			}
		case recHardState:
			h := &raftpb.HardState{}
			if proto.Unmarshal(payload, h) == nil {
				hs = h
			}
		}
	}

	var buf bytes.Buffer
	for _, e := range keep {
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

	tmp := filepath.Join(dir, "wal.log.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, "wal.log")); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}

	// Swap in a handle on the rewritten file, positioned to append.
	_ = w.f.Close()
	nf, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.f = nf
	return nil
}
