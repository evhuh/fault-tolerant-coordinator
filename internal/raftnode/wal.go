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
// ents[i] holds the entry at raft index i+1: with no compaction the log always
// starts at index 1. Day 7 changes that, and this is the function that has to
// learn about a snapshot's base index.
func openWAL(dir string) (w *wal, hs *raftpb.HardState, ents []*raftpb.Entry, hadState bool, err error) {
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
			// A later record at an index we already hold is a leader-forced
			// overwrite: drop the old suffix, then append.
			if n := int(e.GetIndex()) - 1; n >= 0 && n < len(ents) {
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
