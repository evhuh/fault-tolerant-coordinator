package raftnode

import (
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Snapshots on disk.
//
// A snapshot is written to a temp file, fsync'd, and renamed into place, so a
// crash mid-write leaves either the old snapshot or the new one — never a
// half-written file that replay would try to trust.
//
// The WAL is only truncated AFTER the snapshot is durable. A crash between
// the two costs replay time, not correctness: the old entries are still
// there, and replaying them over a restored snapshot is idempotent because
// the apply path is.
const snapshotFile = "snapshot.pb"

func snapshotPath(dir string) string { return filepath.Join(dir, snapshotFile) }

// saveSnapshot writes a raftpb.Snapshot durably.
func saveSnapshot(dir string, snap *raftpb.Snapshot) error {
	b, err := proto.Marshal(snap)
	if err != nil {
		return err
	}
	tmp := snapshotPath(dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
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
	// Rename is atomic within a directory: readers see one file or the other.
	if err := os.Rename(tmp, snapshotPath(dir)); err != nil {
		return err
	}
	// The rename itself must be durable, or a crash can lose it even though
	// the data was synced.
	return syncDir(dir)
}

// loadSnapshot reads the snapshot, if one exists. A missing file is not an
// error: a node that has never snapshotted simply replays its whole log.
func loadSnapshot(dir string) (*raftpb.Snapshot, error) {
	b, err := os.ReadFile(snapshotPath(dir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	snap := &raftpb.Snapshot{}
	if err := proto.Unmarshal(b, snap); err != nil {
		return nil, fmt.Errorf("corrupt snapshot %s: %w", snapshotPath(dir), err)
	}
	return snap, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
