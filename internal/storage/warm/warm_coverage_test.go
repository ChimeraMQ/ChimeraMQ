package warm

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestLSMTotalSize(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultLSMConfig()
	cfg.MemTableCapacity = 64 // force flush quickly

	lsm, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	// Write enough to flush at least one SSTable
	for i := uint64(0); i < 50; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		if err := lsm.Put(key, []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for background flush
	time.Sleep(300 * time.Millisecond)

	size := lsm.TotalSize()
	if size <= 0 {
		t.Errorf("TotalSize = %d, want > 0", size)
	}
}

func TestManifestEntriesAt(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManifest(dir)
	if err != nil {
		t.Fatal(err)
	}

	m.entries = []ManifestEntry{
		{Level: 0, SSTPath: "a.sst"},
		{Level: 1, SSTPath: "b.sst"},
		{Level: 0, SSTPath: "c.sst"},
		{Level: 2, SSTPath: "d.sst"},
	}

	l0 := m.EntriesAt(0)
	if len(l0) != 2 {
		t.Errorf("EntriesAt(0) = %d, want 2", len(l0))
	}

	l1 := m.EntriesAt(1)
	if len(l1) != 1 {
		t.Errorf("EntriesAt(1) = %d, want 1", len(l1))
	}

	l99 := m.EntriesAt(99)
	if len(l99) != 0 {
		t.Errorf("EntriesAt(99) = %d, want 0", len(l99))
	}
}

func TestSSTableSize(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(4096)
	mt.Put([]byte("k1"), []byte("v1"))
	mt.Freeze()

	sst, err := FlushMemTable(mt, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	size1 := sst.Size()
	if size1 <= 0 {
		t.Errorf("Size = %d, want > 0", size1)
	}

	// Close the file and test Size falls back to os.Stat
	sst.mu.Lock()
	sst.file.Close()
	sst.file = nil
	sst.mu.Unlock()

	size2 := sst.Size()
	if size2 != size1 {
		t.Errorf("Size after close = %d, want %d", size2, size1)
	}
}

func TestSSTableSizeMissingFile(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(4096)
	mt.Put([]byte("k1"), []byte("v1"))
	mt.Freeze()

	sst, err := FlushMemTable(mt, dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	sst.Close()

	// Remove the file so os.Stat fails
	os.Remove(sst.path)

	if size := sst.Size(); size != 0 {
		t.Errorf("Size for missing file = %d, want 0", size)
	}
}

func TestCreateEmptySSTable(t *testing.T) {
	dir := t.TempDir()
	sst, err := createEmptySSTable(dir)
	if err != nil {
		t.Fatalf("createEmptySSTable: %v", err)
	}
	defer sst.Close()

	if sst.Size() <= 0 {
		t.Error("empty SSTable should have positive size from footer")
	}
}
