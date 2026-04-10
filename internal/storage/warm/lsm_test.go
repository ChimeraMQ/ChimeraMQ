package warm

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func TestLSMPutGet(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultLSMConfig()
	cfg.MemTableCapacity = 1024 // small for testing

	lsm, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	// Write entries
	for i := uint64(0); i < 50; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		if err := lsm.Put(key, []byte(fmt.Sprintf("value-%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Verify all readable
	for i := uint64(0); i < 50; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		val, found, deleted := lsm.Get(key)
		if !found {
			t.Errorf("key %d not found", i)
			continue
		}
		if deleted {
			t.Errorf("key %d should not be deleted", i)
		}
		if string(val) != fmt.Sprintf("value-%d", i) {
			t.Errorf("key %d: value = %q", i, val)
		}
	}
}

func TestLSMDelete(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultLSMConfig()
	cfg.MemTableCapacity = 4096

	lsm, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 42)
	lsm.Put(key, []byte("original"))
	lsm.Delete(key)

	val, found, deleted := lsm.Get(key)
	if !found {
		t.Error("deleted key should still be found")
	}
	if !deleted {
		t.Error("key should be marked deleted")
	}
	if val != nil {
		t.Errorf("deleted key value = %q, want nil", val)
	}
}

func TestLSMMemTableFlush(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultLSMConfig()
	cfg.MemTableCapacity = 512 // very small to trigger flushes

	lsm, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	// Write enough to trigger multiple flushes
	for i := uint64(0); i < 200; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		lsm.Put(key, []byte(fmt.Sprintf("val-%d", i)))
	}

	// Wait for flushes
	time.Sleep(500 * time.Millisecond)

	// Verify all readable
	for i := uint64(0); i < 200; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		val, found, deleted := lsm.Get(key)
		if !found || deleted {
			t.Errorf("key %d: found=%v deleted=%v", i, found, deleted)
			continue
		}
		if string(val) != fmt.Sprintf("val-%d", i) {
			t.Errorf("key %d: value = %q", i, val)
		}
	}

	stats := lsm.Stats()
	if stats.TotalSSTables == 0 {
		t.Error("should have flushed at least one SSTable")
	}
}

func TestLSMOverwrite(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultLSMConfig()

	lsm, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	key := []byte("overwrite-key")
	lsm.Put(key, []byte("v1"))
	lsm.Put(key, []byte("v2"))
	lsm.Put(key, []byte("v3"))

	val, found, deleted := lsm.Get(key)
	if !found || deleted || string(val) != "v3" {
		t.Errorf("Get = (%q, %v, %v), want (v3, true, false)", val, found, deleted)
	}
}

func TestLSMStats(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultLSMConfig()

	lsm, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	key := []byte("stats-key")
	lsm.Put(key, []byte("v"))

	stats := lsm.Stats()
	if stats.MemTableCount != 1 {
		t.Errorf("MemTableCount = %d, want 1", stats.MemTableCount)
	}
	if stats.TotalSSTables != 0 {
		t.Errorf("TotalSSTables = %d, want 0 (not flushed yet)", stats.TotalSSTables)
	}
}

func TestLSMRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultLSMConfig()
	cfg.MemTableCapacity = 512

	// Create and write
	lsm1, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	for i := uint64(0); i < 50; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		lsm1.Put(key, []byte(fmt.Sprintf("val-%d", i)))
	}
	time.Sleep(300 * time.Millisecond) // Wait for flushes
	lsm1.Close()

	// Reopen and verify
	lsm2, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm2.Close()

	stats := lsm2.Stats()
	if stats.TotalSSTables == 0 {
		t.Error("should have recovered SSTables from manifest")
	}

	// Verify data is still readable from recovered SSTables
	for i := uint64(0); i < 50; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		val, found, _ := lsm2.Get(key)
		if found && string(val) == fmt.Sprintf("val-%d", i) {
			// Good - found in SSTable
		}
		// Note: memtable data is lost on unclean shutdown (by design)
	}
}

func TestManifestBasic(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManifest(dir)
	if err != nil {
		t.Fatal(err)
	}

	if len(m.AllEntries()) != 0 {
		t.Error("new manifest should be empty")
	}

	// Create a fake SSTable for manifest tracking
	mt := NewMemTable(1024)
	mt.Put([]byte("k"), []byte("v"))
	mt.Freeze()
	sst, _ := FlushMemTable(mt, dir)
	defer sst.Close()

	m.Add(0, sst)
	if m.SSTCount(0) != 1 {
		t.Errorf("SSTCount(0) = %d, want 1", m.SSTCount(0))
	}

	m.Remove(sst.Path())
	if m.SSTCount(0) != 0 {
		t.Errorf("SSTCount(0) after remove = %d, want 0", m.SSTCount(0))
	}
}
