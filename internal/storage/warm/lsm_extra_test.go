package warm

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

// --- compactL0 tests ---

func TestCompactL0(t *testing.T) {
	dir := t.TempDir()
	cfg := LSMConfig{
		MemTableCapacity:   64, // tiny so memtable fills fast
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     2,
	}

	lsm, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	// Write enough 8-byte key + value pairs to produce >= 4 L0 SSTables.
	// Each Put adds 8 bytes key + ~10 bytes value = ~18 bytes, so 4 Puts fill a
	// 64-byte memtable. We need 4 SSTables at L0 for compaction to fire.
	// Writing 4 * 4 = 16 entries should be sufficient, but we write extra to
	// account for overhead.
	for i := uint64(0); i < 40; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		if err := lsm.Put(key, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		// Small sleep to let the background flush goroutine drain immutables
		// between writes so they land as separate L0 SSTables.
		time.Sleep(10 * time.Millisecond)
	}

	// Give the background compaction goroutine time to fire.
	time.Sleep(2 * time.Second)

	stats := lsm.Stats()

	// After compaction L0 should be empty (or much smaller) and L1 should have
	// at least one SSTable.
	if len(stats.Levels) < 2 {
		t.Fatalf("expected at least 2 levels, got %d", len(stats.Levels))
	}
	if stats.Levels[0].SSTables >= 4 {
		t.Errorf("L0 still has %d SSTables, compaction did not run", stats.Levels[0].SSTables)
	}
	if stats.Levels[1].SSTables == 0 {
		t.Error("L1 has no SSTables after expected compaction")
	}

	// Data must still be readable.
	for i := uint64(0); i < 40; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		val, found, deleted := lsm.Get(key)
		if !found {
			t.Errorf("key %d not found after compaction", i)
			continue
		}
		if deleted {
			t.Errorf("key %d unexpectedly deleted", i)
		}
		if string(val) != fmt.Sprintf("v%d", i) {
			t.Errorf("key %d: got %q, want %q", i, val, fmt.Sprintf("v%d", i))
		}
	}
}

// --- OldSSTables tests ---

func TestOldSSTables(t *testing.T) {
	dir := t.TempDir()
	cfg := LSMConfig{
		MemTableCapacity:   64,
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     2,
	}

	lsm, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	// Write enough data to trigger L0 compaction, producing L1 SSTables.
	for i := uint64(0); i < 40; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		lsm.Put(key, []byte(fmt.Sprintf("v%d", i)))
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)

	// OldSSTables checks levels >= 1. Since MaxTimestamp defaults to 0 in
	// recovered SSTables, even a 1 nanosecond cutoff treats them as old.
	old := lsm.OldSSTables(1 * time.Nanosecond)
	if len(old) == 0 {
		t.Error("expected at least one old SSTable at L1+, got none")
	}

	// Verify that a very short cutoff returns at least as many SSTables as a
	// longer one (monotonicity).
	short := lsm.OldSSTables(1 * time.Nanosecond)
	long := lsm.OldSSTables(24 * time.Hour)
	if len(short) < len(long) {
		t.Errorf("short cutoff returned %d, long cutoff returned %d — expected short >= long", len(short), len(long))
	}
}

func TestOldSSTablesEmptyLevels(t *testing.T) {
	dir := t.TempDir()
	cfg := LSMConfig{
		MemTableCapacity:   64,
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     2,
	}

	lsm, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	// No data written, so all levels are empty.
	old := lsm.OldSSTables(1 * time.Hour)
	if len(old) != 0 {
		t.Errorf("expected 0 old SSTables on empty tree, got %d", len(old))
	}
}

// --- RemoveSSTable tests ---

func TestRemoveSSTable(t *testing.T) {
	dir := t.TempDir()
	cfg := LSMConfig{
		MemTableCapacity:   64,
		BlockSize:          4096,
		BloomFPRate:        0.01,
		CompactionStrategy: "leveled",
		MaxLevel:           4,
		LevelSizeRatio:     2,
	}

	lsm, err := NewLSMTree(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lsm.Close()

	// Write enough to produce at least one SSTable at L0.
	for i := uint64(0); i < 10; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		lsm.Put(key, []byte(fmt.Sprintf("v%d", i)))
	}
	time.Sleep(500 * time.Millisecond)

	stats := lsm.Stats()
	totalBefore := stats.TotalSSTables
	if totalBefore == 0 {
		t.Fatal("expected at least one SSTable before removal")
	}

	// Grab a pointer to the first SSTable we can find at any level.
	var target *SSTable
	for _, lvl := range lsm.levels {
		if len(lvl.sstables) > 0 {
			target = lvl.sstables[0]
			break
		}
	}
	if target == nil {
		t.Fatal("could not find any SSTable to remove")
	}

	lsm.RemoveSSTable(target)

	stats = lsm.Stats()
	if stats.TotalSSTables >= totalBefore {
		t.Errorf("TotalSSTables = %d, want < %d after removal", stats.TotalSSTables, totalBefore)
	}
}

// --- collectEntries tests ---

func TestCollectEntries(t *testing.T) {
	dir := t.TempDir()

	// Build a MemTable, flush it, and then collect entries from the SSTable.
	mt := NewMemTable(4096)
	for i := uint64(0); i < 10; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		if err := mt.Put(key, []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	mt.Freeze()

	sst, err := FlushMemTable(mt, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	entries := collectEntries(sst)
	if len(entries) == 0 {
		t.Error("collectEntries returned no entries")
	}

	// Verify collected entries contain expected data.
	for i := uint64(0); i < 10; i++ {
		entry, ok := entries[i]
		if !ok {
			t.Errorf("offset %d not found in collected entries", i)
			continue
		}
		want := fmt.Sprintf("val-%d", i)
		if string(entry.Value) != want {
			t.Errorf("offset %d: value = %q, want %q", i, entry.Value, want)
		}
		if entry.Deleted {
			t.Errorf("offset %d: unexpected deleted flag", i)
		}
	}
}

func TestCollectEntriesWithTombstone(t *testing.T) {
	dir := t.TempDir()

	mt := NewMemTable(4096)
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 42)
	mt.Put(key, []byte("alive"))

	// Insert tombstone for the same key — last write wins.
	mt.Delete(key)
	mt.Freeze()

	sst, err := FlushMemTable(mt, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	entries := collectEntries(sst)
	entry, ok := entries[42]
	if !ok {
		t.Fatal("offset 42 not found in collected entries")
	}
	if !entry.Deleted {
		t.Error("expected deleted flag for tombstone entry")
	}
}
