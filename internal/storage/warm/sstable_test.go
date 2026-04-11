package warm

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSSTableFlushAndRead(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(4096)

	// Insert entries with offset keys
	for i := uint64(0); i < 100; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte(fmt.Sprintf("value-%d", i)))
	}
	mt.Freeze()

	sst, err := FlushMemTable(mt, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	// Verify all entries
	for i := uint64(0); i < 100; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		val, found, deleted := sst.Get(key)
		if !found {
			t.Errorf("key %d not found", i)
			continue
		}
		if deleted {
			t.Errorf("key %d should not be deleted", i)
		}
		if string(val) != fmt.Sprintf("value-%d", i) {
			t.Errorf("key %d: value = %q, want %q", i, val, fmt.Sprintf("value-%d", i))
		}
	}

	// Non-existent key
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 999)
	_, found, _ := sst.Get(key)
	if found {
		t.Error("key 999 should not be found")
	}
}

func TestSSTableTombstone(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(4096)

	key := []byte("delete-me")
	mt.Put(key, []byte("original"))
	mt.Delete(key)
	mt.Freeze()

	sst, err := FlushMemTable(mt, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	_, found, deleted := sst.Get(key)
	if !found {
		t.Error("deleted key should still be found")
	}
	if !deleted {
		t.Error("key should be marked as deleted (tombstone)")
	}
}

func TestSSTableReopen(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(4096)

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		mt.Put(key, []byte(fmt.Sprintf("val-%03d", i)))
	}
	mt.Freeze()

	sst1, err := FlushMemTable(mt, dir)
	if err != nil {
		t.Fatal(err)
	}
	path := sst1.Path()
	sst1.Close()

	// Reopen
	sst2, err := OpenSSTable(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sst2.Close()

	val, found, _ := sst2.Get([]byte("key-025"))
	if !found || string(val) != "val-025" {
		t.Errorf("reopened SSTable: Get(key-025) = (%q, %v)", val, found)
	}
}

func TestSSTableBloomRejection(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(4096)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		mt.Put(key, []byte("value"))
	}
	mt.Freeze()

	sst, err := FlushMemTable(mt, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	// A key that was never inserted should be rejected by bloom
	_, found, _ := sst.Get([]byte("zzz-never-inserted"))
	if found {
		t.Error("bloom should have rejected non-existent key")
	}
}

func TestSSTableMetadata(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(4096)

	for i := uint64(10); i <= 20; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte("v"))
	}
	mt.Freeze()

	sst, err := FlushMemTable(mt, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	meta := sst.Metadata()
	if meta.MinOffset != 10 {
		t.Errorf("MinOffset = %d, want 10", meta.MinOffset)
	}
	if meta.MaxOffset != 20 {
		t.Errorf("MaxOffset = %d, want 20", meta.MaxOffset)
	}
	if meta.EntryCount != 11 {
		t.Errorf("EntryCount = %d, want 11", meta.EntryCount)
	}

	// Check file exists
	if _, err := os.Stat(sst.Path()); os.IsNotExist(err) {
		t.Error("SSTable file should exist")
	}
}

func TestBlockIndexSerialization(t *testing.T) {
	bi := NewBlockIndex()
	bi.Add([]byte("aaa"), 0, 100)
	bi.Add([]byte("bbb"), 100, 200)
	bi.Add([]byte("ccc"), 300, 150)

	data, err := bi.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	bi2, err := UnmarshalBlockIndex(data)
	if err != nil {
		t.Fatal(err)
	}

	if bi2.Len() != 3 {
		t.Errorf("Len = %d, want 3", bi2.Len())
	}
	if string(bi2.Entries()[0].FirstKey) != "aaa" {
		t.Errorf("first key = %q, want aaa", bi2.Entries()[0].FirstKey)
	}
}

func TestBlockIndexSearch(t *testing.T) {
	bi := NewBlockIndex()
	bi.Add([]byte("aaa"), 0, 100)
	bi.Add([]byte("bbb"), 100, 200)
	bi.Add([]byte("ddd"), 300, 150)

	entry, ok := bi.Search([]byte("ccc"))
	if !ok {
		t.Fatal("Search should find a block")
	}
	// "ccc" falls between "bbb" and "ddd", so it should be in the "bbb" block
	if string(entry.FirstKey) != "bbb" {
		t.Errorf("Search(ccc) = %q, want bbb", entry.FirstKey)
	}

	entry, ok = bi.Search([]byte("aaa"))
	if !ok || string(entry.FirstKey) != "aaa" {
		t.Errorf("Search(aaa) should return aaa block")
	}

	entry, ok = bi.Search([]byte("zzz"))
	if !ok || string(entry.FirstKey) != "ddd" {
		t.Errorf("Search(zzz) should return ddd block (last)")
	}
}

func TestSSTableFilesCreated(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(4096)
	mt.Put([]byte("k"), []byte("v"))
	mt.Freeze()

	sst, err := FlushMemTable(mt, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	files, _ := filepath.Glob(filepath.Join(dir, "*.dat"))
	if len(files) == 0 {
		t.Error("no .dat files created")
	}
}

func TestBlockCache(t *testing.T) {
	c := newBlockCache(3)

	c.put(10, []byte("a"))
	c.put(20, []byte("b"))
	c.put(30, []byte("c"))

	if data, ok := c.get(10); !ok || string(data) != "a" {
		t.Error("expected cache hit for offset 10")
	}
	if data, ok := c.get(20); !ok || string(data) != "b" {
		t.Error("expected cache hit for offset 20")
	}

	// Adding a 4th entry should evict the first (FIFO)
	c.put(40, []byte("d"))
	if _, ok := c.get(10); ok {
		t.Error("expected offset 10 to be evicted")
	}
	if data, ok := c.get(40); !ok || string(data) != "d" {
		t.Error("expected cache hit for offset 40")
	}

	// Duplicate put should not change order
	c.put(20, []byte("b-new"))
	if data, ok := c.get(20); !ok || string(data) != "b" {
		t.Error("duplicate put should not overwrite existing entry")
	}
}

func TestSSTableBlockCacheHits(t *testing.T) {
	dir := t.TempDir()
	mt := NewMemTable(4096)

	// Insert enough entries to create multiple blocks
	for i := uint64(0); i < 500; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		mt.Put(key, []byte(fmt.Sprintf("value-%d", i)))
	}
	mt.Freeze()

	sst, err := FlushMemTable(mt, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer sst.Close()

	// First read — cold cache
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 42)
	val, found, _ := sst.Get(key)
	if !found || string(val) != "value-42" {
		t.Fatalf("cold read: value=%q, found=%v", val, found)
	}

	// Second read — should hit cache
	val, found, _ = sst.Get(key)
	if !found || string(val) != "value-42" {
		t.Fatalf("warm read: value=%q, found=%v", val, found)
	}

	// Verify cache has entries
	sst.blockCache.mu.Lock()
	cached := len(sst.blockCache.blocks)
	sst.blockCache.mu.Unlock()
	if cached == 0 {
		t.Error("expected block cache to have entries after reads")
	}
}
