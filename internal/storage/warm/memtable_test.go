package warm

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestMemTablePutGet(t *testing.T) {
	mt := NewMemTable(1024)

	mt.Put([]byte("key1"), []byte("val1"))
	mt.Put([]byte("key2"), []byte("val2"))
	mt.Put([]byte("key3"), []byte("val3"))

	val, found, deleted := mt.Get([]byte("key1"))
	if !found || deleted || string(val) != "val1" {
		t.Errorf("Get(key1) = (%q, %v, %v), want (val1, true, false)", val, found, deleted)
	}

	_, found, _ = mt.Get([]byte("nonexistent"))
	if found {
		t.Error("Get(nonexistent) should not be found")
	}
}

func TestMemTableOverwrite(t *testing.T) {
	mt := NewMemTable(1024)

	mt.Put([]byte("key1"), []byte("old"))
	mt.Put([]byte("key1"), []byte("new"))

	val, found, deleted := mt.Get([]byte("key1"))
	if !found || deleted || string(val) != "new" {
		t.Errorf("Get(key1) = (%q, %v, %v), want (new, true, false)", val, found, deleted)
	}
}

func TestMemTableDelete(t *testing.T) {
	mt := NewMemTable(1024)

	mt.Put([]byte("key1"), []byte("val1"))
	mt.Delete([]byte("key1"))

	val, found, deleted := mt.Get([]byte("key1"))
	if !found || !deleted {
		t.Errorf("Get(key1) after delete = (%q, %v, %v), want (_, true, true)", val, found, deleted)
	}
}

func TestMemTableIteratorOrder(t *testing.T) {
	mt := NewMemTable(1024)

	// Insert in non-sorted order
	mt.Put([]byte("charlie"), []byte("3"))
	mt.Put([]byte("alpha"), []byte("1"))
	mt.Put([]byte("bravo"), []byte("2"))
	mt.Put([]byte("delta"), []byte("4"))

	it := mt.Iterator()
	expected := []string{"alpha", "bravo", "charlie", "delta"}
	i := 0
	for it.Next() {
		entry := it.Entry()
		if string(entry.Key) != expected[i] {
			t.Errorf("entry %d: key = %q, want %q", i, entry.Key, expected[i])
		}
		i++
	}
	if i != 4 {
		t.Errorf("iterator yielded %d entries, want 4", i)
	}
}

func TestMemTableFreeze(t *testing.T) {
	mt := NewMemTable(1024)
	mt.Put([]byte("key1"), []byte("val1"))

	mt.Freeze()
	if !mt.IsFrozen() {
		t.Error("should be frozen")
	}

	err := mt.Put([]byte("key2"), []byte("val2"))
	if err == nil {
		t.Error("Put on frozen memtable should fail")
	}

	// Should still be readable
	val, found, _ := mt.Get([]byte("key1"))
	if !found || string(val) != "val1" {
		t.Error("frozen memtable should still be readable")
	}
}

func TestMemTableCapacity(t *testing.T) {
	mt := NewMemTable(100) // small capacity

	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("value-%04d", i))
		mt.Put(key, val)
	}

	if !mt.AtCapacity() {
		t.Error("should be at or above capacity")
	}
}

func TestMemTableOffsetKeys(t *testing.T) {
	mt := NewMemTable(4096)

	// Use offset keys (big-endian uint64) like the LSM will
	for i := uint64(0); i < 100; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)
		val := []byte(fmt.Sprintf("msg-%d", i))
		mt.Put(key, val)
	}

	// Verify iterator returns in offset order
	it := mt.Iterator()
	lastOffset := uint64(0)
	first := true
	for it.Next() {
		offset := binary.BigEndian.Uint64(it.Entry().Key)
		if !first && offset <= lastOffset {
			t.Errorf("offset %d <= last %d, not sorted", offset, lastOffset)
		}
		lastOffset = offset
		first = false
	}
}

func TestMemTableCount(t *testing.T) {
	mt := NewMemTable(4096)
	if mt.Count() != 0 {
		t.Errorf("Count = %d, want 0", mt.Count())
	}

	mt.Put([]byte("a"), []byte("1"))
	mt.Put([]byte("b"), []byte("2"))
	if mt.Count() != 2 {
		t.Errorf("Count = %d, want 2", mt.Count())
	}
}
