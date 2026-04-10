package processing

import (
	"testing"
)

func TestStateStorePutGet(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStateStore("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatal(err)
	}

	val, ok, _ := store.Get([]byte("k1"))
	if !ok {
		t.Fatal("k1 should be found")
	}
	if string(val) != "v1" {
		t.Errorf("value = %q, want %q", val, "v1")
	}
}

func TestStateStoreMiss(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStateStore("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	_, ok, _ := store.Get([]byte("nonexistent"))
	if ok {
		t.Error("nonexistent key should not be found")
	}
}

func TestStateStoreDelete(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStateStore("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.Put([]byte("k1"), []byte("v1"))
	store.Delete([]byte("k1"))

	_, ok, deleted := store.Get([]byte("k1"))
	if ok {
		t.Error("deleted key should not be found (ok=false)")
	}
	if !deleted {
		t.Error("deleted key should have deleted=true")
	}
}

func TestStateStoreFlushClearsCache(t *testing.T) {
	dir := t.TempDir()

	store, err := NewStateStore("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Put([]byte("k1"), []byte("v1"))

	// Before flush: read from cache
	val, ok, _ := store.Get([]byte("k1"))
	if !ok {
		t.Fatal("k1 should be found in cache")
	}
	if string(val) != "v1" {
		t.Errorf("value = %q, want %q", val, "v1")
	}

	// Flush to LSM
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}

	// After flush, data is in LSM memtable — should still be readable
	val2, ok2, _ := store.Get([]byte("k1"))
	if !ok2 {
		t.Fatal("k1 should be readable after flush via LSM memtable")
	}
	if string(val2) != "v1" {
		t.Errorf("value = %q, want %q", val2, "v1")
	}

	store.Close()
}

func TestStateStoreOverwrite(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStateStore("test", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.Put([]byte("k1"), []byte("v1"))
	store.Put([]byte("k1"), []byte("v2"))

	val, ok, _ := store.Get([]byte("k1"))
	if !ok {
		t.Fatal("k1 should be found")
	}
	if string(val) != "v2" {
		t.Errorf("value = %q, want %q", val, "v2")
	}
}

func TestAggregateOp(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStateStore("agg", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var results []string
	wm := NewWindowManager(WindowConfig{
		Type: WindowTumbling,
		Size: 1000000000, // 1s in ns
	}, func(key string, state *WindowState) {
		results = append(results, key+":"+string(state.Data))
	})

	sumFn := func(state []byte, event []byte) (newState []byte, emit bool) {
		return append(state, event...), true
	}

	agg := NewAggregateOp(wm, store, sumFn)

	agg.Process("k1", 100, []byte("a"))
	agg.Process("k1", 200, []byte("b"))

	// Tick past window
	agg.Tick(int64(2e9))

	if len(results) != 1 {
		t.Fatalf("results = %v, want 1", results)
	}
	if results[0] != "k1:ab" {
		t.Errorf("result = %q, want %q", results[0], "k1:ab")
	}
}
