package processing

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestNewStateStoreMkdirError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("read-only dir test unreliable on Windows")
	}

	dir := t.TempDir()
	// Make dir read-only so MkdirAll fails
	os.Chmod(dir, 0o555)
	defer os.Chmod(dir, 0o755)

	_, err := NewStateStore("subdir/newdir", dir)
	if err == nil {
		t.Error("expected error for mkdir failure")
	}
}

func TestStateStoreNameGetter(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStateStore("my-store", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if store.Name() != "my-store" {
		t.Errorf("Name() = %q, want %q", store.Name(), "my-store")
	}
}

func TestStateStoreFlushPersistsData(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStateStore("test", dir)
	if err != nil {
		t.Fatal(err)
	}

	store.Put([]byte("k1"), []byte("v1"))

	if err := store.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Data should be readable after flush
	val, ok, _ := store.Get([]byte("k1"))
	if !ok || string(val) != "v1" {
		t.Errorf("k1: got %q ok=%v, want v1", val, ok)
	}

	store.Close()
}

func TestStateStoreMultipleKeysFlush(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStateStore("test", dir)
	if err != nil {
		t.Fatal(err)
	}

	keys := []string{"alpha", "gamma"}
	for _, k := range keys {
		store.Put([]byte(k), []byte("val-"+k))
	}

	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}

	// Both should exist after flush
	for _, k := range keys {
		val, ok, _ := store.Get([]byte(k))
		if !ok || string(val) != "val-"+k {
			t.Errorf("key %q: got %q ok=%v", k, val, ok)
		}
	}

	store.Close()
}

func TestStateStoreSubDirCreation(t *testing.T) {
	dir := t.TempDir()
	// NewStateStore should create nested dirs
	store, err := NewStateStore("a/b/c", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Verify the directory was created
	fullPath := filepath.Join(dir, "a", "b", "c")
	info, err := os.Stat(fullPath)
	if err != nil || !info.IsDir() {
		t.Errorf("expected directory at %s", fullPath)
	}
}
