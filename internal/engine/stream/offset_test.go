package stream

import (
	"os"
	"testing"
)

func TestOffsetStoreSaveAndGet(t *testing.T) {
	dir, err := os.MkdirTemp("", "offset-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store := NewOffsetStore(dir)

	store.Save("group1", 0, 100)
	store.Save("group1", 1, 200)

	if store.Get("group1", 0) != 100 {
		t.Errorf("expected 100, got %d", store.Get("group1", 0))
	}
	if store.Get("group1", 1) != 200 {
		t.Errorf("expected 200, got %d", store.Get("group1", 1))
	}
}

func TestOffsetStoreMissingGroup(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-test-*")
	defer os.RemoveAll(dir)

	store := NewOffsetStore(dir)
	if store.Get("nonexistent", 0) != 0 {
		t.Errorf("expected 0 for missing group")
	}
}

func TestOffsetStorePersistence(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-test-*")
	defer os.RemoveAll(dir)

	store1 := NewOffsetStore(dir)
	store1.Save("g1", 0, 42)
	store1.Save("g1", 1, 99)

	// Reload
	store2 := NewOffsetStore(dir)
	if store2.Get("g1", 0) != 42 {
		t.Errorf("expected 42 after reload, got %d", store2.Get("g1", 0))
	}
	if store2.Get("g1", 1) != 99 {
		t.Errorf("expected 99 after reload, got %d", store2.Get("g1", 1))
	}
}

func TestOffsetStoreMultipleGroups(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-mg-*")
	defer os.RemoveAll(dir)

	store := NewOffsetStore(dir)
	store.Save("group-a", 0, 100)
	store.Save("group-b", 0, 200)

	if store.Get("group-a", 0) != 100 {
		t.Errorf("group-a = %d, want 100", store.Get("group-a", 0))
	}
	if store.Get("group-b", 0) != 200 {
		t.Errorf("group-b = %d, want 200", store.Get("group-b", 0))
	}
}

func TestOffsetStoreOverwrite(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-overwrite-*")
	defer os.RemoveAll(dir)

	store := NewOffsetStore(dir)
	store.Save("g1", 0, 10)
	store.Save("g1", 0, 20)

	if store.Get("g1", 0) != 20 {
		t.Errorf("overwritten offset = %d, want 20", store.Get("g1", 0))
	}
}

func TestOffsetStoreLoadAllCorruptJSON(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-corrupt-*")
	defer os.RemoveAll(dir)

	// Create a group dir with corrupt JSON
	groupDir := dir + "/consumers/corrupt-group"
	os.MkdirAll(groupDir, 0750)
	os.WriteFile(groupDir+"/offsets.json", []byte("not-json"), 0640)

	// Should not panic — loadAll skips unparseable files
	store := NewOffsetStore(dir)
	if store.Get("corrupt-group", 0) != 0 {
		t.Errorf("corrupt group offset = %d, want 0", store.Get("corrupt-group", 0))
	}
}

func TestOffsetStoreLoadAllSkipsFiles(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-files-*")
	defer os.RemoveAll(dir)

	// Create a plain file (not dir) in consumers — should be skipped
	consDir := dir + "/consumers"
	os.MkdirAll(consDir, 0750)
	os.WriteFile(consDir+"/plain-file", []byte("data"), 0640)

	store := NewOffsetStore(dir)
	// Should not panic
	if store.Get("any", 0) != 0 {
		t.Errorf("expected 0")
	}
}

func TestOffsetStorePersistenceReloadMultipleGroups(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-multi-reload-*")
	defer os.RemoveAll(dir)

	s1 := NewOffsetStore(dir)
	s1.Save("ga", 0, 10)
	s1.Save("gb", 0, 20)
	s1.Save("gc", 2, 30)

	s2 := NewOffsetStore(dir)
	if s2.Get("ga", 0) != 10 {
		t.Errorf("ga/0 = %d, want 10", s2.Get("ga", 0))
	}
	if s2.Get("gb", 0) != 20 {
		t.Errorf("gb/0 = %d, want 20", s2.Get("gb", 0))
	}
	if s2.Get("gc", 2) != 30 {
		t.Errorf("gc/2 = %d, want 30", s2.Get("gc", 2))
	}
}
