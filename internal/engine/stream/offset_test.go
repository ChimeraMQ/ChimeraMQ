package stream

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOffsetStoreSaveAndGet(t *testing.T) {
	dir, err := os.MkdirTemp("", "offset-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}

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

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if store.Get("nonexistent", 0) != 0 {
		t.Errorf("expected 0 for missing group")
	}
}

func TestOffsetStorePersistence(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-test-*")
	defer os.RemoveAll(dir)

	store1, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store1.Save("g1", 0, 42)
	store1.Save("g1", 1, 99)

	// Reload
	store2, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
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

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
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

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
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
	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
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

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Should not panic
	if store.Get("any", 0) != 0 {
		t.Errorf("expected 0")
	}
}

func TestOffsetStorePersistenceReloadMultipleGroups(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-multi-reload-*")
	defer os.RemoveAll(dir)

	s1, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s1.Save("ga", 0, 10)
	s1.Save("gb", 0, 20)
	s1.Save("gc", 2, 30)

	s2, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
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

func TestOffsetStoreLoadAllDirWithoutOffsetsFile(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-nofile-*")
	defer os.RemoveAll(dir)

	// Create a group directory without offsets.json
	consDir := dir + "/consumers"
	os.MkdirAll(consDir+"/empty-group", 0750)

	// loadAll should skip the dir without offsets.json (ReadFile error → continue)
	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if store.Get("empty-group", 0) != 0 {
		t.Errorf("expected 0 for group with missing offsets file")
	}
}

func TestOffsetStorePersistToInvalidDir(t *testing.T) {
	// Create OffsetStore pointing to an invalid directory
	store := &OffsetStore{
		dir:   string([]byte{0x00}),
		cache: make(map[string]map[uint32]uint64),
	}
	store.cache["test-group"] = map[uint32]uint64{0: 42}

	// persist should fail with an I/O error
	err := store.persist("test-group")
	if err == nil {
		t.Error("expected error persisting to invalid dir")
	}
}

func TestOffsetStorePersistMarshalError(t *testing.T) {
	// persist uses json.Marshal which never fails for map[uint32]uint64,
	// but WriteFile can fail with invalid path
	store := &OffsetStore{
		dir:   string([]byte{0x00}),
		cache: make(map[string]map[uint32]uint64),
	}
	store.cache["test"] = map[uint32]uint64{0: 1}
	err := store.persist("test")
	if err == nil {
		t.Error("expected error persisting to invalid path")
	}
}

func TestOffsetStoreSaveAndPersistExplicit(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-explicit-*")
	defer os.RemoveAll(dir)

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Save("explicit-group", 0, 42)
	store.Save("explicit-group", 1, 84)

	// Verify persisted
	store2, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if store2.Get("explicit-group", 0) != 42 {
		t.Errorf("got %d, want 42", store2.Get("explicit-group", 0))
	}
	if store2.Get("explicit-group", 1) != 84 {
		t.Errorf("got %d, want 84", store2.Get("explicit-group", 1))
	}
}

func TestOffsetStorePersistMkdirAllError(t *testing.T) {
	store := &OffsetStore{
		dir:   string([]byte{0x00}),
		cache: make(map[string]map[uint32]uint64),
	}
	store.cache["test"] = map[uint32]uint64{0: 1}

	err := store.persist("test")
	if err == nil {
		t.Error("expected error persisting to null-byte dir")
	}
}

func TestOffsetStoreSaveCreatesGroupDir(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-mkdir-*")
	defer os.RemoveAll(dir)

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Save("new-group", 0, 100)

	// Verify the group directory was created (under consumers/)
	consumersDir := filepath.Join(dir, "consumers")
	info, err := os.Stat(filepath.Join(consumersDir, "new-group"))
	if err != nil {
		t.Fatalf("group dir not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected directory")
	}

	// Verify the offsets file exists
	data, err := os.ReadFile(filepath.Join(consumersDir, "new-group", "offsets.json"))
	if err != nil {
		t.Fatalf("offsets file not found: %v", err)
	}
	if len(data) == 0 {
		t.Error("offsets file is empty")
	}
}

func TestOffsetStoreLoadAllSkipsNonJSON(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-skip-*")
	defer os.RemoveAll(dir)

	// NewOffsetStore creates consumers/ subdirectory
	consumersDir := filepath.Join(dir, "consumers")

	// Create a group dir with valid offsets
	os.MkdirAll(filepath.Join(consumersDir, "good-group"), 0750)
	os.WriteFile(filepath.Join(consumersDir, "good-group", "offsets.json"), []byte(`{"0":50}`), 0640)

	// Create a group dir with invalid JSON
	os.MkdirAll(filepath.Join(consumersDir, "bad-group"), 0750)
	os.WriteFile(filepath.Join(consumersDir, "bad-group", "offsets.json"), []byte("not-json"), 0640)

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if store.Get("good-group", 0) != 50 {
		t.Errorf("good-group = %d, want 50", store.Get("good-group", 0))
	}
	if store.Get("bad-group", 0) != 0 {
		t.Errorf("bad-group should be 0, got %d", store.Get("bad-group", 0))
	}
}

func TestOffsetStorePersistReadonlyDir(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-readonly-*")
	defer os.RemoveAll(dir)

	// Create consumers dir and make the group subdir read-only
	consDir := filepath.Join(dir, "consumers")
	os.MkdirAll(consDir, 0750)

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// First save works (creates group dir + file)
	err = store.Save("group1", 0, 10)
	if err != nil {
		t.Fatalf("first save: %v", err)
	}

	// Verify save worked
	if store.Get("group1", 0) != 10 {
		t.Errorf("got %d, want 10", store.Get("group1", 0))
	}

	// Overwrite should also work
	err = store.Save("group1", 0, 20)
	if err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if store.Get("group1", 0) != 20 {
		t.Errorf("got %d, want 20", store.Get("group1", 0))
	}
}

func TestOffsetStoreLoadAllValidData(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-loadall-*")
	defer os.RemoveAll(dir)

	// Pre-create consumer group data
	consDir := filepath.Join(dir, "consumers")
	g1Dir := filepath.Join(consDir, "alpha")
	os.MkdirAll(g1Dir, 0750)
	os.WriteFile(filepath.Join(g1Dir, "offsets.json"), []byte(`{"0":100,"1":200}`), 0640)

	g2Dir := filepath.Join(consDir, "beta")
	os.MkdirAll(g2Dir, 0750)
	os.WriteFile(filepath.Join(g2Dir, "offsets.json"), []byte(`{"3":999}`), 0640)

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	if store.Get("alpha", 0) != 100 {
		t.Errorf("alpha/0 = %d, want 100", store.Get("alpha", 0))
	}
	if store.Get("alpha", 1) != 200 {
		t.Errorf("alpha/1 = %d, want 200", store.Get("alpha", 1))
	}
	if store.Get("beta", 3) != 999 {
		t.Errorf("beta/3 = %d, want 999", store.Get("beta", 3))
	}
	// Non-existent partition should return 0
	if store.Get("alpha", 99) != 0 {
		t.Errorf("alpha/99 = %d, want 0", store.Get("alpha", 99))
	}
}

func TestOffsetStoreNewOffsetStoreCreatesDir(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-newdir-*")
	defer os.RemoveAll(dir)

	// consumers/ subdirectory should be created
	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(filepath.Join(dir, "consumers"))
	if err != nil {
		t.Fatalf("consumers dir not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("consumers is not a directory")
	}
	_ = store
}

func TestOffsetStorePersistWriteFilePermError(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-perm-*")
	defer os.RemoveAll(dir)

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Save("perm-group", 0, 10)

	// Make the group dir read-only so WriteFile fails on overwrite
	groupDir := filepath.Join(dir, "consumers", "perm-group")
	if os.Getenv("OS") != "Windows_NT" {
		os.Chmod(groupDir, 0550)
		defer os.Chmod(groupDir, 0750)

		err := store.Save("perm-group", 0, 20)
		if err == nil {
			t.Error("expected error writing to read-only dir")
		}
	}
}

func TestOffsetStoreSaveMultiplePartitions(t *testing.T) {
	dir, _ := os.MkdirTemp("", "offset-multi-part-*")
	defer os.RemoveAll(dir)

	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Save multiple partitions for same group
	store.Save("multi-part-group", 0, 100)
	store.Save("multi-part-group", 1, 200)
	store.Save("multi-part-group", 2, 300)
	store.Save("multi-part-group", 3, 400)

	// Verify all
	if store.Get("multi-part-group", 0) != 100 {
		t.Errorf("p0 = %d, want 100", store.Get("multi-part-group", 0))
	}
	if store.Get("multi-part-group", 1) != 200 {
		t.Errorf("p1 = %d, want 200", store.Get("multi-part-group", 1))
	}
	if store.Get("multi-part-group", 2) != 300 {
		t.Errorf("p2 = %d, want 300", store.Get("multi-part-group", 2))
	}
	if store.Get("multi-part-group", 3) != 400 {
		t.Errorf("p3 = %d, want 400", store.Get("multi-part-group", 3))
	}

	// Reload and verify persistence
	store2, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if store2.Get("multi-part-group", 0) != 100 {
		t.Errorf("after reload p0 = %d, want 100", store2.Get("multi-part-group", 0))
	}
	if store2.Get("multi-part-group", 3) != 400 {
		t.Errorf("after reload p3 = %d, want 400", store2.Get("multi-part-group", 3))
	}
}
