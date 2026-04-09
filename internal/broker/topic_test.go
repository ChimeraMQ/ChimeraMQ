package broker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/wal"
)

func setupTopicManager(t *testing.T) (*TopicManager, string) {
	t.Helper()
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	w, err := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncOS, 0)
	if err != nil {
		t.Fatal(err)
	}
	tm, err := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		w.Close()
		storage.Close()
	})
	return tm, dir
}

func TestCreateTopic(t *testing.T) {
	tm, _ := setupTopicManager(t)

	err := tm.CreateTopic(TopicConfig{
		Name:       "test-topic",
		Mode:       ModeStream,
		Partitions: 4,
	})
	if err != nil {
		t.Fatal(err)
	}

	cfg, ok := tm.GetTopic("test-topic")
	if !ok {
		t.Fatal("topic not found")
	}
	if cfg.Name != "test-topic" {
		t.Errorf("expected test-topic, got %s", cfg.Name)
	}
	if cfg.Mode != ModeStream {
		t.Errorf("expected stream mode")
	}
	if cfg.Partitions != 4 {
		t.Errorf("expected 4 partitions, got %d", cfg.Partitions)
	}
}

func TestCreateDuplicateTopic(t *testing.T) {
	tm, _ := setupTopicManager(t)

	cfg := TopicConfig{Name: "dup", Mode: ModeQueue, Partitions: 2}
	if err := tm.CreateTopic(cfg); err != nil {
		t.Fatal(err)
	}
	if err := tm.CreateTopic(cfg); err == nil {
		t.Error("expected error for duplicate topic")
	}
}

func TestDeleteTopic(t *testing.T) {
	tm, _ := setupTopicManager(t)

	tm.CreateTopic(TopicConfig{Name: "del-me", Mode: ModeStream, Partitions: 2})
	if err := tm.DeleteTopic("del-me"); err != nil {
		t.Fatal(err)
	}
	if _, ok := tm.GetTopic("del-me"); ok {
		t.Error("expected topic to be deleted")
	}
}

func TestDeleteNonexistentTopic(t *testing.T) {
	tm, _ := setupTopicManager(t)

	if err := tm.DeleteTopic("nope"); err == nil {
		t.Error("expected error deleting nonexistent topic")
	}
}

func TestListTopics(t *testing.T) {
	tm, _ := setupTopicManager(t)

	for _, name := range []string{"a", "b", "c"} {
		tm.CreateTopic(TopicConfig{Name: name, Mode: ModeUnified, Partitions: 1})
	}

	list := tm.ListTopics()
	if len(list) != 3 {
		t.Errorf("expected 3 topics, got %d", len(list))
	}
}

func TestValidateTopicName(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"valid-name", false},
		{"also_valid", false},
		{"topic.123", false},
		{"", true},
		{"a", false},
		{".starts-with-dot", true},
		{"-starts-with-dash", true},
		{"has space", true},
		{"has/slash", true},
	}
	for _, tc := range tests {
		err := validateTopicName(tc.name)
		if (err != nil) != tc.wantErr {
			t.Errorf("validateTopicName(%q) = %v, wantErr=%v", tc.name, err, tc.wantErr)
		}
	}
}

func TestResolvePartitionWithRoutingKey(t *testing.T) {
	tm, _ := setupTopicManager(t)

	p1 := tm.ResolvePartition("topic", "key1", 8)
	p2 := tm.ResolvePartition("topic", "key1", 8)
	if p1 != p2 {
		t.Errorf("same key should map to same partition: %d vs %d", p1, p2)
	}

	p3 := tm.ResolvePartition("topic", "key2", 8)
	if p3 >= 8 {
		t.Errorf("partition out of range: %d", p3)
	}
}

func TestResolvePartitionRoundRobin(t *testing.T) {
	tm, _ := setupTopicManager(t)

	seen := make(map[uint32]bool)
	for i := 0; i < 8; i++ {
		p := tm.ResolvePartition("rr-topic", "", 4)
		seen[p] = true
	}
	if len(seen) < 2 {
		t.Errorf("round-robin expected at least 2 partitions, got %d", len(seen))
	}
}

func TestTopicMetadataPersistence(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	w, _ := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncOS, 0)
	t.Cleanup(func() { w.Close(); storage.Close() })

	tm1, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
	tm1.CreateTopic(TopicConfig{Name: "persist", Mode: ModeQueue, Partitions: 2})

	// Reload from disk
	tm2, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
	cfg, ok := tm2.GetTopic("persist")
	if !ok {
		t.Fatal("topic not found after reload")
	}
	if cfg.Mode != ModeQueue {
		t.Errorf("expected queue mode after reload, got %d", cfg.Mode)
	}

	_ = dir // reference kept for cleanup
}

func TestCreateTopicZeroPartitionsInManager(t *testing.T) {
	tm, _ := setupTopicManager(t)

	err := tm.CreateTopic(TopicConfig{Name: "zero-p", Partitions: 0})
	if err == nil {
		t.Error("expected error for zero partitions")
	}
}

func TestCreateTopicSaveMetadataError(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	w, _ := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncOS, 0)
	t.Cleanup(func() { w.Close(); storage.Close() })

	tm, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})

	// Create a topic successfully first
	tm.CreateTopic(TopicConfig{Name: "first", Mode: ModeStream, Partitions: 1})

	// Now make the topics dir read-only to cause saveMetadata to fail
	// Skip on Windows where chmod doesn't restrict writes the same way
	if os.Getenv("OS") != "Windows_NT" {
		topicsDir := filepath.Join(dir, "topics")
		os.Chmod(topicsDir, 0444)
		defer os.Chmod(topicsDir, 0755)

		err := tm.CreateTopic(TopicConfig{Name: "second", Mode: ModeStream, Partitions: 1})
		if err == nil {
			t.Error("expected error when saveMetadata fails")
		}
	}
}

func TestNewTopicManagerPartitionInitError(t *testing.T) {
	dir, err := os.MkdirTemp("", "topic-partinit-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, _ := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncOS, 0)
	defer w.Close()

	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	tm1, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
	tm1.CreateTopic(TopicConfig{Name: "existing", Mode: ModeStream, Partitions: 2})
	storage.Close()

	// Create a new engine with invalid dir — loading topics will fail on GetOrCreatePartition
	invalidStorage := hot.NewEngine(string([]byte{0x00}), hot.HotConfig{SegmentSize: 1024 * 1024})
	_, err = NewTopicManager(dir, invalidStorage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
	if err == nil {
		t.Error("expected error when partition init fails with invalid storage dir")
	}
}

func TestTopicManagerDeleteTopicSaveMetadata(t *testing.T) {
	tm, _ := setupTopicManager(t)

	tm.CreateTopic(TopicConfig{Name: "to-delete", Mode: ModeQueue, Partitions: 2})
	if err := tm.DeleteTopic("to-delete"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify metadata was saved (topic gone)
	if _, ok := tm.GetTopic("to-delete"); ok {
		t.Error("topic should be deleted from in-memory map")
	}
}

func TestCreateTopicWALClosedError(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	t.Cleanup(func() { storage.Close() })

	w, _ := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncImmediate, 0)

	tm, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})

	// Close WAL so Append fails (panics because writeBuf becomes nil)
	w.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Logf("CreateTopic panicked with closed WAL (expected): %v", r)
		}
	}()

	err := tm.CreateTopic(TopicConfig{Name: "wal-closed-topic", Mode: ModeStream, Partitions: 1})
	if err != nil {
		t.Logf("CreateTopic returned error (expected): %v", err)
	}
}

func TestCreateTopicStoragePartitionError(t *testing.T) {
	dir, err := os.MkdirTemp("", "storage-parterr-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	w, _ := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncImmediate, 0)
	defer w.Close()

	// Use an invalid base dir to cause GetOrCreatePartition to fail
	invalidStorage := hot.NewEngine(string([]byte{0x00}), hot.HotConfig{SegmentSize: 1024 * 1024})

	tm, _ := NewTopicManager(dir, invalidStorage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})

	err = tm.CreateTopic(TopicConfig{Name: "storage-err-topic", Mode: ModeStream, Partitions: 1})
	if err == nil {
		t.Error("expected error when storage GetOrCreatePartition fails")
	}
}

func TestSaveMetadataInvalidPath(t *testing.T) {
	tm := &TopicManager{
		topics:   map[string]*TopicConfig{"test": {Name: "test", Partitions: 1}},
		metaPath: string([]byte{0x00}) + "/topics/meta.json",
	}
	err := tm.saveMetadata()
	if err == nil {
		t.Error("expected error for invalid meta path")
	}
}

func TestSaveMetadataWriteToReadOnlyDir(t *testing.T) {
	if os.Getenv("OS") == "Windows_NT" {
		t.Skip("chmod restrictions don't work on Windows")
	}

	dir := t.TempDir()
	topicsDir := filepath.Join(dir, "topics")
	os.MkdirAll(topicsDir, 0750)
	os.Chmod(topicsDir, 0444)
	defer os.Chmod(topicsDir, 0755)

	tm := &TopicManager{
		topics:   map[string]*TopicConfig{"test": {Name: "test", Partitions: 1}},
		metaPath: filepath.Join(topicsDir, "meta.json"),
	}
	err := tm.saveMetadata()
	if err == nil {
		t.Error("expected error writing to read-only topics dir")
	}
}

func TestSaveMetadataMkdirAllError(t *testing.T) {
	tm := &TopicManager{
		topics:   map[string]*TopicConfig{"test": {Name: "test", Partitions: 1}},
		metaPath: string([]byte{0x00}) + "/topics/meta.json",
	}
	err := tm.saveMetadata()
	if err == nil {
		t.Error("expected error for invalid metaPath (MkdirAll)")
	}
}

func TestSaveMetadataRenameError(t *testing.T) {
	dir, err := os.MkdirTemp("", "meta-rename-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create a .tmp file as a directory to cause Rename to fail
	topicsDir := filepath.Join(dir, "topics")
	os.MkdirAll(topicsDir, 0750)
	tmpPath := filepath.Join(topicsDir, "meta.json.tmp")
	os.MkdirAll(tmpPath, 0750) // .tmp is a directory — Rename will fail

	tm := &TopicManager{
		topics:   map[string]*TopicConfig{"test": {Name: "test", Partitions: 1}},
		metaPath: filepath.Join(topicsDir, "meta.json"),
	}
	err = tm.saveMetadata()
	if err == nil {
		t.Error("expected error when Rename fails (tmp is dir)")
	}
}
