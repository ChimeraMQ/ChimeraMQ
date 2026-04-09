package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/wal"
)

func TestEnvOverridesNodeName(t *testing.T) {
	os.Setenv("CHIMERA_NODE_NAME", "env-node")
	defer os.Unsetenv("CHIMERA_NODE_NAME")

	os.Setenv("CHIMERA_DATA_DIR", "/env-data")
	defer os.Unsetenv("CHIMERA_DATA_DIR")

	cfg, err := LoadConfig("", nil)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Node.Name != "env-node" {
		t.Errorf("name = %q, want env-node", cfg.Node.Name)
	}
	if cfg.Node.DataDir != "/env-data" {
		t.Errorf("data_dir = %q, want /env-data", cfg.Node.DataDir)
	}
}

func TestEnvOverridesAdminPort(t *testing.T) {
	os.Setenv("CHIMERA_ADMIN_PORT", "9999")
	defer os.Unsetenv("CHIMERA_ADMIN_PORT")

	cfg, err := LoadConfig("", nil)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Listener.AdminPort != 9999 {
		t.Errorf("admin_port = %d, want 9999", cfg.Listener.AdminPort)
	}
}

func TestEnvOverridesLogLevel(t *testing.T) {
	os.Setenv("CHIMERA_LOG_LEVEL", "debug")
	defer os.Unsetenv("CHIMERA_LOG_LEVEL")

	os.Setenv("CHIMERA_LOG_FORMAT", "text")
	defer os.Unsetenv("CHIMERA_LOG_FORMAT")

	cfg, err := LoadConfig("", nil)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("level = %q, want debug", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "text" {
		t.Errorf("format = %q, want text", cfg.Logging.Format)
	}
}

func TestEnvOverridesInvalidPort(t *testing.T) {
	os.Setenv("CHIMERA_LISTEN_PORT", "not-a-number")
	defer os.Unsetenv("CHIMERA_LISTEN_PORT")

	cfg, err := LoadConfig("", nil)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	// Invalid env value should be silently ignored
	if cfg.Listener.Port != 5672 {
		t.Errorf("port should remain default 5672, got %d", cfg.Listener.Port)
	}
}

func TestCLIOverridesBind(t *testing.T) {
	flags := &CLIFlags{Bind: "192.168.1.1"}
	cfg, err := LoadConfig("", flags)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Listener.Bind != "192.168.1.1" {
		t.Errorf("bind = %q, want 192.168.1.1", cfg.Listener.Bind)
	}
}

func TestCLIOverridesLogLevel(t *testing.T) {
	flags := &CLIFlags{LogLevel: "warn"}
	cfg, err := LoadConfig("", flags)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Logging.Level != "warn" {
		t.Errorf("level = %q, want warn", cfg.Logging.Level)
	}
}

func TestLoadConfigNonexistentYAML(t *testing.T) {
	_, err := LoadConfig("/nonexistent/path/chimera.yaml", nil)
	if err == nil {
		t.Error("expected error for nonexistent config file")
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "bad.yaml")
	os.WriteFile(cfgPath, []byte(":\n  invalid: [yaml: content"), 0644)

	_, err := LoadConfig(cfgPath, nil)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input string
		def   time.Duration
		want  time.Duration
	}{
		{"200ms", 100 * time.Millisecond, 200 * time.Millisecond},
		{"1s", 0, time.Second},
		{"", 100 * time.Millisecond, 100 * time.Millisecond},
		{"invalid", 50 * time.Millisecond, 50 * time.Millisecond},
	}
	for _, tt := range tests {
		got := ParseDuration(tt.input, tt.def)
		if got != tt.want {
			t.Errorf("ParseDuration(%q, %v) = %v, want %v", tt.input, tt.def, got, tt.want)
		}
	}
}

func TestCreateTopicZeroPartitions(t *testing.T) {
	tm, _ := setupTopicManager(t)

	err := tm.CreateTopic(TopicConfig{Name: "zero-part", Partitions: 0})
	if err == nil {
		t.Error("expected error for 0 partitions")
	}
}

func TestCreateTopicInvalidName(t *testing.T) {
	tm, _ := setupTopicManager(t)

	err := tm.CreateTopic(TopicConfig{Name: "has space", Partitions: 1})
	if err == nil {
		t.Error("expected error for invalid topic name")
	}
}

func TestCreateTopicEmptyName(t *testing.T) {
	tm, _ := setupTopicManager(t)

	err := tm.CreateTopic(TopicConfig{Name: "", Partitions: 1})
	if err == nil {
		t.Error("expected error for empty topic name")
	}
}

func TestCreateTopicTooLongName(t *testing.T) {
	tm, _ := setupTopicManager(t)

	longName := make([]byte, 256)
	for i := range longName {
		longName[i] = 'a'
	}

	err := tm.CreateTopic(TopicConfig{Name: string(longName), Partitions: 1})
	if err == nil {
		t.Error("expected error for 256-char topic name")
	}
}

func TestCreateTopicWithDLQConfig(t *testing.T) {
	tm, _ := setupTopicManager(t)

	err := tm.CreateTopic(TopicConfig{
		Name:       "dlq-source",
		Mode:       ModeQueue,
		Partitions: 1,
		DLQTopic:   "dlq-target",
		MaxRetries: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	cfg, ok := tm.GetTopic("dlq-source")
	if !ok {
		t.Fatal("topic not found")
	}
	if cfg.DLQTopic != "dlq-target" {
		t.Errorf("DLQ topic = %q, want dlq-target", cfg.DLQTopic)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("max retries = %d, want 3", cfg.MaxRetries)
	}
}

func TestValidateTopicNameValidChars(t *testing.T) {
	valid := []string{"topic", "My.Topic-1_2", "a", "A0._-"}
	for _, name := range valid {
		if err := validateTopicName(name); err != nil {
			t.Errorf("validateTopicName(%q): unexpected error: %v", name, err)
		}
	}
}

func TestValidateTopicNameSpecialChars(t *testing.T) {
	invalid := []string{"topic!", "topic@#", "topic:name", "topic\x00null"}
	for _, name := range invalid {
		if err := validateTopicName(name); err == nil {
			t.Errorf("validateTopicName(%q): expected error", name)
		}
	}
}

func TestNewTopicManagerWithCorruptMetadata(t *testing.T) {
	dir := t.TempDir()
	// Write corrupt metadata
	metaDir := filepath.Join(dir, "topics")
	os.MkdirAll(metaDir, 0750)
	os.WriteFile(filepath.Join(metaDir, "meta.json"), []byte("not-json"), 0640)

	_, err := NewTopicManager(dir, nil, nil)
	if err == nil {
		t.Error("expected error for corrupt metadata")
	}
}

func TestNewTopicManagerWithEmptyMetadata(t *testing.T) {
	dir := t.TempDir()

	// No metadata file — should work fine
	tm, err := NewTopicManager(dir, nil, nil)
	if err != nil {
		t.Fatalf("NewTopicManager with no metadata: %v", err)
	}
	if len(tm.ListTopics()) != 0 {
		t.Error("expected no topics")
	}
}

func TestNewTopicManagerLoadsExistingTopics(t *testing.T) {
	dir := t.TempDir()

	// Create a TopicManager and add a topic
	storage := hot.NewEngine(filepath.Join(dir, "data"), hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	w, err := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	tm, err := NewTopicManager(dir, storage, w)
	if err != nil {
		t.Fatal(err)
	}

	tm.CreateTopic(TopicConfig{
		Name:       "existing-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Reload — should load the existing topic
	tm2, err := NewTopicManager(dir, storage, w)
	if err != nil {
		t.Fatal(err)
	}

	cfg, ok := tm2.GetTopic("existing-topic")
	if !ok {
		t.Fatal("existing-topic not found after reload")
	}
	if cfg.Mode != ModeStream {
		t.Errorf("mode = %d, want ModeStream", cfg.Mode)
	}
	if cfg.Partitions != 1 {
		t.Errorf("partitions = %d, want 1", cfg.Partitions)
	}
}

func TestTopicManagerSaveAndReload(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(filepath.Join(dir, "data"), hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	w, err := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	tm, _ := NewTopicManager(dir, storage, w)

	// Create multiple topics
	for i := 0; i < 3; i++ {
		tm.CreateTopic(TopicConfig{
			Name:       fmt.Sprintf("topic-%d", i),
			Mode:       ModeQueue,
			Partitions: 2,
		})
	}

	// Reload
	tm2, _ := NewTopicManager(dir, storage, w)
	topics := tm2.ListTopics()
	if len(topics) != 3 {
		t.Errorf("expected 3 topics after reload, got %d", len(topics))
	}
}

func TestTopicManagerResolvePartitionWithRoutingKey(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(filepath.Join(dir, "data"), hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	w, _ := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncImmediate, 0)
	defer w.Close()

	tm, _ := NewTopicManager(dir, storage, w)

	// Same routing key should always resolve to same partition
	p1 := tm.ResolvePartition("test", "user-123", 4)
	p2 := tm.ResolvePartition("test", "user-123", 4)
	if p1 != p2 {
		t.Errorf("routing key should produce consistent partition: %d != %d", p1, p2)
	}

	// Different keys should produce different partitions (usually)
	p3 := tm.ResolvePartition("test", "user-456", 4)
	if p3 == p1 && p3 == p2 {
		// Not a hard error — just very unlikely with good hash
	}
}

func TestTopicManagerResolvePartitionRoundRobin(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(filepath.Join(dir, "data"), hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	w, _ := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncImmediate, 0)
	defer w.Close()

	tm, _ := NewTopicManager(dir, storage, w)

	// Without routing key, should round-robin
	p0 := tm.ResolvePartition("rr-topic", "", 4)
	p1 := tm.ResolvePartition("rr-topic", "", 4)
	if p0 == p1 {
		t.Errorf("round-robin should produce different partitions: %d == %d", p0, p1)
	}
}
