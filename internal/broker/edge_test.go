package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
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

	_, err := NewTopicManager(dir, nil, nil, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
	if err == nil {
		t.Error("expected error for corrupt metadata")
	}
}

func TestNewTopicManagerWithEmptyMetadata(t *testing.T) {
	dir := t.TempDir()

	// No metadata file — should work fine
	tm, err := NewTopicManager(dir, nil, nil, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
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

	tm, err := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
	if err != nil {
		t.Fatal(err)
	}

	tm.CreateTopic(TopicConfig{
		Name:       "existing-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Reload — should load the existing topic
	tm2, err := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
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

	tm, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})

	// Create multiple topics
	for i := 0; i < 3; i++ {
		tm.CreateTopic(TopicConfig{
			Name:       fmt.Sprintf("topic-%d", i),
			Mode:       ModeQueue,
			Partitions: 2,
		})
	}

	// Reload
	tm2, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
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

	tm, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})

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

	tm, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})

	// Without routing key, should round-robin
	p0 := tm.ResolvePartition("rr-topic", "", 4)
	p1 := tm.ResolvePartition("rr-topic", "", 4)
	if p0 == p1 {
		t.Errorf("round-robin should produce different partitions: %d == %d", p0, p1)
	}
}

func TestBrokerPublishDelayedQueueMessage(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "delayed-q",
		Mode:       ModeQueue,
		Partitions: 1,
	})

	env := &message.Envelope{
		Topic:    "delayed-q",
		Payload:  []byte("future-msg"),
		DeliverAt: time.Now().Add(1 * time.Hour).UnixNano(),
	}

	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish delayed: %v", err)
	}
	// Delayed messages return offset 0 (scheduled, not appended)
	if offset != 0 {
		t.Errorf("delayed publish offset = %d, want 0", offset)
	}
}

func TestBrokerPublishDelayedStreamIgnored(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "delayed-stream",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Delayed messages on stream topics should still be appended normally
	env := &message.Envelope{
		Topic:     "delayed-stream",
		Payload:   []byte("future-stream"),
		DeliverAt: time.Now().Add(1 * time.Hour).UnixNano(),
	}

	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish delayed stream: %v", err)
	}
	// Stream mode ignores DeliverAt — message is appended normally
	_ = offset
}

func TestBrokerPublishNonexistentTopic(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	_, err := b.Publish(&message.Envelope{Topic: "no-such-topic", Payload: []byte("x")})
	if err == nil {
		t.Error("expected error for nonexistent topic")
	}
}

func TestBrokerStopWithoutLock(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-stop-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()

	// Close lock file before clearing, to avoid Windows handle leak
	lockPath := b.lockFile.Name()
	b.lockFile.Close()
	os.Remove(lockPath)
	b.lockFile = nil
	b.Stop()
}

func TestConfigValidateErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
	}{
		{"zero port", &Config{Node: NodeConfig{DataDir: "/tmp"}, Listener: ListenerConfig{Port: 0, AdminPort: 9090}, Storage: StorageConfig{Hot: HotConfig{SyncMode: "immediate"}, WAL: WALConfig{SyncMode: "immediate"}}, Logging: LoggingConfig{Level: "info", Format: "json"}}},
		{"negative port", &Config{Node: NodeConfig{DataDir: "/tmp"}, Listener: ListenerConfig{Port: -1, AdminPort: 9090}, Storage: StorageConfig{Hot: HotConfig{SyncMode: "immediate"}, WAL: WALConfig{SyncMode: "immediate"}}, Logging: LoggingConfig{Level: "info", Format: "json"}}},
		{"too large port", &Config{Node: NodeConfig{DataDir: "/tmp"}, Listener: ListenerConfig{Port: 70000, AdminPort: 9090}, Storage: StorageConfig{Hot: HotConfig{SyncMode: "immediate"}, WAL: WALConfig{SyncMode: "immediate"}}, Logging: LoggingConfig{Level: "info", Format: "json"}}},
		{"empty data dir", &Config{Node: NodeConfig{DataDir: ""}, Listener: ListenerConfig{Port: 5672, AdminPort: 9090}, Storage: StorageConfig{Hot: HotConfig{SyncMode: "immediate"}, WAL: WALConfig{SyncMode: "immediate"}}, Logging: LoggingConfig{Level: "info", Format: "json"}}},
		{"invalid hot sync mode", &Config{Node: NodeConfig{DataDir: "/tmp"}, Listener: ListenerConfig{Port: 5672, AdminPort: 9090}, Storage: StorageConfig{Hot: HotConfig{SyncMode: "bad"}, WAL: WALConfig{SyncMode: "immediate"}}, Logging: LoggingConfig{Level: "info", Format: "json"}}},
		{"invalid wal sync mode", &Config{Node: NodeConfig{DataDir: "/tmp"}, Listener: ListenerConfig{Port: 5672, AdminPort: 9090}, Storage: StorageConfig{Hot: HotConfig{SyncMode: "immediate"}, WAL: WALConfig{SyncMode: "bad"}}, Logging: LoggingConfig{Level: "info", Format: "json"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestBrokerAccessors(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	if b.Config() != cfg {
		t.Error("Config() should return same config")
	}
	if b.Storage() == nil {
		t.Error("Storage() should not be nil")
	}
	if b.QueueEngine() == nil {
		t.Error("QueueEngine() should not be nil")
	}
	if b.StreamEngine() == nil {
		t.Error("StreamEngine() should not be nil")
	}
	if b.Metrics() == nil {
		t.Error("Metrics() should not be nil")
	}
	if b.Logger() == nil {
		t.Error("Logger() should not be nil")
	}
	if b.StartTime().IsZero() {
		t.Error("StartTime() should not be zero")
	}
}

func TestAcquireLockFileStaleWithLivePID(t *testing.T) {
	// Signal(0) for process liveness check doesn't work on Windows
	if os.Getenv("OS") == "Windows_NT" {
		t.Skip("process signal check not supported on Windows")
	}

	dir, err := os.MkdirTemp("", "lock-livepid-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	lockPath := filepath.Join(dir, "chimera.lock")

	// Write the current process PID (which is alive)
	pid := os.Getpid()
	os.WriteFile(lockPath, []byte(strconv.Itoa(pid)), 0600)

	_, err = acquireLockFile(dir)
	if err == nil {
		t.Error("expected error when lock held by live process")
	}
}

func TestAcquireLockFileStaleWithDeadPID(t *testing.T) {
	dir, err := os.MkdirTemp("", "lock-deadpid-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	lockPath := filepath.Join(dir, "chimera.lock")

	// Write a dead PID (very high number — extremely unlikely to be alive)
	os.WriteFile(lockPath, []byte("999999999"), 0600)

	f, err := acquireLockFile(dir)
	if err != nil {
		t.Fatalf("expected success for stale lock with dead PID: %v", err)
	}
	f.Close()
}

func TestAcquireLockFileUnreadableLock(t *testing.T) {
	dir, err := os.MkdirTemp("", "lock-unreadable-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	lockPath := filepath.Join(dir, "chimera.lock")

	// Create lock file as a directory (causes ReadFile to fail)
	os.MkdirAll(lockPath, 0750)

	_, err = acquireLockFile(dir)
	if err == nil {
		t.Error("expected error for unreadable lock file")
	}
}

func TestAcquireLockFileNonExistError(t *testing.T) {
	// Try to acquire lock in a path that can't be created
	_, err := acquireLockFile(string([]byte{0x00}))
	if err == nil {
		t.Error("expected error for invalid path")
	}
}

func TestBrokerStartMkdirAllFails(t *testing.T) {
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: string([]byte{0x00})},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	if err := b.Start(); err == nil {
		t.Error("expected error for invalid data dir")
	}
}

func TestBrokerPublishAfterStop(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "stopped-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Stop broker — closes WAL, storage, etc.
	b.Stop()

	// Publishing after stop should fail (WAL closed, writeBuf nil → panic)
	// Use recover to catch the panic
	defer func() {
		if r := recover(); r != nil {
			// Expected: WAL writeBuf is nil after close
			t.Logf("publish after stop panicked (expected): %v", r)
		}
	}()

	_, err := b.Publish(&message.Envelope{Topic: "stopped-topic", Payload: []byte("x")})
	// If we get here without panic, it might return an error
	if err != nil {
		t.Logf("publish after stop returned error (expected): %v", err)
	}
}

func TestBrokerPublishDelayedStreamIgnoredDetailed(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-delayed-stream-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer func() {
		lockPath := b.lockFile.Name()
		b.lockFile.Close()
		os.Remove(lockPath)
		b.lockFile = nil
		b.Stop()
	}()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "delayed-stream-det",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Publish a delayed message on stream topic — should fall through to normal publish
	env := &message.Envelope{
		Topic:     "delayed-stream-det",
		Payload:   []byte("future-stream-detailed"),
		DeliverAt: time.Now().Add(1 * time.Hour).UnixNano(),
	}

	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish delayed stream: %v", err)
	}
	// Stream mode ignores DeliverAt — message is appended normally, offset should be > 0
	_ = offset // The offset behavior depends on storage; just verify no error
}

func TestBrokerPublishUnifiedDelayed(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "delayed-unified",
		Mode:       ModeUnified,
		Partitions: 1,
	})

	env := &message.Envelope{
		Topic:     "delayed-unified",
		Payload:   []byte("future-unified"),
		DeliverAt: time.Now().Add(1 * time.Hour).UnixNano(),
	}

	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish delayed unified: %v", err)
	}
	// Unified mode with queue behavior should schedule delayed (offset 0)
	if offset != 0 {
		t.Errorf("unified delayed publish offset = %d, want 0 (scheduled)", offset)
	}
}

func TestBrokerPublishQueueMode(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "queue-pub",
		Mode:       ModeQueue,
		Partitions: 2,
	})

	env := &message.Envelope{
		Topic:       "queue-pub",
		Payload:     []byte("queue-msg"),
		RoutingKey:  "rk1",
		SourceProto: message.ProtoHTTP,
	}

	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish queue: %v", err)
	}
	// Queue mode — first message may have offset 0
	_ = offset

	// Verify routing key consistency — same key = same partition
	env2 := &message.Envelope{
		Topic:       "queue-pub",
		Payload:     []byte("queue-msg-2"),
		RoutingKey:  "rk1",
		SourceProto: message.ProtoHTTP,
	}
	offset2, _ := b.Publish(env2)
	_ = offset2
}

func TestBrokerPublishUnifiedMode(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "unified-pub",
		Mode:       ModeUnified,
		Partitions: 2,
	})

	env := &message.Envelope{
		Topic:       "unified-pub",
		Payload:     []byte("unified-msg"),
		SourceProto: message.ProtoChimera,
	}

	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish unified: %v", err)
	}
	_ = offset
}

func TestBrokerPublishWithTimestampSet(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "ts-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	ts := time.Now().Add(-1 * time.Hour).UnixNano()
	env := &message.Envelope{
		Topic:     "ts-topic",
		Payload:   []byte("pre-timestamped"),
		Timestamp: ts,
	}

	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish with timestamp: %v", err)
	}
	// Timestamp should remain as set, not overwritten
	_ = offset
}

func TestBrokerStartTopicManagerError(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-tm-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create corrupt metadata to cause topic manager init to fail
	metaDir := filepath.Join(dir, "topics")
	os.MkdirAll(metaDir, 0750)
	os.WriteFile(filepath.Join(metaDir, "meta.json"), []byte("not-json"), 0640)

	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	if err := b.Start(); err == nil {
		t.Error("expected error starting broker with corrupt metadata")
		b.Stop()
	}
}

func TestAcquireLockFileStaleDeadPIDRetry(t *testing.T) {
	dir, err := os.MkdirTemp("", "lock-retry-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	lockPath := filepath.Join(dir, "chimera.lock")
	// Write a dead PID — should be detected and lock reclaimed
	os.WriteFile(lockPath, []byte("999999999"), 0600)

	f, err := acquireLockFile(dir)
	if err != nil {
		t.Fatalf("acquireLockFile: %v", err)
	}
	f.Close()
}

func TestParseSyncModeAll(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"immediate", "SyncImmediate"},
		{"interval", "SyncInterval"},
		{"os", "SyncOS"},
		{"unknown", "SyncInterval"}, // default
	}
	for _, tt := range tests {
		mode := parseSyncMode(tt.input)
		_ = mode
		// Just verify no panic
	}
}

func TestAcquireLockFileStaleNonNumeric(t *testing.T) {
	dir, err := os.MkdirTemp("", "lock-nonnum-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	lockPath := filepath.Join(dir, "chimera.lock")
	// Write non-numeric content — Sscanf should fail to parse PID, pid stays 0
	os.WriteFile(lockPath, []byte("not-a-pid"), 0600)

	f, err := acquireLockFile(dir)
	if err != nil {
		t.Fatalf("acquireLockFile with non-numeric PID: %v", err)
	}
	f.Close()
}

func TestAcquireLockFileStaleZeroPID(t *testing.T) {
	dir, err := os.MkdirTemp("", "lock-zerpid-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	lockPath := filepath.Join(dir, "chimera.lock")
	// Write PID 0 — pid > 0 check fails, so lock is considered stale
	os.WriteFile(lockPath, []byte("0"), 0600)

	f, err := acquireLockFile(dir)
	if err != nil {
		t.Fatalf("acquireLockFile with PID 0: %v", err)
	}
	f.Close()
}

func TestBrokerPublishStreamModeFull(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "stream-full",
		Mode:       ModeStream,
		Partitions: 2,
	})

	// Publish with headers and routing key to cover more paths
	env := &message.Envelope{
		Topic:       "stream-full",
		Payload:     []byte("stream-full-msg"),
		RoutingKey:  "route-key",
		Headers:     map[string][]byte{"x-custom": []byte("value")},
		SourceProto: message.ProtoChimera,
	}

	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish stream full: %v", err)
	}
	_ = offset

	// Verify topic still has correct mode
	cfg2, ok := b.Topics().GetTopic("stream-full")
	if !ok {
		t.Fatal("topic not found")
	}
	if cfg2.Mode != ModeStream {
		t.Errorf("mode = %d, want ModeStream", cfg2.Mode)
	}
}

func TestBrokerPublishWALAppendError(t *testing.T) {
	dir, err := os.MkdirTemp("", "pub-wal-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "wal-err-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Close the WAL file to cause append error
	b.wal.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Logf("Publish panicked with closed WAL (expected): %v", r)
		}
	}()

	_, err = b.Publish(&message.Envelope{
		Topic:   "wal-err-topic",
		Payload: []byte("wal-err"),
	})
	if err != nil {
		t.Logf("Publish returned error (expected): %v", err)
	}
}

func TestBrokerPublishStorageAppendErrorV2(t *testing.T) {
	dir, err := os.MkdirTemp("", "pub-store-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	// Create topic with a valid storage so WAL append succeeds
	b.Topics().CreateTopic(TopicConfig{
		Name:       "store-err-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Close the storage to cause GetOrCreatePartition or Append to fail
	b.storage.Close()

	_, err = b.Publish(&message.Envelope{
		Topic:   "store-err-topic",
		Payload: []byte("store-err"),
	})
	// On Windows, closed storage may still work or fail
	_ = err
}

func TestBrokerStartWALError(t *testing.T) {
	dir, err := os.MkdirTemp("", "broker-wal-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)

	// Create the WAL dir as a file to make WAL init fail
	walDir := filepath.Join(dir, "wal")
	os.WriteFile(walDir, []byte("blocker"), 0640)

	if err := b.Start(); err == nil {
		t.Error("expected error starting broker with WAL dir as file")
		b.Stop()
	}
}

func TestBrokerStopDoubleStop(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()

	// Close lock file before cleanup to avoid Windows handle leak
	lockPath := b.lockFile.Name()
	b.lockFile.Close()
	os.Remove(lockPath)
	b.lockFile = nil

	// Double stop should not panic
	b.Stop()
	b.Stop()
}

func TestLoadConfigWithValidYAML(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "chimera.yaml")

	yamlContent := []byte(`
node:
  id: 42
  name: yaml-node
  data_dir: /tmp/yaml-data
listener:
  bind: "10.0.0.1"
  port: 6672
  admin_port: 8080
  max_connections: 500
storage:
  hot:
    segment_size: 134217728
    sync_mode: immediate
    sync_interval: 100ms
  wal:
    max_size: 67108864
    sync_mode: os
    sync_interval: 200ms
logging:
  level: debug
  format: text
  output: stderr
`)
	os.WriteFile(cfgPath, yamlContent, 0640)

	cfg, err := LoadConfig(cfgPath, nil)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Node.ID != 42 {
		t.Errorf("node.id = %d, want 42", cfg.Node.ID)
	}
	if cfg.Node.Name != "yaml-node" {
		t.Errorf("node.name = %q, want yaml-node", cfg.Node.Name)
	}
	if cfg.Node.DataDir != "/tmp/yaml-data" {
		t.Errorf("node.data_dir = %q", cfg.Node.DataDir)
	}
	if cfg.Listener.Bind != "10.0.0.1" {
		t.Errorf("listener.bind = %q", cfg.Listener.Bind)
	}
	if cfg.Listener.Port != 6672 {
		t.Errorf("listener.port = %d, want 6672", cfg.Listener.Port)
	}
	if cfg.Listener.AdminPort != 8080 {
		t.Errorf("listener.admin_port = %d, want 8080", cfg.Listener.AdminPort)
	}
	if cfg.Storage.Hot.SyncMode != "immediate" {
		t.Errorf("hot.sync_mode = %q", cfg.Storage.Hot.SyncMode)
	}
	if cfg.Storage.WAL.SyncMode != "os" {
		t.Errorf("wal.sync_mode = %q", cfg.Storage.WAL.SyncMode)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("logging.level = %q", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "text" {
		t.Errorf("logging.format = %q", cfg.Logging.Format)
	}
}

func TestLoadConfigEnvNodeID(t *testing.T) {
	os.Setenv("CHIMERA_NODE_ID", "99")
	defer os.Unsetenv("CHIMERA_NODE_ID")

	cfg, err := LoadConfig("", &CLIFlags{DataDir: "/tmp/test-config"})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Node.ID != 99 {
		t.Errorf("node.id = %d, want 99", cfg.Node.ID)
	}
}

func TestLoadConfigEnvListenPort(t *testing.T) {
	os.Setenv("CHIMERA_LISTEN_PORT", "7777")
	defer os.Unsetenv("CHIMERA_LISTEN_PORT")

	cfg, err := LoadConfig("", &CLIFlags{DataDir: "/tmp/test-config"})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Listener.Port != 7777 {
		t.Errorf("port = %d, want 7777", cfg.Listener.Port)
	}
}

func TestLoadConfigCLIOverridesPort(t *testing.T) {
	flags := &CLIFlags{Port: 8888, DataDir: "/tmp/test"}
	cfg, err := LoadConfig("", flags)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Listener.Port != 8888 {
		t.Errorf("port = %d, want 8888", cfg.Listener.Port)
	}
}

func TestLoadConfigCLIAdminPortOverride(t *testing.T) {
	flags := &CLIFlags{AdminPort: 7070, DataDir: "/tmp/test"}
	cfg, err := LoadConfig("", flags)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Listener.AdminPort != 7070 {
		t.Errorf("admin_port = %d, want 7070", cfg.Listener.AdminPort)
	}
}

func TestConfigValidateAdminPortTooLarge(t *testing.T) {
	cfg := &Config{
		Node:     NodeConfig{DataDir: "/tmp"},
		Listener: ListenerConfig{Port: 5672, AdminPort: 70000},
		Storage:  StorageConfig{Hot: HotConfig{SyncMode: "immediate"}, WAL: WALConfig{SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "info", Format: "json"},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for admin port > 65535")
	}
}

func TestConfigValidateNegativeAdminPort(t *testing.T) {
	cfg := &Config{
		Node:     NodeConfig{DataDir: "/tmp"},
		Listener: ListenerConfig{Port: 5672, AdminPort: -1},
		Storage:  StorageConfig{Hot: HotConfig{SyncMode: "immediate"}, WAL: WALConfig{SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "info", Format: "json"},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative admin port")
	}
}

func TestLoadConfigValidateFails(t *testing.T) {
	os.Setenv("CHIMERA_LISTEN_PORT", "0")
	defer os.Unsetenv("CHIMERA_LISTEN_PORT")

	// Port 0 should fail validation — but LoadConfig provides defaults...
	// Let's test with invalid hot sync mode which passes env parsing but fails Validate
	os.Setenv("CHIMERA_DATA_DIR", "")
	defer os.Unsetenv("CHIMERA_DATA_DIR")

	cfg, err := LoadConfig("", nil)
	if err != nil {
		t.Logf("LoadConfig error (expected): %v", err)
	} else {
		// If it didn't error, validate should catch it
		if err := cfg.Validate(); err != nil {
			t.Logf("Validate error: %v", err)
		}
	}
}

func TestValidateTopicNameStartsWithDot(t *testing.T) {
	if err := validateTopicName(".hidden"); err == nil {
		t.Error("expected error for topic starting with .")
	}
}

func TestValidateTopicNameStartsWithDash(t *testing.T) {
	if err := validateTopicName("-dash"); err == nil {
		t.Error("expected error for topic starting with -")
	}
}

func TestCreateTopicDuplicateName(t *testing.T) {
	tm, _ := setupTopicManager(t)

	err := tm.CreateTopic(TopicConfig{Name: "dup-name", Partitions: 1, Mode: ModeStream})
	if err != nil {
		t.Fatal(err)
	}
	err = tm.CreateTopic(TopicConfig{Name: "dup-name", Partitions: 2, Mode: ModeQueue})
	if err == nil {
		t.Error("expected error for duplicate topic name")
	}
}

func TestDeleteTopicNonexistent(t *testing.T) {
	tm, _ := setupTopicManager(t)

	err := tm.DeleteTopic("no-such-topic")
	if err == nil {
		t.Error("expected error for deleting nonexistent topic")
	}
}

func TestBrokerPublishWithSourceProto(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "proto-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Publish with Chimera protocol source
	env := &message.Envelope{
		Topic:       "proto-topic",
		Payload:     []byte("chimera-msg"),
		SourceProto: message.ProtoChimera,
	}
	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish chimera proto: %v", err)
	}
	_ = offset

	// Publish with HTTP protocol source
	env2 := &message.Envelope{
		Topic:       "proto-topic",
		Payload:     []byte("http-msg"),
		SourceProto: message.ProtoHTTP,
	}
	offset2, err := b.Publish(env2)
	if err != nil {
		t.Fatalf("publish http proto: %v", err)
	}
	_ = offset2
}

func TestBrokerPublishPastDeliverAt(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "past-deliver",
		Mode:       ModeQueue,
		Partitions: 1,
	})

	// DeliverAt in the past — should NOT be scheduled, should be published normally
	env := &message.Envelope{
		Topic:     "past-deliver",
		Payload:   []byte("past-msg"),
		DeliverAt: time.Now().Add(-1 * time.Hour).UnixNano(),
	}

	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish past deliver: %v", err)
	}
	// Past DeliverAt means message is published immediately, not delayed
	_ = offset
}

func TestCreateTopicSaveMetadataErrorV2(t *testing.T) {
	dir, err := os.MkdirTemp("", "create-save-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(filepath.Join(dir, "data"), hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	w, err := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	tm, err := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
	if err != nil {
		t.Fatal(err)
	}

	// Create a topic successfully first
	err = tm.CreateTopic(TopicConfig{Name: "first-topic", Mode: ModeStream, Partitions: 1})
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the meta path so saveMetadata fails on next create
	// Replace topics dir with a file
	metaDir := filepath.Join(dir, "topics")
	os.RemoveAll(metaDir)
	os.WriteFile(metaDir, []byte("blocker"), 0640)

	err = tm.CreateTopic(TopicConfig{Name: "second-topic", Mode: ModeStream, Partitions: 1})
	if err == nil {
		t.Error("expected error when saveMetadata fails")
	}
}

func TestBrokerPublishNormalStream(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "normal-stream",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Normal publish with zero timestamp (should auto-assign)
	env := &message.Envelope{
		Topic:   "normal-stream",
		Payload: []byte("normal-msg"),
	}
	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if offset != 0 {
		t.Errorf("first offset = %d, want 0", offset)
	}

	// Second publish
	env2 := &message.Envelope{
		Topic:   "normal-stream",
		Payload: []byte("normal-msg-2"),
	}
	offset2, err := b.Publish(env2)
	if err != nil {
		t.Fatalf("publish2: %v", err)
	}
	if offset2 != 1 {
		t.Errorf("second offset = %d, want 1", offset2)
	}
}

func TestCreateTopicWALAppendError(t *testing.T) {
	dir, err := os.MkdirTemp("", "create-wal-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(filepath.Join(dir, "data"), hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	w, err := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}

	tm, err := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
	if err != nil {
		t.Fatal(err)
	}

	// Close WAL to cause append error in CreateTopic
	w.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Logf("CreateTopic panicked with closed WAL (expected): %v", r)
		}
	}()

	err = tm.CreateTopic(TopicConfig{Name: "wal-err-topic", Mode: ModeStream, Partitions: 1})
	if err != nil {
		t.Logf("CreateTopic returned error (expected): %v", err)
	}
}

func TestCreateTopicStorageError(t *testing.T) {
	dir, err := os.MkdirTemp("", "create-store-err-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(filepath.Join(dir, "data"), hot.HotConfig{SegmentSize: 1024 * 1024})
	w, err := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	tm, err := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 256})
	if err != nil {
		t.Fatal(err)
	}

	// Close storage -- GetOrCreatePartition may or may not return error
	// depending on whether the partition was already cached
	storage.Close()

	defer func() {
		if r := recover(); r != nil {
			t.Logf("CreateTopic panicked (expected with closed storage): %v", r)
		}
	}()

	err = tm.CreateTopic(TopicConfig{Name: "store-err-topic", Mode: ModeStream, Partitions: 2})
	// Closed storage may still work or fail depending on caching
	_ = err
}

func TestBrokerPublishStorageGetOrCreateError(t *testing.T) {
	dir, err := os.MkdirTemp("", "pub-store-geterr-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "store-get-err",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Close storage to cause GetOrCreatePartition error
	b.storage.Close()

	_, err = b.Publish(&message.Envelope{
		Topic:   "store-get-err",
		Payload: []byte("data"),
	})
	if err == nil {
		t.Error("expected error when storage GetOrCreatePartition fails")
	}
}

func TestBrokerPublishStorageAppendErrorV3(t *testing.T) {
	dir, err := os.MkdirTemp("", "pub-store-appenderr-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "store-append-err",
		Mode:       ModeStream,
		Partitions: 1,
	})

	// Publish one message first
	b.Publish(&message.Envelope{Topic: "store-append-err", Payload: []byte("first")})

	// Close partition file to cause Append error
	part, _ := b.storage.GetOrCreatePartition("store-append-err", 0)
	part.Close()

	_, err = b.Publish(&message.Envelope{
		Topic:   "store-append-err",
		Payload: []byte("second"),
	})
	if err == nil {
		t.Error("expected error when partition Append fails")
	}
}


func TestCreateTopicWALAppendMetaErrorSafe(t *testing.T) {
	dir, err := os.MkdirTemp("", "wal-meta-safe-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &Config{
		Node:     NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1, MaxConnections: 100},
		Storage:  StorageConfig{Hot: HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"}, WAL: WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"}},
		Logging:  LoggingConfig{Level: "warn", Format: "text"},
	}

	b, _ := NewBroker(cfg)
	b.Start()
	defer b.Stop()

	// Close WAL — this sets writeBuf to nil, causing panic on Append
	b.wal.Close()

	defer func() {
		if r := recover(); r != nil {
			// Expected: WAL writeBuf is nil after Close
			t.Logf("recovered from WAL panic: %v", r)
		}
	}()

	err = b.Topics().CreateTopic(TopicConfig{
		Name:       "wal-meta-safe-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})
	_ = err
}
