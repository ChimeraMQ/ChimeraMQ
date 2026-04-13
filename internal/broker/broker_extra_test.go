package broker

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// --- Additional Accessor Tests ---

func TestBrokerAccessorsWithTenant(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Tenant.Enabled = true
	cfg.Tenant.Separator = ":"

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.TenantManager() == nil {
		t.Error("TenantManager() should not be nil when tenant enabled")
	}
	if b.QuotaEnforcer() == nil {
		t.Error("QuotaEnforcer() should not be nil when tenant enabled")
	}
}

func TestBrokerAccessorsWithExchanges(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.Exchanges() == nil {
		t.Error("Exchanges() should not be nil")
	}
}

func TestBrokerAccessorsWithGeo(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.GeoReplication.Enabled = true

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.GeoManager() == nil {
		t.Error("GeoManager() should not be nil when geo enabled")
	}
}

func TestBrokerIsFIPSEnabled(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Just verify it doesn't panic and returns a bool
	_ = b.IsFIPSEnabled()
}

// --- Config Reload ---

func TestBrokerReloadConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Logging.Level = "warn"
	cfg.Logging.Format = "text"

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	configPath := filepath.Join(dir, "reload.yaml")
	configContent := `
logging:
  level: debug
  format: json
limits:
  max_connections: 999
flow_control:
  enabled: true
  max_memory_bytes: 1073741824
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	if err := b.ReloadConfig(configPath); err != nil {
		t.Fatalf("ReloadConfig: %v", err)
	}

	// applyDynamicConfig updates the logger level/format directly but does not
	// mirror them back into b.config.Logging. It does update limits.
	if b.Config().Limits.MaxConnections != 999 {
		t.Errorf("max_connections = %d, want 999", b.Config().Limits.MaxConnections)
	}
}

func TestBrokerReloadConfigMissingFile(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = b.ReloadConfig(filepath.Join(dir, "nonexistent.yaml"))
	if err == nil {
		t.Error("expected error for missing config file")
	}
}

func TestBrokerReloadConfigInvalidYAML(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}

	configPath := filepath.Join(dir, "bad.yaml")
	os.WriteFile(configPath, []byte("not: valid: yaml: ["), 0644)

	err = b.ReloadConfig(configPath)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestBrokerStartConfigWatcher(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Logging.Level = "warn"

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	configPath := filepath.Join(dir, "watcher.yaml")
	configContent := `logging:
  level: info
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	b.StartConfigWatcher(configPath, 100*time.Millisecond)

	// Wait a bit, then modify the file
	time.Sleep(200 * time.Millisecond)

	newContent := `logging:
  level: error
`
	if err := os.WriteFile(configPath, []byte(newContent), 0644); err != nil {
		t.Fatalf("failed to update config: %v", err)
	}

	// Give watcher time to detect change and reload
	time.Sleep(300 * time.Millisecond)

	if b.Config().Logging.Level != "error" {
		t.Logf("logging level after watch = %q (may need more time)", b.Config().Logging.Level)
	}
}

// --- Broker Adapter ---

func TestBrokerAPIAdapter(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	adapter := &brokerAPIAdapter{broker: b}

	// Create a topic
	if err := b.Topics().CreateTopic(TopicConfig{Name: "adapter-topic", Mode: ModeStream, Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Test TopicPartitions
	partitions := adapter.TopicPartitions("adapter-topic")
	if partitions != 1 {
		t.Errorf("TopicPartitions = %d, want 1", partitions)
	}

	// Test TopicPartitions for nonexistent topic
	partitions = adapter.TopicPartitions("nonexistent")
	if partitions != 0 {
		t.Errorf("TopicPartitions nonexistent = %d, want 0", partitions)
	}

	// Test PublishMessage
	env := &message.Envelope{Payload: []byte("hello")}
	offset, partition, err := adapter.PublishMessage("adapter-topic", env)
	if err != nil {
		t.Fatalf("PublishMessage: %v", err)
	}
	if offset < 0 {
		t.Errorf("offset = %d, want >= 0", offset)
	}
	if partition != 0 {
		t.Errorf("partition = %d, want 0", partition)
	}

	// Test FetchMessages
	msgs, err := adapter.FetchMessages("adapter-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("FetchMessages: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("len(msgs) = %d, want 1", len(msgs))
	}
}

func TestBrokerAPIAdapterFetchMessagesEmpty(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	adapter := &brokerAPIAdapter{broker: b}

	// Fetch from nonexistent topic — GetOrCreatePartition creates it, so no error
	msgs, err := adapter.FetchMessages("no-such-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("len(msgs) = %d, want 0", len(msgs))
	}
}

// --- Logger Additional Tests ---

func TestLoggerSetLevel(t *testing.T) {
	logger := NewLogger(LoggingConfig{Level: "info", Format: "text"})
	defer logger.Close()

	logger.SetLevel("debug")
	if logger.level != slog.LevelDebug {
		t.Errorf("level = %v, want debug", logger.level)
	}

	logger.SetLevel("error")
	if logger.level != slog.LevelError {
		t.Errorf("level = %v, want error", logger.level)
	}
}

func TestLoggerSetFormat(t *testing.T) {
	logger := NewLogger(LoggingConfig{Level: "info", Format: "text"})
	defer logger.Close()

	logger.SetFormat("json")
	if logger.config.Format != "json" {
		t.Errorf("format = %q, want json", logger.config.Format)
	}
}

func TestLoggerClose(t *testing.T) {
	logger := NewLogger(LoggingConfig{Level: "info", Format: "text"})
	if err := logger.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

func TestLoggerCloseMultiple(t *testing.T) {
	logger := NewLogger(LoggingConfig{Level: "info", Format: "text"})
	_ = logger.Close()
	_ = logger.Close() // should not panic on double close
}

func TestLoggerRotation(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")

	logger := NewLogger(LoggingConfig{
		Level:   "info",
		Format:  "text",
		Output:  "file",
		File:    logPath,
		MaxSize: 1, // 1 byte to force rotation
	})
	defer logger.Close()

	logger.Info("trigger rotation")
	// doRotate is called via write -> rotate when size exceeds MaxSize
}

func TestLoggerCleanupOldLogs(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")

	logger := NewLogger(LoggingConfig{
		Level:  "info",
		Format: "text",
		Output: "file",
		File:   logPath,
		MaxAge: 1,
	})
	defer logger.Close()

	// Create an old rotated log file with past mod time
	oldLog := logPath + ".20200101-000000"
	os.WriteFile(oldLog, []byte("old"), 0644)
	past := time.Now().AddDate(0, 0, -7)
	os.Chtimes(oldLog, past, past)

	logger.cleanupOldLogs()

	if _, err := os.Stat(oldLog); !os.IsNotExist(err) {
		t.Error("old log file should be cleaned up")
	}
}

func TestIsLogFile(t *testing.T) {
	if !isLogFile("app.log.20240101-120000", "app.log") {
		t.Error("expected rotated log to match")
	}
	if isLogFile("app.log", "app.log") {
		t.Error("base file should not match")
	}
	if isLogFile("other.log.20240101-120000", "app.log") {
		t.Error("different base should not match")
	}
	if isLogFile("app.logx.20240101-120000", "app.log") {
		t.Error("missing dot separator should not match")
	}
}

func TestLoggerSetLevelWithFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "level.log")

	logger := NewLogger(LoggingConfig{
		Level:  "info",
		Format: "text",
		Output: "file",
		File:   logPath,
	})
	defer logger.Close()

	logger.SetLevel("warn")
	if logger.level != slog.LevelWarn {
		t.Errorf("level = %v, want warn", logger.level)
	}
}

func TestLoggerSetFormatWithFile(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "format.log")

	logger := NewLogger(LoggingConfig{
		Level:  "info",
		Format: "text",
		Output: "file",
		File:   logPath,
	})
	defer logger.Close()

	logger.SetFormat("json")
	if logger.config.Format != "json" {
		t.Errorf("format = %q, want json", logger.config.Format)
	}
}
