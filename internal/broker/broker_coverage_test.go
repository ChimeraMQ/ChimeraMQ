package broker

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/storage/wal"
)

// --- Logger doRotate coverage ---

func TestLoggerDoRotate(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")

	logger := NewLogger(LoggingConfig{
		Level:  "info",
		Format: "text",
		Output: "file",
		File:   logPath,
	})
	defer logger.Close()

	logger.Info("before rotation")

	if err := logger.doRotate(); err != nil {
		t.Fatalf("doRotate: %v", err)
	}

	entries, _ := os.ReadDir(dir)
	var hasBackup bool
	for _, e := range entries {
		name := e.Name()
		if name != "app.log" && len(name) > len("app.log") && name[:len("app.log")] == "app.log" && name[len("app.log")] == '.' {
			hasBackup = true
			break
		}
	}
	if !hasBackup {
		t.Error("expected rotated log file")
	}

	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		t.Error("new log file should exist after rotation")
	}
}

func TestLoggerDoRotateCleanupOldLogs(t *testing.T) {
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

	oldLog := logPath + ".20200101-000000"
	os.WriteFile(oldLog, []byte("old"), 0644)
	past := time.Now().AddDate(0, 0, -7)
	os.Chtimes(oldLog, past, past)

	logger.doRotate()

	if _, err := os.Stat(oldLog); !os.IsNotExist(err) {
		t.Error("old log file should be cleaned up during rotation")
	}
}

// --- applyDynamicConfig coverage ---

func TestBrokerReloadConfigAuthFile(t *testing.T) {
	dir := t.TempDir()
	authFile := filepath.Join(dir, "auth.json")
	os.WriteFile(authFile, []byte(`{"users":{"admin":{"password":"secret"}}}`), 0644)

	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "file"
	cfg.Auth.AuthFile = authFile

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	os.WriteFile(authFile, []byte(`{"users":{"admin":{"password":"newsecret"}}}`), 0644)

	configPath := filepath.Join(dir, "reload.yaml")
	os.WriteFile(configPath, []byte("logging:\n  level: info\n"), 0644)

	if err := b.ReloadConfig(configPath); err != nil {
		t.Fatalf("ReloadConfig: %v", err)
	}
}

func TestBrokerReloadConfigACLAndFlowControl(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.ACL.Enabled = true
	cfg.ACL.DefaultPolicy = "deny"
	cfg.ACL.Entries = []ACLEntryConfig{
		{Principal: "user1", Resource: "topic", Name: "topic1", Operation: "publish", Permission: "allow"},
	}
	cfg.FlowControl.Enabled = true
	cfg.FlowControl.MaxMemoryBytes = 1024

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
acl:
  enabled: true
  default_policy: deny
  entries:
    - principal: user2
      resource: topic
      name: topic2
      operation: consume
      permission: deny
flow_control:
  enabled: true
  max_memory_bytes: 2048
  high_watermark: 0.9
`
	os.WriteFile(configPath, []byte(configContent), 0644)

	if err := b.ReloadConfig(configPath); err != nil {
		t.Fatalf("ReloadConfig: %v", err)
	}

	if b.Config().FlowControl.MaxMemoryBytes != 2048 {
		t.Errorf("max_memory_bytes = %d, want 2048", b.Config().FlowControl.MaxMemoryBytes)
	}

	entries := b.Config().ACL.Entries
	if len(entries) != 1 || entries[0].Principal != "user2" {
		t.Errorf("ACL entries not updated correctly: %+v", entries)
	}
}

// --- Publish edge cases ---

func TestPublishMaxMessageSize(t *testing.T) {
	b, cleanup := setupBrokerForPublish(t)
	defer cleanup()

	b.config.Limits.MaxMessageSize = 10

	b.Topics().CreateTopic(TopicConfig{
		Name:       "size-test",
		Mode:       ModeStream,
		Partitions: 1,
	})

	env := &message.Envelope{
		Topic:   "size-test",
		Payload: []byte("this is more than ten bytes"),
	}
	_, err := b.Publish(env)
	if err == nil {
		t.Error("expected error for oversized message")
	}
}

func TestPublishIdempotentDuplicate(t *testing.T) {
	dir := t.TempDir()
	cfg, _ := LoadConfig("", &CLIFlags{DataDir: dir})
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Idempotent.Enabled = true

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "idem-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	env := &message.Envelope{
		Topic:   "idem-topic",
		Payload: []byte("msg1"),
		Headers: map[string][]byte{
			"x-chimera-producer-id":  []byte("p1"),
			"x-chimera-producer-seq": []byte("1"),
		},
	}
	_, err = b.Publish(env)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}

	env2 := &message.Envelope{
		Topic:   "idem-topic",
		Payload: []byte("msg1-dup"),
		Headers: map[string][]byte{
			"x-chimera-producer-id":  []byte("p1"),
			"x-chimera-producer-seq": []byte("1"),
		},
	}
	offset2, err := b.Publish(env2)
	if err != nil {
		t.Fatalf("duplicate publish: %v", err)
	}
	if offset2 != 0 {
		t.Errorf("duplicate offset = %d, want 0", offset2)
	}
}

func TestPublishFlowControlRejection(t *testing.T) {
	dir := t.TempDir()
	cfg, _ := LoadConfig("", &CLIFlags{DataDir: dir})
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.FlowControl.Enabled = true
	cfg.FlowControl.GlobalRateLimit = 1

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "flow-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	_, err = b.Publish(&message.Envelope{Topic: "flow-topic", Payload: []byte("msg1")})
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}

	_, err = b.Publish(&message.Envelope{Topic: "flow-topic", Payload: []byte("msg2")})
	if err == nil {
		t.Error("expected rate limit error for second publish")
	}
}

func TestPublishGeoReplicationEnabled(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.GeoReplication.Enabled = true
	cfg.GeoReplication.LocalDC = "dc1"

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "geo-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	_, err = b.Publish(&message.Envelope{Topic: "geo-topic", Payload: []byte("geo-msg")})
	if err != nil {
		t.Fatalf("publish with geo-replication: %v", err)
	}
}

func TestPublishSchemaEnforcementMissingID(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Schema.Enabled = true

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	b.Topics().CreateTopic(TopicConfig{
		Name:              "schema-topic",
		Mode:              ModeStream,
		Partitions:        1,
		SchemaEnforcement: true,
	})

	_, err = b.Publish(&message.Envelope{Topic: "schema-topic", Payload: []byte(`{"name":"test"}`)})
	if err == nil {
		t.Error("expected error for missing schema ID")
	}
}

func TestPublishSchemaEnforcementValid(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Schema.Enabled = true

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	sv, err := b.schemaReg.Register("schema-topic", 2, `{"type":"object","properties":{"name":{"type":"string"}}}`)
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}

	b.Topics().CreateTopic(TopicConfig{
		Name:              "schema-topic",
		Mode:              ModeStream,
		Partitions:        1,
		SchemaEnforcement: true,
	})

	env := &message.Envelope{
		Topic:   "schema-topic",
		Payload: []byte(`{"name":"test"}`),
		Headers: map[string][]byte{
			"x-chimera-schema-id": []byte("1"),
		},
	}
	_, err = b.Publish(env)
	if err != nil {
		t.Fatalf("publish with valid schema: %v", err)
	}
	_ = sv
}

func TestBrokerStartWithManyFeatures(t *testing.T) {
	dir := t.TempDir()

	// Generate encryption key
	keyPath := filepath.Join(dir, "encrypt.key")
	if err := os.WriteFile(keyPath, make([]byte, 32), 0600); err != nil {
		t.Fatal(err)
	}
	// Ensure key passes strength checks (not all zeros / weak patterns)
	strongKey := []byte{
		0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
		0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
		0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
		0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00,
	}
	os.WriteFile(keyPath, strongKey, 0600)

	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Storage.Encryption.Enabled = true
	cfg.Storage.Encryption.KeyPath = keyPath
	cfg.Storage.Warm.Enabled = true
	cfg.Storage.Cold.Enabled = true
	cfg.Schema.Enabled = true
	cfg.TTL.Enabled = true
	cfg.DLQ.Enabled = true
	cfg.FlowControl.Enabled = true
	cfg.WASM.Enabled = true
	cfg.Processing.Enabled = true
	cfg.Idempotent.Enabled = true
	cfg.GeoReplication.Enabled = true
	cfg.GeoReplication.LocalDC = "dc1"
	cfg.Audit.Enabled = true
	cfg.Audit.LogPath = filepath.Join(dir, "audit.log")
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "static"
	cfg.Auth.Users = map[string]string{"admin": "secret"}
	cfg.ACL.Enabled = true
	cfg.ACL.DefaultPolicy = "allow"

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start with many features: %v", err)
	}
	defer b.Stop()

	if b.Storage() == nil {
		t.Error("storage should not be nil")
	}
}

func TestPublishTenantQuotaExceeded(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Tenant.Enabled = true
	cfg.Tenant.Separator = "_"
	cfg.Tenant.Tenants = []TenantConfigDef{
		{
			ID:          "t1",
			TopicPrefix: "t1_",
			Quotas: TenantQuotaConfig{
				MaxPublishRate: 1,
			},
		},
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
		Name:       "t1_test",
		Mode:       ModeStream,
		Partitions: 1,
	})

	_, err = b.Publish(&message.Envelope{Topic: "t1_test", Payload: []byte("first")})
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}

	_, err = b.Publish(&message.Envelope{Topic: "t1_test", Payload: []byte("second")})
	if err == nil {
		t.Error("expected tenant quota exceeded error")
	}
}

// --- Logger rotation / cleanup coverage ---

func TestLoggerRotateNilFile(t *testing.T) {
	logger := NewLogger(LoggingConfig{Level: "info", Format: "text"})
	defer logger.Close()

	if err := logger.rotate(); err != nil {
		t.Errorf("rotate with nil file should return nil, got %v", err)
	}
}

func TestLoggerCleanupOldLogsNoMaxAge(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")

	logger := NewLogger(LoggingConfig{
		Level:  "info",
		Format: "text",
		Output: "file",
		File:   logPath,
	})
	defer logger.Close()

	// MaxAge defaults to 0, so cleanup should be a no-op
	logger.cleanupOldLogs()
}

func TestLoggerDoRotateRenameError(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "app.log")

	logger := NewLogger(LoggingConfig{
		Level:  "info",
		Format: "text",
		Output: "file",
		File:   logPath,
	})
	defer logger.Close()

	logger.Info("before rotate")

	// Create a directory with the backup name to cause Rename to fail
	timestamp := time.Now().Format("20060102-150405")
	backupPath := logPath + "." + timestamp
	os.MkdirAll(backupPath, 0750)

	if err := logger.doRotate(); err == nil {
		t.Error("expected error when backup path is a directory")
	}
}

// --- Broker adapter coverage ---

func TestBrokerAPIAdapterPublishMessageError(t *testing.T) {
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

	_, _, err = adapter.PublishMessage("nonexistent-topic", &message.Envelope{Payload: []byte("x")})
	if err == nil {
		t.Error("expected error publishing to nonexistent topic")
	}
}

// --- Topic manager coverage ---

func TestCreateTopicMaxTopicsReached(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	w, _ := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncOS, 0)
	defer w.Close()
	defer storage.Close()

	tm, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1, MaxPartitionsPerTopic: 256})

	if err := tm.CreateTopic(TopicConfig{Name: "first", Mode: ModeStream, Partitions: 1}); err != nil {
		t.Fatalf("first topic: %v", err)
	}
	if err := tm.CreateTopic(TopicConfig{Name: "second", Mode: ModeStream, Partitions: 1}); err == nil {
		t.Error("expected error when max topics reached")
	}
}

func TestCreateTopicMaxPartitionsExceeded(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	w, _ := wal.NewWAL(filepath.Join(dir, "wal"), 4*1024*1024, wal.SyncOS, 0)
	defer w.Close()
	defer storage.Close()

	tm, _ := NewTopicManager(dir, storage, w, LimitsConfig{MaxTopics: 1000, MaxPartitionsPerTopic: 2})

	if err := tm.CreateTopic(TopicConfig{Name: "too-many", Mode: ModeStream, Partitions: 4}); err == nil {
		t.Error("expected error when partitions exceed maximum")
	}
}

// --- isProcessAlive coverage ---

func TestIsProcessAliveExtra(t *testing.T) {
	if !isProcessAlive(os.Getpid()) {
		t.Error("expected current process to be alive")
	}
	if isProcessAlive(999999) {
		t.Error("expected non-existent PID to be not alive")
	}
	if isProcessAlive(-1) {
		t.Error("expected invalid PID to be not alive")
	}
}

// --- FetchMessages coverage ---

func TestFetchMessagesCorruptData(t *testing.T) {
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

	b.Topics().CreateTopic(TopicConfig{Name: "fetch-topic", Mode: ModeStream, Partitions: 1})

	// Write corrupt data directly to storage
	part, _ := b.storage.GetOrCreatePartition("fetch-topic", 0)
	part.Append([]byte("not-a-valid-envelope"))

	adapter := &brokerAPIAdapter{broker: b}
	msgs, err := adapter.FetchMessages("fetch-topic", 0, 0, 10)
	if err != nil {
		t.Fatalf("FetchMessages error: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages from corrupt data, got %d", len(msgs))
	}
}

// --- StartConfigWatcher coverage ---

func TestStartConfigWatcherStop(t *testing.T) {
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

	configPath := filepath.Join(dir, "watch.yaml")
	os.WriteFile(configPath, []byte("logging:\n  level: info\n"), 0644)

	b.StartConfigWatcher(configPath, 50*time.Millisecond)

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)

	// Stop should cancel the watcher context
	b.Stop()
}

// --- Broker.Start auth type coverage ---

func TestBrokerStartWithMTLSAuth(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "mtls"

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start with mTLS auth: %v", err)
	}
	defer b.Stop()
}

func TestBrokerStartWithLDAPAuth(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "ldap"

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start with LDAP auth: %v", err)
	}
	defer b.Stop()
}

func TestBrokerStartWithUnknownAuth(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "unknown"

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start with unknown auth: %v", err)
	}
	defer b.Stop()
}

// --- Publish schema validation failure ---

func TestPublishSchemaValidationFailure(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Schema.Enabled = true

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	// Register a schema that requires "name" to be a string
	_, err = b.schemaReg.Register("schema-topic", 2, `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`)
	if err != nil {
		t.Fatalf("register schema: %v", err)
	}

	b.Topics().CreateTopic(TopicConfig{
		Name:              "schema-topic",
		Mode:              ModeStream,
		Partitions:        1,
		SchemaEnforcement: true,
	})

	env := &message.Envelope{
		Topic:   "schema-topic",
		Payload: []byte(`{"name":123}`), // invalid: name should be string
		Headers: map[string][]byte{
			"x-chimera-schema-id": []byte("1"),
		},
	}
	_, err = b.Publish(env)
	if err == nil {
		t.Error("expected schema validation failure")
	}
}

// --- applyDynamicConfig format update ---

func TestApplyDynamicConfigFormatUpdate(t *testing.T) {
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

	newCfg := *b.Config()
	newCfg.Logging.Format = "json"

	b.applyDynamicConfig(&newCfg)

	if b.Config().Logging.Format != "json" {
		t.Errorf("format = %q, want json", b.Config().Logging.Format)
	}
}

