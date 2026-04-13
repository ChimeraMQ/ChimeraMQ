package audit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewLogger(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
		MaxSize: 1024 * 1024, // 1MB
		MaxAge:  7 * 24 * time.Hour,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	if !logger.enabled {
		t.Error("expected logger to be enabled")
	}

	if logger.logPath != logPath {
		t.Errorf("expected logPath %s, got %s", logPath, logger.logPath)
	}
}

func TestNewLoggerDisabled(t *testing.T) {
	cfg := Config{
		Enabled: false,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	if logger.enabled {
		t.Error("expected logger to be disabled")
	}

	if logger.IsEnabled() {
		t.Error("IsEnabled should return false")
	}
}

func TestNewLoggerToStdout(t *testing.T) {
	cfg := Config{
		Enabled:  true,
		ToStdout: true,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	if !logger.enabled {
		t.Error("expected logger to be enabled")
	}
}

func TestLoggerDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
		// Leave MaxSize and MaxAge as zero to test defaults
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	// Check default values
	if logger.maxSize != 100*1024*1024 {
		t.Errorf("expected default maxSize 100MB, got %d", logger.maxSize)
	}

	if logger.maxAge != 30*24*time.Hour {
		t.Errorf("expected default maxAge 30 days, got %v", logger.maxAge)
	}
}

func TestLogAuth(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	// Log successful auth
	logger.LogAuth("test-user", "192.168.1.1", "login", "success", nil)

	// Log failed auth
	logger.LogAuth("test-user", "192.168.1.1", "login", "failure", err)

	// Read log file
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	if len(content) == 0 {
		t.Error("log file is empty")
	}

	// Verify JSON format
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 2 {
		t.Errorf("expected 2 log lines, got %d", len(lines))
	}

	for i, line := range lines {
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Errorf("line %d is not valid JSON: %v", i, err)
			continue
		}

		if event.Category != CategoryAuth {
			t.Errorf("line %d: expected category AUTH, got %s", i, event.Category)
		}

		if event.Action != "login" {
			t.Errorf("line %d: expected action 'login', got %s", i, event.Action)
		}

		if event.User != "test-user" {
			t.Errorf("line %d: expected user 'test-user', got %s", i, event.User)
		}
	}
}

func TestLogAdmin(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	logger.LogAdmin("admin", "10.0.0.1", "topic", "orders", "create", "success")

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	var event Event
	if err := json.Unmarshal(content, &event); err != nil {
		t.Fatalf("log entry is not valid JSON: %v", err)
	}

	if event.Category != CategoryAdmin {
		t.Errorf("expected category ADMIN, got %s", event.Category)
	}

	if event.Resource != "topic" {
		t.Errorf("expected resource 'topic', got %s", event.Resource)
	}

	if event.ResourceID != "orders" {
		t.Errorf("expected resource_id 'orders', got %s", event.ResourceID)
	}
}

func TestLogMessage(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	logger.LogMessage("publisher1", "events", "publish", 100, "success")

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	var event Event
	if err := json.Unmarshal(content, &event); err != nil {
		t.Fatalf("log entry is not valid JSON: %v", err)
	}

	if event.Category != CategoryMessage {
		t.Errorf("expected category MESSAGE, got %s", event.Category)
	}

	if event.ResourceID != "events" {
		t.Errorf("expected resource_id 'events', got %s", event.ResourceID)
	}
}

func TestLogCluster(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	details := map[string]interface{}{
		"term":    5,
		"leader":  "node-1",
		"members": []string{"node-1", "node-2", "node-3"},
	}

	logger.LogCluster("node-1", "leader_election", "success", details)

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	var event Event
	if err := json.Unmarshal(content, &event); err != nil {
		t.Fatalf("log entry is not valid JSON: %v", err)
	}

	if event.Category != CategoryCluster {
		t.Errorf("expected category CLUSTER, got %s", event.Category)
	}

	if event.Action != "leader_election" {
		t.Errorf("expected action 'leader_election', got %s", event.Action)
	}

	// Verify details were serialized
	if len(event.Details) == 0 {
		t.Error("expected details to be present")
	}
}

func TestLogConfig(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	logger.LogConfig("admin", "10.0.0.1", "retention", "update", "24h", "48h")

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	var event Event
	if err := json.Unmarshal(content, &event); err != nil {
		t.Fatalf("log entry is not valid JSON: %v", err)
	}

	if event.Category != CategoryConfig {
		t.Errorf("expected category CONFIG, got %s", event.Category)
	}

	if event.Level != LevelWarning {
		t.Errorf("expected level WARNING, got %s", event.Level)
	}
}

func TestLogSecurity(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	logger.LogSecurity("unknown", "192.168.1.100", "unauthorized_access", "blocked", "invalid_token")

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	var event Event
	if err := json.Unmarshal(content, &event); err != nil {
		t.Fatalf("log entry is not valid JSON: %v", err)
	}

	if event.Category != CategorySecurity {
		t.Errorf("expected category SECURITY, got %s", event.Category)
	}

	if event.Level != LevelWarning {
		t.Errorf("expected level WARNING, got %s", event.Level)
	}
}

func TestLogWhenDisabled(t *testing.T) {
	cfg := Config{
		Enabled: false,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	// This should not panic or error
	logger.LogAuth("user", "127.0.0.1", "login", "success", nil)
	logger.LogAdmin("admin", "127.0.0.1", "topic", "test", "create", "success")
	logger.LogMessage("user", "topic", "publish", 1, "success")
}

func TestEventTimestamp(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	before := time.Now().UTC()
	logger.LogAuth("user", "127.0.0.1", "login", "success", nil)
	after := time.Now().UTC()

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	var event Event
	if err := json.Unmarshal(content, &event); err != nil {
		t.Fatalf("log entry is not valid JSON: %v", err)
	}

	if event.Timestamp.Before(before) || event.Timestamp.After(after) {
		t.Error("timestamp is not within expected range")
	}
}

func TestCategoryConstants(t *testing.T) {
	if CategoryAuth != "AUTH" {
		t.Errorf("expected CategoryAuth to be 'AUTH', got %s", CategoryAuth)
	}
	if CategoryAdmin != "ADMIN" {
		t.Errorf("expected CategoryAdmin to be 'ADMIN', got %s", CategoryAdmin)
	}
	if CategoryMessage != "MESSAGE" {
		t.Errorf("expected CategoryMessage to be 'MESSAGE', got %s", CategoryMessage)
	}
	if CategoryCluster != "CLUSTER" {
		t.Errorf("expected CategoryCluster to be 'CLUSTER', got %s", CategoryCluster)
	}
	if CategoryConfig != "CONFIG" {
		t.Errorf("expected CategoryConfig to be 'CONFIG', got %s", CategoryConfig)
	}
	if CategorySecurity != "SECURITY" {
		t.Errorf("expected CategorySecurity to be 'SECURITY', got %s", CategorySecurity)
	}
}

func TestLevelConstants(t *testing.T) {
	if LevelInfo != "INFO" {
		t.Errorf("expected LevelInfo to be 'INFO', got %s", LevelInfo)
	}
	if LevelWarning != "WARNING" {
		t.Errorf("expected LevelWarning to be 'WARNING', got %s", LevelWarning)
	}
	if LevelError != "ERROR" {
		t.Errorf("expected LevelError to be 'ERROR', got %s", LevelError)
	}
}

func TestLogRotation(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
		MaxSize: 100, // 100 bytes to trigger rotation quickly
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	// Write enough data to trigger rotation
	for i := 0; i < 10; i++ {
		logger.LogAdmin("admin", "10.0.0.1", "topic", "orders", "create", "success")
	}

	logger.Close()

	// Check that a rotated file exists
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	rotatedFound := false
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "audit.log.") {
			rotatedFound = true
			break
		}
	}

	if !rotatedFound {
		t.Error("expected a rotated log file to exist")
	}
}

func TestCleanupOldLogs(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{
		Enabled: true,
		LogPath: logPath,
		MaxAge:  1 * time.Hour,
	}

	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}
	defer logger.Close()

	// Create an old rotated file with old modification time
	oldLogPath := logPath + ".20230101-120000"
	os.WriteFile(oldLogPath, []byte("old log"), 0644)
	oldTime := time.Now().Add(-48 * time.Hour)
	os.Chtimes(oldLogPath, oldTime, oldTime)

	// Create a recent rotated file
	recentLogPath := logPath + "." + time.Now().Format("20060102-150405")
	os.WriteFile(recentLogPath, []byte("recent log"), 0644)

	// Trigger cleanup
	logger.cleanupOldLogs()

	// Old file should be removed
	if _, err := os.Stat(oldLogPath); !os.IsNotExist(err) {
		t.Error("expected old rotated log to be cleaned up")
	}

	// Recent file should still exist
	if _, err := os.Stat(recentLogPath); os.IsNotExist(err) {
		t.Error("expected recent rotated log to be preserved")
	}
}

func TestNewLoggerMkdirAllFailure(t *testing.T) {
	// Create a file, then try to use it as a directory
	tmpFile, err := os.CreateTemp("", "notadir")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	cfg := Config{
		Enabled: true,
		LogPath: filepath.Join(tmpFile.Name(), "audit.log"),
	}

	_, err = NewLogger(cfg)
	if err == nil {
		t.Fatal("expected error when log path parent is a file")
	}
}

func TestLogWithNilEncoder(t *testing.T) {
	logger := &Logger{
		enabled: true,
		encoder: nil,
	}
	// Should not panic
	logger.Log(Event{Action: "test", Result: "success"})
}
