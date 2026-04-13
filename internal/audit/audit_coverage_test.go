package audit

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLogAuthWithError(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{Enabled: true, LogPath: logPath}
	logger, err := NewLogger(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.Close()

	logger.LogAuth("user", "127.0.0.1", "login", "failure", errors.New("bad password"))

	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}

	var event Event
	if err := json.Unmarshal(content, &event); err != nil {
		t.Fatal(err)
	}

	if event.Level != LevelError {
		t.Errorf("level = %q, want ERROR", event.Level)
	}
	if event.Error != "bad password" {
		t.Errorf("error = %q, want 'bad password'", event.Error)
	}
}

func TestCheckRotationNilFile(t *testing.T) {
	logger := &Logger{maxSize: 100}
	// Should not panic with nil file
	logger.checkRotation()
}

func TestCheckRotationStatError(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{Enabled: true, LogPath: logPath}
	logger, _ := NewLogger(cfg)
	defer logger.Close()

	// Close the file so Stat fails
	logger.mu.Lock()
	logger.file.Close()
	logger.file = nil
	logger.mu.Unlock()

	// Should not panic
	logger.checkRotation()
}

func TestRotateNilFile(t *testing.T) {
	logger := &Logger{maxSize: 100}
	// Should not panic with nil file
	logger.rotate()
}

func TestCleanupOldLogsNoPattern(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{Enabled: true, LogPath: logPath}
	logger, _ := NewLogger(cfg)
	defer logger.Close()

	// Call cleanup when there are no old logs - should be a no-op
	logger.cleanupOldLogs()
}

func TestCleanupOldLogsParseError(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{Enabled: true, LogPath: logPath}
	logger, _ := NewLogger(cfg)
	defer logger.Close()

	// Create a rotated file with an unparsable timestamp
	badLogPath := logPath + ".not-a-timestamp"
	os.WriteFile(badLogPath, []byte("data"), 0644)

	// Should skip files that don't match the expected pattern
	logger.cleanupOldLogs()

	// File should still exist (not removed because timestamp couldn't be parsed)
	if _, err := os.Stat(badLogPath); os.IsNotExist(err) {
		t.Error("file with unparsable timestamp should not be removed")
	}
}

func TestCleanupOldLogsRecentFile(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "audit.log")

	cfg := Config{Enabled: true, LogPath: logPath, MaxAge: 24 * time.Hour}
	logger, _ := NewLogger(cfg)
	defer logger.Close()

	// Create a very recent rotated file
	recentPath := logPath + "." + time.Now().Format("20060102-150405")
	os.WriteFile(recentPath, []byte("recent"), 0644)

	logger.cleanupOldLogs()

	if _, err := os.Stat(recentPath); os.IsNotExist(err) {
		t.Error("recent log should not be cleaned up")
	}
}
