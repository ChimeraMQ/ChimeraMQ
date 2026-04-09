package broker

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewLoggerLevels(t *testing.T) {
	tests := []struct {
		level string
		want  slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"", slog.LevelInfo},
		{"unknown", slog.LevelInfo},
	}
	for _, tt := range tests {
		l := NewLogger(LoggingConfig{Level: tt.level})
		if l.level != tt.want {
			t.Errorf("level %q: got %v, want %v", tt.level, l.level, tt.want)
		}
	}
}

func TestNewLoggerTextFormat(t *testing.T) {
	l := NewLogger(LoggingConfig{Level: "debug", Format: "text"})
	if l == nil {
		t.Fatal("logger is nil")
	}
	// Should not panic
	l.Info("test message", "key", "value")
}

func TestNewLoggerJSONFormat(t *testing.T) {
	l := NewLogger(LoggingConfig{Level: "debug", Format: "json"})
	if l == nil {
		t.Fatal("logger is nil")
	}
	l.Info("test message", "key", "value")
}

func TestNewLoggerFileOutput(t *testing.T) {
	logFile := filepath.Join(os.TempDir(), "chimera-test-log-"+t.Name()+".log")
	defer os.Remove(logFile)

	l := NewLogger(LoggingConfig{
		Level:  "info",
		Format: "json",
		Output: "file",
		File:   logFile,
	})
	if l == nil {
		t.Fatal("logger is nil")
	}
	l.Info("file test")

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log file: %v", err)
	}
	if len(data) == 0 {
		t.Error("log file is empty")
	}

	var entry map[string]interface{}
	// File may contain multiple JSON lines; parse the first one
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("parse JSON log line %q: %v", line, err)
		}
		break
	}
	if entry["msg"] != "file test" {
		t.Errorf("msg = %v, want 'file test'", entry["msg"])
	}
}

func TestNewLoggerInvalidFilePath(t *testing.T) {
	// Invalid file path should fall back to stdout without panicking
	l := NewLogger(LoggingConfig{
		Level:  "info",
		Format: "json",
		Output: "file",
		File:   "/nonexistent/dir/file.log",
	})
	if l == nil {
		t.Fatal("logger is nil")
	}
	l.Info("fallback test")
}

func TestLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	inner := slog.New(handler)

	l := &Logger{inner: inner, level: slog.LevelDebug}
	child := l.With("request_id", "abc123")

	child.Info("child message")

	output := buf.String()
	if !strings.Contains(output, "request_id") {
		t.Error("child logger should contain 'request_id' field")
	}
	if !strings.Contains(output, "child message") {
		t.Error("child logger should contain message")
	}
}

func TestLoggerAllLevels(t *testing.T) {
	l := NewLogger(LoggingConfig{Level: "debug", Format: "json"})
	// None should panic
	l.Debug("debug msg")
	l.Info("info msg")
	l.Warn("warn msg")
	l.Error("error msg")
}
