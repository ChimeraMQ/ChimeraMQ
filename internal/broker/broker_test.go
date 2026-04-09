package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/chimeramq/chimera/internal/storage/wal"
)

func TestParseSyncMode(t *testing.T) {
	tests := []struct {
		input string
		want  wal.SyncMode
	}{
		{"immediate", wal.SyncImmediate},
		{"interval", wal.SyncInterval},
		{"os", wal.SyncOS},
		{"", wal.SyncInterval},
		{"unknown", wal.SyncInterval},
	}
	for _, tt := range tests {
		got := parseSyncMode(tt.input)
		if got != tt.want {
			t.Errorf("parseSyncMode(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsProcessAlive(t *testing.T) {
	// On Windows, Signal(0) doesn't work the same way as Unix.
	// Just test it doesn't panic and returns a boolean.
	_ = isProcessAlive(os.Getpid())
	_ = isProcessAlive(1)
	_ = isProcessAlive(999999999)
}

func TestAcquireLockFile(t *testing.T) {
	dir := t.TempDir()

	f, err := acquireLockFile(dir)
	if err != nil {
		t.Fatalf("acquireLockFile: %v", err)
	}

	// Second acquisition should fail (lock already held by us)
	_, err = acquireLockFile(dir)
	if err == nil {
		t.Error("expected error for duplicate lock acquisition")
	}

	// Close first lock to allow cleanup
	f.Close()
	os.Remove(filepath.Join(dir, "chimera.lock"))
}

func TestAcquireLockFileStaleDetection(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "chimera.lock")

	// Write a lock file with a PID that's definitely dead (very large PID)
	os.WriteFile(lockPath, []byte("999999999\n"), 0600)

	// Should detect stale lock and replace it
	f, err := acquireLockFile(dir)
	if err != nil {
		t.Fatalf("acquireLockFile with stale lock: %v", err)
	}
	f.Close()

	// Verify lock now contains our PID
	data, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("read lock: %v", err)
	}
	var pid int
	fmt.Sscanf(string(data), "%d", &pid)
	if pid != os.Getpid() {
		t.Errorf("lock PID = %d, want %d", pid, os.Getpid())
	}
}

func TestNewBroker(t *testing.T) {
	cfg := defaultConfig()
	cfg.Node.DataDir = t.TempDir()

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	if b.Config() != cfg {
		t.Error("Config() mismatch")
	}
	if b.Metrics() == nil {
		t.Error("Metrics() should not be nil")
	}
}

func TestBrokerStartStop(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}

	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Verify components initialized
	if b.Topics() == nil {
		t.Error("Topics() should not be nil after Start")
	}
	if b.Storage() == nil {
		t.Error("Storage() should not be nil after Start")
	}
	if b.QueueEngine() == nil {
		t.Error("QueueEngine() should not be nil after Start")
	}
	if b.StreamEngine() == nil {
		t.Error("StreamEngine() should not be nil after Start")
	}
	if b.StartTime().IsZero() {
		t.Error("StartTime() should not be zero after Start")
	}
	if b.Logger() == nil {
		t.Error("Logger() should not be nil after Start")
	}

	if err := b.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Lock file should be cleaned up
	if _, err := os.Stat(filepath.Join(dir, "chimera.lock")); !os.IsNotExist(err) {
		t.Error("lock file should be removed after Stop")
	}
}
