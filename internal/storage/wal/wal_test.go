package wal

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func newTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	w, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatalf("NewWAL: %v", err)
	}
	return w, dir
}

func TestWALCreateAndSegment(t *testing.T) {
	w, dir := newTestWAL(t)
	defer w.Close()

	segments, _ := os.ReadDir(filepath.Join(dir))
	walCount := 0
	for _, e := range segments {
		if len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".wal" {
			walCount++
		}
	}
	if walCount != 1 {
		t.Errorf("expected 1 segment, got %d", walCount)
	}
}

func TestWALAppend(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	off1, err := w.Append(EntryMessage, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	off2, err := w.Append(EntryMessage, []byte("world"))
	if err != nil {
		t.Fatal(err)
	}
	if off2 <= off1 {
		t.Errorf("offsets should increase: %d -> %d", off1, off2)
	}
}

func TestWALRotate(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 256, SyncImmediate, 0) // Very small to force rotate
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	for i := 0; i < 50; i++ {
		data := make([]byte, 50)
		copy(data, []byte("test-data"))
		_, err := w.Append(EntryMessage, data)
		if err != nil {
			t.Fatal(err)
		}
	}

	segments, _ := w.listSegments()
	if len(segments) < 2 {
		t.Errorf("expected rotation with maxSize=256, got %d segments", len(segments))
	}
}

func TestWALRecover(t *testing.T) {
	w, dir := newTestWAL(t)

	type testMsg struct {
		Index int    `json:"index"`
		Data  string `json:"data"`
	}

	for i := 0; i < 50; i++ {
		msg, _ := json.Marshal(testMsg{Index: i, Data: "payload"})
		_, err := w.Append(EntryMessage, msg)
		if err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	// Reopen and recover
	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var count atomic.Int32
	err = w2.Recover(0, func(et EntryType, data []byte) error {
		if et != EntryMessage {
			t.Errorf("expected EntryMessage, got %d", et)
		}
		var msg testMsg
		json.Unmarshal(data, &msg)
		if msg.Data != "payload" {
			t.Errorf("unexpected data: %s", msg.Data)
		}
		count.Add(1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 50 {
		t.Errorf("expected 50 entries, got %d", count.Load())
	}
}

func TestWALRecoveryTruncatedEntry(t *testing.T) {
	w, dir := newTestWAL(t)

	for i := 0; i < 10; i++ {
		w.Append(EntryMessage, []byte("data"))
	}
	w.Close()

	// Corrupt last entry by truncating file
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	if len(segments) == 0 {
		t.Fatal("no segments")
	}
	info, _ := os.Stat(segments[0])
	os.Truncate(segments[0], info.Size()-10) // Truncate 10 bytes

	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var count int
	w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})
	if count >= 10 {
		t.Errorf("truncated entry should be skipped, got %d entries", count)
	}
}

func TestWALCheckpoint(t *testing.T) {
	w, dir := newTestWAL(t)
	defer w.Close()

	w.Append(EntryMessage, []byte("data"))
	w.Checkpoint(w.Offset())

	cpPath := filepath.Join(dir, "checkpoint")
	data, err := os.ReadFile(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) == "" {
		t.Error("checkpoint empty")
	}
}

func TestWALTruncate(t *testing.T) {
	w, _ := newTestWAL(t)

	// Write enough to create multiple segments
	for i := 0; i < 300; i++ {
		w.Append(EntryMessage, make([]byte, 50))
	}

	w.Checkpoint(w.Offset())
	w.Truncate()

	segments, _ := w.listSegments()
	if len(segments) != 1 {
		t.Errorf("expected 1 segment after truncate, got %d", len(segments))
	}
	w.Close()
}

func TestWALCorruptCRC(t *testing.T) {
	w, dir := newTestWAL(t)

	for i := 0; i < 5; i++ {
		w.Append(EntryMessage, []byte("good-data"))
	}
	w.Close()

	// Corrupt CRC in last entry
	segments, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	info, _ := os.Stat(segments[0])
	f, _ := os.OpenFile(segments[0], os.O_RDWR, 0640)
	// Write bad byte near CRC position of last entry
	f.WriteAt([]byte{0xFF}, info.Size()-5)
	f.Close()

	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var count int
	w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})
	if count != 4 {
		t.Errorf("expected 4 valid entries before corrupt, got %d", count)
	}
}

func TestWALSyncInterval(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWAL(dir, 4096, SyncInterval, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	w.Append(EntryMessage, []byte("delayed-sync"))
	time.Sleep(100 * time.Millisecond) // Wait for sync ticker

	w.Close()

	// Verify data persisted
	w2, err := NewWAL(dir, 4096, SyncImmediate, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()

	var count int
	w2.Recover(0, func(et EntryType, data []byte) error {
		count++
		return nil
	})
	if count != 1 {
		t.Errorf("expected 1 entry after interval sync, got %d", count)
	}
}
