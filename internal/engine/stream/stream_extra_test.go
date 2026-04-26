package stream

import (
	"os"
	"sync"
	"testing"

	"github.com/chimeramq/chimera/internal/storage/hot"
)

func TestOnConsumerLagCallback(t *testing.T) {
	dir, err := os.MkdirTemp("", "stream-lag-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	offsets, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(storage, offsets)
	defer engine.Close()

	// Establish a high watermark by writing to storage directly
	part, err := storage.GetOrCreatePartition("test-topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	// Append two messages: offsets 0 and 1, so high watermark = 1
	_, _ = part.Append([]byte("msg1"))
	_, _ = part.Append([]byte("msg2"))

	// Join a consumer group
	engine.JoinGroup("lag-group", "test-topic", "member-1", 1, StrategyRange)

	// Set the callback
	var mu sync.Mutex
	var gotLag uint64 = ^uint64(0) // sentinel: not set
	var gotTopic string
	engine.OnConsumerLag = func(topic string, partition uint32, group string, lag uint64) {
		mu.Lock()
		defer mu.Unlock()
		gotTopic = topic
		gotLag = lag
	}

	// Commit offset at 0 — lag should be hw(1) - offset(0) = 1
	engine.CommitOffset("lag-group", 0, 0)

	mu.Lock()
	defer mu.Unlock()
	if gotTopic != "test-topic" {
		t.Errorf("callback topic = %q, want test-topic", gotTopic)
	}
	if gotLag != 1 {
		t.Errorf("expected lag = 1, got %d", gotLag)
	}
}

func TestCommitOffsetNoGroup(t *testing.T) {
	dir, err := os.MkdirTemp("", "stream-nogroup-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	offsets, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(storage, offsets)
	defer engine.Close()

	// CommitOffset for nonexistent group should return nil (no-op)
	err = engine.CommitOffset("nonexistent-group", 0, 42)
	if err != nil {
		t.Errorf("expected nil error for nonexistent group, got %v", err)
	}
}

func TestHeartbeatNoGroup(t *testing.T) {
	dir, err := os.MkdirTemp("", "stream-hb-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	offsets, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(storage, offsets)
	defer engine.Close()

	// Heartbeat for nonexistent group should return nil
	err = engine.Heartbeat("nonexistent-group", "member")
	if err != nil {
		t.Errorf("expected nil error for nonexistent group heartbeat, got %v", err)
	}
}

func TestGetHighWatermark(t *testing.T) {
	dir, err := os.MkdirTemp("", "stream-hw-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()

	offsets, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	engine := NewEngine(storage, offsets)
	defer engine.Close()

	// Before any write, high watermark should be 0
	hw := engine.GetHighWatermark("test-topic", 0)
	if hw != 0 {
		t.Errorf("initial high watermark = %d, want 0", hw)
	}

	// Write to storage to establish high watermark
	part, err := storage.GetOrCreatePartition("test-topic", 0)
	if err != nil {
		t.Fatal(err)
	}
	// First append returns offset 0, second returns offset 1
	_, _ = part.Append([]byte("msg"))
	_, _ = part.Append([]byte("msg2"))

	hw = engine.GetHighWatermark("test-topic", 0)
	if hw != 1 {
		t.Errorf("high watermark after two appends = %d, want 1", hw)
	}
}
