package stream

import (
	"os"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/hot"
)

func TestEngineFetchTimeout(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	// Fetch from a partition with no data — should timeout
	msgs, nextOff, err := eng.Fetch("empty-topic", 0, 0, 10, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("fetch timeout: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages, got %d", len(msgs))
	}
	if nextOff != 0 {
		t.Errorf("nextOffset = %d, want 0", nextOff)
	}
}

func TestEngineFetchLongPoll(t *testing.T) {
	dir, err := os.MkdirTemp("", "fetch-longpoll-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()
	offsets := NewOffsetStore(dir)
	se := NewEngine(storage, offsets)
	defer se.Close()

	// Write data first so hw=0, then fetch from offset 1 which is > hw,
	// triggering long-poll. Then write another record to advance hw to 1.
	part, _ := storage.GetOrCreatePartition("poll-topic", 0)
	env1 := &message.Envelope{Topic: "poll-topic", Payload: []byte("first")}
	data1, _ := message.Marshal(env1)
	part.Append(data1)
	storage.FlushAll()

	done := make(chan struct{})
	go func() {
		defer close(done)
		msgs, _, err := se.Fetch("poll-topic", 0, 1, 10, 500*time.Millisecond)
		if err != nil {
			t.Errorf("long poll fetch: %v", err)
			return
		}
		if len(msgs) != 1 {
			t.Errorf("expected 1 message, got %d", len(msgs))
		}
	}()

	// Wait for the long-poll to register, then write another record
	time.Sleep(50 * time.Millisecond)
	env2 := &message.Envelope{Topic: "poll-topic", Payload: []byte("polled")}
	data2, _ := message.Marshal(env2)
	part.Append(data2)
	storage.FlushAll()

	// Notify waiters
	se.NotifyWaiters("poll-topic", 0)

	select {
	case <-done:
		// Good
	case <-time.After(2 * time.Second):
		t.Error("long poll fetch timed out")
	}
}

func TestEngineFetchLongPollTimeoutExpires(t *testing.T) {
	dir, err := os.MkdirTemp("", "fetch-timeout-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()
	offsets := NewOffsetStore(dir)
	se := NewEngine(storage, offsets)
	defer se.Close()

	// Write one message so hw=0
	part, _ := storage.GetOrCreatePartition("timeout-topic", 0)
	env := &message.Envelope{Topic: "timeout-topic", Payload: []byte("first")}
	data, _ := message.Marshal(env)
	part.Append(data)
	storage.FlushAll()

	// Fetch from offset 10 (> hw=0), long-poll should timeout
	msgs, nextOff, err := se.Fetch("timeout-topic", 0, 10, 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages after timeout, got %d", len(msgs))
	}
	if nextOff != 10 {
		t.Errorf("nextOffset = %d, want 10", nextOff)
	}
}

func TestEngineFetchNotifyWaiterChannel(t *testing.T) {
	dir, err := os.MkdirTemp("", "fetch-notify-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()
	offsets := NewOffsetStore(dir)
	se := NewEngine(storage, offsets)
	defer se.Close()

	// No data yet. Fetch with long-poll, then notify.
	done := make(chan struct{})
	go func() {
		defer close(done)
		msgs, _, err := se.Fetch("notify-topic", 0, 0, 10, 2*time.Second)
		if err != nil {
			t.Errorf("fetch notify: %v", err)
			return
		}
		// Since no data was written, should timeout and return empty
		// But if notified with no new data, it re-reads and returns 0
		if len(msgs) != 0 {
			t.Errorf("expected 0, got %d", len(msgs))
		}
	}()

	// Give goroutine time to register
	time.Sleep(50 * time.Millisecond)

	// Notify without writing — should wake up the waiter
	se.NotifyWaiters("notify-topic", 0)

	select {
	case <-done:
		// good
	case <-time.After(3 * time.Second):
		t.Error("fetch didn't return after notify")
	}
}

func TestHeartbeatExpiresMember(t *testing.T) {
	dir, err := os.MkdirTemp("", "heartbeat-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	defer storage.Close()
	offsets := NewOffsetStore(dir)
	se := NewEngine(storage, offsets)
	defer se.Close()

	// Create a group with very short session timeout
	cg := &ConsumerGroup{
		name:           "hb-test",
		topic:          "hb-topic",
		partitionCount: 4,
		members: map[string]*GroupMember{
			"m1": {ID: "m1", Partitions: []uint32{0, 1}, LastHeartbeat: time.Now().Add(-1 * time.Hour)},
			"m2": {ID: "m2", Partitions: []uint32{2, 3}, LastHeartbeat: time.Now()},
		},
		assignments:    map[uint32]string{0: "m1", 1: "m1", 2: "m2", 3: "m2"},
		committed:      make(map[uint32]uint64),
		strategy:       StrategyRange,
		sessionTimeout: 2 * time.Second,		offsetStore:    offsets,
		stopCh:         make(chan struct{}),
	}
	se.groups["hb-test"] = cg

	go cg.heartbeatLoop()
	defer cg.Stop()

	// Wait for heartbeat loop to expire m1 (tick every ~667ms, m1 expired 1hr ago)
	time.Sleep(1 * time.Second)

	cg.mu.RLock()
	_, m1Exists := cg.members["m1"]
	_, m2Exists := cg.members["m2"]
	cg.mu.RUnlock()

	if m1Exists {
		t.Error("m1 should have been expired")
	}
	if !m2Exists {
		t.Error("m2 should still be alive")
	}

	// After rebalance, m2 should own all partitions
	cg.mu.RLock()
	assignments := cg.Assignments()
	cg.mu.RUnlock()

	if len(assignments) != 4 {
		t.Errorf("expected 4 assignments, got %d", len(assignments))
	}
	for _, memberID := range assignments {
		if memberID != "m2" {
			t.Errorf("all partitions should be assigned to m2, got %q", memberID)
		}
	}
}
