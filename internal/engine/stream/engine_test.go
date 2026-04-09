package stream

import (
	"os"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/hot"
)

func setupEngine(t *testing.T) (*Engine, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "stream-engine-test-*")
	if err != nil {
		t.Fatal(err)
	}
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 1024 * 1024})
	offsets := NewOffsetStore(dir)
	engine := NewEngine(storage, offsets)

	cleanup := func() {
		engine.Close()
		storage.Close()
		os.RemoveAll(dir)
	}
	return engine, cleanup
}

func TestEngineJoinGroup(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	eng.JoinGroup("group1", "topic1", "m1", 4, StrategyRange)

	group := eng.GetGroup("group1")
	if group == nil {
		t.Fatal("group should exist")
	}

	members := group.Members()
	if len(members) != 1 {
		t.Errorf("members = %d, want 1", len(members))
	}
}

func TestEngineJoinGroupCreatesNew(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	if eng.GetGroup("new-group") != nil {
		t.Error("group should not exist yet")
	}

	eng.JoinGroup("new-group", "topic", "m1", 2, StrategyRoundRobin)

	if eng.GetGroup("new-group") == nil {
		t.Error("group should exist after join")
	}
}

func TestEngineLeaveGroup(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	eng.JoinGroup("g1", "topic", "m1", 4, StrategyRange)
	eng.JoinGroup("g1", "topic", "m2", 4, StrategyRange)

	eng.LeaveGroup("g1", "m1")

	group := eng.GetGroup("g1")
	members := group.Members()
	if _, ok := members["m1"]; ok {
		t.Error("m1 should be removed")
	}
	if len(members) != 1 {
		t.Errorf("members = %d, want 1", len(members))
	}
}

func TestEngineLeaveNonexistentGroup(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	// Should not panic
	eng.LeaveGroup("no-such-group", "m1")
}

func TestEngineListGroups(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	groups := eng.ListGroups()
	if len(groups) != 0 {
		t.Errorf("expected empty, got %d", len(groups))
	}

	eng.JoinGroup("g1", "topic", "m1", 2, StrategyRange)
	eng.JoinGroup("g2", "topic", "m1", 2, StrategyRange)
	eng.JoinGroup("g3", "topic", "m1", 2, StrategyRange)

	groups = eng.ListGroups()
	if len(groups) != 3 {
		t.Errorf("expected 3 groups, got %d", len(groups))
	}
}

func TestEngineGetGroupNotFound(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	if eng.GetGroup("nonexistent") != nil {
		t.Error("expected nil for nonexistent group")
	}
}

func TestEngineHeartbeat(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	eng.JoinGroup("g1", "topic", "m1", 4, StrategyRange)

	if err := eng.Heartbeat("g1", "m1"); err != nil {
		t.Errorf("heartbeat failed: %v", err)
	}
}

func TestEngineHeartbeatNonexistentGroup(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	// Should not error
	if err := eng.Heartbeat("no-group", "m1"); err != nil {
		t.Errorf("expected nil error, got: %v", err)
	}
}

func TestEngineCommitOffset(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	eng.JoinGroup("g1", "topic", "m1", 4, StrategyRange)

	if err := eng.CommitOffset("g1", 0, 42); err != nil {
		t.Errorf("commit failed: %v", err)
	}

	group := eng.GetGroup("g1")
	if group.GetCommittedOffset(0) != 42 {
		t.Errorf("offset = %d, want 42", group.GetCommittedOffset(0))
	}
}

func TestEngineCommitOffsetNonexistentGroup(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	// Should not error
	if err := eng.CommitOffset("no-group", 0, 10); err != nil {
		t.Errorf("expected nil error, got: %v", err)
	}
}

func TestEngineFetchNoMessages(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	msgs, nextOffset, err := eng.Fetch("no-topic", 0, 0, 10, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages, got %d", len(msgs))
	}
	_ = nextOffset
}

func TestEngineFetchAfterPublish(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	// Write directly to storage
	part, err := eng.storage.GetOrCreatePartition("fetch-test", 0)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		env := &message.Envelope{
			Topic:    "fetch-test",
			Payload:  []byte("hello"),
			Priority: 0,
		}
		data, _ := message.Marshal(env)
		part.Append(data)
	}

	msgs, nextOffset, err := eng.Fetch("fetch-test", 0, 0, 10, time.Second)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 5 {
		t.Errorf("expected 5 messages, got %d", len(msgs))
	}
	if nextOffset != 5 {
		t.Errorf("nextOffset = %d, want 5", nextOffset)
	}
}

func TestEngineFetchLimited(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	part, _ := eng.storage.GetOrCreatePartition("limit-test", 0)
	for i := 0; i < 10; i++ {
		env := &message.Envelope{Topic: "limit-test", Payload: []byte("x")}
		data, _ := message.Marshal(env)
		part.Append(data)
	}

	msgs, nextOffset, err := eng.Fetch("limit-test", 0, 0, 3, time.Second)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 3 {
		t.Errorf("expected 3 messages (limit), got %d", len(msgs))
	}
	if nextOffset != 3 {
		t.Errorf("nextOffset = %d, want 3", nextOffset)
	}
}

func TestEngineFetchFromMiddle(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	part, _ := eng.storage.GetOrCreatePartition("mid-test", 0)
	for i := 0; i < 10; i++ {
		env := &message.Envelope{Topic: "mid-test", Payload: []byte("x")}
		data, _ := message.Marshal(env)
		part.Append(data)
	}

	msgs, nextOffset, err := eng.Fetch("mid-test", 0, 5, 10, time.Second)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 5 {
		t.Errorf("expected 5 messages, got %d", len(msgs))
	}
	if nextOffset != 10 {
		t.Errorf("nextOffset = %d, want 10", nextOffset)
	}
	if msgs[0].Sequence != 5 {
		t.Errorf("first msg offset = %d, want 5", msgs[0].Sequence)
	}
}

func TestEngineNotifyWaiters(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	// Should not panic
	eng.NotifyWaiters("topic", 0)
}

func TestEngineCloseMultipleGroups(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	eng.JoinGroup("g1", "topic", "m1", 4, StrategyRange)
	eng.JoinGroup("g2", "topic", "m1", 4, StrategyRange)
	eng.JoinGroup("g3", "topic", "m1", 4, StrategyRange)

	// Close should stop all group heartbeat goroutines
	cleanup()
}

func TestEngineFetchWithCorruptMessage(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	part, _ := eng.storage.GetOrCreatePartition("corrupt-test", 0)

	// Write a valid message
	env := &message.Envelope{Topic: "corrupt-test", Payload: []byte("valid")}
	data, _ := message.Marshal(env)
	part.Append(data)

	// Write raw corrupt data (not a valid envelope)
	part.Append([]byte("not-a-valid-message"))

	// Write another valid message
	env2 := &message.Envelope{Topic: "corrupt-test", Payload: []byte("valid2")}
	data2, _ := message.Marshal(env2)
	part.Append(data2)

	msgs, _, err := eng.Fetch("corrupt-test", 0, 0, 10, time.Second)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	// Should skip the corrupt message and still return the two valid ones
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages (skipping corrupt), got %d", len(msgs))
	}
}

func TestEngineFetchWithMaxLessThanAvailable(t *testing.T) {
	eng, cleanup := setupEngine(t)
	defer cleanup()

	part, _ := eng.storage.GetOrCreatePartition("max-test", 0)
	for i := 0; i < 20; i++ {
		env := &message.Envelope{Topic: "max-test", Payload: []byte{byte(i)}}
		data, _ := message.Marshal(env)
		part.Append(data)
	}

	msgs, nextOffset, _ := eng.Fetch("max-test", 0, 0, 5, time.Second)
	if len(msgs) != 5 {
		t.Errorf("expected 5 messages, got %d", len(msgs))
	}
	if nextOffset != 5 {
		t.Errorf("nextOffset = %d, want 5", nextOffset)
	}
}
