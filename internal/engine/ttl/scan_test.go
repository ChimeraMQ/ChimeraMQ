package ttl

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/hot"
)

func TestExpirerScanExpired(t *testing.T) {
	dir := t.TempDir()
	// Very small segment size to force multiple segments
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 512})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("ttl-test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Publish expired messages to fill the first segment
	env := &message.Envelope{
		Topic:     "ttl-test",
		Payload:   make([]byte, 100), // enough to fill small segment
		Timestamp: time.Now().Add(-10 * time.Second).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	data, _ := message.Marshal(env)
	// Write enough messages to roll at least one segment
	for i := 0; i < 10; i++ {
		if _, err := part.Append(data); err != nil {
			t.Fatal(err)
		}
	}

	// Publish a non-expired message to the active segment
	env2 := &message.Envelope{
		Topic:     "ttl-test",
		Payload:   make([]byte, 100),
		Timestamp: time.Now().UnixNano(),
		TTL:       int64(60 * time.Second),
	}
	data2, _ := message.Marshal(env2)
	if _, err := part.Append(data2); err != nil {
		t.Fatal(err)
	}

	expirer := NewExpirer(storage)
	expirer.SetTopicConfig("ttl-test", &TopicTTLConfig{
		DefaultTTL: int64(10 * time.Second),
		Action:     ActionDrop,
	})

	expirer.scan()

	// Log start should have advanced past the expired segment
	ls := part.LogStartOffset()
	t.Logf("log start offset after scan: %d", ls)
	if ls == 0 {
		t.Error("expected log start to advance past expired messages")
	}
}

func TestExpirerScanWithDLQCallback(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("dlq-test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Message that expired 1 hour ago
	env := &message.Envelope{
		Topic:     "dlq-test",
		Payload:   []byte("to-dlq"),
		Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	data, _ := message.Marshal(env)
	part.Append(data)

	var expiredTopics []string
	var expiredPayloads [][]byte
	expirer := NewExpirer(storage)
	expirer.SetOnExpired(func(topic string, env *message.Envelope) {
		expiredTopics = append(expiredTopics, topic)
		expiredPayloads = append(expiredPayloads, env.Payload)
	})
	expirer.SetTopicConfig("dlq-test", &TopicTTLConfig{
		Action: ActionDLQ,
	})

	expirer.scan()

	if len(expiredTopics) != 1 {
		t.Fatalf("expected 1 expired callback, got %d", len(expiredTopics))
	}
	if expiredTopics[0] != "dlq-test" {
		t.Errorf("topic = %q, want %q", expiredTopics[0], "dlq-test")
	}
	if string(expiredPayloads[0]) != "to-dlq" {
		t.Errorf("payload = %q, want %q", string(expiredPayloads[0]), "to-dlq")
	}
}

func TestExpirerScanNoExpired(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("fresh-test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Publish messages that are NOT expired
	for i := 0; i < 5; i++ {
		env := &message.Envelope{
			Topic:     "fresh-test",
			Payload:   []byte("fresh"),
			Timestamp: time.Now().UnixNano(),
			TTL:       int64(60 * time.Second),
		}
		data, _ := message.Marshal(env)
		part.Append(data)
	}

	hw := part.HighWatermark()
	ls := part.LogStartOffset()

	expirer := NewExpirer(storage)
	expirer.SetTopicConfig("fresh-test", &TopicTTLConfig{
		Action: ActionDrop,
	})
	expirer.scan()

	lsAfter := part.LogStartOffset()
	if lsAfter != ls {
		t.Errorf("log start moved from %d to %d, should stay same", ls, lsAfter)
	}
	hwAfter := part.HighWatermark()
	if hwAfter != hw {
		t.Errorf("high watermark changed unexpectedly")
	}
}

func TestExpirerScanZeroTTLNoExpiry(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("no-ttl", 0)
	if err != nil {
		t.Fatal(err)
	}

	env := &message.Envelope{
		Topic:     "no-ttl",
		Payload:   []byte("old-no-ttl"),
		Timestamp: time.Now().Add(-100 * time.Hour).UnixNano(),
		TTL:       0,
	}
	data, _ := message.Marshal(env)
	part.Append(data)

	expirer := NewExpirer(storage)
	expirer.SetTopicConfig("no-ttl", &TopicTTLConfig{
		Action: ActionDrop,
	})
	expirer.scan()

	ls := part.LogStartOffset()
	if ls != 0 {
		t.Errorf("log start = %d, want 0 (zero TTL should never expire)", ls)
	}
}

func TestExpirerScanUnconfiguredTopic(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("unconf", 0)
	if err != nil {
		t.Fatal(err)
	}

	env := &message.Envelope{
		Topic:     "unconf",
		Payload:   []byte("data"),
		Timestamp: time.Now().Add(-10 * time.Hour).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	data, _ := message.Marshal(env)
	part.Append(data)

	expirer := NewExpirer(storage)
	expirer.scan()

	ls := part.LogStartOffset()
	if ls != 0 {
		t.Error("unconfigured topic should not be touched")
	}
}

func TestExpirerMultipleTopics(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part1, _ := storage.GetOrCreatePartition("multi-a", 0)
	part2, _ := storage.GetOrCreatePartition("multi-b", 0)

	// Expired in topic A
	envA := &message.Envelope{
		Topic:     "multi-a",
		Payload:   []byte("expired-a"),
		Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	dataA, _ := message.Marshal(envA)
	part1.Append(dataA)

	// Not expired in topic B
	envB := &message.Envelope{
		Topic:     "multi-b",
		Payload:   []byte("alive-b"),
		Timestamp: time.Now().UnixNano(),
		TTL:       int64(60 * time.Second),
	}
	dataB, _ := message.Marshal(envB)
	part2.Append(dataB)

	expirer := NewExpirer(storage)
	expirer.SetTopicConfig("multi-a", &TopicTTLConfig{Action: ActionDrop})
	expirer.SetTopicConfig("multi-b", &TopicTTLConfig{Action: ActionDrop})

	// DLQ callback test for multi-a
	var dlqCalled bool
	expirer.SetOnExpired(func(topic string, env *message.Envelope) {
		dlqCalled = true
	})
	expirer.SetTopicConfig("multi-a", &TopicTTLConfig{Action: ActionDLQ})

	expirer.scan()

	// multi-a should have triggered DLQ callback
	if !dlqCalled {
		t.Error("multi-a should have triggered DLQ callback for expired message")
	}

	// multi-b should have no expired messages — log start stays at 0
	lsB := part2.LogStartOffset()
	if lsB != 0 {
		t.Error("multi-b should have no expired messages")
	}
}
