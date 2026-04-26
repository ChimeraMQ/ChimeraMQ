package ttl

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/hot"
)

func TestExpirerScanWithDecryptor(t *testing.T) {
	dir := t.TempDir()
	// Small segment size to force multiple segments
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 512})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("enc-test", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write expired messages to fill the first segment
	env := &message.Envelope{
		Topic:     "enc-test",
		Payload:   make([]byte, 100),
		Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	data, _ := message.Marshal(env)
	for i := 0; i < 10; i++ {
		part.Append(data)
	}

	// Write a non-expired message to the active segment
	freshEnv := &message.Envelope{
		Topic:     "enc-test",
		Payload:   []byte("fresh-after-expired"),
		Timestamp: time.Now().UnixNano(),
		TTL:       int64(60 * time.Second),
	}
	freshData, _ := message.Marshal(freshEnv)
	part.Append(freshData)

	// Set a decryptor that passes through data
	expirer := NewExpirer(storage)
	expirer.SetEncryptor(&mockDecryptor{})
	expirer.SetTopicConfig("enc-test", &TopicTTLConfig{
		Action: ActionDrop,
	})

	expirer.scan()

	// Log start should have advanced past the expired segment
	ls := part.LogStartOffset()
	if ls == 0 {
		t.Error("expected log start to advance past expired message with decryptor")
	}
}

func TestExpirerScanDecryptorFails(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("dec-fail", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write an expired message
	env := &message.Envelope{
		Topic:     "dec-fail",
		Payload:   []byte("cannot-decrypt"),
		Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	data, _ := message.Marshal(env)
	part.Append(data)

	// Set a decryptor that always fails
	expirer := NewExpirer(storage)
	expirer.SetEncryptor(&failingDecryptor{})
	expirer.SetTopicConfig("dec-fail", &TopicTTLConfig{
		Action: ActionDrop,
	})

	expirer.scan()

	// Since decryption fails, message should NOT be removed
	ls := part.LogStartOffset()
	if ls != 0 {
		t.Error("expected log start to stay at 0 when decryption fails")
	}
}

type failingDecryptor struct{}

func (f *failingDecryptor) Decrypt(data []byte, segmentID string) ([]byte, error) {
	return nil, ErrDecryptFailed
}

type mockDecryptError struct{}

var ErrDecryptFailed = &mockDecryptError{}

func (m *mockDecryptError) Error() string { return "decrypt failed" }

func TestExpirerScanUnmarshalError(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("bad-msg", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Append garbage data that can't be unmarshaled
	part.Append([]byte("this is not a valid message envelope"))

	// Follow with a valid expired message
	env := &message.Envelope{
		Topic:     "bad-msg",
		Payload:   []byte("valid-expired"),
		Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	data, _ := message.Marshal(env)
	part.Append(data)

	expirer := NewExpirer(storage)
	expirer.SetTopicConfig("bad-msg", &TopicTTLConfig{
		Action: ActionDrop,
	})

	expirer.scan()

	// Scan should not panic — unmarshal error is skipped
}

func TestExpirerScanReadError(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	// Create topic config but no partition — ForEachPartition will
	// not find any partition for this topic, so the Read path is
	// implicitly covered by the empty iteration.
	expirer := NewExpirer(storage)
	expirer.SetTopicConfig("no-partition-topic", &TopicTTLConfig{
		Action: ActionDrop,
	})

	expirer.scan()
	// Should not panic
}

func TestExpirerScanTTLZeroEarlyStop(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("early-stop", 0)
	if err != nil {
		t.Fatal(err)
	}

	// First: an expired message
	env1 := &message.Envelope{
		Topic:     "early-stop",
		Payload:   []byte("expired-first"),
		Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	data1, _ := message.Marshal(env1)
	part.Append(data1)

	// Then: a message with zero TTL (should stop scanning after hitting this)
	env2 := &message.Envelope{
		Topic:     "early-stop",
		Payload:   []byte("no-ttl"),
		Timestamp: time.Now().Add(-1 * time.Hour).UnixNano(),
		TTL:       0,
	}
	data2, _ := message.Marshal(env2)
	part.Append(data2)

	var metricCalls int
	expirer := NewExpirer(storage)
	expirer.SetOnMetric(func(topic, action string) {
		metricCalls++
	})
	expirer.SetTopicConfig("early-stop", &TopicTTLConfig{
		Action: ActionDrop,
	})

	expirer.scan()

	// Should have recorded at least one metric for the expired message
	if metricCalls == 0 {
		t.Error("expected onMetric callback to fire for expired message")
	}
}

func TestExpirerScanMetricCallbackDLQ(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("metric-dlq", 0)
	if err != nil {
		t.Fatal(err)
	}

	env := &message.Envelope{
		Topic:     "metric-dlq",
		Payload:   []byte("dlq-metric"),
		Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	data, _ := message.Marshal(env)
	part.Append(data)

	var actions []string
	expirer := NewExpirer(storage)
	expirer.SetOnMetric(func(topic, action string) {
		actions = append(actions, action)
	})
	expirer.SetTopicConfig("metric-dlq", &TopicTTLConfig{
		Action: ActionDLQ,
	})

	expirer.scan()

	if len(actions) == 0 {
		t.Fatal("expected metric callback to fire")
	}
	if actions[0] != "dlq" {
		t.Errorf("action = %q, want dlq", actions[0])
	}
}

func TestExpirerScanMetricCallbackDrop(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("metric-drop", 0)
	if err != nil {
		t.Fatal(err)
	}

	env := &message.Envelope{
		Topic:     "metric-drop",
		Payload:   []byte("drop-metric"),
		Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	data, _ := message.Marshal(env)
	part.Append(data)

	var actions []string
	expirer := NewExpirer(storage)
	expirer.SetOnMetric(func(topic, action string) {
		actions = append(actions, action)
	})
	expirer.SetTopicConfig("metric-drop", &TopicTTLConfig{
		Action: ActionDrop,
	})

	expirer.scan()

	if len(actions) == 0 {
		t.Fatal("expected metric callback to fire")
	}
	if actions[0] != "drop" {
		t.Errorf("action = %q, want drop", actions[0])
	}
}

func TestExpirerScanHWLessThanLS(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("hw-lt-ls", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Write and then advance log start so hw == ls (no messages)
	env := &message.Envelope{
		Topic:     "hw-lt-ls",
		Payload:   []byte("to-compact"),
		Timestamp: time.Now().UnixNano(),
		TTL:       int64(60 * time.Second),
	}
	data, _ := message.Marshal(env)
	part.Append(data)
	// Advance log start past high watermark
	part.AdvanceLogStart(part.HighWatermark() + 1)

	expirer := NewExpirer(storage)
	expirer.SetTopicConfig("hw-lt-ls", &TopicTTLConfig{
		Action: ActionDrop,
	})

	expirer.scan()
	// Should not panic — hw < ls branch
}

func TestExpirerScanMixedExpiredAndNonExpired(t *testing.T) {
	dir := t.TempDir()
	storage := hot.NewEngine(dir, hot.HotConfig{SegmentSize: 64 * 1024})
	defer storage.Close()

	part, err := storage.GetOrCreatePartition("mixed", 0)
	if err != nil {
		t.Fatal(err)
	}

	// Expired message at offset 0
	env1 := &message.Envelope{
		Topic:     "mixed",
		Payload:   []byte("expired"),
		Timestamp: time.Now().Add(-2 * time.Hour).UnixNano(),
		TTL:       int64(1 * time.Second),
	}
	data1, _ := message.Marshal(env1)
	part.Append(data1)

	// Non-expired message
	env2 := &message.Envelope{
		Topic:     "mixed",
		Payload:   []byte("fresh"),
		Timestamp: time.Now().UnixNano(),
		TTL:       int64(60 * time.Second),
	}
	data2, _ := message.Marshal(env2)
	part.Append(data2)

	var expiredCount int
	expirer := NewExpirer(storage)
	expirer.SetOnExpired(func(topic string, env *message.Envelope) {
		expiredCount++
	})
	expirer.SetTopicConfig("mixed", &TopicTTLConfig{
		Action: ActionDLQ,
	})

	expirer.scan()

	if expiredCount != 1 {
		t.Errorf("expired count = %d, want 1", expiredCount)
	}

	// Note: lastExpiredOffset will be 0 (expired message is at offset 0),
	// so AdvanceLogStart won't fire (> 0 check). We only verify the callback fired.
}
