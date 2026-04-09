package broker

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

func setupBrokerForPublish(t *testing.T) (*Broker, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg, _ := LoadConfig("", &CLIFlags{DataDir: dir})
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		b.Stop()
	}
	return b, cleanup
}

func TestPublishTopicNotFound(t *testing.T) {
	b, cleanup := setupBrokerForPublish(t)
	defer cleanup()

	env := &message.Envelope{Topic: "nonexistent", Payload: []byte("data")}
	_, err := b.Publish(env)
	if err == nil {
		t.Error("expected error for nonexistent topic")
	}
}

func TestPublishHappyPath(t *testing.T) {
	b, cleanup := setupBrokerForPublish(t)
	defer cleanup()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "pub-test",
		Mode:       ModeStream,
		Partitions: 2,
	})

	env := &message.Envelope{
		Topic:       "pub-test",
		Payload:     []byte("hello"),
		ContentType: "text/plain",
		SourceProto: message.ProtoHTTP,
	}
	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if offset != 0 {
		t.Errorf("first offset = %d, want 0", offset)
	}

	// Verify PartitionID was assigned
	if env.PartitionID > 1 {
		t.Errorf("partition = %d, expected 0 or 1", env.PartitionID)
	}

	// Verify UUID was assigned
	if env.MessageID == [16]byte{} {
		t.Error("MessageID should be assigned")
	}

	// Verify timestamp was assigned
	if env.Timestamp == 0 {
		t.Error("Timestamp should be assigned")
	}

	// Verify sequence was set
	if env.Sequence != 0 {
		t.Errorf("sequence = %d, want 0", env.Sequence)
	}
}

func TestPublishMultipleMessages(t *testing.T) {
	b, cleanup := setupBrokerForPublish(t)
	defer cleanup()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "multi",
		Mode:       ModeUnified,
		Partitions: 1,
	})

	for i := 0; i < 10; i++ {
		env := &message.Envelope{
			Topic:   "multi",
			Payload: []byte{byte(i)},
		}
		offset, err := b.Publish(env)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if offset != uint64(i) {
			t.Errorf("offset %d = %d, want %d", i, offset, i)
		}
	}
}

func TestPublishToQueueTopic(t *testing.T) {
	b, cleanup := setupBrokerForPublish(t)
	defer cleanup()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "queue-topic",
		Mode:       ModeQueue,
		Partitions: 1,
	})

	env := &message.Envelope{
		Topic:   "queue-topic",
		Payload: []byte("queue-msg"),
	}
	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish to queue: %v", err)
	}
	if offset != 0 {
		t.Errorf("offset = %d, want 0", offset)
	}
}

func TestPublishDelayedMessage(t *testing.T) {
	b, cleanup := setupBrokerForPublish(t)
	defer cleanup()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "delayed-topic",
		Mode:       ModeQueue,
		Partitions: 1,
	})

	// DeliverAt in the future
	env := &message.Envelope{
		Topic:     "delayed-topic",
		Payload:   []byte("delayed"),
		DeliverAt: time.Now().Add(1 * time.Hour).UnixNano(),
	}
	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish delayed: %v", err)
	}
	// Delayed messages return offset 0 immediately
	if offset != 0 {
		t.Errorf("delayed offset = %d, want 0", offset)
	}
}

func TestPublishWithRoutingKey(t *testing.T) {
	b, cleanup := setupBrokerForPublish(t)
	defer cleanup()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "routed",
		Mode:       ModeStream,
		Partitions: 4,
	})

	env := &message.Envelope{
		Topic:       "routed",
		RoutingKey:  "user-123",
		Payload:     []byte("routed-msg"),
		SourceProto: message.ProtoChimera,
	}
	offset, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish routed: %v", err)
	}
	_ = offset

	// Same routing key should land on same partition
	env2 := &message.Envelope{
		Topic:      "routed",
		RoutingKey: "user-123",
		Payload:    []byte("routed-msg-2"),
	}
	_, err = b.Publish(env2)
	if err != nil {
		t.Fatalf("publish routed 2: %v", err)
	}
	if env2.PartitionID != env.PartitionID {
		t.Errorf("same routing key landed on different partitions: %d vs %d", env.PartitionID, env2.PartitionID)
	}
}

func TestPublishWithExistingTimestamp(t *testing.T) {
	b, cleanup := setupBrokerForPublish(t)
	defer cleanup()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "ts-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	customTS := int64(1700000000000000000)
	env := &message.Envelope{
		Topic:     "ts-topic",
		Payload:   []byte("ts"),
		Timestamp: customTS,
	}
	_, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Timestamp should be preserved
	if env.Timestamp != customTS {
		t.Errorf("timestamp modified: got %d, want %d", env.Timestamp, customTS)
	}
}

func TestPublishWithHeaders(t *testing.T) {
	b, cleanup := setupBrokerForPublish(t)
	defer cleanup()

	b.Topics().CreateTopic(TopicConfig{
		Name:       "hdr-topic",
		Mode:       ModeStream,
		Partitions: 1,
	})

	env := &message.Envelope{
		Topic:    "hdr-topic",
		Payload:  []byte("data"),
		Headers:  map[string][]byte{"trace-id": []byte("abc-123")},
	}
	_, err := b.Publish(env)
	if err != nil {
		t.Fatalf("publish with headers: %v", err)
	}
}
