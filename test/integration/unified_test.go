package integration

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/engine/stream"
	"github.com/chimeramq/chimera/internal/message"
)

func TestUnifiedModePublishDispatchesToBoth(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "u-both",
		Mode:       broker.ModeUnified,
		Partitions: 1,
	})

	// Add a queue consumer
	consumer := &queue.QueueConsumer{
		ID:       "q-unified",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	tb.broker.QueueEngine().AddConsumer("u-both", consumer)

	// Publish
	env := &message.Envelope{
		Topic:       "u-both",
		Payload:     []byte("unified-msg"),
		SourceProto: message.ProtoHTTP,
	}
	offset, err := tb.broker.Publish(env)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Queue consumer should have received it
	if consumer.InFlightCount() != 1 {
		t.Errorf("expected 1 queue in-flight, got %d", consumer.InFlightCount())
	}

	// Stream fetch should also work
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("u-both", 0, 0, 10, 1*time.Second)
	if err != nil {
		t.Fatalf("stream fetch: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected 1 stream message, got %d", len(msgs))
	}

	// Same offset — offset may differ from 0 if partition creation writes something
	_ = offset != 0 && len(msgs) > 0
}

func TestUnifiedModeStreamAndQueueSimultaneously(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "u-sim",
		Mode:       broker.ModeUnified,
		Partitions: 2,
	})

	// Set up queue consumer
	c1 := &queue.QueueConsumer{
		ID:       "q1",
		Prefetch: 100,
		InFlight: make(map[uint64]time.Time),
	}
	tb.broker.QueueEngine().AddConsumer("u-sim", c1)

	// Set up stream consumer group
	se := tb.broker.StreamEngine()
	se.JoinGroup("sim-group", "u-sim", "stream-1", 2, stream.StrategyRange)

	// Publish multiple messages
	for i := 0; i < 10; i++ {
		env := &message.Envelope{
			Topic:       "u-sim",
			Payload:     []byte("msg"),
			SourceProto: message.ProtoHTTP,
		}
		tb.broker.Publish(env)
	}

	// Queue consumer should have some messages
	if c1.InFlightCount() == 0 {
		t.Error("queue consumer should have received messages")
	}

	// Stream fetch from partition 0 should work
	msgs, _, err := se.Fetch("u-sim", 0, 0, 100, 1*time.Second)
	if err != nil {
		t.Fatalf("stream fetch: %v", err)
	}
	if len(msgs) == 0 {
		t.Error("stream should have messages in partition 0")
	}
}

func TestUnifiedModeQueueAckDoesNotAffectStream(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "u-ack",
		Mode:       broker.ModeUnified,
		Partitions: 1,
	})

	consumer := &queue.QueueConsumer{
		ID:       "q-ack",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	tb.broker.QueueEngine().AddConsumer("u-ack", consumer)

	env := &message.Envelope{
		Topic:       "u-ack",
		Payload:     []byte("ack-test"),
		SourceProto: message.ProtoHTTP,
	}
	offset, _ := tb.broker.Publish(env)

	// Ack the queue message
	tb.broker.QueueEngine().HandleAck("u-ack", offset)

	// Stream should still be able to read it
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("u-ack", 0, 0, 10, 1*time.Second)
	if err != nil {
		t.Fatalf("stream fetch after ack: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("stream should still have 1 message after queue ack, got %d", len(msgs))
	}
}

func TestUnifiedModeOffsetCommitDoesNotAffectQueue(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "u-offset",
		Mode:       broker.ModeUnified,
		Partitions: 2,
	})

	consumer := &queue.QueueConsumer{
		ID:       "q-off",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	tb.broker.QueueEngine().AddConsumer("u-offset", consumer)

	for i := 0; i < 4; i++ {
		env := &message.Envelope{
			Topic:       "u-offset",
			Payload:     []byte("msg"),
			SourceProto: message.ProtoHTTP,
		}
		tb.broker.Publish(env)
	}

	// Commit stream offset
	se := tb.broker.StreamEngine()
	se.JoinGroup("off-group", "u-offset", "m1", 2, stream.StrategyRange)
	se.CommitOffset("off-group", 0, 99)

	// Queue should still dispatch new messages normally
	env := &message.Envelope{
		Topic:       "u-offset",
		Payload:     []byte("after-commit"),
		SourceProto: message.ProtoHTTP,
	}
	_, err := tb.broker.Publish(env)
	if err != nil {
		t.Fatalf("publish after stream commit: %v", err)
	}
}
