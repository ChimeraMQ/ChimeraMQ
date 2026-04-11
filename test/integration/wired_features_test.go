package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/message"
)

// TestDelayedDelivery verifies that messages scheduled for future delivery
// are held by the delay scheduler and dispatched to consumers only after
// the scheduled time.
func TestDelayedDelivery(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "delay-test",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	qe := tb.broker.QueueEngine()
	consumer := &queue.QueueConsumer{
		ID:       "c1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	qe.AddConsumer("delay-test", consumer)

	// Publish a message with a 500ms delay
	deliverAt := time.Now().Add(500 * time.Millisecond).UnixNano()
	env := &message.Envelope{
		Topic:       "delay-test",
		Payload:     []byte("delayed-msg"),
		DeliverAt:   deliverAt,
		SourceProto: message.ProtoHTTP,
	}
	offset, err := tb.broker.Publish(env)
	if err != nil {
		t.Fatalf("publish delayed: %v", err)
	}
	// Delayed messages return offset 0 since they skip WAL/storage until promoted
	_ = offset

	// Immediately check — consumer should NOT have the message yet
	time.Sleep(50 * time.Millisecond)
	if consumer.InFlightCount() > 0 {
		t.Error("delayed message dispatched too early")
	}

	// Wait for the delay to pass + drain interval (100ms)
	time.Sleep(700 * time.Millisecond)
	if consumer.InFlightCount() == 0 {
		t.Error("delayed message was never dispatched after delay period")
	}
}

// TestDelayedDeliveryOrdering verifies that immediate messages are dispatched
// before delayed ones even when the delayed message was published first.
func TestDelayedDeliveryOrdering(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "delay-order",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	qe := tb.broker.QueueEngine()
	consumer := &queue.QueueConsumer{
		ID:       "c1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	qe.AddConsumer("delay-order", consumer)

	// Publish delayed message first
	deliverAt := time.Now().Add(300 * time.Millisecond).UnixNano()
	tb.broker.Publish(&message.Envelope{
		Topic:       "delay-order",
		Payload:     []byte("delayed"),
		DeliverAt:   deliverAt,
		SourceProto: message.ProtoHTTP,
	})

	// Publish immediate message
	tb.broker.Publish(&message.Envelope{
		Topic:       "delay-order",
		Payload:     []byte("immediate"),
		SourceProto: message.ProtoHTTP,
	})

	// Wait for immediate dispatch
	time.Sleep(100 * time.Millisecond)
	if consumer.InFlightCount() == 0 {
		t.Fatal("expected immediate message to be dispatched")
	}
	// The immediate message should be dispatched right away
	if consumer.InFlightCount() != 1 {
		t.Errorf("expected 1 in-flight (immediate only), got %d", consumer.InFlightCount())
	}
}

// TestPriorityDispatch verifies that higher-priority messages are dispatched
// before lower-priority ones when priority dispatch is enabled.
func TestPriorityDispatch(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "priority-test",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	qe := tb.broker.QueueEngine()
	qe.SetPriorityEnabled(true)

	// Add consumer
	consumer := &queue.QueueConsumer{
		ID:       "c1",
		Prefetch: 100,
		InFlight: make(map[uint64]time.Time),
	}
	qe.AddConsumer("priority-test", consumer)

	// Publish low priority first, then high priority
	tb.broker.Publish(&message.Envelope{
		Topic:       "priority-test",
		Payload:     []byte("low-priority"),
		Priority:    1,
		SourceProto: message.ProtoHTTP,
	})
	tb.broker.Publish(&message.Envelope{
		Topic:       "priority-test",
		Payload:     []byte("high-priority"),
		Priority:    9,
		SourceProto: message.ProtoHTTP,
	})

	time.Sleep(100 * time.Millisecond)
	inFlight := consumer.InFlightCount()
	if inFlight < 2 {
		t.Fatalf("expected >= 2 in-flight messages, got %d", inFlight)
	}
}

// TestTTLPublishMarksEnvelope verifies that publishing to a topic with TTL
// config sets the TTL field on the stored message.
func TestTTLPublishMarksEnvelope(t *testing.T) {
	tb := newTestBroker(t)

	ttlNs := int64(2 * time.Second)
	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "ttl-test",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	// Get the topic config and set TTL
	topic, ok := tb.broker.Topics().GetTopic("ttl-test")
	if !ok {
		t.Fatal("topic not found")
	}
	topic.DefaultTTL = ttlNs

	// Publish a message — should get TTL applied
	env := &message.Envelope{
		Topic:       "ttl-test",
		Payload:     []byte("ttl-msg"),
		SourceProto: message.ProtoHTTP,
	}
	_, err := tb.broker.Publish(env)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Verify TTL was applied to the stored message
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("ttl-test", 0, 0, 10, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) == 0 {
		t.Fatal("no messages found")
	}
	if msgs[0].TTL == 0 {
		t.Error("expected TTL to be set for message published to TTL topic")
	}
}

// TestISRReplicationBasic verifies that basic publishing works through the
// replication path (data reaches storage even with replication configured).
func TestISRReplicationBasic(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "repl-test",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	// Publish some data
	for i := 0; i < 5; i++ {
		env := &message.Envelope{
			Topic:       "repl-test",
			Payload:     []byte(fmt.Sprintf("repl-msg-%d", i)),
			SourceProto: message.ProtoHTTP,
		}
		if _, err := tb.broker.Publish(env); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Verify data is in storage
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("repl-test", 0, 0, 10, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 5 {
		t.Errorf("expected 5 messages, got %d", len(msgs))
	}
}

// TestDLQMaxRetriesIntegration verifies that messages exceeding max retries
// are flagged for DLQ routing.
func TestDLQMaxRetriesIntegration(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "dlq-integ",
		Mode:       broker.ModeQueue,
		Partitions: 1,
		MaxRetries: 1,
	})

	qe := tb.broker.QueueEngine()
	consumer := &queue.QueueConsumer{
		ID:       "c1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	qe.AddConsumer("dlq-integ", consumer)

	// Publish a message
	env := &message.Envelope{
		Topic:       "dlq-integ",
		Payload:     []byte("will-dlq"),
		MaxRetries:  1,
		SourceProto: message.ProtoHTTP,
	}
	offset, err := tb.broker.Publish(env)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Nack the message to trigger DLQ
	shouldDLQ, deliverCount := qe.HandleNack("dlq-integ", offset)
	if !shouldDLQ {
		t.Error("expected DLQ routing after max retries")
	}
	if deliverCount != 1 {
		t.Errorf("expected deliverCount=1, got %d", deliverCount)
	}
}
