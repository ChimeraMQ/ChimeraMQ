package integration

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/message"
)

func TestQueueProduceAndDispatch(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "q-test",
		Mode:       broker.ModeQueue,
		Partitions: 2,
	})

	consumer := &queue.QueueConsumer{
		ID:       "consumer-1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	tb.broker.QueueEngine().AddConsumer("q-test", consumer)

	env := &message.Envelope{
		Topic:       "q-test",
		Payload:     []byte("hello queue"),
		ContentType: "text/plain",
		SourceProto: message.ProtoHTTP,
	}
	_, err := tb.broker.Publish(env)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	if consumer.InFlightCount() != 1 {
		t.Errorf("expected 1 in-flight message, got %d", consumer.InFlightCount())
	}
}

func TestQueueRoundRobinDispatch(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "q-rr",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	c1 := &queue.QueueConsumer{
		ID:       "c1",
		Prefetch: 100,
		InFlight: make(map[uint64]time.Time),
	}
	c2 := &queue.QueueConsumer{
		ID:       "c2",
		Prefetch: 100,
		InFlight: make(map[uint64]time.Time),
	}
	tb.broker.QueueEngine().AddConsumer("q-rr", c1)
	tb.broker.QueueEngine().AddConsumer("q-rr", c2)

	for i := 0; i < 4; i++ {
		env := &message.Envelope{
			Topic:       "q-rr",
			Payload:     []byte("msg"),
			SourceProto: message.ProtoHTTP,
		}
		tb.broker.Publish(env)
	}

	c1Count := c1.InFlightCount()
	c2Count := c2.InFlightCount()

	if c1Count != 2 || c2Count != 2 {
		t.Errorf("expected 2+2 dispatch, got c1=%d c2=%d", c1Count, c2Count)
	}
}

func TestQueueAckRemovesPending(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "q-ack",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	consumer := &queue.QueueConsumer{
		ID:       "acker",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	tb.broker.QueueEngine().AddConsumer("q-ack", consumer)

	env := &message.Envelope{
		Topic:       "q-ack",
		Payload:     []byte("ack-me"),
		SourceProto: message.ProtoHTTP,
	}
	offset, _ := tb.broker.Publish(env)

	ok := tb.broker.QueueEngine().HandleAck("q-ack", offset)
	if !ok {
		t.Error("expected ack to succeed")
	}
}

func TestQueueNackRequeues(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "q-nack",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	consumer := &queue.QueueConsumer{
		ID:       "nacker",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	tb.broker.QueueEngine().AddConsumer("q-nack", consumer)

	env := &message.Envelope{
		Topic:       "q-nack",
		Payload:     []byte("nack-me"),
		SourceProto: message.ProtoHTTP,
	}
	offset, _ := tb.broker.Publish(env)

	shouldDLQ, _ := tb.broker.QueueEngine().HandleNack("q-nack", offset)
	if shouldDLQ {
		t.Error("expected no DLQ for first nack without max retries set")
	}
}

func TestQueueNackMaxRetriesDLQ(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "q-dlq",
		Mode:       broker.ModeQueue,
		Partitions: 1,
		MaxRetries: 1,
	})

	consumer := &queue.QueueConsumer{
		ID:       "dlq-tester",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	tb.broker.QueueEngine().AddConsumer("q-dlq", consumer)

	env := &message.Envelope{
		Topic:       "q-dlq",
		Payload:     []byte("dlq-me"),
		SourceProto: message.ProtoHTTP,
		MaxRetries:  1,
	}
	offset, _ := tb.broker.Publish(env)

	shouldDLQ, deliverCount := tb.broker.QueueEngine().HandleNack("q-dlq", offset)
	if !shouldDLQ {
		t.Error("expected DLQ routing after max retries")
	}
	if deliverCount != 1 {
		t.Errorf("expected deliverCount=1, got %d", deliverCount)
	}
}

func TestQueueNoConsumers(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "q-no-consumers",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	env := &message.Envelope{
		Topic:       "q-no-consumers",
		Payload:     []byte("orphan"),
		SourceProto: message.ProtoHTTP,
	}

	_, err := tb.broker.Publish(env)
	if err != nil {
		t.Fatalf("publish without consumers should succeed: %v", err)
	}
}

func TestQueuePrefetchCapacity(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "q-prefetch",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	consumer := &queue.QueueConsumer{
		ID:       "prefetch-limited",
		Prefetch: 2,
		InFlight: make(map[uint64]time.Time),
	}
	tb.broker.QueueEngine().AddConsumer("q-prefetch", consumer)

	var offsets []uint64
	for i := 0; i < 5; i++ {
		env := &message.Envelope{
			Topic:       "q-prefetch",
			Payload:     []byte("msg"),
			SourceProto: message.ProtoHTTP,
		}
		off, _ := tb.broker.Publish(env)
		offsets = append(offsets, off)
	}

	inFlight := consumer.InFlightCount()
	if inFlight != 2 {
		t.Errorf("expected 2 in-flight (prefetch limit), got %d", inFlight)
	}

	if len(offsets) > 0 {
		tb.broker.QueueEngine().HandleAck("q-prefetch", offsets[0])
	}
}
