package queue

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

func TestEngineAddConsumer(t *testing.T) {
	e := NewEngine()
	c := &QueueConsumer{ID: "c1", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	e.AddConsumer("topic1", c)

	// Should be able to dispatch to this consumer
	env := &message.Envelope{Payload: []byte("test")}
	consumerID, err := e.TryDispatch("topic1", 0, 0, env)
	if err != nil {
		t.Fatal(err)
	}
	if consumerID != "c1" {
		t.Errorf("expected c1, got %s", consumerID)
	}
}

func TestEngineRemoveConsumer(t *testing.T) {
	e := NewEngine()
	c := &QueueConsumer{ID: "c1", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	e.AddConsumer("topic1", c)
	e.RemoveConsumer("topic1", "c1")

	env := &message.Envelope{Payload: []byte("test")}
	_, err := e.TryDispatch("topic1", 0, 0, env)
	if err != ErrNoConsumers {
		t.Errorf("expected ErrNoConsumers, got %v", err)
	}
}

func TestEngineDispatchNoConsumers(t *testing.T) {
	e := NewEngine()
	env := &message.Envelope{Payload: []byte("test")}
	_, err := e.TryDispatch("no-topic", 0, 0, env)
	if err != ErrNoConsumers {
		t.Errorf("expected ErrNoConsumers, got %v", err)
	}
}

func TestEngineAckAndNack(t *testing.T) {
	e := NewEngine()
	c := &QueueConsumer{ID: "c1", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	e.AddConsumer("topic1", c)

	env := &message.Envelope{Payload: []byte("test")}
	e.TryDispatch("topic1", 0, 0, env)

	// Ack
	if !e.HandleAck("topic1", 0) {
		t.Error("expected ack to succeed")
	}
	// Ack again (already removed)
	if e.HandleAck("topic1", 0) {
		t.Error("expected second ack to fail")
	}
}

func TestEngineNackRequeue(t *testing.T) {
	e := NewEngine()
	c := &QueueConsumer{ID: "c1", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	e.AddConsumer("topic1", c)

	env := &message.Envelope{Payload: []byte("test")}
	e.TryDispatch("topic1", 0, 0, env)

	shouldDLQ, count := e.HandleNack("topic1", 0)
	if shouldDLQ {
		t.Error("should not DLQ without max retries")
	}
	if count != 1 {
		t.Errorf("expected count=1, got %d", count)
	}
}

func TestEngineNackDLQ(t *testing.T) {
	e := NewEngine()
	c := &QueueConsumer{ID: "c1", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	e.AddConsumer("topic1", c)

	env := &message.Envelope{Payload: []byte("test"), MaxRetries: 1}
	e.TryDispatch("topic1", 0, 0, env)

	shouldDLQ, count := e.HandleNack("topic1", 0)
	if !shouldDLQ {
		t.Error("expected DLQ with max retries exhausted")
	}
	if count != 1 {
		t.Errorf("expected count=1, got %d", count)
	}
}

func TestEngineScheduleDelayed(t *testing.T) {
	e := NewEngine()
	env := &message.Envelope{Payload: []byte("delayed"), DeliverAt: time.Now().Add(1 * time.Hour).UnixNano()}
	e.ScheduleDelayed("delayed-topic", env)
	// Should not panic, queue state is created lazily
}
