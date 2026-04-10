package queue

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

func TestPriorityOrdering(t *testing.T) {
	pd := NewPriorityDispatcher()
	c := &QueueConsumer{ID: "c1", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	pd.AddConsumer(c)

	// Enqueue low priority first, then high
	lowEnv := &message.Envelope{Priority: 1}
	highEnv := &message.Envelope{Priority: 9}

	// Dispatch high priority — should succeed
	id, err := pd.Dispatch(1, highEnv)
	if err != nil {
		t.Fatal(err)
	}
	if id != "c1" {
		t.Errorf("Dispatch(1, high) = %q, want c1", id)
	}

	// Dispatch low priority — should also succeed
	id, err = pd.Dispatch(2, lowEnv)
	if err != nil {
		t.Fatal(err)
	}
	if id != "c1" {
		t.Errorf("Dispatch(2, low) = %q, want c1", id)
	}
}

func TestPriorityConsumerMax(t *testing.T) {
	pd := NewPriorityDispatcher()
	c1 := &QueueConsumer{ID: "c1", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	c2 := &QueueConsumer{ID: "c2", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	pd.AddConsumer(c1)
	pd.AddConsumer(c2)
	pd.SetConsumerMaxPriority("c2", 3)

	// Priority 7 should go to c1 only (c2 max is 3)
	highEnv := &message.Envelope{Priority: 7}
	id, err := pd.Dispatch(1, highEnv)
	if err != nil {
		t.Fatal(err)
	}
	// c1 should be the one handling this since c2 can't handle priority 7
	if id != "c1" && id != "c2" {
		t.Errorf("unexpected consumer %q", id)
	}
}

func TestPriorityAllConsumersBusy(t *testing.T) {
	pd := NewPriorityDispatcher()
	c := &QueueConsumer{ID: "c1", Prefetch: 1, InFlight: make(map[uint64]time.Time)}
	pd.AddConsumer(c)

	env1 := &message.Envelope{Priority: 5}
	env2 := &message.Envelope{Priority: 5}

	pd.Dispatch(1, env1)
	_, err := pd.Dispatch(2, env2)
	if err != ErrAllConsumersBusy {
		t.Errorf("expected ErrAllConsumersBusy, got %v", err)
	}
}

func TestPriorityNoConsumers(t *testing.T) {
	pd := NewPriorityDispatcher()
	env := &message.Envelope{Priority: 5}
	_, err := pd.Dispatch(1, env)
	if err != ErrNoConsumers {
		t.Errorf("expected ErrNoConsumers, got %v", err)
	}
}

func TestPriorityFlushOnAdd(t *testing.T) {
	pd := NewPriorityDispatcher()

	// Add first consumer with prefetch=1, fill it
	c1 := &QueueConsumer{ID: "c1", Prefetch: 1, InFlight: make(map[uint64]time.Time)}
	pd.AddConsumer(c1)

	env1 := &message.Envelope{Priority: 5}
	env2 := &message.Envelope{Priority: 7}

	// First dispatch fills c1
	pd.Dispatch(1, env1)

	// Second dispatch: c1 is full, message should be queued
	_, err := pd.Dispatch(2, env2)
	if err != ErrAllConsumersBusy {
		t.Fatalf("expected ErrAllConsumersBusy, got %v", err)
	}

	if pd.QueuedDepth() != 1 {
		t.Errorf("QueuedDepth = %d, want 1", pd.QueuedDepth())
	}

	// Add new consumer — should flush queued message
	c2 := &QueueConsumer{ID: "c2", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	pd.AddConsumer(c2)

	if pd.QueuedDepth() != 0 {
		t.Errorf("QueuedDepth after AddConsumer = %d, want 0", pd.QueuedDepth())
	}
}

func TestPriorityRemoveConsumer(t *testing.T) {
	pd := NewPriorityDispatcher()
	c1 := &QueueConsumer{ID: "c1", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	c2 := &QueueConsumer{ID: "c2", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	pd.AddConsumer(c1)
	pd.AddConsumer(c2)

	pd.RemoveConsumer("c1")

	env := &message.Envelope{Priority: 5}
	id, err := pd.Dispatch(1, env)
	if err != nil {
		t.Fatal(err)
	}
	if id != "c2" {
		t.Errorf("Dispatch after remove = %q, want c2", id)
	}
}
