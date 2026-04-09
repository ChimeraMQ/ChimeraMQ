package queue

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

func TestDispatchRoundRobin(t *testing.T) {
	d := &Dispatcher{visTimeout: 30 * time.Second}
	c1 := &QueueConsumer{ID: "c1", Prefetch: 100, InFlight: make(map[uint64]time.Time)}
	c2 := &QueueConsumer{ID: "c2", Prefetch: 100, InFlight: make(map[uint64]time.Time)}
	d.AddConsumer(c1)
	d.AddConsumer(c2)

	env := &message.Envelope{Payload: []byte("msg")}
	id1, _ := d.Dispatch(0, env)
	id2, _ := d.Dispatch(1, env)

	if id1 == id2 {
		t.Errorf("round-robin should alternate: got %s then %s", id1, id2)
	}
}

func TestDispatchPrefetchFull(t *testing.T) {
	d := &Dispatcher{visTimeout: 30 * time.Second}
	c1 := &QueueConsumer{ID: "c1", Prefetch: 1, InFlight: make(map[uint64]time.Time)}
	d.AddConsumer(c1)

	env := &message.Envelope{Payload: []byte("msg")}
	d.Dispatch(0, env) // fills prefetch

	_, err := d.Dispatch(1, env)
	if err != ErrAllConsumersBusy {
		t.Errorf("expected ErrAllConsumersBusy, got %v", err)
	}
}

func TestDispatchNoConsumers(t *testing.T) {
	d := &Dispatcher{visTimeout: 30 * time.Second}
	env := &message.Envelope{Payload: []byte("msg")}
	_, err := d.Dispatch(0, env)
	if err != ErrNoConsumers {
		t.Errorf("expected ErrNoConsumers, got %v", err)
	}
}

func TestRemoveConsumer(t *testing.T) {
	d := &Dispatcher{visTimeout: 30 * time.Second}
	c1 := &QueueConsumer{ID: "c1", Prefetch: 100, InFlight: make(map[uint64]time.Time)}
	d.AddConsumer(c1)
	d.RemoveConsumer("c1")

	_, err := d.Dispatch(0, &message.Envelope{Payload: []byte("msg")})
	if err != ErrNoConsumers {
		t.Errorf("expected ErrNoConsumers after remove, got %v", err)
	}
}

func TestRemoveConsumerResetsNextIdx(t *testing.T) {
	d := &Dispatcher{visTimeout: 30 * time.Second}
	c1 := &QueueConsumer{ID: "c1", Prefetch: 100, InFlight: make(map[uint64]time.Time)}
	c2 := &QueueConsumer{ID: "c2", Prefetch: 100, InFlight: make(map[uint64]time.Time)}
	c3 := &QueueConsumer{ID: "c3", Prefetch: 100, InFlight: make(map[uint64]time.Time)}
	d.AddConsumer(c1)
	d.AddConsumer(c2)
	d.AddConsumer(c3)

	// Dispatch to advance nextIdx
	env := &message.Envelope{Payload: []byte("msg")}
	d.Dispatch(0, env) // c1
	d.Dispatch(1, env) // c2
	d.Dispatch(2, env) // c3
	d.Dispatch(3, env) // c1 again (nextIdx wraps)

	// Remove consumers to make nextIdx >= len(consumers)
	d.RemoveConsumer("c1")
	d.RemoveConsumer("c2")

	// Should still work — nextIdx is reset to 0
	id, err := d.Dispatch(4, env)
	if err != nil {
		t.Fatalf("dispatch after remove: %v", err)
	}
	if id != "c3" {
		t.Errorf("dispatched to %s, want c3", id)
	}
}

func TestRemoveConsumerNonexistent(t *testing.T) {
	d := &Dispatcher{visTimeout: 30 * time.Second}
	c1 := &QueueConsumer{ID: "c1", Prefetch: 100, InFlight: make(map[uint64]time.Time)}
	d.AddConsumer(c1)

	// Remove nonexistent consumer — should be no-op
	d.RemoveConsumer("no-such-consumer")

	env := &message.Envelope{Payload: []byte("msg")}
	id, err := d.Dispatch(0, env)
	if err != nil {
		t.Fatalf("dispatch after removing nonexistent: %v", err)
	}
	if id != "c1" {
		t.Errorf("dispatched to %s, want c1", id)
	}
}

func TestDispatchSkipsFullConsumers(t *testing.T) {
	d := &Dispatcher{visTimeout: 30 * time.Second}
	c1 := &QueueConsumer{ID: "c1", Prefetch: 1, InFlight: make(map[uint64]time.Time)}
	c2 := &QueueConsumer{ID: "c2", Prefetch: 100, InFlight: make(map[uint64]time.Time)}
	d.AddConsumer(c1)
	d.AddConsumer(c2)

	// Fill c1's prefetch
	env := &message.Envelope{Payload: []byte("msg")}
	d.Dispatch(0, env)

	// Next dispatch should skip c1 and go to c2
	id, err := d.Dispatch(1, env)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if id != "c2" {
		t.Errorf("expected c2 (c1 full), got %s", id)
	}
}
