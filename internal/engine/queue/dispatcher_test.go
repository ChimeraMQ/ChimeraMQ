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
