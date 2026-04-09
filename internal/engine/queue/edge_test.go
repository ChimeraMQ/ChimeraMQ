package queue

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

func TestEngineClose(t *testing.T) {
	e := NewEngine()
	e.AddConsumer("close-topic", &QueueConsumer{ID: "c1", Prefetch: 10, InFlight: make(map[uint64]time.Time)})
	e.AddConsumer("close-topic", &QueueConsumer{ID: "c2", Prefetch: 10, InFlight: make(map[uint64]time.Time)})

	// Should not panic
	e.Close()
}

func TestEngineHandleAckNonexistentTopic(t *testing.T) {
	e := NewEngine()
	if e.HandleAck("no-topic", 0) {
		t.Error("HandleAck on nonexistent topic should return false")
	}
}

func TestEngineHandleNackNonexistentTopic(t *testing.T) {
	e := NewEngine()
	shouldDLQ, count := e.HandleNack("no-topic", 0)
	if shouldDLQ {
		t.Error("HandleNack on nonexistent topic should not trigger DLQ")
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}
}

func TestEngineRemoveConsumerNonexistentTopic(t *testing.T) {
	e := NewEngine()
	// Should not panic
	e.RemoveConsumer("no-topic", "c1")
}

func TestEngineScheduleDelayedAndPromote(t *testing.T) {
	e := NewEngine()
	defer e.Close()

	// Schedule a message due in the very near future
	env := &message.Envelope{
		Topic:     "delay-topic",
		Payload:   []byte("delayed"),
		DeliverAt: time.Now().Add(50 * time.Millisecond).UnixNano(),
	}
	e.ScheduleDelayed("delay-topic", env)

	// Wait for promotion
	time.Sleep(250 * time.Millisecond)

	// Check that the queue has a delayHeap with the promoted message
	e.mu.RLock()
	qs, ok := e.queues["delay-topic"]
	e.mu.RUnlock()
	if !ok {
		t.Fatal("queue should exist for delay-topic")
	}
	if qs.delayHeap == nil {
		t.Fatal("delayHeap should be created")
	}

	// Read from Ready channel
	select {
	case promoted := <-qs.delayHeap.Ready():
		if promoted.Topic != "delay-topic" {
			t.Errorf("promoted topic = %q, want delay-topic", promoted.Topic)
		}
	default:
		t.Error("expected a promoted message on Ready channel")
	}
}

func TestDelaySchedulerScheduleAndReady(t *testing.T) {
	ds := NewDelayScheduler()
	defer ds.Stop()

	// Schedule with past time — should be promoted on next tick
	env := &message.Envelope{
		Topic:     "immediate",
		Payload:   []byte("now"),
		DeliverAt: time.Now().Add(-1 * time.Second).UnixNano(),
	}
	ds.Schedule(env)

	// Wait for promotion tick (100ms interval)
	time.Sleep(250 * time.Millisecond)

	select {
	case got := <-ds.Ready():
		if got.Topic != "immediate" {
			t.Errorf("topic = %q, want immediate", got.Topic)
		}
	default:
		t.Error("expected message to be promoted from delay heap")
	}
}

func TestDelaySchedulerFutureMessage(t *testing.T) {
	ds := NewDelayScheduler()
	defer ds.Stop()

	// Schedule with far-future time
	env := &message.Envelope{
		Topic:     "future",
		Payload:   []byte("later"),
		DeliverAt: time.Now().Add(1 * time.Hour).UnixNano(),
	}
	ds.Schedule(env)

	time.Sleep(250 * time.Millisecond)

	select {
	case <-ds.Ready():
		t.Error("future message should not be promoted yet")
	default:
		// Expected
	}
}

func TestDelaySchedulerMultipleMessages(t *testing.T) {
	ds := NewDelayScheduler()
	defer ds.Stop()

	for i := 0; i < 5; i++ {
		env := &message.Envelope{
			Topic:     "multi-delay",
			Payload:   []byte{byte(i)},
			DeliverAt: time.Now().Add(-1 * time.Second).UnixNano(),
		}
		ds.Schedule(env)
	}

	time.Sleep(250 * time.Millisecond)

	count := 0
	for {
		select {
		case <-ds.Ready():
			count++
		default:
			goto done
		}
	}
done:
	if count != 5 {
		t.Errorf("promoted %d messages, want 5", count)
	}
}

func TestAckTrackerVisibilityTimeout(t *testing.T) {
	at := NewAckTracker(500 * time.Millisecond)
	defer at.Stop()

	at.Track(10, "c1", 0, 3)
	at.Track(20, "c1", 0, 3)
	at.Track(30, "c1", 0, 3)

	// Wait for visibility timeout + ticker (1s) to fire
	time.Sleep(2 * time.Second)

	// Messages should be redelivered via the redeliver channel
	redelivered := make(map[uint64]bool)
	timeout := time.After(500 * time.Millisecond)
	for len(redelivered) < 3 {
		select {
		case off := <-at.RedeliverChan():
			redelivered[off] = true
		case <-timeout:
			t.Fatalf("only got %d redelivered messages, want 3", len(redelivered))
		}
	}
}

func TestAckTrackerAckPreventsRedelivery(t *testing.T) {
	at := NewAckTracker(500 * time.Millisecond)
	defer at.Stop()

	at.Track(10, "c1", 0, 3)
	at.Track(20, "c1", 0, 3)

	// Ack one before timeout
	at.Ack(10)

	time.Sleep(2 * time.Second)

	// Only offset 20 should be redelivered
	select {
	case off := <-at.RedeliverChan():
		if off != 20 {
			t.Errorf("redelivered offset = %d, want 20", off)
		}
	default:
		t.Error("expected offset 20 to be redelivered")
	}

	// No more
	select {
	case off := <-at.RedeliverChan():
		t.Errorf("unexpected redelivery: %d", off)
	default:
		// Expected
	}
}

func TestQueueConsumerInFlightCount(t *testing.T) {
	qc := &QueueConsumer{
		ID:       "c1",
		Prefetch: 5,
		InFlight: map[uint64]time.Time{1: {}, 2: {}, 3: {}},
	}

	if count := qc.InFlightCount(); count != 3 {
		t.Errorf("InFlightCount = %d, want 3", count)
	}
}

func TestDispatcherSkipBusyConsumer(t *testing.T) {
	d := &Dispatcher{visTimeout: 30 * time.Second}

	c1 := &QueueConsumer{ID: "busy", Prefetch: 1, InFlight: map[uint64]time.Time{1: {}}}
	c2 := &QueueConsumer{ID: "free", Prefetch: 10, InFlight: make(map[uint64]time.Time)}
	d.AddConsumer(c1)
	d.AddConsumer(c2)

	env := &message.Envelope{Topic: "test", Payload: []byte("msg")}
	id, err := d.Dispatch(1, env)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if id != "free" {
		t.Errorf("dispatched to %q, want free", id)
	}
}

func TestDLQRouteEmptyTopic(t *testing.T) {
	mgr := NewDLQManager("")
	env := &message.Envelope{Topic: "source", Payload: []byte("data")}
	result, err := mgr.Route(env, "max-retries", 3)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if result != nil {
		t.Error("expected nil for empty DLQ topic")
	}
}

func TestDLQRouteWithRoutingKey(t *testing.T) {
	mgr := NewDLQManager("dlq-target")
	env := &message.Envelope{
		Topic:      "source",
		RoutingKey: "user-123",
		Payload:    []byte("data"),
		Headers:    map[string][]byte{},
	}
	result, err := mgr.Route(env, "test-reason", 2)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Headers["x-chimera-original-routing-key"] == nil {
		t.Error("expected x-chimera-original-routing-key header")
	}
}

func TestDLQRouteNilHeaders(t *testing.T) {
	mgr := NewDLQManager("dlq-target")
	env := &message.Envelope{
		Topic:   "source",
		Payload: []byte("data"),
		// Headers is nil
	}
	result, err := mgr.Route(env, "max-retries", 5)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Headers["x-chimera-original-topic"] == nil {
		t.Error("expected x-chimera-original-topic header")
	}
	if result.Headers["x-chimera-death-count"] == nil {
		t.Error("expected x-chimera-death-count header")
	}
	if result.Topic != "dlq-target" {
		t.Errorf("topic = %q, want dlq-target", result.Topic)
	}
	if result.MessageID == [16]byte{} {
		t.Error("expected new MessageID")
	}
}

func TestEngineTryDispatchNoConsumers(t *testing.T) {
	e := NewEngine()
	defer e.Close()

	env := &message.Envelope{Topic: "test", Payload: []byte("data")}
	_, err := e.TryDispatch("no-topic", 0, 0, env)
	if err != ErrNoConsumers {
		t.Errorf("expected ErrNoConsumers, got %v", err)
	}
}

func TestEngineTryDispatchAndNackDLQ(t *testing.T) {
	e := NewEngine()
	defer e.Close()

	e.AddConsumer("dlq-test", &QueueConsumer{
		ID:       "c1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	})

	env := &message.Envelope{
		Topic:        "dlq-test",
		Payload:      []byte("data"),
		DeliverCount: 2,
		MaxRetries:   3,
	}

	consumerID, err := e.TryDispatch("dlq-test", 0, 0, env)
	if err != nil {
		t.Fatalf("tryDispatch: %v", err)
	}
	if consumerID != "c1" {
		t.Errorf("consumer = %q, want c1", consumerID)
	}

	// Nack — deliverCount (2) + 1 = 3 >= maxRetries (3) → shouldDLQ = true
	shouldDLQ, count := e.HandleNack("dlq-test", 0)
	if !shouldDLQ {
		t.Error("expected shouldDLQ=true after max retries exceeded")
	}
	if count != 3 {
		t.Errorf("deliverCount = %d, want 3", count)
	}
}

func TestEngineHandleAck(t *testing.T) {
	e := NewEngine()
	defer e.Close()

	e.AddConsumer("ack-test", &QueueConsumer{
		ID:       "c1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	})

	env := &message.Envelope{
		Topic:      "ack-test",
		Payload:    []byte("data"),
		MaxRetries: 3,
	}
	e.TryDispatch("ack-test", 0, 0, env)

	if !e.HandleAck("ack-test", 0) {
		t.Error("HandleAck should return true for tracked offset")
	}
}

func TestEngineHandleAckAlreadyAcked(t *testing.T) {
	e := NewEngine()
	defer e.Close()

	e.AddConsumer("ack2", &QueueConsumer{
		ID:       "c1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	})

	env := &message.Envelope{Topic: "ack2", Payload: []byte("data"), MaxRetries: 3}
	e.TryDispatch("ack2", 0, 0, env)

	e.HandleAck("ack2", 0)
	// Second ack should return false (already removed)
	if e.HandleAck("ack2", 0) {
		t.Error("second HandleAck should return false")
	}
}

func TestEngineNackTriggersDLQ(t *testing.T) {
	e := NewEngine()
	defer e.Close()

	// Add consumer
	c := &QueueConsumer{
		ID:       "c1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	e.AddConsumer("dlq-engine-topic", c)

	// Track a message with maxRetries=1
	qs := e.queues["dlq-engine-topic"]
	qs.ackTracker.Track(100, "c1", 0, 1)

	// Nack — deliverCount goes to 1, >= maxRetries(1) → shouldDLQ=true
	shouldDLQ, deliverCount := e.HandleNack("dlq-engine-topic", 100)
	if !shouldDLQ {
		t.Error("expected shouldDLQ=true when deliverCount >= maxRetries")
	}
	if deliverCount != 1 {
		t.Errorf("deliverCount = %d, want 1", deliverCount)
	}
}
