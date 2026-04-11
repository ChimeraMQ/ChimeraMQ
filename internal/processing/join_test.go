package processing

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

func TestJoinImmediateMatch(t *testing.T) {
	var results []*JoinResult
	join := NewJoinOp(
		KeyFromRoutingKey(),
		KeyFromRoutingKey(),
		5*time.Second,
		func(r *JoinResult) { results = append(results, r) },
	)

	left := &message.Envelope{Topic: "orders", RoutingKey: "user-1", Payload: []byte(`{"order":"A"}`)}
	right := &message.Envelope{Topic: "payments", RoutingKey: "user-1", Payload: []byte(`{"payment":"PA"}`)}

	now := time.Now().UnixNano()
	join.AddLeft(left, now)
	join.AddRight(right, now)

	if len(results) != 1 {
		t.Fatalf("expected 1 join result, got %d", len(results))
	}
	if results[0].Left.RoutingKey != "user-1" {
		t.Errorf("left key = %q", results[0].Left.RoutingKey)
	}
	if string(results[0].Right.Payload) != `{"payment":"PA"}` {
		t.Errorf("right payload = %q", results[0].Right.Payload)
	}
}

func TestJoinReverseMatch(t *testing.T) {
	var results []*JoinResult
	join := NewJoinOp(
		KeyFromRoutingKey(),
		KeyFromRoutingKey(),
		5*time.Second,
		func(r *JoinResult) { results = append(results, r) },
	)

	left := &message.Envelope{Topic: "orders", RoutingKey: "user-2", Payload: []byte("order")}
	right := &message.Envelope{Topic: "payments", RoutingKey: "user-2", Payload: []byte("payment")}

	now := time.Now().UnixNano()
	join.AddRight(right, now) // right arrives first
	join.AddLeft(left, now)   // left arrives later — should match

	if len(results) != 1 {
		t.Fatalf("expected 1 join result, got %d", len(results))
	}
}

func TestJoinNoMatch(t *testing.T) {
	var results []*JoinResult
	join := NewJoinOp(
		KeyFromRoutingKey(),
		KeyFromRoutingKey(),
		5*time.Second,
		func(r *JoinResult) { results = append(results, r) },
	)

	now := time.Now().UnixNano()
	join.AddLeft(&message.Envelope{RoutingKey: "user-1"}, now)
	join.AddRight(&message.Envelope{RoutingKey: "user-2"}, now)

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}

	l, r := join.BufferSizes()
	if l != 1 || r != 1 {
		t.Errorf("buffers: left=%d right=%d, want 1/1", l, r)
	}
}

func TestJoinWindowExpiry(t *testing.T) {
	var results []*JoinResult
	join := NewJoinOp(
		KeyFromRoutingKey(),
		KeyFromRoutingKey(),
		100*time.Millisecond,
		func(r *JoinResult) { results = append(results, r) },
	)

	now := time.Now().UnixNano()
	join.AddLeft(&message.Envelope{RoutingKey: "k1"}, now)

	// Tick after window expires
	time.Sleep(150 * time.Millisecond)
	laterNow := time.Now().UnixNano()
	join.Tick(laterNow)

	l, _ := join.BufferSizes()
	if l != 0 {
		t.Errorf("left buffer should be empty after expiry, got %d", l)
	}
}

func TestJoinOutsideWindow(t *testing.T) {
	var results []*JoinResult
	join := NewJoinOp(
		KeyFromRoutingKey(),
		KeyFromRoutingKey(),
		100*time.Millisecond,
		func(r *JoinResult) { results = append(results, r) },
	)

	now := time.Now().UnixNano()
	join.AddLeft(&message.Envelope{RoutingKey: "k1"}, now)

	// Right arrives after window expired
	laterNow := now + int64(200*time.Millisecond)
	join.AddRight(&message.Envelope{RoutingKey: "k1"}, laterNow)

	if len(results) != 0 {
		t.Errorf("expected 0 results (outside window), got %d", len(results))
	}
}

func TestJoinMultipleMatches(t *testing.T) {
	var results []*JoinResult
	join := NewJoinOp(
		KeyFromRoutingKey(),
		KeyFromRoutingKey(),
		5*time.Second,
		func(r *JoinResult) { results = append(results, r) },
	)

	now := time.Now().UnixNano()

	// Multiple lefts with same key
	join.AddLeft(&message.Envelope{RoutingKey: "k1", Payload: []byte("L1")}, now)
	join.AddLeft(&message.Envelope{RoutingKey: "k1", Payload: []byte("L2")}, now)

	// Right matches the first left
	join.AddRight(&message.Envelope{RoutingKey: "k1", Payload: []byte("R1")}, now)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Second right matches the remaining left
	join.AddRight(&message.Envelope{RoutingKey: "k1", Payload: []byte("R2")}, now)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
}

func TestKeyFromHeader(t *testing.T) {
	fn := KeyFromHeader("user-id")
	env := &message.Envelope{
		Headers: map[string][]byte{
			"user-id": []byte("u123"),
		},
	}
	if fn(env) != "u123" {
		t.Errorf("expected u123, got %q", fn(env))
	}

	// Missing header
	empty := &message.Envelope{}
	if fn(empty) != "" {
		t.Errorf("expected empty, got %q", fn(empty))
	}
}

func TestKeyFromPayloadJSON(t *testing.T) {
	fn := KeyFromPayloadJSON("user_id")
	env := &message.Envelope{
		Payload: []byte(`{"user_id": "alice", "amount": 100}`),
	}
	if fn(env) != "alice" {
		t.Errorf("expected alice, got %q", fn(env))
	}

	// Missing field
	env2 := &message.Envelope{
		Payload: []byte(`{"name": "bob"}`),
	}
	if fn(env2) != "" {
		t.Errorf("expected empty, got %q", fn(env2))
	}

	// Invalid JSON
	env3 := &message.Envelope{
		Payload: []byte(`not json`),
	}
	if fn(env3) != "" {
		t.Errorf("expected empty for invalid json, got %q", fn(env3))
	}
}

func TestKeyFromRoutingKey(t *testing.T) {
	fn := KeyFromRoutingKey()
	env := &message.Envelope{RoutingKey: "order-123"}
	if fn(env) != "order-123" {
		t.Errorf("expected order-123, got %q", fn(env))
	}
}

func TestJoinDifferentKeys(t *testing.T) {
	leftFn := KeyFromRoutingKey()
	rightFn := KeyFromHeader("join-key")

	var results []*JoinResult
	join := NewJoinOp(leftFn, rightFn, 5*time.Second,
		func(r *JoinResult) { results = append(results, r) },
	)

	now := time.Now().UnixNano()
	join.AddLeft(&message.Envelope{RoutingKey: "user-1"}, now)
	join.AddRight(&message.Envelope{
		Headers: map[string][]byte{"join-key": []byte("user-1")},
	}, now)

	if len(results) != 1 {
		t.Fatalf("expected 1 result with different key extractors, got %d", len(results))
	}
}
