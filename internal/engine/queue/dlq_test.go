package queue

import (
	"testing"

	"github.com/chimeramq/chimera/internal/message"
)

func TestDLQRoute(t *testing.T) {
	mgr := NewDLQManager("dlq-topic")
	env := &message.Envelope{
		Topic:   "original",
		Payload: []byte("failed-msg"),
		Headers: map[string][]byte{"trace": []byte("abc")},
	}

	routed, err := mgr.Route(env, "max-retries", 3)
	if err != nil {
		t.Fatal(err)
	}
	if routed.Topic != "dlq-topic" {
		t.Errorf("expected dlq-topic, got %s", routed.Topic)
	}
	if string(routed.Headers["x-chimera-original-topic"]) != "original" {
		t.Error("expected x-chimera-original-topic header")
	}
}

func TestDLQRoutePreservesPayload(t *testing.T) {
	mgr := NewDLQManager("dlq")
	env := &message.Envelope{
		Topic:       "src",
		Payload:     []byte("payload-data"),
		ContentType: "text/plain",
	}

	routed, _ := mgr.Route(env, "retry-exhausted", 1)
	if string(routed.Payload) != "payload-data" {
		t.Errorf("payload not preserved: %s", string(routed.Payload))
	}
	if routed.ContentType != "text/plain" {
		t.Error("content type not preserved")
	}
}
