package ws

import (
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
)

func TestNewServer(t *testing.T) {
	s := NewServer(nil)
	if s == nil {
		t.Fatal("server should not be nil")
	}
}

func TestDetectorDetect(t *testing.T) {
	d := Detector{}
	if d.Detect([]byte("GET /ws HTTP/1.1")) {
		t.Error("detector should always return false")
	}
}

func TestDetectorBytesNeeded(t *testing.T) {
	d := Detector{}
	if d.BytesNeeded() != 0 {
		t.Errorf("BytesNeeded = %d, want 0", d.BytesNeeded())
	}
}

func TestServerHandleConnection(t *testing.T) {
	s := NewServer(nil)
	// HandleConnection should be a no-op for WebSocket
	if err := s.HandleConnection(nil, nil); err != nil {
		t.Errorf("HandleConnection should return nil, got %v", err)
	}
}

func TestServerStop(t *testing.T) {
	s := NewServer(nil)
	// Stop with no sessions should be safe
	s.Stop()
}

func TestWSSessionMessage(t *testing.T) {
	msg := wsMessage{
		Op:         "publish",
		Topic:      "test-topic",
		Payload:    "aGVsbG8=",
		RoutingKey: "key1",
	}
	if msg.Op != "publish" {
		t.Errorf("Op = %q", msg.Op)
	}
	if msg.Topic != "test-topic" {
		t.Errorf("Topic = %q", msg.Topic)
	}
}

func TestWSSessionMessageDefaults(t *testing.T) {
	msg := wsMessage{}
	if msg.Op != "" {
		t.Error("default Op should be empty")
	}
	if msg.Partitions != 0 {
		t.Error("default Partitions should be 0")
	}
}

func TestTopicModeConstants(t *testing.T) {
	if broker.ModeStream != 0 {
		t.Errorf("ModeStream = %d, want 0", broker.ModeStream)
	}
	if broker.ModeQueue != 1 {
		t.Errorf("ModeQueue = %d, want 1", broker.ModeQueue)
	}
	if broker.ModeUnified != 2 {
		t.Errorf("ModeUnified = %d, want 2", broker.ModeUnified)
	}
}
