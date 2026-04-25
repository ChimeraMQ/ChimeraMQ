package chimera

import (
	"testing"
)

func TestDetectorDetect(t *testing.T) {
	d := &Detector{}

	// Valid magic bytes
	peek := []byte{FrameMagic0, FrameMagic1, FrameMagic2, FrameMagic3}
	if !d.Detect(peek) {
		t.Error("should detect valid CHMR magic")
	}

	// Too short
	if d.Detect([]byte{FrameMagic0, FrameMagic1}) {
		t.Error("should not detect with too short peek")
	}

	// Wrong magic
	peek = []byte{'A', 'M', 'Q', 'P'}
	if d.Detect(peek) {
		t.Error("should not detect wrong magic")
	}
}

func TestDetectorBytesNeeded(t *testing.T) {
	d := &Detector{}
	if d.BytesNeeded() != 4 {
		t.Errorf("BytesNeeded = %d, want 4", d.BytesNeeded())
	}
}

func TestDecodeBatchPublishEmpty(t *testing.T) {
	batch := decodeBatchPublish(nil)
	if len(batch.Messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(batch.Messages))
	}

	batch = decodeBatchPublish([]byte{0, 0, 0, 0}) // count = 0
	if len(batch.Messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(batch.Messages))
	}
}

func TestDecodeBatchPublishTooShort(t *testing.T) {
	// Less than 4 bytes — can't read count
	batch := decodeBatchPublish([]byte{0, 0, 0})
	if len(batch.Messages) != 0 {
		t.Errorf("expected 0 messages for short data")
	}
}

func TestDecodeBatchPublishTruncated(t *testing.T) {
	// count = 1 but no message body
	data := []byte{0, 0, 0, 1}
	batch := decodeBatchPublish(data)
	if len(batch.Messages) != 0 {
		t.Errorf("expected 0 messages for truncated data")
	}
}

func TestDecodeBatchPublishSingleMessage(t *testing.T) {
	// Build a minimal batch publish payload:
	// [count:4][topicLen:2][topic][rkLen:2][rk][priority:1][ttl:8][deliverAt:8][bodyLen:4][body]
	payload := make([]byte, 0, 64)
	payload = appendUint32(payload, 1)                         // count = 1
	payload = appendUint16(payload, uint16(len("test-topic"))) // topicLen
	payload = append(payload, []byte("test-topic")...)         // topic
	payload = appendUint16(payload, 3)                         // rkLen
	payload = append(payload, []byte("key")...)                // routingKey
	payload = append(payload, 5)                               // priority
	payload = appendUint64(payload, 60000000000)               // TTL (60s in ns)
	payload = appendUint64(payload, 0)                         // deliverAt
	payload = appendUint32(payload, 5)                         // bodyLen
	payload = append(payload, []byte("hello")...)              // body

	batch := decodeBatchPublish(payload)
	if len(batch.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(batch.Messages))
	}
	msg := batch.Messages[0]
	if msg.Topic != "test-topic" {
		t.Errorf("topic = %q, want test-topic", msg.Topic)
	}
	if msg.RoutingKey != "key" {
		t.Errorf("routingKey = %q, want key", msg.RoutingKey)
	}
	if msg.Priority != 5 {
		t.Errorf("priority = %d, want 5", msg.Priority)
	}
	if string(msg.Body) != "\x00\x00\x00\x05hello" {
		t.Errorf("body = %q, want \\x00\\x00\\x00\\x05hello", string(msg.Body))
	}
}

func TestDecodeBatchPublishMissingTopicLen(t *testing.T) {
	data := appendUint32(nil, 1) // count = 1, but no topic data
	batch := decodeBatchPublish(data)
	if len(batch.Messages) != 0 {
		t.Errorf("expected 0 messages when topicLen missing")
	}
}

func TestDecodeBatchPublishMissingRoutingKeyLen(t *testing.T) {
	payload := appendUint32(nil, 1)
	payload = appendUint16(payload, uint16(len("t")))
	payload = append(payload, 't')
	// No routing key length
	batch := decodeBatchPublish(payload)
	if len(batch.Messages) != 0 {
		t.Errorf("expected 0 messages when routing key len missing")
	}
}
