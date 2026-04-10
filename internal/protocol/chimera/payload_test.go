package chimera

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestDecodeConnect(t *testing.T) {
	// Encode: clientID + username + password + keepalive
	var buf []byte
	buf = appendUint16(buf, uint16(len("client1")))
	buf = append(buf, "client1"...)
	buf = appendUint16(buf, uint16(len("user1")))
	buf = append(buf, "user1"...)
	buf = appendUint16(buf, uint16(len("pass1")))
	buf = append(buf, "pass1"...)
	buf = appendUint16(buf, 30)

	p := decodeConnect(buf)
	if p.ClientID != "client1" {
		t.Errorf("ClientID = %q, want client1", p.ClientID)
	}
	if p.Username != "user1" {
		t.Errorf("Username = %q, want user1", p.Username)
	}
	if p.Password != "pass1" {
		t.Errorf("Password = %q, want pass1", p.Password)
	}
	if p.Keepalive != 30 {
		t.Errorf("Keepalive = %d, want 30", p.Keepalive)
	}
}

func TestDecodeConnectEmpty(t *testing.T) {
	p := decodeConnect(nil)
	if p.ClientID != "" {
		t.Errorf("expected empty ClientID, got %q", p.ClientID)
	}
	if p.Keepalive != 0 {
		t.Errorf("expected 0 keepalive, got %d", p.Keepalive)
	}
}

func TestEncodeConnAck(t *testing.T) {
	data := encodeConnAck("server-1", 0)
	r := newReader(data)
	id, _ := r.readString()
	if id != "server-1" {
		t.Errorf("clientID = %q, want server-1", id)
	}
	if r.len() != 1 {
		t.Fatalf("expected 1 remaining byte, got %d", r.len())
	}
	status := r.read(1)[0]
	if status != 0 {
		t.Errorf("status = %d, want 0", status)
	}
}

func TestDecodePublish(t *testing.T) {
	var buf []byte
	buf = appendUint16(buf, uint16(len("my-topic")))
	buf = append(buf, "my-topic"...)
	buf = appendUint16(buf, uint16(len("route-key")))
	buf = append(buf, "route-key"...)
	buf = append(buf, 5)           // priority
	buf = appendUint64(buf, 60000) // TTL
	buf = appendUint64(buf, 0)     // DeliverAt
	buf = append(buf, []byte("hello world")...)

	p := decodePublish(buf)
	if p.Topic != "my-topic" {
		t.Errorf("Topic = %q, want my-topic", p.Topic)
	}
	if p.RoutingKey != "route-key" {
		t.Errorf("RoutingKey = %q, want route-key", p.RoutingKey)
	}
	if p.Priority != 5 {
		t.Errorf("Priority = %d, want 5", p.Priority)
	}
	if p.TTL != 60000 {
		t.Errorf("TTL = %d, want 60000", p.TTL)
	}
	if p.DeliverAt != 0 {
		t.Errorf("DeliverAt = %d, want 0", p.DeliverAt)
	}
	if string(p.Body) != "hello world" {
		t.Errorf("Body = %q, want hello world", p.Body)
	}
}

func TestDecodePublishMinimal(t *testing.T) {
	var buf []byte
	buf = appendUint16(buf, uint16(len("t")))
	buf = append(buf, "t"...)
	buf = appendUint16(buf, 0) // empty routing key

	p := decodePublish(buf)
	if p.Topic != "t" {
		t.Errorf("Topic = %q, want t", p.Topic)
	}
	if p.Priority != 0 {
		t.Errorf("expected 0 priority for short payload, got %d", p.Priority)
	}
}

func TestEncodePubAck(t *testing.T) {
	data := encodePubAck("topic1", 3, 42)
	r := newReader(data)
	topic, _ := r.readString()
	if topic != "topic1" {
		t.Errorf("topic = %q, want topic1", topic)
	}
	partID := binary.BigEndian.Uint32(r.read(4))
	if partID != 3 {
		t.Errorf("partition = %d, want 3", partID)
	}
	offset := binary.BigEndian.Uint64(r.read(8))
	if offset != 42 {
		t.Errorf("offset = %d, want 42", offset)
	}
}

func TestDecodeSubscribe(t *testing.T) {
	var buf []byte
	buf = appendUint16(buf, uint16(len("sub-topic")))
	buf = append(buf, "sub-topic"...)
	buf = append(buf, 1) // mode
	buf = appendUint16(buf, uint16(len("my-group")))
	buf = append(buf, "my-group"...)
	buf = appendUint32(buf, 10)  // prefetch
	buf = appendUint64(buf, 100) // start offset

	p := decodeSubscribe(buf)
	if p.Topic != "sub-topic" {
		t.Errorf("Topic = %q, want sub-topic", p.Topic)
	}
	if p.Mode != 1 {
		t.Errorf("Mode = %d, want 1", p.Mode)
	}
	if p.ConsumerGroup != "my-group" {
		t.Errorf("ConsumerGroup = %q, want my-group", p.ConsumerGroup)
	}
	if p.Prefetch != 10 {
		t.Errorf("Prefetch = %d, want 10", p.Prefetch)
	}
	if p.StartOffset != 100 {
		t.Errorf("StartOffset = %d, want 100", p.StartOffset)
	}
}

func TestDecodeSubscribeMinimal(t *testing.T) {
	var buf []byte
	buf = appendUint16(buf, uint16(len("t")))
	buf = append(buf, "t"...)

	p := decodeSubscribe(buf)
	if p.Topic != "t" {
		t.Errorf("Topic = %q, want t", p.Topic)
	}
}

func TestEncodeSubAck(t *testing.T) {
	data := encodeSubAck("topic1", true)
	r := newReader(data)
	topic, _ := r.readString()
	if topic != "topic1" {
		t.Errorf("topic = %q, want topic1", topic)
	}
	success := r.read(1)[0]
	if success != 1 {
		t.Errorf("success = %d, want 1", success)
	}

	data2 := encodeSubAck("topic2", false)
	r2 := newReader(data2)
	_, _ = r2.readString() // read and skip topic
	_ = r2.read(1)         // skip length byte from readString remainder
	// Re-read properly
	r2 = newReader(data2)
	_, _ = r2.readString()
	success2 := r2.read(1)[0]
	if success2 != 0 {
		t.Errorf("success = %d, want 0", success2)
	}
}

func TestDecodeAck(t *testing.T) {
	var buf []byte
	buf = appendUint16(buf, uint16(len("ack-topic")))
	buf = append(buf, "ack-topic"...)
	buf = appendUint32(buf, 2) // partition
	buf = appendUint64(buf, 10)
	buf = appendUint64(buf, 20)
	buf = appendUint64(buf, 30)

	p := decodeAck(buf)
	if p.Topic != "ack-topic" {
		t.Errorf("Topic = %q, want ack-topic", p.Topic)
	}
	if p.PartitionID != 2 {
		t.Errorf("PartitionID = %d, want 2", p.PartitionID)
	}
	if len(p.Offsets) != 3 {
		t.Fatalf("Offsets len = %d, want 3", len(p.Offsets))
	}
	if p.Offsets[0] != 10 || p.Offsets[1] != 20 || p.Offsets[2] != 30 {
		t.Errorf("Offsets = %v, want [10 20 30]", p.Offsets)
	}
}

func TestDecodeAckNoOffsets(t *testing.T) {
	var buf []byte
	buf = appendUint16(buf, uint16(len("t")))
	buf = append(buf, "t"...)
	buf = appendUint32(buf, 0)

	p := decodeAck(buf)
	if p.Topic != "t" {
		t.Errorf("Topic = %q, want t", p.Topic)
	}
	if len(p.Offsets) != 0 {
		t.Errorf("expected no offsets, got %d", len(p.Offsets))
	}
}

func TestDecodeCreateTopic(t *testing.T) {
	var buf []byte
	buf = appendUint16(buf, uint16(len("new-topic")))
	buf = append(buf, "new-topic"...)
	buf = appendUint16(buf, uint16(len("stream")))
	buf = append(buf, "stream"...)
	buf = appendUint32(buf, 8)

	p := decodeCreateTopic(buf)
	if p.Name != "new-topic" {
		t.Errorf("Name = %q, want new-topic", p.Name)
	}
	if p.Mode != "stream" {
		t.Errorf("Mode = %q, want stream", p.Mode)
	}
	if p.Partitions != 8 {
		t.Errorf("Partitions = %d, want 8", p.Partitions)
	}
}

func TestDecodeCreateTopicMinimal(t *testing.T) {
	var buf []byte
	buf = appendUint16(buf, uint16(len("x")))
	buf = append(buf, "x"...)
	buf = appendUint16(buf, 0)

	p := decodeCreateTopic(buf)
	if p.Name != "x" {
		t.Errorf("Name = %q, want x", p.Name)
	}
	if p.Partitions != 0 {
		t.Errorf("Partitions = %d, want 0", p.Partitions)
	}
}

func TestEncodeError(t *testing.T) {
	data := encodeError(404, "not found")
	r := newReader(data)
	code := binary.BigEndian.Uint16(r.read(2))
	if code != 404 {
		t.Errorf("code = %d, want 404", code)
	}
	msg, _ := r.readString()
	if msg != "not found" {
		t.Errorf("msg = %q, want 'not found'", msg)
	}
}

func TestPayloadReader(t *testing.T) {
	data := []byte{0x00, 0x03, 'a', 'b', 'c', 0xFF}
	r := newReader(data)

	s, err := r.readString()
	if err != nil {
		t.Fatalf("readString: %v", err)
	}
	if s != "abc" {
		t.Errorf("string = %q, want abc", s)
	}
	rem := r.read(1)
	if len(rem) != 1 || rem[0] != 0xFF {
		t.Errorf("remaining = %v, want [0xFF]", rem)
	}
}

func TestPayloadReaderEOF(t *testing.T) {
	r := newReader([]byte{0x00, 0x05}) // claims 5 bytes but only 0 available
	_, err := r.readString()
	if err == nil {
		t.Error("expected error for short string")
	}
}

func TestPayloadReadBeyondEnd(t *testing.T) {
	r := newReader([]byte{0x01, 0x02})
	out := r.read(10) // tries to read 10, only 2 available
	if len(out) != 2 {
		t.Errorf("read(10) returned %d bytes, want 2", len(out))
	}
}

func TestAppendUintHelpers(t *testing.T) {
	var buf []byte
	buf = appendUint16(buf, 0x0102)
	buf = appendUint32(buf, 0x01020304)
	buf = appendUint64(buf, 0x0102030405060708)

	r := bytes.NewReader(buf)

	var u16 [2]byte
	r.Read(u16[:])
	if binary.BigEndian.Uint16(u16[:]) != 0x0102 {
		t.Error("uint16 mismatch")
	}

	var u32 [4]byte
	r.Read(u32[:])
	if binary.BigEndian.Uint32(u32[:]) != 0x01020304 {
		t.Error("uint32 mismatch")
	}

	var u64 [8]byte
	r.Read(u64[:])
	if binary.BigEndian.Uint64(u64[:]) != 0x0102030405060708 {
		t.Error("uint64 mismatch")
	}
}
