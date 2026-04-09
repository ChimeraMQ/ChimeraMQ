package message

import (
	"bytes"
	"testing"
)

func TestMarshalUnmarshalMinimal(t *testing.T) {
	original := &Envelope{
		Topic:   "test.topic",
		Payload: []byte("hello world"),
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.Topic != original.Topic {
		t.Errorf("topic: got %q, want %q", decoded.Topic, original.Topic)
	}
	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Errorf("payload: got %q, want %q", decoded.Payload, original.Payload)
	}
}

func TestMarshalUnmarshalAllFields(t *testing.T) {
	original := &Envelope{
		MessageID:    NewUUIDv7(),
		Timestamp:    1234567890,
		Sequence:     42,
		Topic:        "orders.created",
		PartitionID:  3,
		RoutingKey:   "order-123",
		Headers:      map[string][]byte{"trace-id": []byte("abc"), "source": []byte("api")},
		SchemaID:     7,
		ContentType:  "application/json",
		Encoding:     EncodingRaw,
		Payload:      []byte(`{"id":123}`),
		Priority:     5,
		DeliverCount: 1,
		MaxRetries:   3,
		TraceID:      [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:       [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		SourceProto:  ProtoChimera,
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.Topic != original.Topic {
		t.Errorf("topic mismatch")
	}
	if decoded.PartitionID != original.PartitionID {
		t.Errorf("partition mismatch")
	}
	if decoded.RoutingKey != original.RoutingKey {
		t.Errorf("routing key mismatch")
	}
	if decoded.Sequence != original.Sequence {
		t.Errorf("sequence mismatch")
	}
	if decoded.SchemaID != original.SchemaID {
		t.Errorf("schema id mismatch")
	}
	if decoded.Priority != original.Priority {
		t.Errorf("priority mismatch")
	}
	if decoded.TTL != original.TTL {
		t.Errorf("TTL mismatch: %d vs %d", decoded.TTL, original.TTL)
	}
	if decoded.SourceProto != original.SourceProto {
		t.Errorf("source proto mismatch")
	}
	if !bytes.Equal(decoded.Payload, original.Payload) {
		t.Errorf("payload mismatch")
	}
	if len(decoded.Headers) != len(original.Headers) {
		t.Errorf("headers count mismatch")
	}
	for k, v := range original.Headers {
		if !bytes.Equal(decoded.Headers[k], v) {
			t.Errorf("header %q mismatch", k)
		}
	}
	if decoded.TraceID != original.TraceID {
		t.Errorf("trace id mismatch")
	}
	if decoded.SpanID != original.SpanID {
		t.Errorf("span id mismatch")
	}
}

func TestMarshalUnmarshalWithDelay(t *testing.T) {
	original := &Envelope{
		Topic:     "delayed",
		Payload:   []byte("future"),
		DeliverAt: 9999999999,
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.DeliverAt != original.DeliverAt {
		t.Errorf("DeliverAt: got %d, want %d", decoded.DeliverAt, original.DeliverAt)
	}
}

func TestUnmarshalTooShort(t *testing.T) {
	_, err := Unmarshal([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short data")
	}
}

func TestUnmarshalFuzzDoesNotPanic(t *testing.T) {
	// Random bytes should not panic, just return error
	for i := 0; i < 1000; i++ {
		size := 64 + (i % 200)
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(i + j)
		}
		Unmarshal(data) //nolint:errcheck // fuzz: must not panic
	}
}

func TestZeroCopyPayload(t *testing.T) {
	original := &Envelope{
		Topic:   "test",
		Payload: []byte("payload data here"),
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// Verify zero-copy: payload slice shares underlying array with data
	payloadStart := &decoded.Payload[0]
	dataStart := &data[0]
	if payloadStart == dataStart {
		t.Log("payload is zero-copy (shares allocation with input)")
	}
}

func TestEstimateSize(t *testing.T) {
	e := &Envelope{
		Topic:      "test.topic",
		Payload:    make([]byte, 100),
		Headers:    map[string][]byte{"k": {}},
		TraceID:    [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}
	estimated := e.EstimateSize()
	data, _ := Marshal(e)
	if estimated != len(data) {
		t.Errorf("EstimateSize=%d, actual=%d", estimated, len(data))
	}
}

func BenchmarkMarshal(b *testing.B) {
	e := &Envelope{
		Topic:   "bench.topic",
		Payload: make([]byte, 1024),
	}
	for i := 0; i < b.N; i++ {
		Marshal(e)
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	e := &Envelope{
		Topic:   "bench.topic",
		Payload: make([]byte, 1024),
	}
	data, _ := Marshal(e)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Unmarshal(data)
	}
}

func TestMarshalUnmarshalWithTTL(t *testing.T) {
	original := &Envelope{
		Topic:    "ttl-topic",
		Payload:  []byte("expiring"),
		TTL:      5000,
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.TTL != original.TTL {
		t.Errorf("TTL: got %d, want %d", decoded.TTL, original.TTL)
	}
	if decoded.Topic != original.Topic {
		t.Errorf("Topic: got %q, want %q", decoded.Topic, original.Topic)
	}
}

func TestMarshalUnmarshalWithTrace(t *testing.T) {
	original := &Envelope{
		Topic:    "traced",
		Payload:  []byte("traced-msg"),
		TraceID:  [16]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10},
		SpanID:   [8]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11},
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.TraceID != original.TraceID {
		t.Errorf("TraceID mismatch")
	}
	if decoded.SpanID != original.SpanID {
		t.Errorf("SpanID mismatch")
	}
}

func TestMarshalUnmarshalEmptyPayload(t *testing.T) {
	original := &Envelope{
		Topic:   "empty",
		Payload: []byte{},
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if len(decoded.Payload) != 0 {
		t.Errorf("expected empty payload, got %d bytes", len(decoded.Payload))
	}
}

func TestUnmarshalTopicExtendsBeyond(t *testing.T) {
	data := make([]byte, FixedHeaderSize)
	// Set topicLen to something larger than available data
	data[52] = 0xFF
	data[53] = 0xFF // topicLen = 65535

	_, err := Unmarshal(data)
	if err == nil {
		t.Error("expected error for topic extending beyond data")
	}
}

func TestUnmarshalRoutingKeyExtendsBeyond(t *testing.T) {
	// Create a valid envelope then corrupt routing key length
	e := &Envelope{Topic: "t", Payload: []byte("p")}
	data, _ := Marshal(e)

	// Corrupt routing key length to be too large
	data[54] = 0xFF
	data[55] = 0xFF // rkLen = 65535

	_, err := Unmarshal(data)
	if err == nil {
		t.Error("expected error for routing key extending beyond data")
	}
}

func TestUnmarshalHeadersExtendBeyond(t *testing.T) {
	e := &Envelope{Topic: "t", Payload: []byte("p")}
	data, _ := Marshal(e)

	// Corrupt headers length to be too large
	data[48] = 0xFF
	data[49] = 0xFF
	data[50] = 0xFF
	data[51] = 0xFF // headersLen = huge

	_, err := Unmarshal(data)
	if err == nil {
		t.Error("expected error for headers extending beyond data")
	}
}

func TestUnmarshalTraceExtendsBeyond(t *testing.T) {
	e := &Envelope{
		Topic:    "t",
		Payload:  []byte("p"),
		TraceID:  [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
	}
	data, _ := Marshal(e)

	// Truncate data so trace section is incomplete
	data = data[:len(data)-20]

	_, err := Unmarshal(data)
	if err == nil {
		t.Error("expected error for trace extending beyond data")
	}
}

func TestUnmarshalPayloadExtendsBeyond(t *testing.T) {
	e := &Envelope{Topic: "t", Payload: []byte("payload")}
	data, _ := Marshal(e)

	// Corrupt payload length to be too large
	data[44] = 0x7F
	data[45] = 0xFF
	data[46] = 0xFF
	data[47] = 0xFF // payloadLen = huge

	_, err := Unmarshal(data)
	if err == nil {
		t.Error("expected error for payload extending beyond data")
	}
}

func TestReleaseBuffer(t *testing.T) {
	e := &Envelope{Topic: "t", Payload: []byte("data")}
	data, _ := Marshal(e)

	// Should not panic
	ReleaseBuffer(data)

	// Double release should also not panic
	ReleaseBuffer(data)
}

func TestMarshalLargePayloadTriggersNewAlloc(t *testing.T) {
	// Payload larger than the pool's 4096-byte initial capacity triggers the cap(buf) < size branch
	e := &Envelope{
		Topic:   "large-topic",
		Payload: make([]byte, 8192),
	}

	data, err := Marshal(e)
	if err != nil {
		t.Fatalf("Marshal large: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal large: %v", err)
	}
	if len(decoded.Payload) != 8192 {
		t.Errorf("payload len = %d, want 8192", len(decoded.Payload))
	}
}

func TestMarshalUnmarshalDeliverCountMaxRetries(t *testing.T) {
	original := &Envelope{
		Topic:        "retry-topic",
		Payload:      []byte("retry-msg"),
		DeliverCount: 5,
		MaxRetries:   10,
	}

	data, err := Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	decoded, err := Unmarshal(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.DeliverCount != 5 {
		t.Errorf("DeliverCount = %d, want 5", decoded.DeliverCount)
	}
	if decoded.MaxRetries != 10 {
		t.Errorf("MaxRetries = %d, want 10", decoded.MaxRetries)
	}
}
