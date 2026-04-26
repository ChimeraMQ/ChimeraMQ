package message

import (
	"testing"
)

func TestMarshalUnmarshalWithHeaders(t *testing.T) {
	orig := &Envelope{
		Topic:   "hdr-topic",
		Payload: []byte("data"),
		Headers: map[string][]byte{
			"trace-id": []byte("abc-123"),
			"source":   []byte("test"),
			"x-custom": []byte("value: !@#$%"),
		},
	}

	data, err := Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}

	got, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}

	if len(got.Headers) != 3 {
		t.Fatalf("headers count = %d, want 3", len(got.Headers))
	}
	if string(got.Headers["trace-id"]) != "abc-123" {
		t.Errorf("trace-id = %q, want abc-123", got.Headers["trace-id"])
	}
	if string(got.Headers["x-custom"]) != "value: !@#$%" {
		t.Errorf("x-custom = %q", got.Headers["x-custom"])
	}
}

func TestUnmarshalTruncatedTopic(t *testing.T) {
	orig := &Envelope{
		Topic:   "test",
		Payload: []byte("data"),
	}
	data, _ := Marshal(orig)

	_, err := Unmarshal(data[:FixedHeaderSize])
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestProtocolTypeString(t *testing.T) {
	tests := []struct {
		proto   ProtocolType
		wantStr string
	}{
		{ProtoChimera, "chimera"},
		{ProtoAMQP, "amqp"},
		{ProtoMQTT, "mqtt"},
		{ProtoWS, "websocket"},
		{ProtoHTTP, "http"},
		{ProtoSTOMP, "stomp"},
		{ProtoNATS, "nats"},
		{ProtoGRPC, "grpc"},
		{ProtocolType(99), "unknown"},
	}
	for _, tt := range tests {
		got := tt.proto.String()
		if got != tt.wantStr {
			t.Errorf("ProtocolType(%d).String() = %q, want %q", tt.proto, got, tt.wantStr)
		}
	}
}

func TestMarshalUnmarshalFullEnvelope(t *testing.T) {
	orig := &Envelope{
		Topic:        "full-topic",
		RoutingKey:   "rk-123",
		PartitionID:  3,
		SchemaID:     42,
		Priority:     7,
		Encoding:     EncodingSnappy,
		SourceProto:  ProtoHTTP,
		ContentType:  "application/json",
		Payload:      []byte(`{"key":"value"}`),
		Headers:      map[string][]byte{"h1": []byte("v1")},
		TTL:          30000000000,
		DeliverCount: 2,
		MaxRetries:   5,
	}
	copy(orig.TraceID[:], []byte("traceid12345678"))
	copy(orig.SpanID[:], []byte("spanid12"))
	uid := NewUUIDv7()
	copy(orig.MessageID[:], uid[:])
	orig.Timestamp = 1700000000000000000
	orig.Sequence = 42

	data, err := Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}

	got, err := Unmarshal(data)
	if err != nil {
		t.Fatal(err)
	}

	if got.Topic != orig.Topic {
		t.Errorf("topic = %q, want %q", got.Topic, orig.Topic)
	}
	if got.RoutingKey != orig.RoutingKey {
		t.Errorf("routingKey = %q, want %q", got.RoutingKey, orig.RoutingKey)
	}
	if got.PartitionID != orig.PartitionID {
		t.Errorf("partitionID = %d, want %d", got.PartitionID, orig.PartitionID)
	}
	if got.SchemaID != orig.SchemaID {
		t.Errorf("schemaID = %d, want %d", got.SchemaID, orig.SchemaID)
	}
	if got.Priority != orig.Priority {
		t.Errorf("priority = %d, want %d", got.Priority, orig.Priority)
	}
	if got.Encoding != orig.Encoding {
		t.Errorf("encoding = %d, want %d", got.Encoding, orig.Encoding)
	}
	if got.SourceProto != orig.SourceProto {
		t.Errorf("sourceProto = %d, want %d", got.SourceProto, orig.SourceProto)
	}
	if got.Sequence != orig.Sequence {
		t.Errorf("sequence = %d, want %d", got.Sequence, orig.Sequence)
	}
	if got.Timestamp != orig.Timestamp {
		t.Errorf("timestamp = %d, want %d", got.Timestamp, orig.Timestamp)
	}
	if got.TTL != orig.TTL {
		t.Errorf("TTL = %d, want %d", got.TTL, orig.TTL)
	}
	if got.MessageID != orig.MessageID {
		t.Errorf("messageID mismatch")
	}
	if got.TraceID != orig.TraceID {
		t.Errorf("traceID mismatch")
	}
	if got.SpanID != orig.SpanID {
		t.Errorf("spanID mismatch")
	}
	if string(got.Headers["h1"]) != "v1" {
		t.Errorf("header h1 = %q, want v1", got.Headers["h1"])
	}
}

func TestEnvelopeEstimateSize(t *testing.T) {
	env := &Envelope{
		Topic:      "test",
		RoutingKey: "rk",
		Payload:    []byte("hello"),
	}
	size := env.EstimateSize()
	expected := FixedHeaderSize + len("test") + len("rk") + len("hello")
	if size != expected {
		t.Errorf("EstimateSize = %d, want %d", size, expected)
	}

	// With tracing headers
	var tid [16]byte
	tid[0] = 1
	var sid [8]byte
	sid[0] = 2
	env2 := &Envelope{
		Topic:   "t",
		Payload: []byte("x"),
		TraceID: tid,
		SpanID:  sid,
		Headers: map[string][]byte{"k": []byte("v")},
	}
	size2 := env2.EstimateSize()
	expected2 := FixedHeaderSize + len("t") + len("x") + 2 + len("k") + 4 + len("v") + 24
	if size2 != expected2 {
		t.Errorf("EstimateSize with trace = %d, want %d", size2, expected2)
	}
}
