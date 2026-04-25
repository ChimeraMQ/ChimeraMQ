package message

import (
	"testing"
)

// BenchmarkMarshalSmall measures allocations for a minimal message (no headers, no routing key).
func BenchmarkMarshalSmall(b *testing.B) {
	env := &Envelope{
		Topic:   "test-topic",
		Payload: []byte("hello"),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Marshal(env)
	}
}

// BenchmarkMarshalWithHeaders measures allocations for a message with headers.
func BenchmarkMarshalWithHeaders(b *testing.B) {
	env := &Envelope{
		Topic:       "test-topic",
		RoutingKey:  "orders.created",
		Headers:     map[string][]byte{"x-source": []byte("grpc"), "x-trace-id": []byte("abc-123")},
		ContentType: "application/json",
		Payload:     []byte(`{"id":1,"event":"created"}`),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Marshal(env)
	}
}

// BenchmarkMarshalLarge measures allocations for a large payload (100KB).
func BenchmarkMarshalLarge(b *testing.B) {
	payload := make([]byte, 100*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	env := &Envelope{
		Topic:   "large-topic",
		Payload: payload,
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Marshal(env)
	}
}

// BenchmarkMarshalWithTraceID measures allocations with tracing fields.
func BenchmarkMarshalWithTraceID(b *testing.B) {
	env := &Envelope{
		Topic:   "traced-topic",
		Payload: []byte("trace-payload"),
	}
	copy(env.TraceID[:], []byte("trace-id-12345"))
	copy(env.SpanID[:], []byte("span-12"))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Marshal(env)
	}
}

// BenchmarkUnmarshalSmall measures Unmarshal throughput.
func BenchmarkUnmarshalSmall(b *testing.B) {
	env := &Envelope{
		Topic:   "test-topic",
		Payload: []byte("hello"),
	}
	data, _ := Marshal(env)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Unmarshal(data)
	}
}

// BenchmarkUnmarshalWithHeaders measures Unmarshal throughput with headers.
func BenchmarkUnmarshalWithHeaders(b *testing.B) {
	env := &Envelope{
		Topic:       "test-topic",
		RoutingKey:  "orders.created",
		Headers:     map[string][]byte{"x-source": []byte("grpc"), "x-trace-id": []byte("abc-123")},
		ContentType: "application/json",
		Payload:     []byte(`{"id":1,"event":"created"}`),
	}
	data, _ := Marshal(env)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = Unmarshal(data)
	}
}

// BenchmarkMarshalUnmarshalRoundTrip measures full round-trip overhead.
func BenchmarkMarshalUnmarshalRoundTrip(b *testing.B) {
	env := &Envelope{
		Topic:       "roundtrip-topic",
		RoutingKey:  "test.key",
		Headers:     map[string][]byte{"x-source": []byte("test")},
		ContentType: "application/json",
		Payload:     []byte(`{"msg":"hello world"}`),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, _ := Marshal(env)
		_, _ = Unmarshal(data)
	}
}

// BenchmarkMarshalHeaders measures header-only marshal.
func BenchmarkMarshalHeaders(b *testing.B) {
	headers := map[string][]byte{
		"content-type":  []byte("application/json"),
		"x-request-id":  []byte("req-12345"),
		"x-trace-id":    []byte("trace-abc"),
		"x-user-id":     []byte("user-999"),
		"x-correlation": []byte("corr-xyz"),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = marshalHeaders(headers)
	}
}
