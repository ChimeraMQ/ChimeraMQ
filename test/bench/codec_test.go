package bench

import (
	"testing"

	"github.com/chimeramq/chimera/internal/message"
)

// BenchmarkCodecEncode benchmarks message encoding.
func BenchmarkCodecEncode(b *testing.B) {
	env := &message.Envelope{
		Topic:       "bench-codec",
		RoutingKey:  "key-123",
		Payload:     make([]byte, 1024),
		ContentType: "application/json",
	}
	// Fill payload
	for i := range env.Payload {
		env.Payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = message.Marshal(env)
	}
}

// BenchmarkCodecDecode benchmarks message decoding.
func BenchmarkCodecDecode(b *testing.B) {
	env := &message.Envelope{
		Topic:      "bench-codec",
		RoutingKey: "key-123",
		Payload:    make([]byte, 1024),
	}
	data, _ := message.Marshal(env)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = message.Unmarshal(data)
	}
}

// BenchmarkCodecRoundTrip benchmarks encode+decode cycle.
func BenchmarkCodecRoundTrip(b *testing.B) {
	env := &message.Envelope{
		Topic:      "bench-rt",
		RoutingKey: "key",
		Payload:    make([]byte, 512),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, _ := message.Marshal(env)
		_, _ = message.Unmarshal(data)
	}
}

// BenchmarkCodecSmallPayload benchmarks with small payloads.
func BenchmarkCodecSmallPayload(b *testing.B) {
	env := &message.Envelope{
		Topic:   "bench-small",
		Payload: []byte("hello"),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = message.Marshal(env)
	}
}

// BenchmarkCodecLargePayload benchmarks with 64KB payloads.
func BenchmarkCodecLargePayload(b *testing.B) {
	env := &message.Envelope{
		Topic:   "bench-large-codec",
		Payload: make([]byte, 64*1024),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, _ := message.Marshal(env)
		_, _ = message.Unmarshal(data)
	}
}

// BenchmarkCodecWithHeaders benchmarks encoding with headers.
func BenchmarkCodecWithHeaders(b *testing.B) {
	env := &message.Envelope{
		Topic:   "bench-hdr-codec",
		Payload: make([]byte, 256),
		Headers: map[string][]byte{
			"trace-id": []byte("abc123"),
			"source":   []byte("bench"),
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = message.Marshal(env)
	}
}
