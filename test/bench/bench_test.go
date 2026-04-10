package bench

import (
	"testing"

	"github.com/chimeramq/chimera/internal/message"
)

// BenchmarkEnvelopeEstimateSize benchmarks the size estimation of envelopes.
func BenchmarkEnvelopeEstimateSize(b *testing.B) {
	env := &message.Envelope{
		Topic:      "bench-topic",
		RoutingKey: "bench-key",
		Payload:    make([]byte, 1024),
		Headers:    map[string][]byte{"h1": []byte("v1")},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = env.EstimateSize()
	}
}

// BenchmarkEnvelopeCreate benchmarks creating envelopes.
func BenchmarkEnvelopeCreate(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = &message.Envelope{
			Topic:      "bench-topic",
			RoutingKey: "key",
			Payload:    make([]byte, 256),
		}
	}
}

// BenchmarkEnvelopeCreateParallel benchmarks parallel envelope creation.
func BenchmarkEnvelopeCreateParallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = &message.Envelope{
				Topic:   "bench-topic",
				Payload: make([]byte, 256),
			}
		}
	})
}

// BenchmarkEnvelopeLargePayload benchmarks with large payloads.
func BenchmarkEnvelopeLargePayload(b *testing.B) {
	payload := make([]byte, 64*1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = &message.Envelope{
			Topic:   "bench-large",
			Payload: payload,
		}
	}
}

// BenchmarkEnvelopeWithHeaders benchmarks envelopes with multiple headers.
func BenchmarkEnvelopeWithHeaders(b *testing.B) {
	headers := map[string][]byte{
		"trace-id":  []byte("abc-123-def-456"),
		"source":    []byte("producer-1"),
		"timestamp": []byte("2024-01-01T00:00:00Z"),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = &message.Envelope{
			Topic:   "bench-headers",
			Headers: headers,
			Payload: make([]byte, 512),
		}
	}
}
