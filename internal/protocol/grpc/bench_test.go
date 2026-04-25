package grpc

import (
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
	proto "github.com/chimeramq/chimera/internal/protocol/grpc/proto"
)

// BenchmarkEnvelopeToProto measures proto conversion throughput.
func BenchmarkEnvelopeToProto(b *testing.B) {
	env := &message.Envelope{
		Topic:       "bench-topic",
		PartitionID: 0,
		Sequence:    42,
		RoutingKey:  "bench.key",
		Priority:    5,
		Headers:     map[string][]byte{"x-source": []byte("bench")},
		ContentType: "application/json",
		Payload:     []byte(`{"id":1,"event":"test"}`),
		SchemaID:    123,
		Timestamp:   1700000000000000000,
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = envelopeToProto(env)
	}
}

// BenchmarkEnvelopeToProtoLarge measures proto conversion with a large payload.
func BenchmarkEnvelopeToProtoLarge(b *testing.B) {
	payload := make([]byte, 10*1024) // 10KB
	for i := range payload {
		payload[i] = byte(i % 256)
	}
	env := &message.Envelope{
		Topic:   "bench-large",
		Payload: payload,
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = envelopeToProto(env)
	}
}

// BenchmarkTopicModeConversion measures mode conversion overhead.
func BenchmarkTopicModeConversion(b *testing.B) {
	b.Run("queue", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = topicModeFromProto("queue")
			_ = topicModeToProto(broker.ModeQueue)
		}
	})
	b.Run("stream", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = topicModeFromProto("stream")
			_ = topicModeToProto(broker.ModeStream)
		}
	})
	b.Run("unified", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = topicModeFromProto("unified")
			_ = topicModeToProto(broker.ModeUnified)
		}
	})
}

// BenchmarkPublishProtoConversion measures full proto→envelope→proto round-trip.
func BenchmarkPublishProtoConversion(b *testing.B) {
	req := &proto.PublishRequest{
		Topic:       "bench-topic",
		RoutingKey:  "bench.key",
		Priority:    5,
		TtlMs:       60000,
		Headers:     map[string][]byte{"x-source": []byte("bench")},
		ContentType: "application/json",
		Payload:     []byte(`{"id":1,"event":"test"}`),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		env := &message.Envelope{
			Topic:       req.Topic,
			RoutingKey:  req.RoutingKey,
			Priority:    uint8(req.Priority),
			TTL:         req.TtlMs * 1e6,
			DeliverAt:   req.DeliverAtMs * 1e6,
			Headers:     req.Headers,
			ContentType: req.ContentType,
			Payload:     req.Payload,
			SourceProto: message.ProtoGRPC,
		}
		_, _ = message.Marshal(env)
	}
}
