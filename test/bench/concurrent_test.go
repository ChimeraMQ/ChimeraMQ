package bench

import (
	"os"
	"sync"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
)

// BenchmarkE2EPublishParallel benchmarks concurrent publish from multiple goroutines.
func BenchmarkE2EPublishParallel(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-par-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "bench", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 19996, AdminPort: 19896},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "warn", Format: "text", Output: "stdout"},
	}

	bkr, _ := broker.NewBroker(cfg)
	bkr.Start()
	defer bkr.Stop()

	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name:       "bench-par-topic",
		Mode:       broker.ModeUnified,
		Partitions: 4,
	})

	payload := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			env := &message.Envelope{
				Topic:       "bench-par-topic",
				Payload:     payload,
				ContentType: "application/octet-stream",
				SourceProto: message.ProtoHTTP,
			}
			if _, err := bkr.Publish(env); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkE2EPublishMultiTopic benchmarks publish across multiple topics.
func BenchmarkE2EPublishMultiTopic(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-multi-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "bench", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 19995, AdminPort: 19895},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "warn", Format: "text", Output: "stdout"},
	}

	bkr, _ := broker.NewBroker(cfg)
	bkr.Start()
	defer bkr.Stop()

	topics := []string{"orders", "payments", "notifications", "analytics"}
	for _, t := range topics {
		bkr.Topics().CreateTopic(broker.TopicConfig{
			Name:       t,
			Mode:       broker.ModeUnified,
			Partitions: 4,
		})
	}

	payload := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		topic := topics[i%len(topics)]
		env := &message.Envelope{
			Topic:       topic,
			Payload:     payload,
			ContentType: "application/octet-stream",
			SourceProto: message.ProtoHTTP,
		}
		if _, err := bkr.Publish(env); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkE2EPublishConcurrentMultiTopic benchmarks parallel publish across topics.
func BenchmarkE2EPublishConcurrentMultiTopic(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-cmt-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "bench", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 19994, AdminPort: 19894},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "warn", Format: "text", Output: "stdout"},
	}

	bkr, _ := broker.NewBroker(cfg)
	bkr.Start()
	defer bkr.Stop()

	topics := []string{"events-a", "events-b", "events-c", "events-d"}
	for _, t := range topics {
		bkr.Topics().CreateTopic(broker.TopicConfig{
			Name:       t,
			Mode:       broker.ModeUnified,
			Partitions: 4,
		})
	}

	payload := make([]byte, 256)

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	numGoroutines := 4
	count := b.N / numGoroutines

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(topicIdx int) {
			defer wg.Done()
			topic := topics[topicIdx%len(topics)]
			for i := 0; i < count; i++ {
				env := &message.Envelope{
					Topic:       topic,
					Payload:     payload,
					ContentType: "application/octet-stream",
					SourceProto: message.ProtoHTTP,
				}
				if _, err := bkr.Publish(env); err != nil {
					b.Error(err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// BenchmarkCodecEncodeParallel benchmarks parallel message encoding.
func BenchmarkCodecEncodeParallel(b *testing.B) {
	env := &message.Envelope{
		Topic:       "bench-par-codec",
		RoutingKey:  "key-123",
		Payload:     make([]byte, 1024),
		ContentType: "application/json",
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = message.Marshal(env)
		}
	})
}

// BenchmarkCodecDecodeParallel benchmarks parallel message decoding.
func BenchmarkCodecDecodeParallel(b *testing.B) {
	env := &message.Envelope{
		Topic:      "bench-par-decode",
		RoutingKey: "key-123",
		Payload:    make([]byte, 1024),
	}
	data, _ := message.Marshal(env)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = message.Unmarshal(data)
		}
	})
}
