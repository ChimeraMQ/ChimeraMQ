package bench

import (
	"os"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
)

func BenchmarkE2EPublish(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-e2e-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "bench", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 19999, AdminPort: 19899},
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
		Name:       "bench-topic",
		Mode:       broker.ModeUnified,
		Partitions: 4,
	})

	env := &message.Envelope{
		Topic:       "bench-topic",
		Payload:     make([]byte, 256),
		ContentType: "application/octet-stream",
		SourceProto: message.ProtoHTTP,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := bkr.Publish(env); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkE2EPublishStream(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-e2e-stream-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "bench", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 19998, AdminPort: 19898},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 1}},
		Logging:  broker.LoggingConfig{Level: "warn", Format: "text", Output: "stdout"},
	}

	bkr, _ := broker.NewBroker(cfg)
	bkr.Start()
	defer bkr.Stop()

	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name:       "bench-stream",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	payload := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		env := &message.Envelope{
			Topic:       "bench-stream",
			Payload:     payload,
			ContentType: "application/octet-stream",
			SourceProto: message.ProtoHTTP,
		}
		if _, err := bkr.Publish(env); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkE2EPublishQueue(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-e2e-queue-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "bench", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 19997, AdminPort: 19897},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 1}},
		Logging:  broker.LoggingConfig{Level: "warn", Format: "text", Output: "stdout"},
	}

	bkr, _ := broker.NewBroker(cfg)
	bkr.Start()
	defer bkr.Stop()

	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name:       "bench-queue",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	// No consumers — measures pure publish + queue dispatch attempt overhead

	payload := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		env := &message.Envelope{
			Topic:       "bench-queue",
			Payload:     payload,
			ContentType: "application/octet-stream",
			SourceProto: message.ProtoHTTP,
		}
		if _, err := bkr.Publish(env); err != nil {
			b.Fatal(err)
		}
	}
}
