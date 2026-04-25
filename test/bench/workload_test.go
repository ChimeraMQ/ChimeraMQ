package bench

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/stream"
	"github.com/chimeramq/chimera/internal/message"
)

var benchPortCounter atomic.Int64

// nextPort returns a unique port for each benchmark to avoid conflicts.
func nextPort() int {
	return int(20000 + benchPortCounter.Add(1))
}

// BenchmarkMixedPublishOnly benchmarks pure publish with no consumers.
func BenchmarkMixedPublishOnly(b *testing.B) {
	benchmarkMixed(b, 1, 0)
}

// BenchmarkMixedPublishConsumeBalanced benchmarks concurrent publish and consume.
func BenchmarkMixedPublishConsumeBalanced(b *testing.B) {
	benchmarkMixed(b, 2, 2)
}

// BenchmarkMixedConsumerHeavy benchmarks more consumers than producers.
func BenchmarkMixedConsumerHeavy(b *testing.B) {
	benchmarkMixed(b, 1, 4)
}

func benchmarkMixed(b *testing.B, publishers, consumers int) {
	dir, err := os.MkdirTemp("", fmt.Sprintf("bench-mixed-p%dc%d-*", publishers, consumers))
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	port := nextPort()
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "bench", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: port, AdminPort: port + 1},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "warn", Format: "text", Output: "stdout"},
	}

	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		b.Fatal(err)
	}
	bkr.Start()
	defer bkr.Stop()

	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name:       "bench-mixed",
		Mode:       broker.ModeUnified,
		Partitions: 4,
	})

	// Get partition for reading
	part, err := bkr.Storage().GetOrCreatePartition("bench-mixed", 0)
	if err != nil {
		b.Fatal(err)
	}

	var published, consumed atomic.Int64
	stopCh := make(chan struct{})
	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup

	// Publishers
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			count := b.N / publishers
			for i := 0; i < count; i++ {
				select {
				case <-stopCh:
					return
				default:
					env := &message.Envelope{
						Topic:       "bench-mixed",
						Payload:     []byte(fmt.Sprintf("p%d-msg-%d", pid, i)),
						ContentType: "application/octet-stream",
						SourceProto: message.ProtoHTTP,
					}
					if _, err := bkr.Publish(env); err != nil {
						b.Error(err)
						return
					}
					published.Add(1)
				}
			}
		}(p)
	}

	// Consumers: read from partition directly
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			count := b.N / consumers
			var offset uint64
			for i := 0; i < count; i++ {
				select {
				case <-stopCh:
					return
				default:
					// Small sleep to let publishers produce messages
					time.Sleep(100 * time.Microsecond)
					batch, err := part.ReadRange(offset, offset+10, 10)
					if err == nil && len(batch) > 0 {
						consumed.Add(int64(len(batch)))
						offset += uint64(len(batch))
					}
				}
			}
		}(c)
	}

	wg.Wait()
	b.ReportMetric(float64(published.Load()), "published")
	b.ReportMetric(float64(consumed.Load()), "consumed")
}

// BenchmarkConsumerGroupRebalance benchmarks consumer group join/leave under load.
func BenchmarkConsumerGroupRebalance(b *testing.B) {
	b.ReportAllocs()

	dir, err := os.MkdirTemp("", "bench-rebalance-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := stream.NewOffsetStore(dir)
	if err != nil {
		b.Fatal(err)
	}

	cg := stream.NewConsumerGroup("bench-group", "bench-topic", 8, stream.StrategyRange, store)
	defer cg.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		memberID := fmt.Sprintf("member-%d", i%16)
		cg.Join(memberID)
		cg.Heartbeat(memberID)
		if i%4 == 3 {
			cg.Leave(memberID)
		}
	}
}

// BenchmarkConsumerGroupRebalanceUnderLoad benchmarks consumer group
// joins/leaves while messages are being published concurrently.
func BenchmarkConsumerGroupRebalanceUnderLoad(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-rebalance-load-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	port := nextPort()
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "bench", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: port, AdminPort: port + 1},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 8}},
		Logging:  broker.LoggingConfig{Level: "warn", Format: "text", Output: "stdout"},
	}

	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		b.Fatal(err)
	}
	bkr.Start()
	defer bkr.Stop()

	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name:       "bench-rebalance",
		Mode:       broker.ModeStream,
		Partitions: 8,
	})

	var published, joins, leaves atomic.Int64
	stopCh := make(chan struct{})

	b.ResetTimer()
	b.ReportAllocs()

	// Background publisher
	var pubWg sync.WaitGroup
	pubWg.Add(1)
	go func() {
		defer pubWg.Done()
		i := 0
		for {
			select {
			case <-stopCh:
				return
			default:
				bkr.Publish(&message.Envelope{
					Topic:       "bench-rebalance",
					Payload:     []byte(fmt.Sprintf("msg-%d", i)),
					SourceProto: message.ProtoHTTP,
				})
				published.Add(1)
				i++
			}
		}
	}()

	// Consumer churn: create groups, join, heartbeat, leave, repeat
	var churnWg sync.WaitGroup
	for c := 0; c < 4; c++ {
		churnWg.Add(1)
		go func(consumerID int) {
			defer churnWg.Done()
			memberID := fmt.Sprintf("churn-%d", consumerID)
			for i := 0; i < b.N/4; i++ {
				select {
				case <-stopCh:
					return
				default:
					groupDir, _ := os.MkdirTemp("", "bench-cg-*")
					store, _ := stream.NewOffsetStore(groupDir)
					cg := stream.NewConsumerGroup("bench-rebalance-group", "bench-rebalance", 8, stream.StrategyRange, store)
					cg.Join(memberID)
					joins.Add(1)
					cg.Heartbeat(memberID)
					cg.Leave(memberID)
					cg.Stop()
					os.RemoveAll(groupDir)
					leaves.Add(1)
				}
			}
		}(c)
	}

	churnWg.Wait()
	close(stopCh)
	pubWg.Wait()

	b.ReportMetric(float64(published.Load()), "published")
	b.ReportMetric(float64(joins.Load()), "joins")
	b.ReportMetric(float64(leaves.Load()), "leaves")
}

// BenchmarkTierMigrationHotOnly benchmarks publish with only hot storage active.
func BenchmarkTierMigrationHotOnly(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-tier-hotonly-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	port := nextPort()
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "bench", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: port, AdminPort: port + 1},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1 * 1024 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 1}},
		Logging:  broker.LoggingConfig{Level: "warn", Format: "text", Output: "stdout"},
	}

	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		b.Fatal(err)
	}
	bkr.Start()
	defer bkr.Stop()

	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name:       "bench-tier-hot",
		Mode:       broker.ModeUnified,
		Partitions: 1,
	})

	payload := make([]byte, 256)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := bkr.Publish(&message.Envelope{
			Topic:       "bench-tier-hot",
			Payload:     payload,
			SourceProto: message.ProtoHTTP,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPublishLatency measures per-message publish latency distribution.
func BenchmarkPublishLatency(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-latency-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	port := nextPort()
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "bench", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: port, AdminPort: port + 1},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "warn", Format: "text", Output: "stdout"},
	}

	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		b.Fatal(err)
	}
	bkr.Start()
	defer bkr.Stop()

	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name:       "bench-latency",
		Mode:       broker.ModeUnified,
		Partitions: 4,
	})

	payload := make([]byte, 256)

	b.ResetTimer()
	b.ReportAllocs()

	var minNs, maxNs, totalNs int64
	first := true

	for i := 0; i < b.N; i++ {
		start := time.Now()
		_, err := bkr.Publish(&message.Envelope{
			Topic:       "bench-latency",
			Payload:     payload,
			SourceProto: message.ProtoHTTP,
		})
		elapsed := time.Since(start).Nanoseconds()
		if err != nil {
			b.Fatal(err)
		}

		if first {
			minNs = elapsed
			first = false
		}
		if elapsed < minNs {
			minNs = elapsed
		}
		if elapsed > maxNs {
			maxNs = elapsed
		}
		totalNs += elapsed
	}

	avgNs := totalNs / int64(b.N)
	b.ReportMetric(float64(avgNs), "ns/op-avg")
	b.ReportMetric(float64(minNs), "ns/op-min")
	b.ReportMetric(float64(maxNs), "ns/op-max")
}

// BenchmarkConsumerGroupOffsets benchmarks offset commit performance under load.
func BenchmarkConsumerGroupOffsets(b *testing.B) {
	b.ReportAllocs()

	dir, err := os.MkdirTemp("", "bench-offsets-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := stream.NewOffsetStore(dir)
	if err != nil {
		b.Fatal(err)
	}

	cg := stream.NewConsumerGroup("bench-offset-group", "bench-offset-topic", 8, stream.StrategyRange, store)
	defer cg.Stop()

	cg.Join("offset-member")

	b.ResetTimer()

	var committed atomic.Int64
	for i := 0; i < b.N; i++ {
		partition := uint32(i % 8)
		offset := uint64(i)
		if err := cg.CommitOffset(partition, offset); err != nil {
			b.Fatal(err)
		}
		committed.Add(1)
	}

	b.ReportMetric(float64(committed.Load()), "offsets_committed")
}
