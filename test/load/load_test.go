package load

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/message"
)

// LoadConfig configures a load test.
type LoadConfig struct {
	Producers     int    // Number of concurrent producer goroutines
	MessagesPer   int    // Messages per producer
	PayloadSize   int    // Bytes per message payload
	Partitions    uint32 // Topic partitions
	Mode          broker.TopicMode
	BatchSize     int // Messages per batch before reporting
	ConsumerCount int // Queue consumers (queue mode only)
	Prefetch      int // Consumer prefetch (queue mode only)
}

// LoadResult holds load test metrics.
type LoadResult struct {
	TotalMessages int64
	Duration      time.Duration
	Throughput    float64 // messages/sec
	LatencyMin    time.Duration
	LatencyMax    time.Duration
	LatencyAvg    time.Duration
	LatencyP99    time.Duration
	Errors        int64
}

func (r *LoadResult) String() string {
	return fmt.Sprintf(
		"msgs=%d dur=%v throughput=%.0f msg/s avg=%v p99=%v errors=%d",
		r.TotalMessages, r.Duration.Round(time.Millisecond),
		r.Throughput, r.LatencyAvg, r.LatencyP99, r.Errors,
	)
}

// RunLoadTest executes a producer load test against an in-process broker.
func RunLoadTest(t *testing.T, cfg LoadConfig) *LoadResult {
	t.Helper()

	// Create broker
	tmpDir := t.TempDir()
	port := 29000 + int(atomic.AddInt64(&portSeq, 1)%1000)
	adminPort := port + 1000

	brokerCfg := &broker.Config{
		Node: broker.NodeConfig{
			ID:      1,
			Name:    "load-test",
			DataDir: tmpDir,
		},
		Listener: broker.ListenerConfig{
			Bind:           "127.0.0.1",
			Port:           port,
			AdminPort:      adminPort,
			MaxConnections: 1000,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{
				SegmentSize:  64 * 1024 * 1024,
				SyncMode:     "os",
				SyncInterval: "1s",
			},
			WAL: broker.WALConfig{
				MaxSize:      64 * 1024 * 1024,
				SyncMode:     "os",
				SyncInterval: "1s",
			},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{
				Partitions: cfg.Partitions,
				Mode:       "unified",
			},
		},
		Logging: broker.LoggingConfig{
			Level:  "error",
			Format: "text",
			Output: "stdout",
		},
	}

	b, err := broker.NewBroker(brokerCfg)
	if err != nil {
		t.Fatalf("create broker: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	defer b.Stop()

	topicName := "load-topic"
	b.Topics().CreateTopic(broker.TopicConfig{
		Name:       topicName,
		Mode:       cfg.Mode,
		Partitions: cfg.Partitions,
	})

	// Add queue consumers if queue mode
	if cfg.Mode == broker.ModeQueue || cfg.Mode == broker.ModeUnified {
		qe := b.QueueEngine()
		for i := 0; i < cfg.ConsumerCount; i++ {
			c := &queue.QueueConsumer{
				ID:       fmt.Sprintf("c-%d", i),
				Prefetch: cfg.Prefetch,
				InFlight: make(map[uint64]time.Time),
			}
			qe.AddConsumer(topicName, c)
		}
	}

	// Warm up
	payload := make([]byte, cfg.PayloadSize)
	for i := range payload {
		payload[i] = byte('A' + i%26)
	}

	// Run producers
	var (
		totalErrors   int64
		totalMessages int64
		latencies     []time.Duration
		latMu         sync.Mutex
		wg            sync.WaitGroup
	)

	start := time.Now()

	for p := 0; p < cfg.Producers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for i := 0; i < cfg.MessagesPer; i++ {
				msgStart := time.Now()
				env := &message.Envelope{
					Topic:       topicName,
					Payload:     payload,
					SourceProto: message.ProtoHTTP,
				}
				_, err := b.Publish(env)
				lat := time.Since(msgStart)
				if err != nil {
					atomic.AddInt64(&totalErrors, 1)
					continue
				}
				atomic.AddInt64(&totalMessages, 1)
				latMu.Lock()
				latencies = append(latencies, lat)
				latMu.Unlock()
			}
		}(p)
	}

	wg.Wait()
	duration := time.Since(start)

	// Compute results
	result := &LoadResult{
		TotalMessages: totalMessages,
		Duration:      duration,
		Throughput:    float64(totalMessages) / duration.Seconds(),
		Errors:        totalErrors,
	}

	if len(latencies) > 0 {
		// Sort latencies for percentile computation
		result.LatencyMin = minDur(latencies)
		result.LatencyMax = maxDur(latencies)
		result.LatencyAvg = avgDur(latencies)
		result.LatencyP99 = percentileDur(latencies, 99)
	}

	return result
}

// Benchmark helpers

func minDur(d []time.Duration) time.Duration {
	m := d[0]
	for _, v := range d[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func maxDur(d []time.Duration) time.Duration {
	m := d[0]
	for _, v := range d[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

func avgDur(d []time.Duration) time.Duration {
	var sum int64
	for _, v := range d {
		sum += int64(v)
	}
	return time.Duration(sum / int64(len(d)))
}

func percentileDur(d []time.Duration, pct int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	// Simple sort-based percentile
	sorted := make([]time.Duration, len(d))
	copy(sorted, d)
	// Insertion sort for small slices
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	idx := (pct * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

var portSeq int64
