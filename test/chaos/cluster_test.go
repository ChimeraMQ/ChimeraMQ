package chaos

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

// TestChaosConcurrentPublish verifies safe concurrent publishing from
// many goroutines to the same topic.
func TestChaosConcurrentPublish(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "chaos-pub",
		Mode:       broker.ModeStream,
		Partitions: 4,
	})

	var (
		wg      sync.WaitGroup
		errors  atomic.Int64
		success atomic.Int64
	)

	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				env := &message.Envelope{
					Topic:       "chaos-pub",
					Payload:     []byte(fmt.Sprintf("g%d-m%d", id, i)),
					SourceProto: message.ProtoHTTP,
				}
				if _, err := tb.broker.Publish(env); err != nil {
					errors.Add(1)
				} else {
					success.Add(1)
				}
			}
		}(g)
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Errorf("%d publish errors", errors.Load())
	}
	if success.Load() != 1000 {
		t.Errorf("success count = %d, want 1000", success.Load())
	}

	// Verify data integrity
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("chaos-pub", 0, 0, 1000, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) < 100 {
		t.Errorf("only %d messages in partition 0", len(msgs))
	}
}

// TestChaosConcurrentCreateTopic verifies safe concurrent topic creation.
func TestChaosConcurrentCreateTopic(t *testing.T) {
	tb := newTestBroker(t)

	var (
		wg      sync.WaitGroup
		created atomic.Int64
		dupes   atomic.Int64
	)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("concurrent-topic-%d", idx%10) // 10 unique names, 20 goroutines
			err := tb.broker.Topics().CreateTopic(broker.TopicConfig{
				Name:       name,
				Mode:       broker.ModeStream,
				Partitions: 2,
			})
			if err != nil {
				dupes.Add(1)
			} else {
				created.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if created.Load() != 10 {
		t.Errorf("created = %d, want 10", created.Load())
	}
	if dupes.Load() != 10 {
		t.Errorf("dupes = %d, want 10", dupes.Load())
	}
}

// TestChaosConcurrentPubSub verifies concurrent publishing while consuming.
func TestChaosConcurrentPubSub(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "chaos-pubsub",
		Mode:       broker.ModeStream,
		Partitions: 2,
	})

	var wg sync.WaitGroup
	var pubErrors atomic.Int64

	// Start publishers
	for p := 0; p < 4; p++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				env := &message.Envelope{
					Topic:       "chaos-pubsub",
					Payload:     []byte(fmt.Sprintf("p%d-%d", id, i)),
					SourceProto: message.ProtoHTTP,
				}
				if _, err := tb.broker.Publish(env); err != nil {
					pubErrors.Add(1)
				}
			}
		}(p)
	}

	// Start concurrent fetchers
	var fetchErrors atomic.Int64
	for f := 0; f < 2; f++ {
		wg.Add(1)
		go func(partition uint32) {
			defer wg.Done()
			se := tb.broker.StreamEngine()
			for attempt := 0; attempt < 10; attempt++ {
				msgs, _, err := se.Fetch("chaos-pubsub", partition, 0, 100, 0)
				if err != nil {
					fetchErrors.Add(1)
				}
				_ = msgs
				time.Sleep(10 * time.Millisecond)
			}
		}(uint32(f))
	}

	wg.Wait()

	if pubErrors.Load() > 0 {
		t.Errorf("%d publish errors", pubErrors.Load())
	}
}

// TestChaosRapidCreateDelete tests rapid topic creation and deletion.
func TestChaosRapidCreateDelete(t *testing.T) {
	tb := newTestBroker(t)

	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("rapid-%d", i)
		tb.broker.Topics().CreateTopic(broker.TopicConfig{
			Name:       name,
			Mode:       broker.ModeStream,
			Partitions: 1,
		})
		tb.broker.Topics().DeleteTopic(name)
	}

	// Should be able to create again
	err := tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "rapid-0",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})
	if err != nil {
		t.Errorf("recreate after delete: %v", err)
	}
}

// TestChaosQueueConcurrentAckNack tests concurrent ack/nack operations.
func TestChaosQueueConcurrentAckNack(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "chaos-ack",
		Mode:       broker.ModeQueue,
		Partitions: 1,
	})

	qe := tb.broker.QueueEngine()

	// Add 5 consumers
	for i := 0; i < 5; i++ {
		c := &queue.QueueConsumer{
			ID:       fmt.Sprintf("c-%d", i),
			Prefetch: 100,
			InFlight: make(map[uint64]time.Time),
		}
		qe.AddConsumer("chaos-ack", c)
	}

	// Publish messages
	var offsets []uint64
	for i := 0; i < 20; i++ {
		env := &message.Envelope{
			Topic:       "chaos-ack",
			Payload:     []byte("msg"),
			SourceProto: message.ProtoHTTP,
		}
		off, _ := tb.broker.Publish(env)
		offsets = append(offsets, off)
	}

	// Concurrently ack/nack
	var wg sync.WaitGroup
	for i, off := range offsets {
		wg.Add(1)
		go func(idx int, offset uint64) {
			defer wg.Done()
			if idx%2 == 0 {
				qe.HandleAck("chaos-ack", offset)
			} else {
				qe.HandleNack("chaos-ack", offset)
			}
		}(i, off)
	}

	wg.Wait()
	// No panics = success
}

// TestChaosMixedModes publishes to both stream and queue modes concurrently.
func TestChaosMixedModes(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "chaos-mixed",
		Mode:       broker.ModeUnified,
		Partitions: 2,
	})

	var wg sync.WaitGroup
	var errors atomic.Int64

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			env := &message.Envelope{
				Topic:       "chaos-mixed",
				Payload:     []byte(fmt.Sprintf("msg-%d", idx)),
				SourceProto: message.ProtoHTTP,
			}
			if _, err := tb.broker.Publish(env); err != nil {
				errors.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if errors.Load() > 0 {
		t.Errorf("%d errors during mixed-mode chaos test", errors.Load())
	}

	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("chaos-mixed", 0, 0, 200, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) < 50 {
		t.Errorf("expected >= 50 messages, got %d", len(msgs))
	}
}
