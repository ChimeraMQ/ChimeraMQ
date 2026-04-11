package load

import (
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
)

func TestLoadSingleProducer(t *testing.T) {
	result := RunLoadTest(t, LoadConfig{
		Producers:   1,
		MessagesPer: 1000,
		PayloadSize: 256,
		Partitions:  4,
		Mode:        broker.ModeStream,
	})

	t.Logf("Single producer: %s", result)
	if result.Errors > 0 {
		t.Errorf("%d errors during load test", result.Errors)
	}
	if result.TotalMessages < 1000 {
		t.Errorf("only %d messages published, want 1000", result.TotalMessages)
	}
	if result.Throughput < 1000 {
		t.Errorf("throughput %.0f msg/s too low", result.Throughput)
	}
}

func TestLoadMultiProducer(t *testing.T) {
	result := RunLoadTest(t, LoadConfig{
		Producers:   4,
		MessagesPer: 500,
		PayloadSize: 256,
		Partitions:  4,
		Mode:        broker.ModeStream,
	})

	t.Logf("Multi producer (4x500): %s", result)
	if result.Errors > 0 {
		t.Errorf("%d errors during load test", result.Errors)
	}
	if result.TotalMessages < 2000 {
		t.Errorf("only %d messages published, want 2000", result.TotalMessages)
	}
}

func TestLoadLargePayload(t *testing.T) {
	result := RunLoadTest(t, LoadConfig{
		Producers:   2,
		MessagesPer: 500,
		PayloadSize: 4096,
		Partitions:  4,
		Mode:        broker.ModeStream,
	})

	t.Logf("Large payload (4KB): %s", result)
	if result.Errors > 0 {
		t.Errorf("%d errors during load test", result.Errors)
	}
}

func TestLoadQueueMode(t *testing.T) {
	result := RunLoadTest(t, LoadConfig{
		Producers:     2,
		MessagesPer:   500,
		PayloadSize:   256,
		Partitions:    1,
		Mode:          broker.ModeQueue,
		ConsumerCount: 2,
		Prefetch:      100,
	})

	t.Logf("Queue mode (2 consumers): %s", result)
	if result.Errors > 0 {
		t.Errorf("%d errors during load test", result.Errors)
	}
}

func TestLoadUnifiedMode(t *testing.T) {
	result := RunLoadTest(t, LoadConfig{
		Producers:     2,
		MessagesPer:   500,
		PayloadSize:   256,
		Partitions:    4,
		Mode:          broker.ModeUnified,
		ConsumerCount: 2,
		Prefetch:      100,
	})

	t.Logf("Unified mode (2 consumers): %s", result)
	if result.Errors > 0 {
		t.Errorf("%d errors during load test", result.Errors)
	}
}

func TestLoadHighConcurrency(t *testing.T) {
	result := RunLoadTest(t, LoadConfig{
		Producers:   8,
		MessagesPer: 500,
		PayloadSize: 128,
		Partitions:  8,
		Mode:        broker.ModeStream,
	})

	t.Logf("High concurrency (8x500): %s", result)
	if result.Errors > 0 {
		t.Errorf("%d errors during load test", result.Errors)
	}
	if result.Throughput < 1000 {
		t.Errorf("throughput %.0f msg/s too low", result.Throughput)
	}
}
