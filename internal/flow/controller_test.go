package flow

import (
	"testing"
	"time"
)

func TestNewController(t *testing.T) {
	c := NewController(Config{Enabled: true})
	if !c.Enabled() {
		t.Error("should be enabled")
	}
}

func TestControllerDisabled(t *testing.T) {
	c := NewController(Config{})
	if c.Enabled() {
		t.Error("should be disabled")
	}
	// All operations should pass through
	if !c.AllowPublish("topic") {
		t.Error("should allow when disabled")
	}
	if c.IsOverHighWatermark() {
		t.Error("should not be over watermark when disabled")
	}
}

func TestMemoryBackpressure(t *testing.T) {
	c := NewController(Config{
		Enabled:        true,
		MaxMemoryBytes: 1000,
		HighWatermark:  0.8,
		LowWatermark:   0.5,
	})

	if c.IsOverHighWatermark() {
		t.Error("should not be over watermark initially")
	}

	c.Alloc(900)
	if !c.IsOverHighWatermark() {
		t.Error("should be over high watermark at 90%")
	}

	c.Free(500)
	if c.IsOverHighWatermark() {
		t.Error("should not be over high watermark after free")
	}
}

func TestMemoryPressure(t *testing.T) {
	c := NewController(Config{
		Enabled:        true,
		MaxMemoryBytes: 1000,
	})
	if c.MemoryPressure() != 0 {
		t.Errorf("pressure = %f, want 0", c.MemoryPressure())
	}
	c.Alloc(500)
	if p := c.MemoryPressure(); p != 0.5 {
		t.Errorf("pressure = %f, want 0.5", p)
	}
}

func TestMemoryUsage(t *testing.T) {
	c := NewController(Config{Enabled: true})
	c.Alloc(100)
	c.Alloc(200)
	if c.MemoryUsage() != 300 {
		t.Errorf("usage = %d, want 300", c.MemoryUsage())
	}
	c.Free(100)
	if c.MemoryUsage() != 200 {
		t.Errorf("usage = %d, want 200", c.MemoryUsage())
	}
}

func TestAllowPublishNoLimit(t *testing.T) {
	c := NewController(Config{Enabled: true})
	for i := 0; i < 100; i++ {
		if !c.AllowPublish("topic") {
			t.Error("should allow with no rate limit")
		}
	}
}

func TestAllowPublishGlobalLimit(t *testing.T) {
	c := NewController(Config{
		Enabled:         true,
		GlobalRateLimit: 5,
	})
	allowed := 0
	for i := 0; i < 10; i++ {
		if c.AllowPublish("topic") {
			allowed++
		}
	}
	if allowed != 5 {
		t.Errorf("allowed %d, want 5", allowed)
	}
}

func TestTopicRateLimit(t *testing.T) {
	c := NewController(Config{Enabled: true})
	c.SetTopicRateLimit("limited", 3)

	allowed := 0
	for i := 0; i < 5; i++ {
		if c.AllowPublish("limited") {
			allowed++
		}
	}
	if allowed != 3 {
		t.Errorf("allowed %d, want 3", allowed)
	}

	// Unrelated topic should be unlimited
	if !c.AllowPublish("unlimited") {
		t.Error("unlimited topic should allow")
	}
}

func TestRemoveTopicRateLimit(t *testing.T) {
	c := NewController(Config{Enabled: true})
	c.SetTopicRateLimit("limited", 1)
	c.AllowPublish("limited")
	c.RemoveTopicRateLimit("limited")

	if !c.AllowPublish("limited") {
		t.Error("should allow after removing limit")
	}
}

func TestConnectionTracking(t *testing.T) {
	c := NewController(Config{Enabled: true})
	if c.ConnectionCount() != 0 {
		t.Error("should start at 0")
	}
	c.Connect()
	c.Connect()
	if c.ConnectionCount() != 2 {
		t.Errorf("count = %d, want 2", c.ConnectionCount())
	}
	c.Disconnect()
	if c.ConnectionCount() != 1 {
		t.Errorf("count = %d, want 1", c.ConnectionCount())
	}
}

func TestConnectionLimit(t *testing.T) {
	c := NewController(Config{
		Enabled:        true,
		MaxConnections: 2,
	})
	if !c.Connect() {
		t.Error("first connect should succeed")
	}
	if !c.Connect() {
		t.Error("second connect should succeed")
	}
	if c.Connect() {
		t.Error("third connect should fail (limit)")
	}
	c.Disconnect()
	if !c.Connect() {
		t.Error("connect after disconnect should succeed")
	}
}

func TestConnectionLimitDisabled(t *testing.T) {
	c := NewController(Config{Enabled: true, MaxConnections: 0})
	for i := 0; i < 100; i++ {
		if !c.Connect() {
			t.Error("should always connect with no limit")
		}
	}
}

func TestSlowConsumerDetection(t *testing.T) {
	c := NewController(Config{
		Enabled:         true,
		SlowConsumerTTL: 50 * time.Millisecond,
		MaxSlowTicks:    2,
	})

	c.RecordDelivery("c1")
	if c.IsSlowConsumer("c1") {
		t.Error("should not be slow initially")
	}

	// Wait for TTL to expire
	time.Sleep(80 * time.Millisecond)

	// Tick 1: should increment ticks to 1 but not evict (needs 2)
	evicted := c.TickSlowConsumers()
	if len(evicted) != 0 {
		t.Errorf("first tick should not evict, got %d", len(evicted))
	}
	// ticks=1, maxSlowTicks=2 → not slow yet
	if c.IsSlowConsumer("c1") {
		t.Error("should not be slow after 1 tick with maxSlowTicks=2")
	}

	// Tick 2: should evict (ticks reaches maxSlowTicks=2)
	time.Sleep(60 * time.Millisecond)
	evicted = c.TickSlowConsumers()
	if len(evicted) != 1 || evicted[0] != "c1" {
		t.Errorf("expected c1 eviction, got %v", evicted)
	}
}

func TestSlowConsumerReset(t *testing.T) {
	c := NewController(Config{
		Enabled:         true,
		SlowConsumerTTL: 50 * time.Millisecond,
		MaxSlowTicks:    2,
	})

	c.RecordDelivery("c1")
	time.Sleep(80 * time.Millisecond)
	c.TickSlowConsumers() // tick 1

	// Record delivery resets ticks
	c.RecordDelivery("c1")
	if c.IsSlowConsumer("c1") {
		t.Error("should not be slow after delivery")
	}
}

func TestRemoveConsumer(t *testing.T) {
	c := NewController(Config{Enabled: true})
	c.RecordDelivery("c1")
	c.RemoveConsumer("c1")
	if c.IsSlowConsumer("c1") {
		t.Error("should not be slow after removal")
	}
}

func TestWatermarkDefaults(t *testing.T) {
	c := NewController(Config{
		Enabled:        true,
		MaxMemoryBytes: 1000,
		HighWatermark:  0,
		LowWatermark:   0,
	})
	// Should use defaults
	c.Alloc(860)
	if !c.IsOverHighWatermark() {
		t.Error("default high watermark 0.85, 86% should trigger")
	}
}
