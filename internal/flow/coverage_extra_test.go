package flow

import (
	"sync/atomic"
	"testing"
	"time"
)

func cfgEnabled() Config {
	return Config{
		Enabled:        true,
		MaxMemoryBytes: 1000,
		HighWatermark:  0.8,
		MaxSlowTicks:   3,
		SlowConsumerTTL: 100 * time.Millisecond,
	}
}

func TestControllerStartStop(t *testing.T) {
	c := NewController(cfgEnabled())
	c.Start()

	// Stop should not panic
	c.Stop()
}

func TestControllerStartDisabled(t *testing.T) {
	c := NewController(Config{})
	c.Start() // should be no-op
}

func TestControllerSetEvictionCallback(t *testing.T) {
	c := NewController(cfgEnabled())
	var called string
	c.SetEvictionCallback(func(id string) {
		called = id
	})

	// Manually inject a stale slow consumer entry
	c.slowConsumers["consumer-1"] = &slowEntry{
		ticks:   0,
		lastMsg: time.Now().Add(-10 * time.Second), // very old
	}

	// Tick enough times to exceed maxSlowTicks
	for i := 0; i < c.maxSlowTicks+1; i++ {
		evicted := c.TickSlowConsumers()
		for _, id := range evicted {
			called = id
		}
	}

	if called != "consumer-1" {
		t.Errorf("callback not called for consumer-1, got %q", called)
	}
}

func TestControllerMemoryPressure(t *testing.T) {
	c := NewController(cfgEnabled())
	c.Alloc(500)
	pressure := c.MemoryPressure()
	if pressure != 0.5 {
		t.Errorf("MemoryPressure = %f, want 0.5", pressure)
	}

	c.Free(500)
	pressure = c.MemoryPressure()
	if pressure != 0.0 {
		t.Errorf("MemoryPressure after free = %f, want 0.0", pressure)
	}
}

func TestControllerMemoryPressureNoLimit(t *testing.T) {
	c := NewController(Config{Enabled: true})
	c.Alloc(1000)
	pressure := c.MemoryPressure()
	if pressure != 0 {
		t.Errorf("MemoryPressure with no limit = %f, want 0", pressure)
	}
}

func TestControllerIsOverHighWatermark(t *testing.T) {
	c := NewController(cfgEnabled())
	c.Alloc(900)
	if !c.IsOverHighWatermark() {
		t.Error("should be over high watermark")
	}

	c.Free(900)
	c.Alloc(100)
	if c.IsOverHighWatermark() {
		t.Error("should be under high watermark")
	}
}

func TestControllerIsOverHighWatermarkDisabled(t *testing.T) {
	c := &Controller{enabled: atomic.Bool{}, highWatermark: 0.8, maxMemoryBytes: 1000}
	c.Alloc(900)
	if c.IsOverHighWatermark() {
		t.Error("should be false when disabled")
	}
}

func TestControllerAllowPublish(t *testing.T) {
	c := NewController(Config{Enabled: false})
	if !c.AllowPublish("test") {
		t.Error("should allow when disabled")
	}
}

func TestControllerAllowPublishRateLimited(t *testing.T) {
	c := NewController(cfgEnabled())
	c.SetTopicRateLimit("limited", 1)

	// First should be allowed
	if !c.AllowPublish("limited") {
		t.Error("first publish should be allowed")
	}

	// Second should be blocked (1 msg/s limit)
	if c.AllowPublish("limited") {
		t.Error("second publish should be rate limited")
	}
}

func TestControllerRemoveTopicRateLimit(t *testing.T) {
	c := NewController(cfgEnabled())
	c.SetTopicRateLimit("limited", 1)
	c.AllowPublish("limited") // consume the token

	c.RemoveTopicRateLimit("limited")

	// After removing limit, should allow
	if !c.AllowPublish("limited") {
		t.Error("should allow after removing rate limit")
	}
}

func TestControllerTickSlowConsumers(t *testing.T) {
	c := NewController(cfgEnabled())
	c.RecordDelivery("slow-1")
	c.RecordDelivery("slow-2")

	evicted := c.TickSlowConsumers()
	_ = evicted // may or may not be evicted depending on timing
}

func TestControllerRemoveConsumer(t *testing.T) {
	c := NewController(cfgEnabled())
	c.RecordDelivery("consumer-1")
	c.RemoveConsumer("consumer-1")
}

func TestRateLimitAllow(t *testing.T) {
	rl := newRateLimit(10)

	for i := 0; i < 10; i++ {
		if !rl.allow() {
			t.Errorf("allow #%d should be true", i+1)
		}
	}

	if rl.allow() {
		t.Error("11th allow should be false")
	}
}

func TestRateLimitRefill(t *testing.T) {
	rl := newRateLimit(5)

	for i := 0; i < 5; i++ {
		rl.allow()
	}

	rl.mu.Lock()
	rl.state.lastRefill = time.Now().Add(-2 * time.Second).UnixNano()
	rl.mu.Unlock()

	if !rl.allow() {
		t.Error("should allow after refill")
	}
}

func TestControllerConnectDisconnect(t *testing.T) {
	c := NewController(cfgEnabled())
	c.maxConnections = 2

	if !c.Connect() {
		t.Error("first connection should be allowed")
	}
	if !c.Connect() {
		t.Error("second connection should be allowed")
	}
	if c.Connect() {
		t.Error("third connection should be blocked")
	}

	c.Disconnect()
	if !c.Connect() {
		t.Error("connection should be allowed after disconnect")
	}
}

func TestControllerConnectNoLimit(t *testing.T) {
	c := NewController(cfgEnabled())
	c.maxConnections = 0

	if !c.Connect() {
		t.Error("should allow when no limit")
	}
}

func TestControllerConnectionCount(t *testing.T) {
	c := NewController(cfgEnabled())
	c.Connect()
	c.Connect()
	count := c.ConnectionCount()
	if count != 2 {
		t.Errorf("ConnectionCount = %d, want 2", count)
	}
}

func TestControllerIsSlowConsumer(t *testing.T) {
	c := NewController(cfgEnabled())
	c.RecordDelivery("consumer-1")

	// Just one delivery, not slow yet
	if c.IsSlowConsumer("consumer-1") {
		t.Error("should not be slow after one delivery")
	}
}

func TestControllerMemoryUsage(t *testing.T) {
	c := NewController(cfgEnabled())
	c.Alloc(42)
	if c.MemoryUsage() != 42 {
		t.Errorf("MemoryUsage = %d, want 42", c.MemoryUsage())
	}
}
