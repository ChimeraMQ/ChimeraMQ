package flow

import (
	"sync"
	"sync/atomic"
	"time"
)

// rateLimitState holds the mutable state for rate limiting.
// All fields are protected by the mutex in rateLimit.
type rateLimitState struct {
	tokens     int64
	lastRefill int64 // unix nanoseconds
}

// Controller manages backpressure and flow control.
type Controller struct {
	mu sync.RWMutex

	// Per-topic rate limiting
	topicLimits map[string]*rateLimit
	globalLimit *rateLimit

	// Memory-based backpressure
	maxMemoryBytes  int64
	usedMemoryBytes atomic.Int64
	highWatermark   float64 // 0.0–1.0, default 0.85

	// Slow consumer tracking
	slowThreshold time.Duration
	slowConsumers map[string]*slowEntry // consumerID → entry
	maxSlowTicks  int

	// Connection tracking
	connectionCount atomic.Int64
	maxConnections  int64

	enabled atomic.Bool
}

type rateLimit struct {
	mu         sync.Mutex
	state      rateLimitState
	maxTokens  int64
	refillRate int64 // tokens per second
}

type slowEntry struct {
	ticks   int
	lastMsg time.Time
}

// Config holds flow control configuration.
type Config struct {
	Enabled         bool
	MaxMemoryBytes  int64
	HighWatermark   float64
	MaxConnections  int64
	SlowConsumerTTL time.Duration
	MaxSlowTicks    int
	GlobalRateLimit int64 // messages/sec, 0 = unlimited
}

// NewController creates a new flow controller.
func NewController(cfg Config) *Controller {
	hw := cfg.HighWatermark
	if hw <= 0 || hw > 1 {
		hw = 0.85
	}
	maxSlow := cfg.MaxSlowTicks
	if maxSlow <= 0 {
		maxSlow = 3
	}
	slowTTL := cfg.SlowConsumerTTL
	if slowTTL <= 0 {
		slowTTL = 30 * time.Second
	}

	c := &Controller{
		topicLimits:    make(map[string]*rateLimit),
		highWatermark:  hw,
		maxMemoryBytes: cfg.MaxMemoryBytes,
		maxConnections: cfg.MaxConnections,
		slowThreshold:  slowTTL,
		maxSlowTicks:   maxSlow,
		slowConsumers:  make(map[string]*slowEntry),
	}
	if cfg.GlobalRateLimit > 0 {
		c.globalLimit = newRateLimit(cfg.GlobalRateLimit)
	}
	if cfg.Enabled {
		c.enabled.Store(true)
	}
	return c
}

// Enabled returns whether flow control is active.
func (c *Controller) Enabled() bool { return c.enabled.Load() }

// --- Memory backpressure ---

// Alloc increases the memory usage counter.
func (c *Controller) Alloc(bytes int64) {
	c.usedMemoryBytes.Add(bytes)
}

// Free decreases the memory usage counter.
func (c *Controller) Free(bytes int64) {
	c.usedMemoryBytes.Add(-bytes)
}

// IsOverHighWatermark returns true if memory usage exceeds the high watermark.
func (c *Controller) IsOverHighWatermark() bool {
	if !c.enabled.Load() || c.maxMemoryBytes <= 0 {
		return false
	}
	used := c.usedMemoryBytes.Load()
	return float64(used) > float64(c.maxMemoryBytes)*c.highWatermark
}

// MemoryUsage returns current memory usage in bytes.
func (c *Controller) MemoryUsage() int64 {
	return c.usedMemoryBytes.Load()
}

// MemoryPressure returns a value from 0.0 to 1.0+ indicating memory pressure.
func (c *Controller) MemoryPressure() float64 {
	if c.maxMemoryBytes <= 0 {
		return 0
	}
	return float64(c.usedMemoryBytes.Load()) / float64(c.maxMemoryBytes)
}

// --- Rate limiting ---

// AllowPublish checks if a publish to the given topic is allowed.
// Returns true if the publish should proceed.
func (c *Controller) AllowPublish(topic string) bool {
	if !c.enabled.Load() {
		return true
	}

	// Check global rate limit first
	if c.globalLimit != nil && !c.globalLimit.allow() {
		return false
	}

	// Check topic-specific rate limit
	c.mu.RLock()
	lim := c.topicLimits[topic]
	c.mu.RUnlock()

	if lim != nil && !lim.allow() {
		return false
	}

	return true
}

// SetTopicRateLimit sets the publish rate limit for a topic.
func (c *Controller) SetTopicRateLimit(topic string, msgsPerSec int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.topicLimits[topic] = newRateLimit(msgsPerSec)
}

// RemoveTopicRateLimit removes the rate limit for a topic.
func (c *Controller) RemoveTopicRateLimit(topic string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.topicLimits, topic)
}

// --- Connection tracking ---

// Connect increments the connection counter. Returns false if limit exceeded.
func (c *Controller) Connect() bool {
	if !c.enabled.Load() || c.maxConnections <= 0 {
		c.connectionCount.Add(1)
		return true
	}
	new := c.connectionCount.Add(1)
	if new > c.maxConnections {
		c.connectionCount.Add(-1)
		return false
	}
	return true
}

// Disconnect decrements the connection counter.
func (c *Controller) Disconnect() {
	c.connectionCount.Add(-1)
}

// ConnectionCount returns the current number of connections.
func (c *Controller) ConnectionCount() int64 {
	return c.connectionCount.Load()
}

// --- Slow consumer detection ---

// RecordDelivery records a message delivery to a consumer.
func (c *Controller) RecordDelivery(consumerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.slowConsumers[consumerID]; ok {
		e.lastMsg = time.Now()
		e.ticks = 0
	} else {
		c.slowConsumers[consumerID] = &slowEntry{
			lastMsg: time.Now(),
		}
	}
}

// IsSlowConsumer checks if a consumer is considered slow.
// Returns true if the consumer has been slow for too many consecutive checks.
func (c *Controller) IsSlowConsumer(consumerID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.slowConsumers[consumerID]
	if !ok {
		return false
	}
	return e.ticks >= c.maxSlowTicks
}

// TickSlowConsumers checks all consumers and marks those that haven't received messages recently.
// Returns the list of consumer IDs that should be disconnected.
func (c *Controller) TickSlowConsumers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	var evict []string

	for id, e := range c.slowConsumers {
		if now.Sub(e.lastMsg) > c.slowThreshold {
			e.ticks++
			if e.ticks >= c.maxSlowTicks {
				evict = append(evict, id)
			}
		}
	}

	// Remove evicted consumers
	for _, id := range evict {
		delete(c.slowConsumers, id)
	}

	return evict
}

// RemoveConsumer removes a consumer from tracking.
func (c *Controller) RemoveConsumer(consumerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.slowConsumers, consumerID)
}

// --- rateLimit internals ---

func newRateLimit(perSec int64) *rateLimit {
	return &rateLimit{
		maxTokens:  perSec,
		refillRate: perSec,
		state: rateLimitState{
			tokens:     perSec,
			lastRefill: time.Now().UnixNano(),
		},
	}
}

func (rl *rateLimit) allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now().UnixNano()
	last := rl.state.lastRefill
	elapsed := now - last

	if elapsed >= int64(time.Second) {
		secs := elapsed / int64(time.Second)
		current := rl.state.tokens
		refill := secs * rl.refillRate
		newTokens := current + refill
		if newTokens > rl.maxTokens {
			newTokens = rl.maxTokens
		}
		rl.state.tokens = newTokens
		rl.state.lastRefill = now
	}

	if rl.state.tokens <= 0 {
		return false
	}
	rl.state.tokens--
	return true
}
