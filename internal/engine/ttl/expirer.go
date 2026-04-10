package ttl

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/hot"
)

// Action determines what happens to expired messages.
type Action uint8

const (
	ActionDrop Action = 0
	ActionDLQ  Action = 1
)

// TopicTTLConfig holds TTL settings for a topic.
type TopicTTLConfig struct {
	DefaultTTL int64 // Nanoseconds, 0 = no default
	Action     Action
}

// Expirer scans partitions and removes expired messages.
type Expirer struct {
	mu      sync.RWMutex
	storage *hot.Engine
	configs map[string]*TopicTTLConfig // topic -> config
	onExpired func(topic string, env *message.Envelope) // optional callback

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewExpirer creates a new TTL expirer.
func NewExpirer(storage *hot.Engine) *Expirer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Expirer{
		storage: storage,
		configs: make(map[string]*TopicTTLConfig),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// SetOnExpired sets the callback for expired messages (for DLQ routing).
func (e *Expirer) SetOnExpired(fn func(topic string, env *message.Envelope)) {
	e.onExpired = fn
}

// SetTopicConfig configures TTL for a topic.
func (e *Expirer) SetTopicConfig(topic string, cfg *TopicTTLConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.configs[topic] = cfg
}

// RemoveTopic removes TTL tracking for a topic.
func (e *Expirer) RemoveTopic(topic string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.configs, topic)
}

// Start begins the background expiry scan.
func (e *Expirer) Start() {
	e.wg.Add(1)
	go e.run()
}

// Stop shuts down the expirer.
func (e *Expirer) Stop() {
	e.cancel()
	e.wg.Wait()
}

// IsExpired checks if a message has expired based on its TTL.
func IsExpired(env *message.Envelope) bool {
	if env.TTL == 0 {
		return false
	}
	return env.Timestamp+env.TTL < time.Now().UnixNano()
}

// ApplyDefaultTTL sets the topic's default TTL if the message has no TTL set.
func ApplyDefaultTTL(env *message.Envelope, defaultTTL int64) {
	if env.TTL == 0 && defaultTTL > 0 {
		env.TTL = defaultTTL
	}
}

func (e *Expirer) run() {
	defer e.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.scan()
		}
	}
}

func (e *Expirer) scan() {
	e.mu.RLock()
	topics := make([]string, 0, len(e.configs))
	for t := range e.configs {
		topics = append(topics, t)
	}
	configs := make(map[string]*TopicTTLConfig, len(e.configs))
	for k, v := range e.configs {
		configs[k] = v
	}
	e.mu.RUnlock()

	now := time.Now().UnixNano()

	// Use ForEachPartition to scan all partitions dynamically
	for _, topic := range topics {
		cfg := configs[topic]
		if cfg == nil {
			continue
		}

		e.storage.ForEachPartition(func(t string, partID uint32, part *hot.Partition) bool {
			if t != topic {
				return true
			}

			hw := part.HighWatermark()
			ls := part.LogStartOffset()
			if hw < ls || hw == 0 {
				return true
			}

			// Batch scan: check up to 100 messages per tick
			checked := 0
			var lastExpiredOffset uint64
			hasExpired := false

			for offset := ls; offset <= hw && checked < 100; offset++ {
				data, err := part.Read(offset)
				if err != nil {
					continue
				}
				checked++

				env, err := message.Unmarshal(data)
				if err != nil {
					continue
				}

				if env.TTL == 0 {
					// Stop scanning: messages are ordered by time, if we hit
					// a non-expiring message with no TTL, we can stop since
					// subsequent messages are newer.
					if hasExpired {
						break
					}
					continue
				}

				if env.Timestamp+env.TTL < now {
					// Message expired
					if cfg.Action == ActionDLQ && e.onExpired != nil {
						e.onExpired(topic, env)
					}
					lastExpiredOffset = offset
					hasExpired = true
				} else if hasExpired {
					// Reached a non-expired message after expired ones — stop
					break
				}
			}

			// Delete expired messages by advancing the log start
			if hasExpired && lastExpiredOffset > 0 {
				removed := part.AdvanceLogStart(lastExpiredOffset + 1)
				if removed > 0 {
					log.Printf("ttl: expired messages on %s/%d, removed %d segments up to offset %d", topic, partID, removed, lastExpiredOffset)
				}
			}

			return true
		})
	}
}
