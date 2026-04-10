package queue

import (
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// maxPriorityLevels is the maximum number of priority levels (0-9).
const maxPriorityLevels = 10

// DispatcherInterface abstracts message dispatching.
type DispatcherInterface interface {
	Dispatch(offset uint64, env *message.Envelope) (string, error)
	AddConsumer(c *QueueConsumer)
	RemoveConsumer(id string)
}

// pendingPriorityMsg is a message waiting to be dispatched.
type pendingPriorityMsg struct {
	offset uint64
	env    *message.Envelope
}

// PriorityDispatcher routes messages by priority using per-level queues.
// Priority levels 0-9, where 9 is highest.
type PriorityDispatcher struct {
	mu           sync.Mutex
	levels       [maxPriorityLevels][]pendingPriorityMsg
	consumers    []*QueueConsumer
	consumerMaxP map[string]uint8 // consumer ID → max priority they handle
	nextIdx      int              // round-robin index within a priority level
}

// NewPriorityDispatcher creates a new priority-based dispatcher.
func NewPriorityDispatcher() *PriorityDispatcher {
	return &PriorityDispatcher{
		consumerMaxP: make(map[string]uint8),
	}
}

// SetConsumerMaxPriority sets the maximum priority a consumer can handle.
func (pd *PriorityDispatcher) SetConsumerMaxPriority(consumerID string, maxP uint8) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	if maxP > 9 {
		maxP = 9
	}
	pd.consumerMaxP[consumerID] = maxP
}

// Dispatch assigns a message to the highest-priority-capable available consumer.
func (pd *PriorityDispatcher) Dispatch(offset uint64, env *message.Envelope) (string, error) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	if len(pd.consumers) == 0 {
		return "", ErrNoConsumers
	}

	priority := env.Priority
	if priority > 9 {
		priority = 9
	}

	// Try to dispatch immediately: scan from highest to lowest priority
	// For the message's own priority, try to dispatch it directly.
	// If no consumer can take it, queue it.

	// First, try to dispatch this message directly
	if consumerID := pd.tryDispatchToConsumer(offset, priority); consumerID != "" {
		return consumerID, nil
	}

	// No consumer available — also try to flush higher-priority queued messages
	// and then retry
	pd.flushQueued()

	// Try again after flushing
	if consumerID := pd.tryDispatchToConsumer(offset, priority); consumerID != "" {
		return consumerID, nil
	}

	// Queue the message
	pd.levels[priority] = append(pd.levels[priority], pendingPriorityMsg{
		offset: offset,
		env:    env,
	})

	return "", ErrAllConsumersBusy
}

// tryDispatchToConsumer tries to find a consumer that can handle the given priority.
func (pd *PriorityDispatcher) tryDispatchToConsumer(offset uint64, priority uint8) string {
	checked := 0
	for checked < len(pd.consumers) {
		consumer := pd.consumers[pd.nextIdx]
		pd.nextIdx = (pd.nextIdx + 1) % len(pd.consumers)
		checked++

		maxP := pd.consumerMaxP[consumer.ID]
		if maxP == 0 {
			maxP = 9 // default: handle all priorities
		}
		if priority > maxP {
			continue // consumer can't handle this priority
		}

		consumer.mu.Lock()
		if len(consumer.InFlight) < consumer.Prefetch {
			consumer.InFlight[offset] = time.Now()
			consumer.mu.Unlock()
			return consumer.ID
		}
		consumer.mu.Unlock()
	}
	return ""
}

// flushQueued tries to dispatch queued messages from highest to lowest priority.
func (pd *PriorityDispatcher) flushQueued() {
	for level := maxPriorityLevels - 1; level >= 0; level-- {
		queue := pd.levels[level]
		var remaining []pendingPriorityMsg
		for _, msg := range queue {
			if id := pd.tryDispatchToConsumer(msg.offset, uint8(level)); id != "" {
				continue // dispatched
			}
			remaining = append(remaining, msg)
		}
		pd.levels[level] = remaining
	}
}

// AddConsumer registers a new consumer.
func (pd *PriorityDispatcher) AddConsumer(c *QueueConsumer) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.consumers = append(pd.consumers, c)
	if _, ok := pd.consumerMaxP[c.ID]; !ok {
		pd.consumerMaxP[c.ID] = 9 // default: handle all priorities
	}

	// Flush queued messages now that a new consumer is available
	pd.flushQueued()
}

// RemoveConsumer removes a consumer.
func (pd *PriorityDispatcher) RemoveConsumer(id string) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	for i, c := range pd.consumers {
		if c.ID == id {
			pd.consumers = append(pd.consumers[:i], pd.consumers[i+1:]...)
			if pd.nextIdx >= len(pd.consumers) && len(pd.consumers) > 0 {
				pd.nextIdx = 0
			}
			break
		}
	}
	delete(pd.consumerMaxP, id)
}

// QueuedDepth returns the total number of queued messages across all priority levels.
func (pd *PriorityDispatcher) QueuedDepth() int {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	total := 0
	for _, q := range pd.levels {
		total += len(q)
	}
	return total
}
