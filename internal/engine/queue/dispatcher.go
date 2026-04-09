package queue

import (
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// Dispatcher routes messages to consumers round-robin with prefetch awareness.
type Dispatcher struct {
	mu         sync.Mutex
	consumers  []*QueueConsumer
	nextIdx    int
	visTimeout time.Duration
}

// Dispatch assigns a message to the next available consumer.
func (d *Dispatcher) Dispatch(offset uint64, env *message.Envelope) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.consumers) == 0 {
		return "", ErrNoConsumers
	}

	checked := 0
	for checked < len(d.consumers) {
		consumer := d.consumers[d.nextIdx]
		d.nextIdx = (d.nextIdx + 1) % len(d.consumers)
		checked++

		consumer.mu.Lock()
		if len(consumer.InFlight) < consumer.Prefetch {
			consumer.InFlight[offset] = time.Now()
			consumer.mu.Unlock()
			return consumer.ID, nil
		}
		consumer.mu.Unlock()
	}

	return "", ErrAllConsumersBusy
}

// AddConsumer registers a new consumer.
func (d *Dispatcher) AddConsumer(c *QueueConsumer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.consumers = append(d.consumers, c)
}

// RemoveConsumer removes a consumer and reclaims in-flight messages.
func (d *Dispatcher) RemoveConsumer(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i, c := range d.consumers {
		if c.ID == id {
			d.consumers = append(d.consumers[:i], d.consumers[i+1:]...)
			if d.nextIdx >= len(d.consumers) && len(d.consumers) > 0 {
				d.nextIdx = 0
			}
			return
		}
	}
}
