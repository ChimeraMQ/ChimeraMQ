package queue

import (
	"errors"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

var (
	ErrNoConsumers     = errors.New("no consumers available")
	ErrAllConsumersBusy = errors.New("all consumers at prefetch capacity")
)

// QueueConsumer represents a connected queue consumer.
type QueueConsumer struct {
	ID       string
	Prefetch int
	mu       sync.Mutex
	InFlight map[uint64]time.Time
}

// InFlightCount returns the number of messages currently in-flight.
func (c *QueueConsumer) InFlightCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.InFlight)
}

// QueueState tracks the state of a single queue.
type QueueState struct {
	mu         sync.Mutex
	topicName  string
	consumers  map[string]*QueueConsumer
	dispatcher DispatcherInterface
	ackTracker *AckTracker
	dlqManager *DLQManager
	delayHeap  *DelayScheduler
}

// Engine manages all queue-mode topics.
type Engine struct {
	mu      sync.RWMutex
	queues  map[string]*QueueState
}

// NewEngine creates a new queue engine.
func NewEngine() *Engine {
	return &Engine{
		queues: make(map[string]*QueueState),
	}
}

// Close stops all background goroutines for every queue.
func (e *Engine) Close() {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for _, qs := range e.queues {
		if qs.ackTracker != nil {
			qs.ackTracker.Stop()
		}
		if qs.delayHeap != nil {
			qs.delayHeap.Stop()
		}
	}
}

// AddConsumer registers a queue consumer for a topic.
func (e *Engine) AddConsumer(topic string, consumer *QueueConsumer) {
	e.mu.Lock()
	defer e.mu.Unlock()

	qs, ok := e.queues[topic]
	if !ok {
		qs = &QueueState{
			topicName:  topic,
			consumers:  make(map[string]*QueueConsumer),
			dispatcher: &Dispatcher{visTimeout: 30 * time.Second},
			ackTracker: NewAckTracker(30 * time.Second),
		}
		e.queues[topic] = qs
	}

	qs.mu.Lock()
	qs.consumers[consumer.ID] = consumer
	qs.dispatcher.AddConsumer(consumer)
	qs.mu.Unlock()
}

// RemoveConsumer removes a queue consumer.
func (e *Engine) RemoveConsumer(topic, consumerID string) {
	e.mu.RLock()
	qs, ok := e.queues[topic]
	e.mu.RUnlock()
	if !ok {
		return
	}

	qs.mu.Lock()
	delete(qs.consumers, consumerID)
	qs.dispatcher.RemoveConsumer(consumerID)
	qs.mu.Unlock()
}

// TryDispatch attempts to dispatch a message to a consumer.
func (e *Engine) TryDispatch(topic string, partID uint32, offset uint64, env *message.Envelope) (string, error) {
	e.mu.RLock()
	qs, ok := e.queues[topic]
	e.mu.RUnlock()
	if !ok {
		return "", ErrNoConsumers
	}

	consumerID, err := qs.dispatcher.Dispatch(offset, env)
	if err != nil {
		return "", err
	}

	qs.ackTracker.Track(offset, consumerID, env.DeliverCount, env.MaxRetries)
	return consumerID, nil
}

// HandleAck processes an acknowledgment.
func (e *Engine) HandleAck(topic string, offset uint64) bool {
	e.mu.RLock()
	qs, ok := e.queues[topic]
	e.mu.RUnlock()
	if !ok {
		return false
	}
	return qs.ackTracker.Ack(offset)
}

// HandleNack processes a negative acknowledgment.
func (e *Engine) HandleNack(topic string, offset uint64) (bool, uint32) {
	e.mu.RLock()
	qs, ok := e.queues[topic]
	e.mu.RUnlock()
	if !ok {
		return false, 0
	}
	return qs.ackTracker.Nack(offset)
}

// ScheduleDelayed adds a message to the delay scheduler.
func (e *Engine) ScheduleDelayed(topic string, env *message.Envelope) {
	e.mu.Lock()
	defer e.mu.Unlock()

	qs, ok := e.queues[topic]
	if !ok {
		qs = &QueueState{
			topicName:  topic,
			consumers:  make(map[string]*QueueConsumer),
			dispatcher: &Dispatcher{visTimeout: 30 * time.Second},
			ackTracker: NewAckTracker(30 * time.Second),
		}
		e.queues[topic] = qs
	}

	if qs.delayHeap == nil {
		qs.delayHeap = NewDelayScheduler()
	}
	qs.delayHeap.Schedule(env)
}
