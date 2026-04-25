package queue

import (
	"sync"
	"time"
)

// PendingMsg tracks an in-flight message awaiting acknowledgment.
type PendingMsg struct {
	Offset       uint64
	ConsumerID   string
	DeliveredAt  time.Time
	DeliverCount uint32
	MaxRetries   uint32
}

// AckTracker manages message acknowledgment state and visibility timeouts.
type AckTracker struct {
	mu            sync.Mutex
	pending       map[uint64]*PendingMsg
	visTimeout    time.Duration
	redeliverChan chan uint64
	stopChan      chan struct{}
}

// NewAckTracker creates a new ack tracker with the given visibility timeout.
func NewAckTracker(visTimeout time.Duration) *AckTracker {
	at := &AckTracker{
		pending:       make(map[uint64]*PendingMsg),
		visTimeout:    visTimeout,
		redeliverChan: make(chan uint64, 10000),
		stopChan:      make(chan struct{}),
	}
	go at.visibilityTimeoutLoop()
	return at
}

// Track adds a message to the pending set.
func (at *AckTracker) Track(offset uint64, consumerID string, deliverCount, maxRetries uint32) {
	at.mu.Lock()
	defer at.mu.Unlock()
	at.pending[offset] = &PendingMsg{
		Offset:       offset,
		ConsumerID:   consumerID,
		DeliveredAt:  time.Now(),
		DeliverCount: deliverCount,
		MaxRetries:   maxRetries,
	}
}

// Ack removes a message from the pending set.
func (at *AckTracker) Ack(offset uint64) bool {
	at.mu.Lock()
	defer at.mu.Unlock()
	_, ok := at.pending[offset]
	if ok {
		delete(at.pending, offset)
	}
	return ok
}

// Nack marks a message for redelivery or DLQ routing.
func (at *AckTracker) Nack(offset uint64) (shouldDLQ bool, deliverCount uint32) {
	at.mu.Lock()
	defer at.mu.Unlock()

	pending, ok := at.pending[offset]
	if !ok {
		return false, 0
	}

	pending.DeliverCount++
	delete(at.pending, offset)

	if pending.MaxRetries > 0 && pending.DeliverCount >= pending.MaxRetries {
		return true, pending.DeliverCount
	}

	select {
	case at.redeliverChan <- offset:
	default:
	}
	return false, pending.DeliverCount
}

// RedeliverChan returns the channel of offsets to redeliver.
func (at *AckTracker) RedeliverChan() <-chan uint64 {
	return at.redeliverChan
}

// Stop terminates the visibility timeout goroutine.
func (at *AckTracker) Stop() {
	close(at.stopChan)
}

// PendingCount returns the number of messages currently in-flight.
func (at *AckTracker) PendingCount() int {
	at.mu.Lock()
	defer at.mu.Unlock()
	return len(at.pending)
}

func (at *AckTracker) visibilityTimeoutLoop() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			at.mu.Lock()
			now := time.Now()
			for offset, pending := range at.pending {
				if now.Sub(pending.DeliveredAt) > at.visTimeout {
					delete(at.pending, offset)
					select {
					case at.redeliverChan <- offset:
					default:
					}
				}
			}
			at.mu.Unlock()
		case <-at.stopChan:
			return
		}
	}
}
