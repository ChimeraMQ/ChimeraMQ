package stream

import "sync"

// WaiterRegistry notifies stream consumers when new messages arrive.
type WaiterRegistry struct {
	mu      sync.RWMutex
	waiters map[string]map[uint32][]chan struct{} // topic → partition → channels
}

// NewWaiterRegistry creates a new waiter registry.
func NewWaiterRegistry() *WaiterRegistry {
	return &WaiterRegistry{
		waiters: make(map[string]map[uint32][]chan struct{}),
	}
}

// Register creates a notification channel for a topic/partition.
func (wr *WaiterRegistry) Register(topic string, partID uint32) chan struct{} {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	ch := make(chan struct{}, 1)
	if wr.waiters[topic] == nil {
		wr.waiters[topic] = make(map[uint32][]chan struct{})
	}
	wr.waiters[topic][partID] = append(wr.waiters[topic][partID], ch)
	return ch
}

// Unregister removes a notification channel.
func (wr *WaiterRegistry) Unregister(topic string, partID uint32, ch chan struct{}) {
	wr.mu.Lock()
	defer wr.mu.Unlock()

	channels := wr.waiters[topic][partID]
	for i, c := range channels {
		if c == ch {
			wr.waiters[topic][partID] = append(channels[:i], channels[i+1:]...)
			break
		}
	}
}

// Notify wakes up all waiters for a topic/partition.
func (wr *WaiterRegistry) Notify(topic string, partID uint32) {
	wr.mu.RLock()
	defer wr.mu.RUnlock()

	if topicWaiters, ok := wr.waiters[topic]; ok {
		if channels, ok := topicWaiters[partID]; ok {
			for _, ch := range channels {
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		}
	}
}
