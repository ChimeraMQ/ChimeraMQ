package queue

import (
	"container/heap"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

type delayedMsg struct {
	deliverAt time.Time
	envelope  *message.Envelope
}

type delayHeap []delayedMsg

func (h delayHeap) Len() int            { return len(h) }
func (h delayHeap) Less(i, j int) bool  { return h[i].deliverAt.Before(h[j].deliverAt) }
func (h delayHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *delayHeap) Push(x interface{}) { *h = append(*h, x.(delayedMsg)) }
func (h *delayHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// DelayScheduler manages delayed messages with a min-heap.
type DelayScheduler struct {
	mu      sync.Mutex
	heap    delayHeap
	ticker  *time.Ticker
	readyCh chan *message.Envelope
	stopCh  chan struct{}
}

// NewDelayScheduler creates and starts the delay scheduler.
func NewDelayScheduler() *DelayScheduler {
	ds := &DelayScheduler{
		heap:    make(delayHeap, 0),
		ticker:  time.NewTicker(100 * time.Millisecond),
		readyCh: make(chan *message.Envelope, 10000),
		stopCh:  make(chan struct{}),
	}
	heap.Init(&ds.heap)
	go ds.promotionLoop()
	return ds
}

// Schedule adds a message to be delivered later.
func (ds *DelayScheduler) Schedule(env *message.Envelope) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	deliverAt := time.Unix(0, env.DeliverAt)
	heap.Push(&ds.heap, delayedMsg{
		deliverAt: deliverAt,
		envelope:  env,
	})
}

// Ready returns the channel that receives messages ready for delivery.
func (ds *DelayScheduler) Ready() <-chan *message.Envelope {
	return ds.readyCh
}

// Stop terminates the scheduler.
func (ds *DelayScheduler) Stop() {
	ds.ticker.Stop()
	close(ds.stopCh)
}

func (ds *DelayScheduler) promotionLoop() {
	for {
		select {
		case <-ds.ticker.C:
			ds.mu.Lock()
			now := time.Now()
			for ds.heap.Len() > 0 && ds.heap[0].deliverAt.Before(now) {
				item := heap.Pop(&ds.heap).(delayedMsg)
				select {
				case ds.readyCh <- item.envelope:
				default:
				}
			}
			ds.mu.Unlock()
		case <-ds.stopCh:
			return
		}
	}
}
