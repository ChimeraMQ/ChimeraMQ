package stream

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/hot"
)

// Engine manages stream-mode topics, consumer groups, and long-poll fetches.
type Engine struct {
	mu      sync.RWMutex
	groups  map[string]*ConsumerGroup // groupName → group
	storage *hot.Engine
	offsets *OffsetStore
	waiters *WaiterRegistry
	closed  atomic.Bool
}

// NewEngine creates a new stream engine.
func NewEngine(storage *hot.Engine, offsets *OffsetStore) *Engine {
	return &Engine{
		groups:  make(map[string]*ConsumerGroup),
		storage: storage,
		offsets: offsets,
		waiters: NewWaiterRegistry(),
	}
}

// Close stops all consumer group heartbeat goroutines.
func (se *Engine) Close() {
	if !se.closed.CompareAndSwap(false, true) {
		return
	}
	se.mu.Lock()
	defer se.mu.Unlock()
	for _, cg := range se.groups {
		cg.Stop()
	}
}

// JoinGroup adds a member to a consumer group (creating it if needed).
func (se *Engine) JoinGroup(groupName, topic string, memberID string, partitionCount uint32, strategy RebalanceStrategy) {
	if se.closed.Load() {
		return
	}
	se.mu.Lock()
	defer se.mu.Unlock()

	cg, ok := se.groups[groupName]
	if !ok {
		cg = NewConsumerGroup(groupName, topic, partitionCount, strategy, se.offsets)
		se.groups[groupName] = cg
	}
	cg.Join(memberID)
}

// LeaveGroup removes a member from a consumer group.
func (se *Engine) LeaveGroup(groupName, memberID string) {
	se.mu.RLock()
	cg, ok := se.groups[groupName]
	se.mu.RUnlock()
	if !ok {
		return
	}
	cg.Leave(memberID)
}

// Heartbeat updates a member's heartbeat in a consumer group.
func (se *Engine) Heartbeat(groupName, memberID string) error {
	se.mu.RLock()
	cg, ok := se.groups[groupName]
	se.mu.RUnlock()
	if !ok {
		return nil
	}
	return cg.Heartbeat(memberID)
}

// CommitOffset persists a consumer group offset.
func (se *Engine) CommitOffset(groupName string, partitionID uint32, offset uint64) error {
	se.mu.RLock()
	cg, ok := se.groups[groupName]
	se.mu.RUnlock()
	if !ok {
		return nil
	}
	return cg.CommitOffset(partitionID, offset)
}

// GetGroup returns a consumer group by name.
func (se *Engine) GetGroup(name string) *ConsumerGroup {
	se.mu.RLock()
	defer se.mu.RUnlock()
	return se.groups[name]
}

// ListGroups returns all consumer group names.
func (se *Engine) ListGroups() []string {
	se.mu.RLock()
	defer se.mu.RUnlock()
	names := make([]string, 0, len(se.groups))
	for name := range se.groups {
		names = append(names, name)
	}
	return names
}

// Fetch reads messages from a partition with optional long-poll.
func (se *Engine) Fetch(topic string, partitionID uint32, fromOffset uint64, maxMessages int, maxWait time.Duration) ([]*message.Envelope, uint64, error) {
	if se.closed.Load() {
		return nil, fromOffset, fmt.Errorf("engine is closed")
	}
	part, err := se.storage.GetOrCreatePartition(topic, partitionID)
	if err != nil {
		return nil, fromOffset, err
	}

	hw := part.HighWatermark()
	if fromOffset <= hw {
		return se.readMessages(part, fromOffset, hw, maxMessages)
	}

	// Long-poll: wait for new data
	ch := se.waiters.Register(topic, partitionID)
	defer se.waiters.Unregister(topic, partitionID, ch)

	select {
	case <-ch:
		hw = part.HighWatermark()
		return se.readMessages(part, fromOffset, hw, maxMessages)
	case <-time.After(maxWait):
		return nil, fromOffset, nil
	}
}

// NotifyWaiters wakes up consumers waiting on a topic/partition.
func (se *Engine) NotifyWaiters(topic string, partID uint32) {
	se.waiters.Notify(topic, partID)
}

func (se *Engine) readMessages(part *hot.Partition, from, to uint64, max int) ([]*message.Envelope, uint64, error) {
	var msgs []*message.Envelope

	end := to
	if from+uint64(max)-1 < end {
		end = from + uint64(max) - 1
	}

	for offset := from; offset <= end; offset++ {
		data, err := part.Read(offset)
		if err != nil {
			break
		}
		env, err := message.Unmarshal(data)
		if err != nil {
			continue
		}
		env.Sequence = offset
		msgs = append(msgs, env)
	}

	nextOffset := from + uint64(len(msgs))
	return msgs, nextOffset, nil
}
