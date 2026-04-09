package stream

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// RebalanceStrategy determines how partitions are assigned to members.
type RebalanceStrategy uint8

const (
	StrategyRange      RebalanceStrategy = 0
	StrategyRoundRobin RebalanceStrategy = 1
	StrategySticky     RebalanceStrategy = 2
)

// GroupMember represents a consumer in a group.
type GroupMember struct {
	ID            string
	Partitions    []uint32
	LastHeartbeat time.Time
}

// ConsumerGroup manages a group of stream consumers.
type ConsumerGroup struct {
	mu             sync.RWMutex
	name           string
	topic          string
	partitionCount uint32
	members        map[string]*GroupMember
	assignments    map[uint32]string // partitionID → memberID
	committed      map[uint32]uint64 // partitionID → offset
	strategy       RebalanceStrategy
	sessionTimeout time.Duration
	offsetStore    *OffsetStore

	stopCh chan struct{}
}

// NewConsumerGroup creates a new consumer group.
func NewConsumerGroup(name, topic string, partitionCount uint32, strategy RebalanceStrategy, offsetStore *OffsetStore) *ConsumerGroup {
	cg := &ConsumerGroup{
		name:           name,
		topic:          topic,
		partitionCount: partitionCount,
		members:        make(map[string]*GroupMember),
		assignments:    make(map[uint32]string),
		committed:      make(map[uint32]uint64),
		strategy:       strategy,
		sessionTimeout: 30 * time.Second,
		offsetStore:    offsetStore,
		stopCh:         make(chan struct{}),
	}
	cg.loadCommittedOffsets()
	go cg.heartbeatLoop()
	return cg
}

// Join adds a member and triggers rebalance.
func (cg *ConsumerGroup) Join(memberID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	cg.members[memberID] = &GroupMember{
		ID:            memberID,
		LastHeartbeat: time.Now(),
	}
	cg.rebalance()
}

// Leave removes a member and triggers rebalance.
func (cg *ConsumerGroup) Leave(memberID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	delete(cg.members, memberID)
	cg.rebalance()
}

// Heartbeat updates member's last heartbeat.
func (cg *ConsumerGroup) Heartbeat(memberID string) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	m, ok := cg.members[memberID]
	if !ok {
		return fmt.Errorf("member %q not in group", memberID)
	}
	m.LastHeartbeat = time.Now()
	return nil
}

// CommitOffset persists a committed offset.
func (cg *ConsumerGroup) CommitOffset(partitionID uint32, offset uint64) error {
	cg.mu.Lock()
	cg.committed[partitionID] = offset
	cg.mu.Unlock()
	return cg.offsetStore.Save(cg.name, partitionID, offset)
}

// GetCommittedOffset returns the committed offset for a partition.
func (cg *ConsumerGroup) GetCommittedOffset(partitionID uint32) uint64 {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return cg.committed[partitionID]
}

// Assignments returns the current partition assignments.
func (cg *ConsumerGroup) Assignments() map[uint32]string {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	result := make(map[uint32]string, len(cg.assignments))
	for k, v := range cg.assignments {
		result[k] = v
	}
	return result
}

// Members returns the group members.
func (cg *ConsumerGroup) Members() map[string]*GroupMember {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	result := make(map[string]*GroupMember, len(cg.members))
	for k, v := range cg.members {
		result[k] = v
	}
	return result
}

// Stop terminates the heartbeat loop.
func (cg *ConsumerGroup) Stop() {
	select {
	case <-cg.stopCh:
		// Already stopped
	default:
		close(cg.stopCh)
	}
}

func (cg *ConsumerGroup) loadCommittedOffsets() {
	for i := uint32(0); i < cg.partitionCount; i++ {
		if off := cg.offsetStore.Get(cg.name, i); off > 0 {
			cg.committed[i] = off
		}
	}
}

func (cg *ConsumerGroup) rebalance() {
	for k := range cg.assignments {
		delete(cg.assignments, k)
	}
	for _, m := range cg.members {
		m.Partitions = nil
	}

	if len(cg.members) == 0 {
		return
	}

	partitions := make([]uint32, cg.partitionCount)
	for i := uint32(0); i < cg.partitionCount; i++ {
		partitions[i] = i
	}

	memberIDs := make([]string, 0, len(cg.members))
	for id := range cg.members {
		memberIDs = append(memberIDs, id)
	}
	sort.Strings(memberIDs)

	switch cg.strategy {
	case StrategyRange:
		cg.rebalanceRange(partitions, memberIDs)
	default:
		cg.rebalanceRoundRobin(partitions, memberIDs)
	}
}

func (cg *ConsumerGroup) rebalanceRange(partitions []uint32, members []string) {
	n := len(partitions)
	m := len(members)
	perMember := n / m
	remainder := n % m

	idx := 0
	for i, memberID := range members {
		count := perMember
		if i < remainder {
			count++
		}
		for j := 0; j < count && idx < n; j++ {
			cg.assignments[partitions[idx]] = memberID
			cg.members[memberID].Partitions = append(cg.members[memberID].Partitions, partitions[idx])
			idx++
		}
	}
}

func (cg *ConsumerGroup) rebalanceRoundRobin(partitions []uint32, members []string) {
	for i, partID := range partitions {
		memberID := members[i%len(members)]
		cg.assignments[partID] = memberID
		cg.members[memberID].Partitions = append(cg.members[memberID].Partitions, partID)
	}
}

func (cg *ConsumerGroup) heartbeatLoop() {
	ticker := time.NewTicker(cg.sessionTimeout / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cg.mu.Lock()
			now := time.Now()
			for id, m := range cg.members {
				if now.Sub(m.LastHeartbeat) > cg.sessionTimeout {
					delete(cg.members, id)
					cg.rebalance()
				}
			}
			cg.mu.Unlock()
		case <-cg.stopCh:
			return
		}
	}
}
