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
	cg.members[memberID] = &GroupMember{
		ID:            memberID,
		LastHeartbeat: time.Now(),
	}
	cg.mu.Unlock()
	cg.rebalance()
}

// Leave removes a member and triggers rebalance.
func (cg *ConsumerGroup) Leave(memberID string) {
	cg.mu.Lock()
	delete(cg.members, memberID)
	cg.mu.Unlock()
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

// Topic returns the topic name for this consumer group.
func (cg *ConsumerGroup) Topic() string {
	return cg.topic
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

// Members returns a deep copy of the group members.
func (cg *ConsumerGroup) Members() map[string]GroupMember {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	result := make(map[string]GroupMember, len(cg.members))
	for k, v := range cg.members {
		result[k] = GroupMember{
			ID:            v.ID,
			Partitions:    append([]uint32(nil), v.Partitions...),
			LastHeartbeat: v.LastHeartbeat,
		}
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
	cg.mu.Lock()
	defer cg.mu.Unlock()

	// Save previous assignments for sticky strategy
	prevAssignments := make(map[uint32]string, len(cg.assignments))
	for k, v := range cg.assignments {
		prevAssignments[k] = v
	}

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
	case StrategySticky:
		cg.rebalanceSticky(partitions, memberIDs, prevAssignments)
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

// rebalanceSticky tries to keep existing partition assignments stable while
// maintaining balance. It first preserves assignments for returning members,
// then rebalances by stealing from overloaded members to underloaded ones.
func (cg *ConsumerGroup) rebalanceSticky(partitions []uint32, members []string, prev map[uint32]string) {
	memberSet := make(map[string]bool, len(members))
	for _, m := range members {
		memberSet[m] = true
	}

	// Phase 1: Keep existing assignments for members still present
	for _, partID := range partitions {
		if prevMember, ok := prev[partID]; ok && memberSet[prevMember] {
			cg.assignments[partID] = prevMember
			cg.members[prevMember].Partitions = append(cg.members[prevMember].Partitions, partID)
		}
	}

	// Phase 2: Collect unassigned partitions (from departed members)
	var unassigned []uint32
	for _, partID := range partitions {
		if _, assigned := cg.assignments[partID]; !assigned {
			unassigned = append(unassigned, partID)
		}
	}

	// Phase 3: Distribute unassigned to members with fewest partitions
	for _, partID := range unassigned {
		chosen := cg.memberWithFewest(members)
		cg.assignments[partID] = chosen
		cg.members[chosen].Partitions = append(cg.members[chosen].Partitions, partID)
	}

	// Phase 4: Rebalance — steal from overloaded to underloaded
	target := len(partitions) / len(members)

	for {
		// Find most overloaded and most underloaded
		var overMember, underMember string
		overCount, underCount := 0, len(partitions)+1

		for _, memberID := range members {
			cnt := len(cg.members[memberID].Partitions)
			maxAllowed := target
			// First 'remainder' members can have target+1
			if cnt > maxAllowed+1 && cnt > overCount {
				overCount = cnt
				overMember = memberID
			}
			if cnt < target && cnt < underCount {
				underCount = cnt
				underMember = memberID
			}
		}

		if overMember == "" || underMember == "" {
			break
		}

		// Steal one partition from overMember to underMember
		stolenPart := cg.members[overMember].Partitions[len(cg.members[overMember].Partitions)-1]
		cg.members[overMember].Partitions = cg.members[overMember].Partitions[:len(cg.members[overMember].Partitions)-1]
		cg.members[underMember].Partitions = append(cg.members[underMember].Partitions, stolenPart)
		cg.assignments[stolenPart] = underMember
	}
}

func (cg *ConsumerGroup) memberWithFewest(members []string) string {
	minCount := int(^uint(0) >> 1)
	var chosen string
	for _, memberID := range members {
		cnt := len(cg.members[memberID].Partitions)
		if cnt < minCount {
			minCount = cnt
			chosen = memberID
		}
	}
	return chosen
}

func (cg *ConsumerGroup) heartbeatLoop() {
	ticker := time.NewTicker(cg.sessionTimeout / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cg.mu.Lock()
			now := time.Now()
			var expired []string
			for id, m := range cg.members {
				if now.Sub(m.LastHeartbeat) > cg.sessionTimeout {
					expired = append(expired, id)
				}
			}
			for _, id := range expired {
				delete(cg.members, id)
			}
			cg.mu.Unlock()
			if len(expired) > 0 {
				cg.rebalance()
			}
		case <-cg.stopCh:
			return
		}
	}
}
