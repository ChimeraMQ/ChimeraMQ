package replication

import (
	"fmt"
	"sync"

	"github.com/chimeramq/chimera/internal/cluster/raft"
)

// ISRSet tracks in-sync replicas for a partition.
type ISRSet struct {
	mu       sync.RWMutex
	topic    string
	partition uint32
	leaderID raft.NodeID
	replicas map[raft.NodeID]ReplicaState
	isr      map[raft.NodeID]struct{}
	maxLag   int64
}

// ReplicaState tracks a replica's replication progress.
type ReplicaState struct {
	NodeID     raft.NodeID
	LEO        uint64 // Log end offset
	LastCaught uint64 // Last offset confirmed caught up
	Active     bool
}

// NewISRSet creates a new ISR tracker.
func NewISRSet(topic string, partition uint32, leaderID raft.NodeID, maxLag int64) *ISRSet {
	return &ISRSet{
		topic:     topic,
		partition: partition,
		leaderID:  leaderID,
		replicas:  make(map[raft.NodeID]ReplicaState),
		isr:       make(map[raft.NodeID]struct{}),
		maxLag:    maxLag,
	}
}

// AddReplica adds a replica to the replica set and ISR.
func (isr *ISRSet) AddReplica(nodeID raft.NodeID) {
	isr.mu.Lock()
	defer isr.mu.Unlock()
	isr.replicas[nodeID] = ReplicaState{NodeID: nodeID, Active: true}
	isr.isr[nodeID] = struct{}{}
}

// RemoveReplica removes a replica from both the replica set and ISR.
func (isr *ISRSet) RemoveReplica(nodeID raft.NodeID) {
	isr.mu.Lock()
	defer isr.mu.Unlock()
	delete(isr.replicas, nodeID)
	delete(isr.isr, nodeID)
}

// UpdateLEO updates a replica's log end offset.
func (isr *ISRSet) UpdateLEO(nodeID raft.NodeID, leo uint64) {
	isr.mu.Lock()
	defer isr.mu.Unlock()
	if state, ok := isr.replicas[nodeID]; ok {
		state.LEO = leo
		state.Active = true
		isr.replicas[nodeID] = state
	}
}

// ShrinkISR removes a replica from the ISR (too slow or crashed).
func (isr *ISRSet) ShrinkISR(nodeID raft.NodeID) {
	isr.mu.Lock()
	defer isr.mu.Unlock()
	delete(isr.isr, nodeID)
}

// ExpandISR adds a caught-up replica back to the ISR.
func (isr *ISRSet) ExpandISR(nodeID raft.NodeID) bool {
	isr.mu.Lock()
	defer isr.mu.Unlock()
	if state, ok := isr.replicas[nodeID]; ok && state.Active {
		isr.isr[nodeID] = struct{}{}
		return true
	}
	return false
}

// IsInSync checks if a replica is in the ISR.
func (isr *ISRSet) IsInSync(nodeID raft.NodeID) bool {
	isr.mu.RLock()
	defer isr.mu.RUnlock()
	_, ok := isr.isr[nodeID]
	return ok
}

// ISRSize returns the current ISR size.
func (isr *ISRSet) ISRSize() int {
	isr.mu.RLock()
	defer isr.mu.RUnlock()
	return len(isr.isr)
}

// ISRMembers returns the list of ISR members.
func (isr *ISRSet) ISRMembers() []raft.NodeID {
	isr.mu.RLock()
	defer isr.mu.RUnlock()
	result := make([]raft.NodeID, 0, len(isr.isr))
	for id := range isr.isr {
		result = append(result, id)
	}
	return result
}

// UpdateISR checks all replicas and updates ISR membership based on lag.
func (isr *ISRSet) UpdateISR(leaderHW uint64) {
	isr.mu.Lock()
	defer isr.mu.Unlock()

	for id, state := range isr.replicas {
		if id == isr.leaderID {
			continue
		}

		lag := int64(leaderHW) - int64(state.LEO)
		_, inISR := isr.isr[id]

		if inISR && lag > isr.maxLag {
			// Too far behind, remove from ISR
			delete(isr.isr, id)
		} else if !inISR && lag <= isr.maxLag && state.Active {
			// Caught up, add back to ISR
			isr.isr[id] = struct{}{}
		}
	}
}

// HasQuorum checks if a quorum of ISR members have reached the given offset.
func (isr *ISRSet) HasQuorum(offset uint64) bool {
	isr.mu.RLock()
	defer isr.mu.RUnlock()

	count := 1 // leader always has it
	for id := range isr.isr {
		if id == isr.leaderID {
			continue
		}
		if state, ok := isr.replicas[id]; ok && state.LEO >= offset {
			count++
		}
	}

	isrSize := len(isr.isr) + 1 // +1 for leader
	return count > isrSize/2
}

// String returns a debug string.
func (isr *ISRSet) String() string {
	isr.mu.RLock()
	defer isr.mu.RUnlock()
	return fmt.Sprintf("ISR(%s:%d) leader=%s isr=%v replicas=%d",
		isr.topic, isr.partition, isr.leaderID, isr.ISRMembers(), len(isr.replicas))
}
