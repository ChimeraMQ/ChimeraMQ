package replication

import (
	"fmt"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/cluster/raft"
)

// AckPolicy controls when a write is acknowledged to the client.
type AckPolicy int

const (
	AckLeader AckPolicy = iota // Ack after local write
	AckQuorum                  // Ack after majority of ISR confirms
	AckAll                     // Ack after all ISR confirms
)

func ParseAckPolicy(s string) AckPolicy {
	switch s {
	case "leader":
		return AckLeader
	case "all":
		return AckAll
	default:
		return AckQuorum
	}
}

// Replicator manages leader-side replication for a partition.
type Replicator struct {
	mu        sync.Mutex
	topic     string
	partition uint32
	leaderID  raft.NodeID
	epoch     uint64
	policy    AckPolicy
	isr       *ISRSet
	hw        uint64 // High watermark
	transport ReplicationTransport
}

// ReplicationTransport sends replication requests to followers.
type ReplicationTransport interface {
	Replicate(nodeID raft.NodeID, req *ReplicateRequest) error
	FetchEntries(nodeID raft.NodeID, req *FetchRequest) (*FetchResponse, error)
}

// NewReplicator creates a new per-partition replication manager.
func NewReplicator(topic string, partition uint32, leaderID raft.NodeID, policy AckPolicy, maxLag int64) *Replicator {
	return &Replicator{
		topic:     topic,
		partition: partition,
		leaderID:  leaderID,
		policy:    policy,
		isr:       NewISRSet(topic, partition, leaderID, maxLag),
	}
}

// SetTransport sets the replication transport.
func (r *Replicator) SetTransport(t ReplicationTransport) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transport = t
}

// ReplicateWrite writes to leader and replicates to followers.
func (r *Replicator) ReplicateWrite(data []byte, offset uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Leader writes locally
	r.hw = offset + 1

	req := &ReplicateRequest{
		Topic:     r.topic,
		Partition: r.partition,
		Epoch:     r.epoch,
		Offset:    offset,
		Data:      data,
	}

	// Replicate to ISR
	isrMembers := r.isr.ISRMembers()
	var wg sync.WaitGroup
	errCh := make(chan error, len(isrMembers))

	for _, nodeID := range isrMembers {
		if nodeID == r.leaderID {
			continue
		}
		wg.Add(1)
		go func(nid raft.NodeID) {
			defer wg.Done()
			if r.transport != nil {
				if err := r.transport.Replicate(nid, req); err != nil {
					errCh <- err
				} else {
					r.isr.UpdateLEO(nid, offset+1)
				}
			}
		}(nodeID)
	}
	wg.Wait()
	close(errCh)

	switch r.policy {
	case AckLeader:
		// Already written locally, done
		return nil
	case AckQuorum:
		if r.isr.HasQuorum(offset + 1) {
			return nil
		}
		return fmt.Errorf("quorum not reached for %s:%d offset=%d", r.topic, r.partition, offset)
	case AckAll:
		if r.isr.ISRSize()+1 == len(isrMembers) {
			return nil
		}
		return fmt.Errorf("not all ISR confirmed for %s:%d", r.topic, r.partition)
	}

	return nil
}

// SetEpoch updates the leader epoch (on leadership change).
func (r *Replicator) SetEpoch(epoch uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.epoch = epoch
}

// HW returns the current high watermark.
func (r *Replicator) HW() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hw
}

// ISR returns the ISR set.
func (r *Replicator) ISR() *ISRSet {
	return r.isr
}

// CheckISRHealth periodically checks ISR health.
func (r *Replicator) CheckISRHealth() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.isr.UpdateISR(r.hw)
}

// StartHealthCheck starts periodic ISR health monitoring.
func (r *Replicator) StartHealthCheck(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			r.CheckISRHealth()
		}
	}
}
