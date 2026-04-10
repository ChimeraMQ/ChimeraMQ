package replication

import (
	"fmt"

	"github.com/chimeramq/chimera/internal/cluster/raft"
)

// LocalStorage is the interface used by FollowerReplica to persist replicated data.
type LocalStorage interface {
	Append(topic string, partition uint32, data []byte) (uint64, error)
}

// ReplicateRequest is sent from leader to follower.
type ReplicateRequest struct {
	Topic     string
	Partition uint32
	Epoch     uint64
	Offset    uint64
	Data      []byte
}

// FetchRequest is sent from leader to follower to pull entries for catch-up.
type FetchRequest struct {
	Topic     string
	Partition uint32
	Offset    uint64
	MaxBytes  int
}

// FetchResponse contains entries for catch-up.
type FetchResponse struct {
	Entries []ReplicateRequest
	LEO     uint64
}

// FollowerReplica manages replication from a leader on a follower node.
type FollowerReplica struct {
	topic      string
	partition  uint32
	leaderID   raft.NodeID
	localEpoch uint64
	leo        uint64
	storage    LocalStorage
}

// NewFollowerReplica creates a new follower replica handler.
func NewFollowerReplica(topic string, partition uint32, leaderID raft.NodeID, storage LocalStorage) *FollowerReplica {
	return &FollowerReplica{
		topic:     topic,
		partition: partition,
		leaderID:  leaderID,
		storage:   storage,
	}
}

// Replicate receives entries from the leader and appends locally.
func (f *FollowerReplica) Replicate(req *ReplicateRequest) error {
	// Epoch check: reject stale writes
	if req.Epoch < f.localEpoch {
		return nil // stale, ignore
	}
	f.localEpoch = req.Epoch

	// Persist the data to local storage
	if f.storage != nil && len(req.Data) > 0 {
		offset, err := f.storage.Append(req.Topic, req.Partition, req.Data)
		if err != nil {
			return fmt.Errorf("local append: %w", err)
		}
		f.leo = offset + 1
	} else {
		f.leo = req.Offset + 1
	}

	return nil
}

// LEO returns the follower's log end offset.
func (f *FollowerReplica) LEO() uint64 {
	return f.leo
}

// SetEpoch sets the current leader epoch.
func (f *FollowerReplica) SetEpoch(epoch uint64) {
	if epoch > f.localEpoch {
		f.localEpoch = epoch
	}
}

// LeaderID returns the current leader.
func (f *FollowerReplica) LeaderID() raft.NodeID {
	return f.leaderID
}

// Topic returns the topic name.
func (f *FollowerReplica) Topic() string {
	return f.topic
}

// Partition returns the partition ID.
func (f *FollowerReplica) Partition() uint32 {
	return f.partition
}
