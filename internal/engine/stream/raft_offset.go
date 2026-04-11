package stream

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/chimeramq/chimera/internal/cluster/raft"
)

// RaftOffsetStore persists consumer group offsets through Raft consensus.
// When clustering is disabled, it falls back to the local JSON OffsetStore.
type RaftOffsetStore struct {
	local *OffsetStore // always available as fallback and local cache

	mu       sync.RWMutex
	raftNode *raft.RaftNode // nil when clustering is disabled
}

// NewRaftOffsetStore creates an offset store that can optionally replicate
// through Raft. If raftNode is nil, it uses local JSON persistence only.
func NewRaftOffsetStore(dataDir string, raftNode *raft.RaftNode) *RaftOffsetStore {
	return &RaftOffsetStore{
		local:    NewOffsetStore(dataDir),
		raftNode: raftNode,
	}
}

// offsetCommand is the Raft command payload for offset commits.
type offsetCommand struct {
	Group       string          `json:"group"`
	PartitionID uint32          `json:"partition_id"`
	Offset      uint64          `json:"offset"`
}

// Save persists an offset. If Raft is enabled, it proposes the offset
// through Raft consensus. Otherwise it writes to local JSON.
func (s *RaftOffsetStore) Save(group string, partitionID uint32, offset uint64) error {
	if s.raftNode != nil && s.raftNode.IsLeader() {
		cmd := raft.Command{
			Type: raft.CmdCommitOffset,
			Data: mustMarshalOffset(offsetCommand{
				Group:       group,
				PartitionID: partitionID,
				Offset:      offset,
			}),
		}
		_, err := s.raftNode.Propose(mustMarshalOffset(cmd))
		if err != nil {
			// Fall back to local on proposal failure
			return s.local.Save(group, partitionID, offset)
		}
		// Also update local cache immediately for fast reads
		s.mu.Lock()
		s.local.cache[group][partitionID] = offset
		s.mu.Unlock()
		return nil
	}
	return s.local.Save(group, partitionID, offset)
}

// Get returns the committed offset for a group and partition.
// Always reads from local cache for speed.
func (s *RaftOffsetStore) Get(group string, partitionID uint32) uint64 {
	return s.local.Get(group, partitionID)
}

// ApplyOffset applies a replicated offset command to the local store.
// Called by the FSM when a CmdCommitOffset is committed through Raft.
func (s *RaftOffsetStore) ApplyOffset(group string, partitionID uint32, offset uint64) error {
	return s.local.Save(group, partitionID, offset)
}

func mustMarshalOffset(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal offset command: %v", err))
	}
	return data
}
