package raft

import (
	"encoding/json"
	"fmt"
	"sync"
)

// CommandType identifies a metadata command.
type CommandType string

const (
	CmdCreateTopic     CommandType = "create_topic"
	CmdDeleteTopic     CommandType = "delete_topic"
	CmdAssignPartition CommandType = "assign_partition"
	CmdJoinGroup       CommandType = "join_group"
	CmdLeaveGroup      CommandType = "leave_group"
	CmdCommitOffset    CommandType = "commit_offset"
)

// Command is a metadata operation applied via Raft.
type Command struct {
	Type CommandType     `json:"type"`
	Data json.RawMessage `json:"data"`
}

// TopicEntry stores topic metadata in the FSM.
type TopicEntry struct {
	Name       string   `json:"name"`
	Mode       string   `json:"mode"`
	Partitions uint32   `json:"partitions"`
	ReplicaSet []NodeID `json:"replica_set"`
}

// PartitionAssignment maps a partition to its replicas.
type PartitionAssignment struct {
	Topic     string   `json:"topic"`
	Partition uint32   `json:"partition"`
	Leader    NodeID   `json:"leader"`
	Replicas  []NodeID `json:"replicas"`
}

// GroupMember is a consumer group member.
type GroupMember struct {
	ID         string   `json:"id"`
	Topics     []string `json:"topics"`
	Partitions []uint32 `json:"partitions"`
}

// GroupEntry stores consumer group state in the FSM.
type GroupEntry struct {
	Name    string                  `json:"name"`
	Members map[string]*GroupMember `json:"members"`
}

// MetadataFSM is the finite state machine for cluster metadata.
type MetadataFSM struct {
	mu          sync.RWMutex
	topics      map[string]*TopicEntry
	assignments map[string]*PartitionAssignment // key: "topic:partition"
	groups      map[string]*GroupEntry
}

// NewMetadataFSM creates a new metadata FSM.
func NewMetadataFSM() *MetadataFSM {
	return &MetadataFSM{
		topics:      make(map[string]*TopicEntry),
		assignments: make(map[string]*PartitionAssignment),
		groups:      make(map[string]*GroupEntry),
	}
}

// Apply applies a committed log entry to the state machine.
func (f *MetadataFSM) Apply(entry LogEntry) error {
	var cmd Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return err
	}

	switch cmd.Type {
	case CmdCreateTopic:
		return f.applyCreateTopic(cmd.Data)
	case CmdDeleteTopic:
		return f.applyDeleteTopic(cmd.Data)
	case CmdAssignPartition:
		return f.applyAssignPartition(cmd.Data)
	case CmdJoinGroup:
		return f.applyJoinGroup(cmd.Data)
	case CmdLeaveGroup:
		return f.applyLeaveGroup(cmd.Data)
	case CmdCommitOffset:
		return nil // Offset commits applied by RaftOffsetStore directly
	}
	return nil
}

func (f *MetadataFSM) applyCreateTopic(data json.RawMessage) error {
	var entry TopicEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topics[entry.Name] = &entry
	return nil
}

func (f *MetadataFSM) applyDeleteTopic(data json.RawMessage) error {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.topics, req.Name)
	// Clean up partition assignments
	prefix := req.Name + ":"
	for k := range f.assignments {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(f.assignments, k)
		}
	}
	return nil
}

func (f *MetadataFSM) applyAssignPartition(data json.RawMessage) error {
	var pa PartitionAssignment
	if err := json.Unmarshal(data, &pa); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := pa.Topic + ":" + itoa(pa.Partition)
	f.assignments[key] = &pa
	return nil
}

func (f *MetadataFSM) applyJoinGroup(data json.RawMessage) error {
	var req struct {
		Group  string   `json:"group"`
		Member string   `json:"member"`
		Topics []string `json:"topics"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	grp, ok := f.groups[req.Group]
	if !ok {
		grp = &GroupEntry{Name: req.Group, Members: make(map[string]*GroupMember)}
		f.groups[req.Group] = grp
	}
	grp.Members[req.Member] = &GroupMember{
		ID:     req.Member,
		Topics: req.Topics,
	}
	return nil
}

func (f *MetadataFSM) applyLeaveGroup(data json.RawMessage) error {
	var req struct {
		Group  string `json:"group"`
		Member string `json:"member"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if grp, ok := f.groups[req.Group]; ok {
		delete(grp.Members, req.Member)
		if len(grp.Members) == 0 {
			delete(f.groups, req.Group)
		}
	}
	return nil
}

// GetTopic returns topic metadata.
func (f *MetadataFSM) GetTopic(name string) *TopicEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.topics[name]
}

// ListTopics returns all topics.
func (f *MetadataFSM) ListTopics() []*TopicEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	result := make([]*TopicEntry, 0, len(f.topics))
	for _, t := range f.topics {
		result = append(result, t)
	}
	return result
}

// GetAssignment returns partition assignment.
func (f *MetadataFSM) GetAssignment(topic string, partition uint32) *PartitionAssignment {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.assignments[topic+":"+itoa(partition)]
}

// GetGroup returns consumer group state.
func (f *MetadataFSM) GetGroup(name string) *GroupEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.groups[name]
}

// Snapshot returns a serializable snapshot of the FSM state.
func (f *MetadataFSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	snap := struct {
		Topics      map[string]*TopicEntry          `json:"topics"`
		Assignments map[string]*PartitionAssignment `json:"assignments"`
		Groups      map[string]*GroupEntry          `json:"groups"`
	}{
		Topics:      f.topics,
		Assignments: f.assignments,
		Groups:      f.groups,
	}
	return json.Marshal(snap)
}

// Restore restores FSM state from a snapshot.
func (f *MetadataFSM) Restore(data []byte) error {
	var snap struct {
		Topics      map[string]*TopicEntry          `json:"topics"`
		Assignments map[string]*PartitionAssignment `json:"assignments"`
		Groups      map[string]*GroupEntry          `json:"groups"`
	}
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topics = snap.Topics
	f.assignments = snap.Assignments
	f.groups = snap.Groups
	return nil
}

func itoa(n uint32) string {
	return fmt.Sprintf("%d", n)
}
