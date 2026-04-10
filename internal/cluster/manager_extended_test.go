package cluster

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/cluster/raft"
)

func TestManagerStartStop(t *testing.T) {
	dir := t.TempDir()
	cfg := ClusterConfig{
		NodeID:            "node-1",
		DataDir:           dir,
		Peers:             []string{"127.0.0.1:9000", "127.0.0.1:9001"},
		ElectionTimeout:   500 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		SnapshotInterval:  1 * time.Hour,
		MaxLogEntries:     100000,
		GossipBindPort:    0,
		ProbeInterval:     1 * time.Second,
		ProbeTimeout:      500 * time.Millisecond,
		ReplicationFactor: 3,
		MinISR:            2,
		AckPolicy:         "quorum",
	}

	m, err := NewManager(cfg)
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Start(); err != nil {
		t.Fatal(err)
	}

	// Verify components are initialized
	if m.RaftNode() == nil {
		t.Error("raft node should not be nil after Start")
	}
	if m.FSM() == nil {
		t.Error("FSM should not be nil after Start")
	}

	m.Stop()
}

func TestManagerStartIdempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := ClusterConfig{
		NodeID:          "node-1",
		DataDir:         dir,
		ElectionTimeout: 500 * time.Millisecond,
		GossipBindPort:  0,
	}

	m, _ := NewManager(cfg)
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}

	// Second start should be no-op
	if err := m.Start(); err != nil {
		t.Fatalf("second Start: %v", err)
	}

	m.Stop()
}

func marshalCmd(cmdType raft.CommandType, data interface{}) ([]byte, error) {
	raw, _ := json.Marshal(data)
	cmd := raft.Command{Type: cmdType, Data: raw}
	return json.Marshal(cmd)
}

func TestManagerWithRaftNode(t *testing.T) {
	dir := t.TempDir()
	raftCfg := raft.Config{
		NodeID:            "node-1",
		Peers:             []raft.NodeID{"node-2"},
		ElectionTimeout:   500 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		SnapshotInterval:  1 * time.Hour,
		DataDir:           dir,
	}
	node, err := raft.NewRaftNode(raftCfg)
	if err != nil {
		t.Fatal(err)
	}

	cfg := ClusterConfig{NodeID: "node-1", DataDir: dir}
	m, _ := NewManager(cfg)
	m.raftNode = node

	// Test FSM delegates correctly
	fsm := m.FSM()
	if fsm == nil {
		t.Fatal("FSM should not be nil")
	}

	// Apply a topic creation directly to the FSM
	cmdData, _ := marshalCmd(raft.CmdCreateTopic, raft.TopicEntry{
		Name: "test-topic", Mode: "stream", Partitions: 4,
	})
	fsm.Apply(raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryCommand, Data: cmdData})

	topic := m.GetTopic("test-topic")
	if topic == nil {
		t.Fatal("GetTopic should return the topic")
	}
	if topic.Partitions != 4 {
		t.Errorf("partitions = %d, want 4", topic.Partitions)
	}

	topics := m.ListTopics()
	if len(topics) != 1 {
		t.Errorf("ListTopics = %d, want 1", len(topics))
	}

	// Test assignment
	paCmdData, _ := marshalCmd(raft.CmdAssignPartition, raft.PartitionAssignment{
		Topic: "test-topic", Partition: 0, Leader: "node-1",
	})
	fsm.Apply(raft.LogEntry{Index: 2, Term: 1, Type: raft.EntryCommand, Data: paCmdData})

	pa := m.GetAssignment("test-topic", 0)
	if pa == nil {
		t.Fatal("GetAssignment should return assignment")
	}
	if pa.Leader != raft.NodeID("node-1") {
		t.Errorf("leader = %q, want node-1", pa.Leader)
	}
}

func TestManagerStopAfterStart(t *testing.T) {
	dir := t.TempDir()
	cfg := ClusterConfig{
		NodeID:          "node-1",
		DataDir:         dir,
		ElectionTimeout: 500 * time.Millisecond,
		GossipBindPort:  0,
	}

	m, _ := NewManager(cfg)
	m.Start()

	// IsLeader should work (returns false since no election completed yet)
	_ = m.IsLeader()
	_ = m.LeaderID()

	m.Stop()

	// After stop, raft node is still set but stopped
	if m.RaftNode() == nil {
		t.Error("RaftNode should still be accessible after Stop")
	}
}
