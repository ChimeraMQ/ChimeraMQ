package cluster

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/cluster/raft"
	"github.com/chimeramq/chimera/internal/cluster/replication"
)

func TestManagerMembersWithSWIM(t *testing.T) {
	cfg := ClusterConfig{
		NodeID:         "node-1",
		DataDir:        t.TempDir(),
		GossipBindPort: 0,
		ProbeInterval:  1 * time.Second,
		ProbeTimeout:   500 * time.Millisecond,
	}
	m, _ := NewManager(cfg)
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	members := m.Members()
	if members == nil {
		t.Log("Members() returned nil — expected with no other nodes")
	}

	count := m.AliveCount()
	if count < 1 {
		t.Errorf("AliveCount = %d, want >= 1", count)
	}
}

func TestManagerIsLeaderAfterStart(t *testing.T) {
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
	defer m.Stop()

	_ = m.IsLeader()
	_ = m.LeaderID()
}

func TestManagerStartParsePeers(t *testing.T) {
	dir := t.TempDir()
	cfg := ClusterConfig{
		NodeID:            "node-1",
		DataDir:           dir,
		Peers:             []string{"127.0.0.1:19000", "127.0.0.1:19001"},
		ElectionTimeout:   500 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		GossipBindPort:    0,
	}
	m, _ := NewManager(cfg)
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	if m.raftNode == nil {
		t.Fatal("raft node should be initialized")
	}
}

func newFSMOnlyManager(t *testing.T) (*Manager, *raft.MetadataFSM) {
	t.Helper()
	dir := t.TempDir()
	raftCfg := raft.Config{
		NodeID:            "node-1",
		Peers:             []raft.NodeID{"node-2"},
		ElectionTimeout:   500 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		SnapshotInterval:  1 * time.Hour,
		DataDir:           dir,
	}
	node, _ := raft.NewRaftNode(raftCfg)
	// Don't call Start/Stop — just use FSM directly

	cfg := ClusterConfig{NodeID: "node-1", DataDir: dir}
	m, _ := NewManager(cfg)
	m.raftNode = node
	return m, node.FSM()
}

func TestManagerDeleteTopicFSM(t *testing.T) {
	m, fsm := newFSMOnlyManager(t)

	cmdData, _ := json.Marshal(raft.Command{
		Type: raft.CmdCreateTopic,
		Data: mustMarshal(raft.TopicEntry{Name: "del-me", Mode: "stream", Partitions: 1}),
	})
	fsm.Apply(raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryCommand, Data: cmdData})

	if m.GetTopic("del-me") == nil {
		t.Fatal("topic should exist")
	}

	delCmd, _ := json.Marshal(raft.Command{
		Type: raft.CmdDeleteTopic,
		Data: mustMarshal(map[string]string{"name": "del-me"}),
	})
	fsm.Apply(raft.LogEntry{Index: 2, Term: 1, Type: raft.EntryCommand, Data: delCmd})

	if m.GetTopic("del-me") != nil {
		t.Error("topic should be deleted")
	}
}

func TestManagerAssignmentFSM(t *testing.T) {
	m, fsm := newFSMOnlyManager(t)

	paCmd, _ := json.Marshal(raft.Command{
		Type: raft.CmdAssignPartition,
		Data: mustMarshal(raft.PartitionAssignment{
			Topic: "test-topic", Partition: 0, Leader: "node-1", Replicas: []raft.NodeID{"node-1", "node-2"},
		}),
	})
	fsm.Apply(raft.LogEntry{Index: 1, Term: 1, Type: raft.EntryCommand, Data: paCmd})

	pa := m.GetAssignment("test-topic", 0)
	if pa == nil {
		t.Fatal("assignment should exist")
	}
	if pa.Leader != raft.NodeID("node-1") {
		t.Errorf("leader = %q, want node-1", pa.Leader)
	}
}

func mustMarshal(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}

func TestReplicationTransportAdapterReplicate(t *testing.T) {
	m, _ := newFSMOnlyManager(t)
	adapter := &replicationTransportAdapter{raftNode: m.raftNode}

	err := adapter.Replicate("node-2", &replication.ReplicateRequest{Data: []byte("test")})
	if err == nil {
		t.Error("expected error when replicating through non-leader raft node")
	}
}

func TestReplicationTransportAdapterFetchEntries(t *testing.T) {
	m, _ := newFSMOnlyManager(t)
	adapter := &replicationTransportAdapter{raftNode: m.raftNode}

	resp, err := adapter.FetchEntries("node-2", &replication.FetchRequest{})
	if err != nil {
		t.Fatalf("FetchEntries error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

