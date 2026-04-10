package cluster

import (
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	cfg := ClusterConfig{
		NodeID:            "node-1",
		DataDir:           t.TempDir(),
		Peers:             []string{"localhost:5673"},
		GossipBindPort:    5674,
		ReplicationFactor: 3,
		MinISR:            2,
		AckPolicy:         "quorum",
	}

	m, err := NewManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if m == nil {
		t.Fatal("manager should not be nil")
	}
}

func TestManagerNotStarted(t *testing.T) {
	cfg := ClusterConfig{NodeID: "node-1", DataDir: t.TempDir()}
	m, _ := NewManager(cfg)

	if m.IsLeader() {
		t.Error("should not be leader when not started")
	}
	if m.LeaderID() != "" {
		t.Error("leader ID should be empty when not started")
	}
	if members := m.Members(); members != nil {
		t.Error("members should be nil when not started")
	}
	if count := m.AliveCount(); count != 1 {
		t.Errorf("alive count = %d, want 1", count)
	}
	if topic := m.GetTopic("test"); topic != nil {
		t.Error("topic should be nil when not started")
	}
	if list := m.ListTopics(); list != nil {
		t.Error("topic list should be nil when not started")
	}
	if pa := m.GetAssignment("test", 0); pa != nil {
		t.Error("assignment should be nil when not started")
	}
}

func TestManagerStopIdempotent(t *testing.T) {
	cfg := ClusterConfig{NodeID: "node-1", DataDir: t.TempDir()}
	m, _ := NewManager(cfg)

	// Stop without start should be safe
	m.Stop()
	m.Stop() // Double stop should not panic
}

func TestManagerProposeNotStarted(t *testing.T) {
	cfg := ClusterConfig{NodeID: "node-1", DataDir: t.TempDir()}
	m, _ := NewManager(cfg)

	if err := m.ProposeCreateTopic("test", "stream", 4, nil); err == nil {
		t.Error("expected error when proposing without raft")
	}
	if err := m.ProposeDeleteTopic("test"); err == nil {
		t.Error("expected error when proposing without raft")
	}
	if err := m.AssignPartition("test", 0, "node-1", nil); err == nil {
		t.Error("expected error when assigning without raft")
	}
}

func TestManagerRaftNodeNil(t *testing.T) {
	cfg := ClusterConfig{NodeID: "node-1", DataDir: t.TempDir()}
	m, _ := NewManager(cfg)

	if m.RaftNode() != nil {
		t.Error("raft node should be nil before start")
	}
	if m.FSM() != nil {
		t.Error("FSM should be nil before start")
	}
}

func TestManagerNewReplicator(t *testing.T) {
	cfg := ClusterConfig{
		NodeID:    "node-1",
		DataDir:   t.TempDir(),
		AckPolicy: "quorum",
	}
	m, _ := NewManager(cfg)

	rep := m.NewReplicator("test-topic", 0)
	if rep == nil {
		t.Fatal("replicator should not be nil")
	}
}

func TestToNodeIDs(t *testing.T) {
	input := []string{"node-1", "node-2", "node-3"}
	ids := toNodeIDs(input)
	if len(ids) != 3 {
		t.Fatalf("expected 3 IDs, got %d", len(ids))
	}
	for i, id := range ids {
		if string(id) != input[i] {
			t.Errorf("ids[%d] = %q, want %q", i, id, input[i])
		}
	}
}

func TestToNodeIDsEmpty(t *testing.T) {
	ids := toNodeIDs(nil)
	if len(ids) != 0 {
		t.Errorf("expected empty slice, got %d", len(ids))
	}
}

func TestClusterConfigDefaults(t *testing.T) {
	cfg := ClusterConfig{
		NodeID:            "node-1",
		DataDir:           t.TempDir(),
		Peers:             []string{"a:5673", "b:5673", "c:5673"},
		ElectionTimeout:   time.Second,
		HeartbeatInterval: 150 * time.Millisecond,
		SnapshotInterval:  5 * time.Minute,
		MaxLogEntries:     100000,
		GossipBindPort:    5674,
		GossipSeeds:       []string{"a:5674", "b:5674"},
		ProbeInterval:     time.Second,
		ProbeTimeout:      500 * time.Millisecond,
		IndirectNodes:     3,
		SuspicionTimeout:  5 * time.Second,
		ReplicationFactor: 3,
		MinISR:            2,
		AckPolicy:         "quorum",
		SyncMode:          "sync",
		MaxLag:            10000,
	}

	m, err := NewManager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if m.nodeID != "node-1" {
		t.Errorf("nodeID = %q, want %q", m.nodeID, "node-1")
	}
}
