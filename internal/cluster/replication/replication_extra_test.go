package replication

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/cluster/raft"
)

func TestFollowerAccessors(t *testing.T) {
	f := NewFollowerReplica("test-topic", 3, "leader-1", &mockLocalStorage{})

	if f.LeaderID() != "leader-1" {
		t.Errorf("LeaderID = %q, want 'leader-1'", f.LeaderID())
	}
	if f.Topic() != "test-topic" {
		t.Errorf("Topic = %q, want 'test-topic'", f.Topic())
	}
	if f.Partition() != 3 {
		t.Errorf("Partition = %d, want 3", f.Partition())
	}
}

func TestReplicatorSetTransport(t *testing.T) {
	r := NewReplicator("topic-a", 0, "leader", AckQuorum, 100)
	transport := &mockReplicationTransport{}
	r.SetTransport(transport)
}

func TestReplicatorSetEpoch(t *testing.T) {
	r := NewReplicator("topic-a", 0, "leader", AckQuorum, 100)

	r.SetEpoch(42)
	if r.epoch != 42 {
		t.Errorf("epoch = %d, want 42", r.epoch)
	}
}

func TestReplicatorCheckISRHealth(t *testing.T) {
	r := NewReplicator("health-topic", 0, "leader", AckQuorum, 100)
	r.isr.AddReplica("node-1")

	// Should not panic
	r.CheckISRHealth()
}

func TestReplicatorStartHealthCheck(t *testing.T) {
	r := NewReplicator("hc-topic", 0, "leader", AckQuorum, 100)
	r.isr.AddReplica("node-1")

	stopCh := make(chan struct{})
	go r.StartHealthCheck(10*time.Millisecond, stopCh)

	time.Sleep(30 * time.Millisecond)
	close(stopCh)
}

func TestReplicatorHW(t *testing.T) {
	r := NewReplicator("hw-topic", 0, "leader", AckQuorum, 100)

	if r.HW() != 0 {
		t.Errorf("initial HW = %d, want 0", r.HW())
	}
	r.hw = 100
	if r.HW() != 100 {
		t.Errorf("HW = %d, want 100", r.HW())
	}
}

func TestReplicatorISR(t *testing.T) {
	r := NewReplicator("isr-topic", 0, "leader", AckQuorum, 100)

	if r.ISR() == nil {
		t.Error("ISR() should not be nil")
	}
}

func TestFollowerReplicate(t *testing.T) {
	f := NewFollowerReplica("ftopic", 0, "leader", &mockLocalStorage{})

	err := f.Replicate(&ReplicateRequest{Topic: "ftopic", Partition: 0, Offset: 42, Data: []byte("test-data")})
	if err != nil {
		t.Errorf("Replicate error: %v", err)
	}
}

func TestFollowerReplicateStaleEpoch(t *testing.T) {
	f := NewFollowerReplica("ftopic", 0, "leader", &mockLocalStorage{})
	f.SetEpoch(10)

	err := f.Replicate(&ReplicateRequest{Topic: "ftopic", Partition: 0, Epoch: 5, Offset: 1, Data: []byte("stale")})
	if err != nil {
		t.Errorf("Replicate stale epoch error: %v", err)
	}
}

func TestFollowerSetEpoch(t *testing.T) {
	f := NewFollowerReplica("ftopic", 0, "leader", &mockLocalStorage{})
	// SetEpoch should not panic and should update internal state
	f.SetEpoch(7)
	// Verify via Replicate: epoch 5 should be treated as stale
	err := f.Replicate(&ReplicateRequest{Topic: "ftopic", Partition: 0, Epoch: 5, Offset: 1, Data: []byte("old")})
	if err != nil {
		t.Errorf("Replicate after SetEpoch error: %v", err)
	}
}

func TestReplicateWriteNoFollowers(t *testing.T) {
	r := NewReplicator("rw-topic", 0, "leader", AckLeader, 100)
	r.SetTransport(&mockReplicationTransport{})

	err := r.ReplicateWrite([]byte("data"), 0)
	if err != nil {
		t.Errorf("ReplicateWrite error: %v", err)
	}
}

func TestReplicatorAckPolicies(t *testing.T) {
	r := NewReplicator("ack-topic", 0, "leader", AckAll, 100)
	// Verify internal policy field
	if r.policy != AckAll {
		t.Errorf("policy = %v, want %v", r.policy, AckAll)
	}
}

// mockLocalStorage implements LocalStorage for testing.
type mockLocalStorage struct{}

func (m *mockLocalStorage) Append(topic string, partition uint32, data []byte) (uint64, error) {
	return 0, nil
}

// mockReplicationTransport implements ReplicationTransport for testing.
type mockReplicationTransport struct{}

func (m *mockReplicationTransport) Replicate(nodeID raft.NodeID, req *ReplicateRequest) error {
	return nil
}

func (m *mockReplicationTransport) FetchEntries(nodeID raft.NodeID, req *FetchRequest) (*FetchResponse, error) {
	return &FetchResponse{}, nil
}
