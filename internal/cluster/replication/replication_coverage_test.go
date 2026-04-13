package replication

import (
	"errors"
	"testing"

	"github.com/chimeramq/chimera/internal/cluster/raft"
)

// mockFailingReplicationTransport returns errors for specific nodes.
type mockFailingReplicationTransport struct {
	failNodes map[raft.NodeID]bool
}

func (m *mockFailingReplicationTransport) Replicate(nodeID raft.NodeID, req *ReplicateRequest) error {
	if m.failNodes[nodeID] {
		return errors.New("transport error")
	}
	return nil
}

func (m *mockFailingReplicationTransport) FetchEntries(nodeID raft.NodeID, req *FetchRequest) (*FetchResponse, error) {
	return &FetchResponse{}, nil
}

func TestReplicateWriteAckQuorumSuccess(t *testing.T) {
	r := NewReplicator("quorum-topic", 0, "leader", AckQuorum, 100)
	r.isr.AddReplica("node-1")
	r.isr.AddReplica("node-2")
	r.isr.UpdateLEO("node-1", 1)
	r.isr.UpdateLEO("node-2", 1)

	r.SetTransport(&mockReplicationTransport{})

	err := r.ReplicateWrite([]byte("data"), 0)
	if err != nil {
		t.Errorf("AckQuorum should succeed with all ISR confirming: %v", err)
	}
}

func TestReplicateWriteAckQuorumFailure(t *testing.T) {
	r := NewReplicator("quorum-topic", 0, "leader", AckQuorum, 100)
	r.isr.AddReplica("node-1")
	r.isr.AddReplica("node-2")
	// Only leader has offset, followers don't — quorum = 2 needed (3 total, > 3/2 = 1)
	// Actually count=1, isrSize=3, count > 1 is false, so it should fail

	r.SetTransport(&mockFailingReplicationTransport{
		failNodes: map[raft.NodeID]bool{"node-1": true, "node-2": true},
	})

	err := r.ReplicateWrite([]byte("data"), 0)
	if err == nil {
		t.Error("AckQuorum should fail when followers don't confirm")
	}
}

func TestReplicateWriteAckAllSuccess(t *testing.T) {
	r := NewReplicator("all-topic", 0, "leader", AckAll, 100)
	r.isr.AddReplica("node-1")
	r.isr.UpdateLEO("node-1", 1)

	r.SetTransport(&mockReplicationTransport{})

	err := r.ReplicateWrite([]byte("data"), 0)
	if err != nil {
		t.Errorf("AckAll should succeed with all confirming: %v", err)
	}
}

func TestReplicateWriteAckAllFailure(t *testing.T) {
	r := NewReplicator("all-topic", 0, "leader", AckAll, 100)
	r.isr.AddReplica("node-1")
	r.isr.AddReplica("node-2")

	// Only node-1 confirms, node-2 fails
	r.SetTransport(&mockFailingReplicationTransport{
		failNodes: map[raft.NodeID]bool{"node-2": true},
	})

	err := r.ReplicateWrite([]byte("data"), 0)
	if err == nil {
		t.Error("AckAll should fail when not all ISR confirm")
	}
}

func TestReplicateWriteTransportErrors(t *testing.T) {
	r := NewReplicator("err-topic", 0, "leader", AckLeader, 100)
	r.isr.AddReplica("node-1")
	r.isr.AddReplica("node-2")

	r.SetTransport(&mockFailingReplicationTransport{
		failNodes: map[raft.NodeID]bool{"node-1": true, "node-2": true},
	})

	// AckLeader should succeed regardless of transport errors
	err := r.ReplicateWrite([]byte("data"), 0)
	if err != nil {
		t.Errorf("AckLeader should succeed despite transport errors: %v", err)
	}
}

func TestExpandISRInactiveReplica(t *testing.T) {
	isr := NewISRSet("test-topic", 0, "leader", 100)
	isr.AddReplica("node-1")
	isr.ShrinkISR("node-1")

	// Mark as inactive by updating LEO on a non-existent node won't work,
	// so we need to manipulate internal state directly
	isr.mu.Lock()
	state := isr.replicas["node-1"]
	state.Active = false
	isr.replicas["node-1"] = state
	isr.mu.Unlock()

	ok := isr.ExpandISR("node-1")
	if ok {
		t.Error("ExpandISR should fail for inactive replica")
	}
}

func TestReplicateWriteNoTransport(t *testing.T) {
	r := NewReplicator("no-transport", 0, "leader", AckQuorum, 100)
	r.isr.AddReplica("node-1")

	// No transport set
	err := r.ReplicateWrite([]byte("data"), 0)
	if err == nil {
		t.Error("AckQuorum should fail without transport")
	}
}
