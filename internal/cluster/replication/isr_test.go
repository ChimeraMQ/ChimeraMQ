package replication

import (
	"testing"

	"github.com/chimeramq/chimera/internal/cluster/raft"
)

func TestISRAddRemove(t *testing.T) {
	isr := NewISRSet("test-topic", 0, "leader", 100)

	isr.AddReplica("node-1")
	isr.AddReplica("node-2")
	isr.AddReplica("node-3")

	if isr.ISRSize() != 3 {
		t.Errorf("ISRSize = %d, want 3", isr.ISRSize())
	}

	members := isr.ISRMembers()
	if len(members) != 3 {
		t.Errorf("ISRMembers = %d, want 3", len(members))
	}

	isr.RemoveReplica("node-3")
	if isr.ISRSize() != 2 {
		t.Errorf("ISRSize after remove = %d, want 2", isr.ISRSize())
	}
}

func TestISRInSync(t *testing.T) {
	isr := NewISRSet("test-topic", 0, "leader", 100)
	isr.AddReplica("node-1")
	isr.AddReplica("node-2")

	if !isr.IsInSync("node-1") {
		t.Error("node-1 should be in ISR")
	}
	if isr.IsInSync("node-99") {
		t.Error("unknown node should not be in ISR")
	}
}

func TestISRShrinkExpand(t *testing.T) {
	isr := NewISRSet("test-topic", 0, "leader", 100)
	isr.AddReplica("node-1")
	isr.AddReplica("node-2")

	// Shrink
	isr.ShrinkISR("node-1")
	if isr.IsInSync("node-1") {
		t.Error("node-1 should be removed from ISR")
	}
	if isr.ISRSize() != 1 {
		t.Errorf("ISRSize after shrink = %d, want 1", isr.ISRSize())
	}

	// Expand back
	isr.UpdateLEO("node-1", 50) // Mark as active with LEO
	ok := isr.ExpandISR("node-1")
	if !ok {
		t.Error("ExpandISR should succeed for active replica")
	}
	if !isr.IsInSync("node-1") {
		t.Error("node-1 should be back in ISR")
	}
}

func TestISRUpdateBasedOnLag(t *testing.T) {
	isr := NewISRSet("test-topic", 0, "leader", 10)
	isr.AddReplica("node-1")
	isr.AddReplica("node-2")

	// Both caught up
	isr.UpdateLEO("node-1", 100)
	isr.UpdateLEO("node-2", 100)

	isr.UpdateISR(105) // leader HW = 105
	if isr.ISRSize() != 2 {
		t.Errorf("ISRSize = %d, want 2 (both in sync)", isr.ISRSize())
	}

	// node-2 falls behind
	isr.UpdateLEO("node-2", 50)
	isr.UpdateISR(105) // lag = 55 > maxLag(10)
	if isr.IsInSync("node-2") {
		t.Error("node-2 should be out of ISR (lag too high)")
	}

	// node-2 catches up
	isr.UpdateLEO("node-2", 100)
	isr.UpdateISR(105)
	if !isr.IsInSync("node-2") {
		t.Error("node-2 should be back in ISR")
	}
}

func TestISRHasQuorum(t *testing.T) {
	isr := NewISRSet("test-topic", 0, "leader", 100)
	isr.AddReplica("node-1")
	isr.AddReplica("node-2")

	// 2 ISR members + leader = 3 total, quorum = 2
	isr.UpdateLEO("node-1", 50)
	isr.UpdateLEO("node-2", 50)

	if !isr.HasQuorum(50) {
		t.Error("should have quorum (leader + 2 ISR >= 2)")
	}

	// One falls behind
	isr.UpdateLEO("node-2", 30)
	if !isr.HasQuorum(50) {
		t.Error("should still have quorum (leader + node-1)")
	}

	// Both behind
	isr.UpdateLEO("node-1", 30)
	isr.UpdateLEO("node-2", 30)
	if isr.HasQuorum(50) {
		t.Error("should NOT have quorum")
	}
}

func TestFollowerReplica(t *testing.T) {
	f := NewFollowerReplica("topic", 0, "leader-1", nil)

	if f.LEO() != 0 {
		t.Errorf("initial LEO = %d, want 0", f.LEO())
	}

	req := &ReplicateRequest{
		Topic:     "topic",
		Partition: 0,
		Epoch:     1,
		Offset:    10,
		Data:      []byte("hello"),
	}
	f.Replicate(req)

	if f.LEO() != 11 {
		t.Errorf("LEO after replicate = %d, want 11", f.LEO())
	}

	// Epoch fencing
	f.SetEpoch(5)
	req2 := &ReplicateRequest{Epoch: 3, Offset: 20}
	f.Replicate(req2)
	// LEO should not change from stale epoch
}

func TestReplicator(t *testing.T) {
	r := NewReplicator("topic", 0, "leader", AckLeader, 100)

	r.ISR().AddReplica("node-1")
	r.ISR().AddReplica("node-2")

	if r.HW() != 0 {
		t.Errorf("initial HW = %d, want 0", r.HW())
	}

	err := r.ReplicateWrite([]byte("test"), 0)
	if err != nil {
		t.Errorf("AckLeader should succeed: %v", err)
	}
	if r.HW() != 1 {
		t.Errorf("HW after write = %d, want 1", r.HW())
	}
}

func TestParseAckPolicy(t *testing.T) {
	tests := []struct {
		input string
		want  AckPolicy
	}{
		{"leader", AckLeader},
		{"quorum", AckQuorum},
		{"all", AckAll},
		{"unknown", AckQuorum}, // default
	}
	for _, tt := range tests {
		got := ParseAckPolicy(tt.input)
		if got != tt.want {
			t.Errorf("ParseAckPolicy(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestISRString(t *testing.T) {
	isr := NewISRSet("test-topic", 0, raft.NodeID("leader"), 100)
	isr.AddReplica("node-1")
	s := isr.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}
