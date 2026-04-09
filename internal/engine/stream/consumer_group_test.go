package stream

import (
	"os"
	"testing"
)

func TestConsumerGroupJoin(t *testing.T) {
	cg := newTestGroup(t)
	defer cg.Stop()

	cg.Join("m1")
	members := cg.Members()
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if _, ok := members["m1"]; !ok {
		t.Error("member m1 not found")
	}
}

func TestConsumerGroupJoinMultiple(t *testing.T) {
	cg := newTestGroup(t)
	defer cg.Stop()

	cg.Join("m1")
	cg.Join("m2")
	cg.Join("m3")

	if len(cg.Members()) != 3 {
		t.Errorf("expected 3 members, got %d", len(cg.Members()))
	}
}

func TestConsumerGroupLeave(t *testing.T) {
	cg := newTestGroup(t)
	defer cg.Stop()

	cg.Join("m1")
	cg.Join("m2")
	cg.Leave("m1")

	if len(cg.Members()) != 1 {
		t.Errorf("expected 1 member after leave, got %d", len(cg.Members()))
	}
}

func TestConsumerGroupRangeAssignment(t *testing.T) {
	// 4 partitions, 2 members, range → m1 gets [0,1], m2 gets [2,3]
	cg := NewConsumerGroup("test", "topic", 4, StrategyRange, newTestOffsetStore(t))
	defer cg.Stop()

	cg.Join("m1")
	cg.Join("m2")

	assignments := cg.Assignments()
	if len(assignments) != 4 {
		t.Fatalf("expected 4 assignments, got %d", len(assignments))
	}
	if assignments[0] != "m1" || assignments[1] != "m1" {
		t.Errorf("expected m1 for partitions 0,1")
	}
	if assignments[2] != "m2" || assignments[3] != "m2" {
		t.Errorf("expected m2 for partitions 2,3")
	}
}

func TestConsumerGroupRoundRobinAssignment(t *testing.T) {
	cg := NewConsumerGroup("test", "topic", 4, StrategyRoundRobin, newTestOffsetStore(t))
	defer cg.Stop()

	cg.Join("m1")
	cg.Join("m2")

	assignments := cg.Assignments()
	if assignments[0] != "m1" || assignments[1] != "m2" ||
		assignments[2] != "m1" || assignments[3] != "m2" {
		t.Errorf("unexpected round-robin: %v", assignments)
	}
}

func TestConsumerGroupHeartbeat(t *testing.T) {
	cg := newTestGroup(t)
	defer cg.Stop()

	cg.Join("m1")
	if err := cg.Heartbeat("m1"); err != nil {
		t.Errorf("heartbeat failed: %v", err)
	}
	if err := cg.Heartbeat("unknown"); err == nil {
		t.Error("expected error for unknown member")
	}
}

func TestConsumerGroupCommitOffset(t *testing.T) {
	cg := newTestGroup(t)
	defer cg.Stop()

	cg.Join("m1")
	cg.CommitOffset(0, 42)
	cg.CommitOffset(1, 7)

	if cg.GetCommittedOffset(0) != 42 {
		t.Errorf("expected 42, got %d", cg.GetCommittedOffset(0))
	}
	if cg.GetCommittedOffset(1) != 7 {
		t.Errorf("expected 7, got %d", cg.GetCommittedOffset(1))
	}
}

func TestConsumerGroupRebalanceOnLeave(t *testing.T) {
	cg := NewConsumerGroup("test", "topic", 4, StrategyRange, newTestOffsetStore(t))
	defer cg.Stop()

	cg.Join("m1")
	cg.Join("m2")

	// m2 leaves → m1 should get all 4 partitions
	cg.Leave("m2")

	assignments := cg.Assignments()
	for p, member := range assignments {
		if member != "m1" {
			t.Errorf("expected m1 for partition %d, got %s", p, member)
		}
	}
}

func newTestGroup(t *testing.T) *ConsumerGroup {
	t.Helper()
	return NewConsumerGroup("test", "topic", 4, StrategyRange, newTestOffsetStore(t))
}

func newTestOffsetStore(t *testing.T) *OffsetStore {
	t.Helper()
	dir, err := os.MkdirTemp("", "offset-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return NewOffsetStore(dir)
}

func TestConsumerGroupLoadCommittedOffsetsOnCreate(t *testing.T) {
	dir, err := os.MkdirTemp("", "offset-cg-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Pre-populate offsets for group "preload"
	store := NewOffsetStore(dir)
	store.Save("preload", 0, 100)
	store.Save("preload", 2, 200)

	// Create a group — should load pre-existing offsets
	cg := NewConsumerGroup("preload", "topic", 4, StrategyRange, store)
	defer cg.Stop()

	if cg.GetCommittedOffset(0) != 100 {
		t.Errorf("committed[0] = %d, want 100", cg.GetCommittedOffset(0))
	}
	if cg.GetCommittedOffset(2) != 200 {
		t.Errorf("committed[2] = %d, want 200", cg.GetCommittedOffset(2))
	}
	// Partition 1 was never committed → should be 0
	if cg.GetCommittedOffset(1) != 0 {
		t.Errorf("committed[1] = %d, want 0", cg.GetCommittedOffset(1))
	}
}

func TestConsumerGroupStickyStrategy(t *testing.T) {
	// Sticky falls through to the default (round-robin) path
	cg := NewConsumerGroup("sticky", "topic", 3, StrategySticky, newTestOffsetStore(t))
	defer cg.Stop()

	cg.Join("m1")
	cg.Join("m2")

	assignments := cg.Assignments()
	if len(assignments) != 3 {
		t.Fatalf("expected 3 assignments, got %d", len(assignments))
	}
}

func TestConsumerGroupStopIdempotent(t *testing.T) {
	cg := newTestGroup(t)
	cg.Join("m1")

	// Double stop should not panic
	cg.Stop()
	cg.Stop()
}

func TestConsumerGroupNoMembersRebalance(t *testing.T) {
	cg := NewConsumerGroup("empty", "topic", 4, StrategyRange, newTestOffsetStore(t))
	defer cg.Stop()

	// No members joined — assignments should be empty
	assignments := cg.Assignments()
	if len(assignments) != 0 {
		t.Errorf("expected 0 assignments with no members, got %d", len(assignments))
	}
}

func TestConsumerGroupCommitAndGetOffset(t *testing.T) {
	store := newTestOffsetStore(t)
	cg := NewConsumerGroup("cg", "topic", 4, StrategyRoundRobin, store)
	defer cg.Stop()

	// Commit and get in sequence
	cg.CommitOffset(0, 10)
	cg.CommitOffset(0, 20) // overwrite

	if cg.GetCommittedOffset(0) != 20 {
		t.Errorf("offset after overwrite = %d, want 20", cg.GetCommittedOffset(0))
	}

	// Uncommitted partition should be 0
	if cg.GetCommittedOffset(3) != 0 {
		t.Errorf("uncommitted partition = %d, want 0", cg.GetCommittedOffset(3))
	}
}

func TestConsumerGroupRangeUnevenPartitions(t *testing.T) {
	// 5 partitions, 2 members — range: m1 gets 3, m2 gets 2
	cg := NewConsumerGroup("uneven", "topic", 5, StrategyRange, newTestOffsetStore(t))
	defer cg.Stop()

	cg.Join("m1")
	cg.Join("m2")

	assignments := cg.Assignments()
	if len(assignments) != 5 {
		t.Fatalf("expected 5 assignments, got %d", len(assignments))
	}

	// m1 should have 3 partitions, m2 should have 2
	m1Count, m2Count := 0, 0
	for _, m := range assignments {
		if m == "m1" {
			m1Count++
		} else {
			m2Count++
		}
	}
	if m1Count != 3 || m2Count != 2 {
		t.Errorf("m1=%d, m2=%d, want 3 and 2", m1Count, m2Count)
	}
}
