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
	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestConsumerGroupLoadCommittedOffsetsOnCreate(t *testing.T) {
	dir, err := os.MkdirTemp("", "offset-cg-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Pre-populate offsets for group "preload"
	store, err := NewOffsetStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	store.Save("preload", 0, 100)
	store.Save("preload", 2, 200)

	// Create a group -- should load pre-existing offsets
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
	cg := NewConsumerGroup("sticky", "topic", 6, StrategySticky, newTestOffsetStore(t))
	defer cg.Stop()

	// Join m1 and m2 -- all 6 partitions assigned
	cg.Join("m1")
	cg.Join("m2")

	// Record initial assignments
	first := cg.Assignments()
	if len(first) != 6 {
		t.Fatalf("expected 6 assignments, got %d", len(first))
	}

	// Join m3 -- triggers rebalance, but m1 and m2 keep most partitions
	cg.Join("m3")
	second := cg.Assignments()
	if len(second) != 6 {
		t.Fatalf("expected 6 assignments after m3 join, got %d", len(second))
	}

	// Verify sticky: at least some of m1's original partitions are still assigned to m1
	m1Kept := 0
	for partID, memberID := range second {
		if memberID == "m1" && first[partID] == "m1" {
			m1Kept++
		}
	}
	if m1Kept == 0 {
		t.Error("sticky rebalance should preserve some of m1's original partitions")
	}

	// Verify balanced: each member should have ~2 partitions
	counts := make(map[string]int)
	for _, memberID := range second {
		counts[memberID]++
	}
	for _, memberID := range []string{"m1", "m2", "m3"} {
		c := counts[memberID]
		if c < 1 || c > 3 {
			t.Errorf("member %s has %d partitions, expected 1-3", memberID, c)
		}
	}
}

func TestConsumerGroupStickyPreservesOnLeave(t *testing.T) {
	cg := NewConsumerGroup("sticky-leave", "topic", 6, StrategySticky, newTestOffsetStore(t))
	defer cg.Stop()

	cg.Join("m1")
	cg.Join("m2")
	cg.Join("m3")

	before := cg.Assignments()

	// m3 leaves -- m1 and m2 should keep their partitions
	cg.Leave("m3")
	after := cg.Assignments()

	if len(after) != 6 {
		t.Fatalf("expected 6 assignments after leave, got %d", len(after))
	}

	// m1's partitions that were assigned before should still be assigned to m1
	for partID, memberID := range before {
		if memberID == "m1" {
			if after[partID] != "m1" {
				t.Errorf("m1 lost partition %d after m3 left (was m1, now %s)", partID, after[partID])
			}
		}
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

	// No members joined -- assignments should be empty
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
	// 5 partitions, 2 members -- range: m1 gets 3, m2 gets 2
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

func TestConsumerGroupRebalanceRoundRobin(t *testing.T) {
	cg := NewConsumerGroup("rr-test", "topic", 4, StrategyRoundRobin, newTestOffsetStore(t))
	defer cg.Stop()

	cg.Join("c1")
	cg.Join("c2")

	// Round-robin should distribute partitions
	assignments := cg.Assignments()
	if len(assignments) != 4 {
		t.Fatalf("expected 4 assignments, got %d", len(assignments))
	}
	// Round-robin: c1 gets 0,2; c2 gets 1,3
	if assignments[0] != "c1" || assignments[1] != "c2" ||
		assignments[2] != "c1" || assignments[3] != "c2" {
		t.Errorf("unexpected round-robin assignments: %v", assignments)
	}
}

func TestConsumerGroupRebalanceEmpty(t *testing.T) {
	cg := NewConsumerGroup("empty-rr", "topic", 4, StrategyRoundRobin, newTestOffsetStore(t))
	defer cg.Stop()

	// No members -- assignments should be empty
	assignments := cg.Assignments()
	if len(assignments) != 0 {
		t.Errorf("expected 0 assignments with no members, got %d", len(assignments))
	}
}

func TestConsumerGroupRemoveAllMembers(t *testing.T) {
	cg := NewConsumerGroup("remove-all", "topic", 4, StrategyRange, newTestOffsetStore(t))
	defer cg.Stop()

	cg.Join("c1")
	cg.Join("c2")

	cg.Leave("c1")
	cg.Leave("c2")

	members := cg.Members()
	if len(members) != 0 {
		t.Errorf("expected 0 members, got %d", len(members))
	}
	assignments := cg.Assignments()
	if len(assignments) != 0 {
		t.Errorf("expected 0 assignments, got %d", len(assignments))
	}
}
