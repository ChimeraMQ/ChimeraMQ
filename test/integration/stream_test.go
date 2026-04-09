package integration

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/stream"
	"github.com/chimeramq/chimera/internal/message"
)

func TestStreamProduceAndFetch(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "s-fetch",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	// Publish 3 messages
	for i := 0; i < 3; i++ {
		env := &message.Envelope{
			Topic:       "s-fetch",
			Payload:     []byte("msg"),
			SourceProto: message.ProtoHTTP,
		}
		_, err := tb.broker.Publish(env)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Fetch from offset 0
	se := tb.broker.StreamEngine()
	msgs, nextOffset, err := se.Fetch("s-fetch", 0, 0, 10, 1*time.Second)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	if nextOffset != 3 {
		t.Errorf("expected nextOffset=3, got %d", nextOffset)
	}
}

func TestStreamFetchFromMiddle(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "s-mid",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	for i := 0; i < 5; i++ {
		env := &message.Envelope{
			Topic:       "s-mid",
			Payload:     []byte("msg"),
			SourceProto: message.ProtoHTTP,
		}
		tb.broker.Publish(env)
	}

	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("s-mid", 0, 2, 10, 1*time.Second)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	if len(msgs) != 3 {
		t.Errorf("expected 3 messages from offset 2, got %d", len(msgs))
	}
}

func TestStreamFetchLimitedMaxMessages(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "s-limit",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	for i := 0; i < 10; i++ {
		env := &message.Envelope{
			Topic:       "s-limit",
			Payload:     []byte("msg"),
			SourceProto: message.ProtoHTTP,
		}
		tb.broker.Publish(env)
	}

	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("s-limit", 0, 0, 3, 1*time.Second)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	if len(msgs) != 3 {
		t.Errorf("expected 3 messages (max limit), got %d", len(msgs))
	}
}

func TestStreamConsumerGroupJoin(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "s-cg",
		Mode:       broker.ModeStream,
		Partitions: 4,
	})

	se := tb.broker.StreamEngine()
	se.JoinGroup("test-group", "s-cg", "member-1", 4, stream.StrategyRange)

	cg := se.GetGroup("test-group")
	if cg == nil {
		t.Fatal("expected consumer group to exist")
	}

	members := cg.Members()
	if len(members) != 1 {
		t.Errorf("expected 1 member, got %d", len(members))
	}

	assignments := cg.Assignments()
	if len(assignments) != 4 {
		t.Errorf("expected 4 assignments for single member, got %d", len(assignments))
	}
}

func TestStreamConsumerGroupRangeRebalance(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "s-range",
		Mode:       broker.ModeStream,
		Partitions: 4,
	})

	se := tb.broker.StreamEngine()
	se.JoinGroup("range-group", "s-range", "m1", 4, stream.StrategyRange)
	se.JoinGroup("range-group", "s-range", "m2", 4, stream.StrategyRange)

	cg := se.GetGroup("range-group")
	assignments := cg.Assignments()

	// 4 partitions, 2 members, range → member1 gets [0,1], member2 gets [2,3]
	m1Parts := 0
	m2Parts := 0
	for _, member := range assignments {
		if member == "m1" {
			m1Parts++
		} else {
			m2Parts++
		}
	}

	if m1Parts != 2 || m2Parts != 2 {
		t.Errorf("expected 2+2 range assignment, got m1=%d m2=%d", m1Parts, m2Parts)
	}
}

func TestStreamConsumerGroupRoundRobinRebalance(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "s-rr",
		Mode:       broker.ModeStream,
		Partitions: 4,
	})

	se := tb.broker.StreamEngine()
	se.JoinGroup("rr-group", "s-rr", "m1", 4, stream.StrategyRoundRobin)
	se.JoinGroup("rr-group", "s-rr", "m2", 4, stream.StrategyRoundRobin)

	cg := se.GetGroup("rr-group")
	assignments := cg.Assignments()

	// 4 partitions, 2 members, round-robin → p0→m1, p1→m2, p2→m1, p3→m2
	if assignments[0] != "m1" || assignments[1] != "m2" ||
		assignments[2] != "m1" || assignments[3] != "m2" {
		t.Errorf("unexpected round-robin assignments: %v", assignments)
	}
}

func TestStreamConsumerGroupLeave(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "s-leave",
		Mode:       broker.ModeStream,
		Partitions: 4,
	})

	se := tb.broker.StreamEngine()
	se.JoinGroup("leave-group", "s-leave", "m1", 4, stream.StrategyRange)
	se.JoinGroup("leave-group", "s-leave", "m2", 4, stream.StrategyRange)

	// m2 leaves
	se.LeaveGroup("leave-group", "m2")

	cg := se.GetGroup("leave-group")
	members := cg.Members()
	if len(members) != 1 {
		t.Errorf("expected 1 member after leave, got %d", len(members))
	}

	// All 4 partitions should be assigned to m1
	assignments := cg.Assignments()
	if len(assignments) != 4 {
		t.Errorf("expected 4 assignments after rebalance, got %d", len(assignments))
	}
}

func TestStreamOffsetCommitAndResume(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "s-offset",
		Mode:       broker.ModeStream,
		Partitions: 1, // single partition for predictable offsets
	})

	// Publish messages
	for i := 0; i < 5; i++ {
		env := &message.Envelope{
			Topic:       "s-offset",
			Payload:     []byte("msg"),
			SourceProto: message.ProtoHTTP,
		}
		tb.broker.Publish(env)
	}

	se := tb.broker.StreamEngine()
	se.JoinGroup("offset-group", "s-offset", "m1", 1, stream.StrategyRange)

	// Commit offset 3 for partition 0
	err := se.CommitOffset("offset-group", 0, 3)
	if err != nil {
		t.Fatalf("commit offset: %v", err)
	}

	cg := se.GetGroup("offset-group")
	committed := cg.GetCommittedOffset(0)
	if committed != 3 {
		t.Errorf("expected committed offset 3, got %d", committed)
	}

	// Fetch from committed offset
	msgs, _, err := se.Fetch("s-offset", 0, 3, 10, 1*time.Second)
	if err != nil {
		t.Fatalf("fetch from committed: %v", err)
	}

	// Should get messages from offset 3 onwards (3 and 4)
	if len(msgs) < 1 {
		t.Error("expected at least 1 message from offset 3")
	}
}

func TestStreamLongPollTimeout(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "s-poll",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	se := tb.broker.StreamEngine()

	// Fetch from offset 1 (beyond high watermark of 0 on empty partition)
	start := time.Now()
	msgs, _, err := se.Fetch("s-poll", 0, 1, 10, 200*time.Millisecond)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	if len(msgs) != 0 {
		t.Errorf("expected 0 messages on empty partition, got %d", len(msgs))
	}

	// Should return after timeout (not immediately)
	if elapsed < 100*time.Millisecond {
		t.Errorf("long-poll returned too quickly: %v", elapsed)
	}
}
