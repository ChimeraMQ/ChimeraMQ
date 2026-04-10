package integration

import (
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/stream"
	"github.com/chimeramq/chimera/internal/message"
)

func TestGracefulShutdownPublishInFlight(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "graceful-test",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	// Publish some messages
	for i := 0; i < 10; i++ {
		env := &message.Envelope{
			Topic:       "graceful-test",
			Payload:     []byte("graceful-msg"),
			SourceProto: message.ProtoHTTP,
		}
		if _, err := tb.broker.Publish(env); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Stop broker gracefully
	tb.broker.Stop()

	// Recreate and verify data persisted
	cfg := tb.broker.Config()
	b2, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b2.Start(); err != nil {
		t.Fatal(err)
	}
	defer b2.Stop()

	se := b2.StreamEngine()
	msgs, _, err := se.Fetch("graceful-test", 0, 0, 20, 0)
	if err != nil {
		t.Fatalf("fetch after graceful shutdown: %v", err)
	}
	if len(msgs) != 10 {
		t.Errorf("expected 10 messages after graceful shutdown, got %d", len(msgs))
	}
}

func TestGracefulShutdownMultipleTopics(t *testing.T) {
	tb := newTestBroker(t)

	for _, name := range []string{"topic-a", "topic-b", "topic-c"} {
		tb.broker.Topics().CreateTopic(broker.TopicConfig{
			Name:       name,
			Mode:       broker.ModeStream,
			Partitions: 2,
		})
		for i := 0; i < 3; i++ {
			env := &message.Envelope{
				Topic:       name,
				Payload:     []byte(name + "-msg"),
				SourceProto: message.ProtoHTTP,
			}
			tb.broker.Publish(env)
		}
	}

	tb.broker.Stop()

	cfg := tb.broker.Config()
	b2, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b2.Start(); err != nil {
		t.Fatal(err)
	}
	defer b2.Stop()

	topics := b2.Topics().ListTopics()
	if len(topics) != 3 {
		t.Errorf("expected 3 topics, got %d", len(topics))
	}

	for _, name := range []string{"topic-a", "topic-b", "topic-c"} {
		se := b2.StreamEngine()
		var total int
		for p := uint32(0); p < 2; p++ {
			msgs, _, err := se.Fetch(name, p, 0, 10, 0)
			if err != nil {
				t.Errorf("fetch %s p%d: %v", name, p, err)
				continue
			}
			total += len(msgs)
		}
		if total != 3 {
			t.Errorf("topic %s: expected 3 total messages, got %d", name, total)
		}
	}
}

func TestGracefulShutdownConsumerGroups(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "cg-test",
		Mode:       broker.ModeStream,
		Partitions: 2,
	})

	se := tb.broker.StreamEngine()
	se.JoinGroup("my-group", "cg-test", "member-1", 2, stream.StrategyRange)

	// Publish and commit
	for i := 0; i < 5; i++ {
		env := &message.Envelope{
			Topic:       "cg-test",
			Payload:     []byte("msg"),
			SourceProto: message.ProtoHTTP,
		}
		tb.broker.Publish(env)
	}
	se.CommitOffset("my-group", 0, 3)
	se.CommitOffset("my-group", 1, 2)

	// Graceful stop
	tb.broker.Stop()

	// Restart and verify offsets
	cfg := tb.broker.Config()
	b2, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b2.Start(); err != nil {
		t.Fatal(err)
	}
	defer b2.Stop()

	se2 := b2.StreamEngine()
	se2.JoinGroup("my-group", "cg-test", "member-1", 2, stream.StrategyRange)

	cg := se2.GetGroup("my-group")
	if cg == nil {
		t.Fatal("consumer group not found after restart")
	}
	if off := cg.GetCommittedOffset(0); off != 3 {
		t.Errorf("partition 0 offset: got %d, want 3", off)
	}
	if off := cg.GetCommittedOffset(1); off != 2 {
		t.Errorf("partition 1 offset: got %d, want 2", off)
	}
}

func TestGracefulShutdownPublishAfterRestart(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "continue-test",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	// Publish before shutdown
	for i := 0; i < 5; i++ {
		tb.broker.Publish(&message.Envelope{
			Topic:       "continue-test",
			Payload:     []byte("before"),
			SourceProto: message.ProtoHTTP,
		})
	}

	tb.broker.Stop()

	// Restart and publish more
	cfg := tb.broker.Config()
	b2, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b2.Start(); err != nil {
		t.Fatal(err)
	}
	defer b2.Stop()

	for i := 0; i < 5; i++ {
		if _, err := b2.Publish(&message.Envelope{
			Topic:       "continue-test",
			Payload:     []byte("after"),
			SourceProto: message.ProtoHTTP,
		}); err != nil {
			t.Fatalf("publish after restart: %v", err)
		}
	}

	se := b2.StreamEngine()
	msgs, _, err := se.Fetch("continue-test", 0, 0, 20, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 10 {
		t.Errorf("expected 10 total messages (5 before + 5 after), got %d", len(msgs))
	}
}
