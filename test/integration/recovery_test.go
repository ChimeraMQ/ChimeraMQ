package integration

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/stream"
	"github.com/chimeramq/chimera/internal/message"
)

func TestRecoveryTopicMetadata(t *testing.T) {
	tb := newTestBroker(t)

	// Create topic
	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "recover-topic",
		Mode:       broker.ModeStream,
		Partitions: 2,
	})

	// Stop and recreate broker
	tb.recreateBroker(t)

	// Verify topic still exists
	topic, ok := tb.broker.Topics().GetTopic("recover-topic")
	if !ok {
		t.Fatal("topic not found after recovery")
	}
	if topic.Name != "recover-topic" {
		t.Errorf("expected name=recover-topic, got %s", topic.Name)
	}
	if topic.Partitions != 2 {
		t.Errorf("expected 2 partitions, got %d", topic.Partitions)
	}

}

func TestRecoveryMultipleTopics(t *testing.T) {
	tb := newTestBroker(t)

	topics := []struct {
		name       string
		mode       broker.TopicMode
		partitions uint32
	}{
		{"topic-a", broker.ModeStream, 2},
		{"topic-b", broker.ModeQueue, 4},
		{"topic-c", broker.ModeUnified, 1},
	}

	for _, tc := range topics {
		tb.broker.Topics().CreateTopic(broker.TopicConfig{
			Name:       tc.name,
			Mode:       tc.mode,
			Partitions: tc.partitions,
		})
	}

	tb.recreateBroker(t)

	list := tb.broker.Topics().ListTopics()
	if len(list) != 3 {
		t.Errorf("expected 3 topics after recovery, got %d", len(list))
	}
}

func TestRecoveryPublishDataSurvives(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "recover-data",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	// Publish messages before restart
	for i := 0; i < 5; i++ {
		env := &message.Envelope{
			Topic:       "recover-data",
			Payload:     []byte("pre-restart-msg"),
			SourceProto: message.ProtoHTTP,
		}
		_, err := tb.broker.Publish(env)
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	tb.recreateBroker(t)

	// Fetch should still return the data
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("recover-data", 0, 0, 10, 0)
	if err != nil {
		t.Fatalf("fetch after recovery: %v", err)
	}
	if len(msgs) != 5 {
		t.Errorf("expected 5 messages after recovery, got %d", len(msgs))
	}
}

func TestRecoveryNewPublishesWork(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "recover-new",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	// Publish before restart
	env := &message.Envelope{
		Topic:       "recover-new",
		Payload:     []byte("old"),
		SourceProto: message.ProtoHTTP,
	}
	tb.broker.Publish(env)

	tb.recreateBroker(t)

	// Publish after restart
	env2 := &message.Envelope{
		Topic:       "recover-new",
		Payload:     []byte("new"),
		SourceProto: message.ProtoHTTP,
	}
	_, err := tb.broker.Publish(env2)
	if err != nil {
		t.Fatalf("publish after recovery: %v", err)
	}

	// Fetch all
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("recover-new", 0, 0, 10, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages, got %d", len(msgs))
	}
}

func TestRecoveryOffsetStorePersists(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "recover-offsets",
		Mode:       broker.ModeStream,
		Partitions: 2,
	})

	se := tb.broker.StreamEngine()
	se.JoinGroup("persist-group", "recover-offsets", "m1", 2, stream.StrategyRange)

	// Commit offset
	se.CommitOffset("persist-group", 0, 42)
	se.CommitOffset("persist-group", 1, 7)

	tb.recreateBroker(t)

	// Create a new stream engine will reload offsets
	// The broker recreates the stream engine, so we need to create a new group
	se2 := tb.broker.StreamEngine()
	se2.JoinGroup("persist-group", "recover-offsets", "m1", 2, stream.StrategyRange)

	cg := se2.GetGroup("persist-group")
	if cg == nil {
		t.Fatal("consumer group not found")
	}

	off0 := cg.GetCommittedOffset(0)
	off1 := cg.GetCommittedOffset(1)

	if off0 != 42 {
		t.Errorf("expected committed offset 42 for p0, got %d", off0)
	}
	if off1 != 7 {
		t.Errorf("expected committed offset 7 for p1, got %d", off1)
	}
}

func TestRecoveryTopicCreateAfterRestart(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "pre-restart",
		Mode:       broker.ModeQueue,
		Partitions: 2,
	})

	tb.recreateBroker(t)

	// Create a new topic after restart
	err := tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "post-restart",
		Mode:       broker.ModeStream,
		Partitions: 4,
	})
	if err != nil {
		t.Fatalf("create topic after restart: %v", err)
	}

	// Both should exist
	list := tb.broker.Topics().ListTopics()
	if len(list) != 2 {
		t.Errorf("expected 2 topics, got %d", len(list))
	}
}

func TestRecoveryMetaJSONFormat(t *testing.T) {
	tmpDir := tmpDataDir(t)

	cfg := &broker.Config{
		Node: broker.NodeConfig{ID: 1, Name: "test", DataDir: tmpDir},
		Listener: broker.ListenerConfig{
			Bind: "127.0.0.1", Port: 19999, AdminPort: 19899,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "warn", Format: "text", Output: "stdout"},
	}

	b, _ := broker.NewBroker(cfg)
	b.Start()
	defer b.Stop()

	b.Topics().CreateTopic(broker.TopicConfig{
		Name:       "json-test",
		Mode:       broker.ModeUnified,
		Partitions: 8,
	})

	// Read meta.json directly
	metaData, err := os.ReadFile(tmpDir + "/topics/meta.json")
	if err != nil {
		t.Fatalf("read meta.json: %v", err)
	}

	var topics []*broker.TopicConfig
	if err := json.Unmarshal(metaData, &topics); err != nil {
		t.Fatalf("parse meta.json: %v", err)
	}

	found := false
	for _, tc := range topics {
		if tc.Name == "json-test" {
			found = true
			if tc.Mode != broker.ModeUnified {
				t.Errorf("expected unified mode, got %d", tc.Mode)
			}
			if tc.Partitions != 8 {
				t.Errorf("expected 8 partitions, got %d", tc.Partitions)
			}
		}
	}
	if !found {
		t.Error("json-test topic not found in meta.json")
	}
}
