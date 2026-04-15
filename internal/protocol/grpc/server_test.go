package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
	proto "github.com/chimeramq/chimera/internal/protocol/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func setupGRPCServer(t *testing.T) (*Server, string, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg, _ := broker.LoadConfig("", &broker.CLIFlags{DataDir: dir})
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.GRPCPort = 0 // let OS pick a port

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}

	srv, err := NewServer(b)
	if err != nil {
		t.Fatal(err)
	}

	// Start server in background
	go func() {
		_ = srv.Serve()
	}()

	// Wait for server to be ready
	time.Sleep(100 * time.Millisecond)

	addr := srv.listener.Addr().String()

	cleanup := func() {
		srv.Stop()
		b.Stop()
	}
	return srv, addr, cleanup
}

func newClient(t *testing.T, addr string) (proto.ChimeraServiceClient, *grpc.ClientConn) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	return proto.NewChimeraServiceClient(conn), conn
}

func TestGRPCCreateTopic(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	resp, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-test-topic",
			Mode:       "stream",
			Partitions: 2,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if !resp.Created {
		t.Error("expected topic to be created")
	}
	if resp.Config.Name != "grpc-test-topic" {
		t.Errorf("topic name = %q, want %q", resp.Config.Name, "grpc-test-topic")
	}
	if resp.Config.Partitions != 2 {
		t.Errorf("partitions = %d, want 2", resp.Config.Partitions)
	}
}

func TestGRPCPublish(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Create topic first
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-pub-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Publish a message
	resp, err := client.Publish(context.Background(), &proto.PublishRequest{
		Topic:       "grpc-pub-topic",
		Payload:     []byte("hello from grpc"),
		ContentType: "text/plain",
		Headers:     map[string][]byte{"x-source": []byte("grpc-test")},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if resp.Topic != "grpc-pub-topic" {
		t.Errorf("topic = %q, want grpc-pub-topic", resp.Topic)
	}
	if resp.Offset != 0 {
		t.Errorf("offset = %d, want 0", resp.Offset)
	}
}

func TestGRPCPublishBatch(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Create topic first
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-batch-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Batch publish
	resp, err := client.PublishBatch(context.Background(), &proto.PublishBatchRequest{
		Messages: []*proto.PublishRequest{
			{Topic: "grpc-batch-topic", Payload: []byte("msg-1")},
			{Topic: "grpc-batch-topic", Payload: []byte("msg-2")},
			{Topic: "grpc-batch-topic", Payload: []byte("msg-3")},
		},
	})
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if resp.SuccessCount != 3 {
		t.Errorf("success count = %d, want 3", resp.SuccessCount)
	}
	if resp.FailureCount != 0 {
		t.Errorf("failure count = %d, want 0", resp.FailureCount)
	}
}

func TestGRPCGetTopicInfo(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Create topic
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-info-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Publish a message
	_, err = client.Publish(context.Background(), &proto.PublishRequest{
		Topic:   "grpc-info-topic",
		Payload: []byte("test"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Get topic info
	info, err := client.GetTopicInfo(context.Background(), &proto.GetTopicInfoRequest{
		Topic: "grpc-info-topic",
	})
	if err != nil {
		t.Fatalf("GetTopicInfo: %v", err)
	}
	if info.Config.Name != "grpc-info-topic" {
		t.Errorf("topic name = %q, want grpc-info-topic", info.Config.Name)
	}
	if len(info.Partitions) != 1 {
		t.Errorf("partitions count = %d, want 1", len(info.Partitions))
	}
}

func TestGRPCGetTopicInfoNotFound(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	_, err := client.GetTopicInfo(context.Background(), &proto.GetTopicInfoRequest{
		Topic: "nonexistent",
	})
	if err == nil {
		t.Error("expected error for nonexistent topic")
	}
}

func TestGRPCDeleteTopic(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Create topic
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-del-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Delete topic
	resp, err := client.DeleteTopic(context.Background(), &proto.DeleteTopicRequest{
		Topic: "grpc-del-topic",
	})
	if err != nil {
		t.Fatalf("DeleteTopic: %v", err)
	}
	if !resp.Deleted {
		t.Error("expected topic to be deleted")
	}

	// Verify topic no longer exists
	_, err = client.GetTopicInfo(context.Background(), &proto.GetTopicInfoRequest{
		Topic: "grpc-del-topic",
	})
	if err == nil {
		t.Error("expected error after topic deletion")
	}
}

func TestGRPCHandlerCreateTopicInvalidName(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{})
	if err == nil {
		t.Error("expected error for empty config")
	}
}

func TestGRPCHandlerHealth(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	resp, err := client.Health(context.Background(), &proto.HealthRequest{})
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if !resp.Healthy {
		t.Error("expected healthy to be true")
	}
	if resp.Version != "0.1.0" {
		t.Errorf("version = %q, want 0.1.0", resp.Version)
	}
}

func TestGRPCHandlerDuplicateTopic(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	req := &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-dup-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	}

	_, err := client.CreateTopic(context.Background(), req)
	if err != nil {
		t.Fatalf("first CreateTopic: %v", err)
	}

	_, err = client.CreateTopic(context.Background(), req)
	if err == nil {
		t.Error("expected error for duplicate topic")
	}
}

func TestGRPCHandlerDeleteNonexistentTopic(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	_, err := client.DeleteTopic(context.Background(), &proto.DeleteTopicRequest{
		Topic: "nonexistent-topic",
	})
	if err == nil {
		t.Error("expected error for deleting nonexistent topic")
	}
}

func TestEnvelopToProto(t *testing.T) {
	env := &message.Envelope{
		Topic:       "test-topic",
		PartitionID: 1,
		Sequence:    42,
		RoutingKey:  "test-key",
		Priority:    5,
		Headers:     map[string][]byte{"key": []byte("value")},
		ContentType: "application/json",
		Payload:     []byte("test-payload"),
		SchemaID:    123,
		Timestamp:   1700000000000000000, // ns
	}

	msg := envelopeToProto(env)

	if msg.Topic != "test-topic" {
		t.Errorf("topic = %q, want test-topic", msg.Topic)
	}
	if msg.PartitionId != 1 {
		t.Errorf("partition_id = %d, want 1", msg.PartitionId)
	}
	if msg.Offset != 42 {
		t.Errorf("offset = %d, want 42", msg.Offset)
	}
	if msg.RoutingKey != "test-key" {
		t.Errorf("routing_key = %q, want test-key", msg.RoutingKey)
	}
	if msg.Priority != 5 {
		t.Errorf("priority = %d, want 5", msg.Priority)
	}
	if string(msg.Payload) != "test-payload" {
		t.Errorf("payload = %q, want test-payload", msg.Payload)
	}
	if msg.SchemaId != 123 {
		t.Errorf("schema_id = %d, want 123", msg.SchemaId)
	}
}

func TestTopicModeConversion(t *testing.T) {
	tests := []struct {
		protoMode string
		wantMode  broker.TopicMode
	}{
		{"queue", broker.ModeQueue},
		{"stream", broker.ModeStream},
		{"unified", broker.ModeUnified},
		{"", broker.ModeStream}, // default
		{"unknown", broker.ModeStream},
	}

	for _, tt := range tests {
		got := topicModeFromProto(tt.protoMode)
		if got != tt.wantMode {
			t.Errorf("topicModeFromProto(%q) = %d, want %d", tt.protoMode, got, tt.wantMode)
		}
	}

	// Test reverse
	if topicModeToProto(broker.ModeQueue) != "queue" {
		t.Error("topicModeToProto(ModeQueue) != queue")
	}
	if topicModeToProto(broker.ModeStream) != "stream" {
		t.Error("topicModeToProto(ModeStream) != stream")
	}
	if topicModeToProto(broker.ModeUnified) != "unified" {
		t.Error("topicModeToProto(ModeUnified) != unified")
	}
}
