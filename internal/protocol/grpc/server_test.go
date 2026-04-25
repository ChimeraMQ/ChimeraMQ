package grpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
	proto "github.com/chimeramq/chimera/internal/protocol/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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

func TestGRPCConsumeStream(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Create a topic
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-stream-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Publish a message before starting consume stream
	_, err = client.Publish(context.Background(), &proto.PublishRequest{
		Topic:   "grpc-stream-topic",
		Payload: []byte("stream-msg-1"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Start consume stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Subscribe to topic
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Subscribe{
			Subscribe: &proto.SubscribeRequest{
				Topic:       "grpc-stream-topic",
				StartOffset: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait for message to arrive
	var received bool
	for i := 0; i < 20; i++ {
		resp, err := stream.Recv()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if msg := resp.GetMessage(); msg != nil {
			if string(msg.Payload) == "stream-msg-1" {
				received = true
				break
			}
		}
	}
	if !received {
		t.Error("expected to receive streamed message")
	}
}

func TestGRPCFetchMessages(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Create topic and publish messages
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-fetch-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	for i := 0; i < 5; i++ {
		_, err = client.Publish(context.Background(), &proto.PublishRequest{
			Topic:   "grpc-fetch-topic",
			Payload: []byte(fmt.Sprintf("fetch-msg-%d", i)),
		})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	// Start consume stream and subscribe
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Subscribe{
			Subscribe: &proto.SubscribeRequest{
				Topic:       "grpc-fetch-topic",
				StartOffset: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait for messages to arrive from the streaming subscription
	var received []string
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			if len(received) < 5 {
				t.Errorf("expected at least 5 messages, got %d", len(received))
			}
			return
		default:
		}

		resp, err := stream.Recv()
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if msg := resp.GetMessage(); msg != nil {
			received = append(received, string(msg.Payload))
			if len(received) >= 5 {
				return
			}
		}
	}
}

func TestGRPCAckNack(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Send each action with a small delay to ensure server processes them
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Ack{
			Ack: &proto.AckRequest{Topic: "test-topic", Offset: 1},
		},
	})
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Nack{
			Nack: &proto.NackRequest{Topic: "test-topic", Offset: 2, Requeue: true},
		},
	})
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_CommitOffset{
			CommitOffset: &proto.CommitOffsetRequest{Topic: "test-topic", GroupId: "g1", Offset: 5},
		},
	})
	if err != nil {
		t.Fatalf("CommitOffset: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Unsubscribe{
			Unsubscribe: &proto.UnsubscribeRequest{Topic: "test-topic"},
		},
	})
	if err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
}

func TestGRPCConsumeSubscribeWithoutPriorTopic(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Subscribe to a topic that hasn't been explicitly created yet
	// gRPC auto-creates partitions, so this should succeed
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Subscribe{
			Subscribe: &proto.SubscribeRequest{
				Topic:       "auto-created-topic",
				StartOffset: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Publish a message to the auto-created topic
	_, err = client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "auto-created-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	_, err = client.Publish(context.Background(), &proto.PublishRequest{
		Topic:   "auto-created-topic",
		Payload: []byte("auto-msg"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Wait for message
	var received bool
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		if msg := resp.GetMessage(); msg != nil && string(msg.Payload) == "auto-msg" {
			received = true
			break
		}
	}
	if !received {
		t.Error("expected to receive message from subscribed topic")
	}
}

func TestGRPCFetchAndUnsubscribe(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Create topic and publish
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-fu-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	_, err = client.Publish(context.Background(), &proto.PublishRequest{
		Topic:   "grpc-fu-topic",
		Payload: []byte("fu-msg"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Subscribe first
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Subscribe{
			Subscribe: &proto.SubscribeRequest{
				Topic:       "grpc-fu-topic",
				StartOffset: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait for the streamed message to arrive from the subscription
	var gotStreamMsg bool
	deadline := time.After(2 * time.Second)
	for !gotStreamMsg {
		select {
		case <-deadline:
			t.Error("timed out waiting for streamed message")
			return
		default:
		}
		resp, err := stream.Recv()
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if msg := resp.GetMessage(); msg != nil && string(msg.Payload) == "fu-msg" {
			gotStreamMsg = true
		}
	}

	// Now send a fetch request
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Fetch{
			Fetch: &proto.FetchRequest{MaxMessages: 10},
		},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// Send unsubscribe
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Unsubscribe{
			Unsubscribe: &proto.UnsubscribeRequest{Topic: "grpc-fu-topic"},
		},
	})
	if err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
}

func setupGRPCServerWithAuth(t *testing.T) (*Server, string, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg, _ := broker.LoadConfig("", &broker.CLIFlags{DataDir: dir})
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.GRPCPort = 0

	// Enable static auth with a known token
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "static"
	cfg.Auth.Tokens = map[string]string{
		"test-admin-token": "admin",
	}

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

	go func() {
		_ = srv.Serve()
	}()

	time.Sleep(100 * time.Millisecond)

	addr := srv.listener.Addr().String()

	cleanup := func() {
		srv.Stop()
		b.Stop()
	}
	return srv, addr, cleanup
}

func newClientWithToken(t *testing.T, addr, token string) (proto.ChimeraServiceClient, *grpc.ClientConn) {
	t.Helper()
	authUnary := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
	authStream := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return streamer(ctx, desc, cc, method, opts...)
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(authUnary),
		grpc.WithStreamInterceptor(authStream),
	)
	if err != nil {
		t.Fatal(err)
	}
	return proto.NewChimeraServiceClient(conn), conn
}

func TestGRPCAuthInterceptorRejectsUnauthenticated(t *testing.T) {
	_, addr, cleanup := setupGRPCServerWithAuth(t)
	defer cleanup()

	// Connect without auth token
	client, conn := newClient(t, addr)
	defer conn.Close()

	// CreateTopic should fail without auth
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name: "auth-test",
			Mode: "stream",
		},
	})
	if err == nil {
		t.Error("expected auth error without token")
	}
}

func TestGRPCAuthInterceptorAcceptsValidToken(t *testing.T) {
	_, addr, cleanup := setupGRPCServerWithAuth(t)
	defer cleanup()

	// Connect with valid auth token
	client, conn := newClientWithToken(t, addr, "test-admin-token")
	defer conn.Close()

	// CreateTopic should succeed with valid token
	resp, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "auth-success",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic with auth: %v", err)
	}
	if !resp.Created {
		t.Error("expected topic to be created")
	}
}

func TestGRPCAuthInterceptorRejectsInvalidToken(t *testing.T) {
	_, addr, cleanup := setupGRPCServerWithAuth(t)
	defer cleanup()

	// Connect with invalid token
	client, conn := newClientWithToken(t, addr, "wrong-token")
	defer conn.Close()

	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name: "should-fail",
			Mode: "stream",
		},
	})
	if err == nil {
		t.Error("expected auth error with invalid token")
	}
}

func TestGRPCPublishWithAuth(t *testing.T) {
	_, addr, cleanup := setupGRPCServerWithAuth(t)
	defer cleanup()

	client, conn := newClientWithToken(t, addr, "test-admin-token")
	defer conn.Close()

	// Create topic
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-auth-pub-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Publish with auth
	resp, err := client.Publish(context.Background(), &proto.PublishRequest{
		Topic:   "grpc-auth-pub-topic",
		Payload: []byte("auth-msg"),
	})
	if err != nil {
		t.Fatalf("Publish with auth: %v", err)
	}
	if resp.Topic != "grpc-auth-pub-topic" {
		t.Errorf("topic = %q, want grpc-auth-pub-topic", resp.Topic)
	}
}

func TestGRPCConsumeWithAuth(t *testing.T) {
	_, addr, cleanup := setupGRPCServerWithAuth(t)
	defer cleanup()

	client, conn := newClientWithToken(t, addr, "test-admin-token")
	defer conn.Close()

	// Create topic and publish
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "grpc-auth-stream-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	_, err = client.Publish(context.Background(), &proto.PublishRequest{
		Topic:   "grpc-auth-stream-topic",
		Payload: []byte("stream-msg"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Subscribe
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Subscribe{
			Subscribe: &proto.SubscribeRequest{
				Topic:       "grpc-auth-stream-topic",
				StartOffset: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait for message
	var received bool
	for {
		resp, err := stream.Recv()
		if err != nil {
			break
		}
		if msg := resp.GetMessage(); msg != nil && string(msg.Payload) == "stream-msg" {
			received = true
			break
		}
	}
	if !received {
		t.Error("expected to receive streamed message with auth")
	}
}
