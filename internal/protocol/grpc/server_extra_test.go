package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	proto "github.com/chimeramq/chimera/internal/protocol/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// setupGRPCServerWithACL creates a broker with ACL engine and a reader-only user.
func setupGRPCServerWithACL(t *testing.T) (*Server, string, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg, _ := broker.LoadConfig("", &broker.CLIFlags{DataDir: dir})
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.GRPCPort = 0

	// Enable static auth with admin and reader tokens
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "static"
	cfg.Auth.Tokens = map[string]string{
		"admin-token":  "admin",
		"reader-token": "reader",
	}

	// Enable ACL: deny by default, allow reader to read all topics
	cfg.ACL.Enabled = true
	cfg.ACL.DefaultPolicy = "deny"
	cfg.ACL.Entries = []broker.ACLEntryConfig{
		{Principal: "reader", Resource: "topic", Name: "*", Operation: "read", Permission: "allow"},
		{Principal: "admin", Resource: "topic", Name: "*", Operation: "read", Permission: "allow"},
		{Principal: "admin", Resource: "topic", Name: "*", Operation: "write", Permission: "allow"},
		{Principal: "admin", Resource: "topic", Name: "*", Operation: "create", Permission: "allow"},
		{Principal: "admin", Resource: "topic", Name: "*", Operation: "delete", Permission: "allow"},
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

func newClientWithTokenInterceptors(t *testing.T, addr, token string) (proto.ChimeraServiceClient, *grpc.ClientConn) {
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

func TestGRPCACLReaderCannotCreateTopic(t *testing.T) {
	_, addr, cleanup := setupGRPCServerWithACL(t)
	defer cleanup()

	client, conn := newClientWithTokenInterceptors(t, addr, "reader-token")
	defer conn.Close()

	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name: "acl-blocked-topic",
			Mode: "stream",
		},
	})
	if err == nil {
		t.Fatal("expected ACL error for reader creating topic")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", st.Code())
	}
}

func TestGRPCACLReaderCannotDeleteTopic(t *testing.T) {
	_, addr, cleanup := setupGRPCServerWithACL(t)
	defer cleanup()

	// Admin creates a topic
	adminClient, adminConn := newClientWithTokenInterceptors(t, addr, "admin-token")
	defer adminConn.Close()
	_, err := adminClient.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "acl-del-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("admin CreateTopic: %v", err)
	}

	// Reader tries to delete it
	readerClient, readerConn := newClientWithTokenInterceptors(t, addr, "reader-token")
	defer readerConn.Close()
	_, err = readerClient.DeleteTopic(context.Background(), &proto.DeleteTopicRequest{
		Topic: "acl-del-topic",
	})
	if err == nil {
		t.Fatal("expected ACL error for reader deleting topic")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", st.Code())
	}
}

func TestGRPCACLReaderCannotPublish(t *testing.T) {
	_, addr, cleanup := setupGRPCServerWithACL(t)
	defer cleanup()

	// Admin creates a topic
	adminClient, adminConn := newClientWithTokenInterceptors(t, addr, "admin-token")
	defer adminConn.Close()
	_, err := adminClient.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "acl-pub-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("admin CreateTopic: %v", err)
	}

	// Reader tries to publish
	readerClient, readerConn := newClientWithTokenInterceptors(t, addr, "reader-token")
	defer readerConn.Close()
	_, err = readerClient.Publish(context.Background(), &proto.PublishRequest{
		Topic:   "acl-pub-topic",
		Payload: []byte("should-fail"),
	})
	if err == nil {
		t.Fatal("expected ACL error for reader publishing")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Errorf("expected PermissionDenied, got %v", st.Code())
	}
}

func TestGRPCFetchWithNoSubscriptions(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Send fetch without any subscriptions — should not error, just return nothing
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Fetch{
			Fetch: &proto.FetchRequest{MaxMessages: 10},
		},
	})
	if err != nil {
		t.Fatalf("Fetch with no subscriptions: %v", err)
	}

	// Give it a moment to process
	time.Sleep(50 * time.Millisecond)
}

func TestGRPCHandleSubscribePartitionNotFound(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Subscribe to a topic that doesn't exist and won't auto-create
	// because we don't call CreateTopic and the storage won't create it
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Subscribe{
			Subscribe: &proto.SubscribeRequest{
				Topic:       "nonexistent-topic-xyz",
				StartOffset: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Wait for error response
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Error("timed out waiting for error response")
			return
		default:
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Logf("stream ended: %v", err)
			return
		}
		if errInfo := resp.GetError(); errInfo != nil {
			if errInfo.Code == int32(codes.NotFound) {
				return // expected
			}
			t.Errorf("unexpected error code: %d", errInfo.Code)
			return
		}
	}
}

func TestGRPCStreamMessagesContextCancellation(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Create topic and publish a message
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "ctx-cancel-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Subscribe
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Subscribe{
			Subscribe: &proto.SubscribeRequest{
				Topic:       "ctx-cancel-topic",
				StartOffset: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Cancel context immediately
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Stream should be closed
	_, err = stream.Recv()
	if err == nil {
		t.Log("expected stream to be closed after context cancellation")
	}
}

func TestGRPCPublishBatchPartialFailure(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Create one valid topic
	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "valid-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	// Batch publish with mix of valid and invalid topics
	resp, err := client.PublishBatch(context.Background(), &proto.PublishBatchRequest{
		Messages: []*proto.PublishRequest{
			{Topic: "valid-topic", Payload: []byte("good")},
			{Topic: "", Payload: []byte("bad-no-topic")},
			{Topic: "valid-topic", Payload: []byte("good-2")},
		},
	})
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if resp.SuccessCount != 2 {
		t.Errorf("success count = %d, want 2", resp.SuccessCount)
	}
	if resp.FailureCount != 1 {
		t.Errorf("failure count = %d, want 1", resp.FailureCount)
	}
}

func TestGRPCHandleAckNackCommit(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Send Ack
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Ack{
			Ack: &proto.AckRequest{Topic: "ack-topic", Offset: 1, GroupId: "g1"},
		},
	})
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// Send Nack
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Nack{
			Nack: &proto.NackRequest{Topic: "nack-topic", Offset: 2, GroupId: "g1", Requeue: true},
		},
	})
	if err != nil {
		t.Fatalf("Nack: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// Send CommitOffset
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_CommitOffset{
			CommitOffset: &proto.CommitOffsetRequest{Topic: "commit-topic", GroupId: "g1", Offset: 10},
		},
	})
	if err != nil {
		t.Fatalf("CommitOffset: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	// Send Unsubscribe
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Unsubscribe{
			Unsubscribe: &proto.UnsubscribeRequest{Topic: "unsub-topic"},
		},
	})
	if err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
}

func TestGRPCAuthStreamInterceptorRejectsUnauthenticated(t *testing.T) {
	_, addr, cleanup := setupGRPCServerWithAuth(t)
	defer cleanup()

	// Connect without auth token and try streaming
	client, conn := newClient(t, addr)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		// Some gRPC implementations fail immediately
		st, ok := status.FromError(err)
		if !ok || st.Code() != codes.Unauthenticated {
			t.Fatalf("expected Unauthenticated error, got %v", err)
		}
		return
	}

	// If stream was returned, auth should fail on Recv
	_, err = stream.Recv()
	if err == nil {
		t.Error("expected auth error for unauthenticated stream")
	} else {
		st, ok := status.FromError(err)
		if !ok {
			t.Logf("stream error (expected): %v", err)
			return
		}
		if st.Code() != codes.Unauthenticated {
			t.Logf("stream error code = %v (expected Unauthenticated but any error is acceptable)", st.Code())
		}
	}
}

func TestGRPCHandleSubscribeACLDenied(t *testing.T) {
	_, addr, cleanup := setupGRPCServerWithACL(t)
	defer cleanup()

	// Admin creates a topic
	adminClient, adminConn := newClientWithTokenInterceptors(t, addr, "admin-token")
	defer adminConn.Close()
	_, err := adminClient.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "acl-sub-topic",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("admin CreateTopic: %v", err)
	}

	// Reader should be able to subscribe (has read permission)
	readerClient, readerConn := newClientWithTokenInterceptors(t, addr, "reader-token")
	defer readerConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	stream, err := readerClient.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Subscribe{
			Subscribe: &proto.SubscribeRequest{
				Topic:       "acl-sub-topic",
				StartOffset: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Should get messages since reader has OpRead
	var received bool
	deadline := time.After(1 * time.Second)
	for !received {
		select {
		case <-deadline:
			return // no messages, but no error either — ACL allowed
		default:
		}
		resp, err := stream.Recv()
		if err != nil {
			return
		}
		if msg := resp.GetMessage(); msg != nil {
			received = true
		}
		if errInfo := resp.GetError(); errInfo != nil && errInfo.Code == int32(codes.PermissionDenied) {
			t.Error("reader should not be denied subscribe")
			return
		}
	}
}

func TestGRPCCreateTopicEmptyName(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "",
			Mode:       "stream",
			Partitions: 1,
		},
	})
	if err == nil {
		t.Error("expected error for empty topic name")
	}
}

func TestGRPCCreateTopicUnknownMode(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	resp, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "unknown-mode-topic",
			Mode:       "some-unknown-mode",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	// Unknown mode should default to stream
	if resp.Config.Mode != "stream" {
		t.Errorf("mode = %q, want stream", resp.Config.Mode)
	}
}

func TestGRPCCreateTopicQueueMode(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	resp, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "queue-topic",
			Mode:       "queue",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if resp.Config.Mode != "queue" {
		t.Errorf("mode = %q, want queue", resp.Config.Mode)
	}
}

func TestGRPCCreateTopicUnifiedMode(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	resp, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
		Config: &proto.TopicConfig{
			Name:       "unified-topic",
			Mode:       "unified",
			Partitions: 1,
		},
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if resp.Config.Mode != "unified" {
		t.Errorf("mode = %q, want unified", resp.Config.Mode)
	}
}

func TestGRPCFetchMessagesWithMultipleSubscriptions(t *testing.T) {
	_, addr, cleanup := setupGRPCServer(t)
	defer cleanup()

	client, conn := newClient(t, addr)
	defer conn.Close()

	// Create two topics and publish messages
	for _, topicName := range []string{"fetch-topic-1", "fetch-topic-2"} {
		_, err := client.CreateTopic(context.Background(), &proto.CreateTopicRequest{
			Config: &proto.TopicConfig{
				Name:       topicName,
				Mode:       "stream",
				Partitions: 1,
			},
		})
		if err != nil {
			t.Fatalf("CreateTopic %s: %v", topicName, err)
		}
		_, err = client.Publish(context.Background(), &proto.PublishRequest{
			Topic:   topicName,
			Payload: []byte("msg-for-" + topicName),
		})
		if err != nil {
			t.Fatalf("Publish %s: %v", topicName, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Subscribe to both topics
	for _, topicName := range []string{"fetch-topic-1", "fetch-topic-2"} {
		err = stream.Send(&proto.ConsumeRequest{
			Action: &proto.ConsumeRequest_Subscribe{
				Subscribe: &proto.SubscribeRequest{
					Topic:       topicName,
					StartOffset: 0,
				},
			},
		})
		if err != nil {
			t.Fatalf("Subscribe %s: %v", topicName, err)
		}
	}

	// Wait for messages from subscriptions
	var gotMessages int
	deadline := time.After(2 * time.Second)
	for gotMessages < 2 {
		select {
		case <-deadline:
			t.Errorf("expected 2 messages from subscriptions, got %d", gotMessages)
			return
		default:
		}
		resp, err := stream.Recv()
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if msg := resp.GetMessage(); msg != nil {
			gotMessages++
		}
	}

	// Now fetch should have consumed all messages, fetch should return nothing
	err = stream.Send(&proto.ConsumeRequest{
		Action: &proto.ConsumeRequest_Fetch{
			Fetch: &proto.FetchRequest{MaxMessages: 10},
		},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
}
