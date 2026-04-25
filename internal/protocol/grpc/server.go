// Package grpc implements the ChimeraMQ gRPC protocol adapter.
//
// It provides a gRPC service for publishing and consuming messages with
// bidirectional streaming support. The adapter runs on a dedicated port
// configured via listener.grpc_port.
package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
	proto "github.com/chimeramq/chimera/internal/protocol/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// authInterceptor middleware checks auth before allowing unary RPCs.
// Health RPC is always allowed.
func authInterceptor(b *broker.Broker) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Skip auth for Health RPC
		if info.FullMethod == "/chimera.ChimeraService/Health" {
			return handler(ctx, req)
		}

		ctx, id, err := authenticateGRPCRequest(b, ctx)
		if err != nil {
			return nil, err
		}

		// ACL check for topic operations using actual topic name
		aclEngine := b.ACLEngine()
		if aclEngine != nil {
			var topic string
			var op auth.Operation
			var needsACL bool
			switch r := req.(type) {
			case *proto.PublishRequest:
				topic = r.Topic
				op = auth.OpWrite
				needsACL = true
			case *proto.CreateTopicRequest:
				if r.Config != nil {
					topic = r.Config.Name
				}
				op = auth.OpCreate
				needsACL = true
			case *proto.DeleteTopicRequest:
				topic = r.Topic
				op = auth.OpDelete
				needsACL = true
			}
			if needsACL && topic != "" && !aclEngine.Check(id, auth.ResourceTopic, topic, op) {
				return nil, status.Errorf(codes.PermissionDenied, "operation denied by ACL")
			}
		}

		return handler(ctx, req)
	}
}

// authStreamInterceptor middleware checks auth before allowing streaming RPCs.
func authStreamInterceptor(b *broker.Broker) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Skip auth for Health RPC
		if info.FullMethod == "/chimera.ChimeraService/Health" {
			return handler(srv, ss)
		}

		ctx := ss.Context()
		_, id, err := authenticateGRPCRequest(b, ctx)
		if err != nil {
			return err
		}

		// Attach identity to context for downstream ACL checks
		ctx = context.WithValue(ctx, authIdentityKey{}, id)

		// Wrap the stream with the authenticated context
		wrapped := &wrappedServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}

		return handler(srv, wrapped)
	}
}

type authIdentityKey struct{}

// wrappedServerStream carries the authenticated context through the stream.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// authenticateGRPCRequest extracts and validates credentials from metadata.
func authenticateGRPCRequest(b *broker.Broker, ctx context.Context) (context.Context, *auth.Identity, error) {
	// Extract credentials from metadata
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, nil, status.Errorf(codes.Unauthenticated, "missing metadata")
	}

	// If auth is disabled (nil provider), allow all traffic
	authProvider := b.AuthProvider()
	if authProvider == nil {
		return ctx, &auth.Identity{}, nil
	}

	var token string
	if vals := md.Get("authorization"); len(vals) > 0 {
		token = vals[0]
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}
	}

	creds := auth.Credentials{Token: token}
	id, err := authProvider.Authenticate(ctx, creds)
	if err != nil {
		return ctx, nil, status.Errorf(codes.Unauthenticated, "auth failed: %v", err)
	}

	return ctx, id, nil
}

// GetIdentityFromContext retrieves the authenticated identity from context.
func GetIdentityFromContext(ctx context.Context) *auth.Identity {
	if id, ok := ctx.Value(authIdentityKey{}).(*auth.Identity); ok {
		return id
	}
	return nil
}

// topicModeFromProto converts a proto mode string to broker.TopicMode.
func topicModeFromProto(mode string) broker.TopicMode {
	switch mode {
	case "queue":
		return broker.ModeQueue
	case "unified":
		return broker.ModeUnified
	default:
		return broker.ModeStream
	}
}

// topicModeToProto converts broker.TopicMode to a proto mode string.
func topicModeToProto(mode broker.TopicMode) string {
	switch mode {
	case broker.ModeQueue:
		return "queue"
	case broker.ModeUnified:
		return "unified"
	default:
		return "stream"
	}
}

// Server implements the ChimeraMQ gRPC service.
type Server struct {
	proto.UnimplementedChimeraServiceServer

	broker   *broker.Broker
	listener net.Listener
	grpcSrv  *grpc.Server

	clients   sync.Map
	clientSeq atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewServer creates a new gRPC protocol server.
func NewServer(b *broker.Broker) (*Server, error) {
	addr := fmt.Sprintf("%s:%d", b.Config().Listener.Bind, b.Config().Listener.GRPCPort)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("grpc listen: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &Server{
		broker:   b,
		listener: listener,
		ctx:      ctx,
		cancel:   cancel,
	}

	s.grpcSrv = grpc.NewServer(
		grpc.MaxRecvMsgSize(10*1024*1024), // 10MB max message size
		grpc.UnaryInterceptor(authInterceptor(b)),
		grpc.StreamInterceptor(authStreamInterceptor(b)),
	)
	proto.RegisterChimeraServiceServer(s.grpcSrv, s)

	return s, nil
}

// Serve starts the gRPC server and blocks until stopped.
func (s *Server) Serve() error {
	s.broker.Logger().Info("gRPC adapter listening",
		"addr", s.listener.Addr().String(),
	)
	return s.grpcSrv.Serve(s.listener)
}

// Stop gracefully shuts down the gRPC server.
func (s *Server) Stop() {
	s.cancel()
	s.grpcSrv.GracefulStop()
	s.wg.Wait()
}

// --- RPC Implementations ---

// Publish handles single message publish.
func (s *Server) Publish(ctx context.Context, req *proto.PublishRequest) (*proto.PublishResponse, error) {
	env := &message.Envelope{
		Topic:       req.Topic,
		RoutingKey:  req.RoutingKey,
		Priority:    uint8(req.Priority),
		TTL:         req.TtlMs * 1e6,       // ms -> ns
		DeliverAt:   req.DeliverAtMs * 1e6, // ms -> ns
		Headers:     req.Headers,
		ContentType: req.ContentType,
		Payload:     req.Payload,
		SourceProto: message.ProtoGRPC,
	}

	offset, err := s.broker.Publish(env)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "publish failed: %v", err)
	}

	return &proto.PublishResponse{
		Topic:       env.Topic,
		PartitionId: env.PartitionID,
		Offset:      offset,
	}, nil
}

// PublishBatch handles batch message publish.
func (s *Server) PublishBatch(ctx context.Context, req *proto.PublishBatchRequest) (*proto.PublishBatchResponse, error) {
	results := make([]*proto.BatchPublishResult, len(req.Messages))
	var successCount, failureCount int

	for i, msg := range req.Messages {
		env := &message.Envelope{
			Topic:       msg.Topic,
			RoutingKey:  msg.RoutingKey,
			Priority:    uint8(msg.Priority),
			TTL:         msg.TtlMs * 1e6,
			DeliverAt:   msg.DeliverAtMs * 1e6,
			Headers:     msg.Headers,
			ContentType: msg.ContentType,
			Payload:     msg.Payload,
			SourceProto: message.ProtoGRPC,
		}

		offset, err := s.broker.Publish(env)
		if err != nil {
			s.broker.Logger().Error("batch publish failed", "index", i, "error", err)
			results[i] = &proto.BatchPublishResult{Ok: false}
			failureCount++
		} else {
			results[i] = &proto.BatchPublishResult{
				Ok:          true,
				PartitionId: env.PartitionID,
				Offset:      offset,
			}
			successCount++
		}
	}

	return &proto.PublishBatchResponse{
		Results:      results,
		SuccessCount: int32(successCount),
		FailureCount: int32(failureCount),
	}, nil
}

// Consume handles bidirectional streaming for message consumption.
func (s *Server) Consume(stream proto.ChimeraService_ConsumeServer) error {
	s.broker.RecordConnection("grpc")
	defer s.broker.RecordDisconnection("grpc")
	clientID := fmt.Sprintf("grpc-%d", s.clientSeq.Add(1))
	client := &grpcClientConn{
		clientID:      clientID,
		subscriptions: make(map[string]subscription),
		stream:        stream,
	}

	s.clients.Store(clientID, client)
	defer s.clients.Delete(clientID)

	s.broker.Logger().Info("grpc client connected", "client_id", clientID)

	// Heartbeat ticker
	hbDone := make(chan struct{})
	defer close(hbDone)
	s.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.broker.Logger().Error("grpc heartbeat panic", "client_id", clientID, "recover", r)
			}
		}()
		defer s.wg.Done()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-hbDone:
				return
			case <-ticker.C:
				_ = stream.Send(&proto.ConsumeResponse{
					Event: &proto.ConsumeResponse_Heartbeat{
						Heartbeat: &proto.Heartbeat{
							ServerTimestampMs: time.Now().UnixMilli(),
						},
					},
				})
			case <-stream.Context().Done():
				return
			}
		}
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			s.broker.Logger().Debug("grpc consume stream ended", "client_id", clientID)
			return nil
		}

		switch action := req.Action.(type) {
		case *proto.ConsumeRequest_Subscribe:
			s.handleSubscribe(client, action.Subscribe)
		case *proto.ConsumeRequest_Fetch:
			s.handleFetch(client, action.Fetch)
		case *proto.ConsumeRequest_Ack:
			s.handleAck(client, action.Ack)
		case *proto.ConsumeRequest_Nack:
			s.handleNack(client, action.Nack)
		case *proto.ConsumeRequest_CommitOffset:
			s.handleCommitOffset(client, action.CommitOffset)
		case *proto.ConsumeRequest_Unsubscribe:
			s.handleUnsubscribe(client, action.Unsubscribe)
		}
	}
}

// CreateTopic handles topic creation.
func (s *Server) CreateTopic(ctx context.Context, req *proto.CreateTopicRequest) (*proto.CreateTopicResponse, error) {
	cfg := req.Config
	if cfg == nil {
		return nil, status.Error(codes.InvalidArgument, "config is required")
	}

	topicCfg := broker.TopicConfig{
		Name:       cfg.Name,
		Mode:       topicModeFromProto(cfg.Mode),
		Partitions: cfg.Partitions,
	}

	tm := s.broker.Topics()
	if tm == nil {
		return nil, status.Error(codes.Unavailable, "topic manager not available")
	}

	if err := tm.CreateTopic(topicCfg); err != nil {
		return nil, status.Errorf(codes.Internal, "create topic failed: %v", err)
	}

	existingCfg, _ := tm.GetTopic(cfg.Name)
	respCfg := &proto.TopicConfig{
		Name:       topicCfg.Name,
		Mode:       topicModeToProto(topicCfg.Mode),
		Partitions: topicCfg.Partitions,
	}
	if existingCfg != nil {
		respCfg.RetentionMs = existingCfg.RetentionTime.Milliseconds()
	}

	return &proto.CreateTopicResponse{
		Created: true,
		Config:  respCfg,
	}, nil
}

// DeleteTopic handles topic deletion.
func (s *Server) DeleteTopic(ctx context.Context, req *proto.DeleteTopicRequest) (*proto.DeleteTopicResponse, error) {
	tm := s.broker.Topics()
	if tm == nil {
		return nil, status.Error(codes.Unavailable, "topic manager not available")
	}
	if err := tm.DeleteTopic(req.Topic); err != nil {
		return nil, status.Errorf(codes.Internal, "delete topic failed: %v", err)
	}
	return &proto.DeleteTopicResponse{Deleted: true}, nil
}

// GetTopicInfo returns topic information.
func (s *Server) GetTopicInfo(ctx context.Context, req *proto.GetTopicInfoRequest) (*proto.GetTopicInfoResponse, error) {
	tm := s.broker.Topics()
	if tm == nil {
		return nil, status.Error(codes.Unavailable, "topic manager not available")
	}

	tc, ok := tm.GetTopic(req.Topic)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "topic %q not found", req.Topic)
	}

	partitions := make([]*proto.PartitionInfo, 0, tc.Partitions)
	for i := uint32(0); i < tc.Partitions; i++ {
		part, err := s.broker.Storage().GetOrCreatePartition(req.Topic, i)
		if err != nil {
			continue
		}
		partitions = append(partitions, &proto.PartitionInfo{
			Id:         i,
			HeadOffset: part.HighWatermark(),
			TailOffset: part.LogStartOffset(),
		})
	}

	return &proto.GetTopicInfoResponse{
		Config: &proto.TopicConfig{
			Name:       tc.Name,
			Mode:       topicModeToProto(tc.Mode),
			Partitions: tc.Partitions,
		},
		Partitions: partitions,
	}, nil
}

// Health returns service health status.
func (s *Server) Health(ctx context.Context, req *proto.HealthRequest) (*proto.HealthResponse, error) {
	return &proto.HealthResponse{
		Healthy:  true,
		Version:  "0.1.0",
		UptimeMs: time.Since(s.broker.StartTime()).Milliseconds(),
	}, nil
}

// --- Stream Handlers ---

func (s *Server) handleSubscribe(client *grpcClientConn, req *proto.SubscribeRequest) {
	// ACL check for subscription
	aclEngine := s.broker.ACLEngine()
	if aclEngine != nil {
		id := GetIdentityFromContext(client.stream.Context())
		if id != nil && !aclEngine.Check(id, auth.ResourceTopic, req.Topic, auth.OpRead) {
			_ = client.stream.Send(&proto.ConsumeResponse{
				Event: &proto.ConsumeResponse_Error{
					Error: &proto.ErrorInfo{Message: fmt.Sprintf("subscribe denied by ACL: %s", req.Topic), Code: int32(codes.PermissionDenied)},
				},
			})
			return
		}
	}

	part, err := s.broker.Storage().GetOrCreatePartition(req.Topic, 0)
	if err != nil {
		_ = client.stream.Send(&proto.ConsumeResponse{
			Event: &proto.ConsumeResponse_Error{
				Error: &proto.ErrorInfo{Message: fmt.Sprintf("topic not found: %s", req.Topic), Code: int32(codes.NotFound)},
			},
		})
		return
	}

	client.mu.Lock()
	client.subscriptions[req.Topic] = subscription{
		topic:      req.Topic,
		groupID:    req.GroupId,
		nextOffset: req.StartOffset,
		partition:  part,
	}
	client.mu.Unlock()

	// Start streaming messages for this subscription
	s.wg.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.broker.Logger().Error("grpc stream panic", "topic", req.Topic, "recover", r)
			}
		}()
		defer s.wg.Done()
		s.streamMessages(client, req.Topic)
	}()
}

func (s *Server) streamMessages(client *grpcClientConn, topic string) {
	client.mu.RLock()
	sub, ok := client.subscriptions[topic]
	client.mu.RUnlock()
	if !ok {
		return
	}

	// Read messages from the partition starting at nextOffset
	for {
		client.mu.RLock()
		offset := sub.nextOffset
		client.mu.RUnlock()

		data, err := sub.partition.Read(offset)
		if err != nil {
			// No messages yet, wait a bit
			select {
			case <-client.stream.Context().Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		// Decrypt if at-rest encryption is enabled
		if enc := s.broker.Encryptor(); enc != nil {
			segmentID := topic + "/0"
			decrypted, decErr := enc.Decrypt(data, segmentID)
			if decErr == nil {
				data = decrypted
			}
		}

		env, err := message.Unmarshal(data)
		if err != nil {
			continue
		}

		msg := envelopeToProto(env)
		if err := client.stream.Send(&proto.ConsumeResponse{
			Event: &proto.ConsumeResponse_Message{Message: msg},
		}); err != nil {
			return
		}

		s.broker.Metrics().MessageOut(env.Topic, env.PartitionID, sub.groupID)

		client.mu.Lock()
		sub.nextOffset = offset + 1
		client.subscriptions[topic] = sub
		client.mu.Unlock()
	}
}

func (s *Server) handleFetch(client *grpcClientConn, req *proto.FetchRequest) {
	client.mu.RLock()
	subs := make([]subscription, 0, len(client.subscriptions))
	for _, sub := range client.subscriptions {
		subs = append(subs, sub)
	}
	client.mu.RUnlock()

	maxPerSub := int(req.MaxMessages)
	if maxPerSub <= 0 {
		maxPerSub = 10
	}

	for _, sub := range subs {
		sent := 0
		for sent < maxPerSub {
			client.mu.RLock()
			offset := sub.nextOffset
			client.mu.RUnlock()

			data, err := sub.partition.Read(offset)
			if err != nil {
				break
			}

			// Decrypt if at-rest encryption is enabled
			if enc := s.broker.Encryptor(); enc != nil {
				segmentID := sub.topic + "/0"
				decrypted, decErr := enc.Decrypt(data, segmentID)
				if decErr == nil {
					data = decrypted
				}
			}

			env, err := message.Unmarshal(data)
			if err != nil {
				client.mu.Lock()
				sub.nextOffset = offset + 1
				client.subscriptions[sub.topic] = sub
				client.mu.Unlock()
				continue
			}

			msg := envelopeToProto(env)
			if err := client.stream.Send(&proto.ConsumeResponse{
				Event: &proto.ConsumeResponse_Message{Message: msg},
			}); err != nil {
				return
			}

			s.broker.Metrics().MessageOut(env.Topic, env.PartitionID, "")

			client.mu.Lock()
			sub.nextOffset = offset + 1
			client.subscriptions[sub.topic] = sub
			client.mu.Unlock()
			sent++
		}
	}
}

func (s *Server) handleAck(client *grpcClientConn, req *proto.AckRequest) {
	s.broker.Logger().Debug("grpc ack", "topic", req.Topic, "group", req.GroupId, "offset", req.Offset)
}

func (s *Server) handleNack(client *grpcClientConn, req *proto.NackRequest) {
	s.broker.Logger().Debug("grpc nack", "topic", req.Topic, "group", req.GroupId, "requeue", req.Requeue)
}

func (s *Server) handleCommitOffset(client *grpcClientConn, req *proto.CommitOffsetRequest) {
	s.broker.Logger().Debug("grpc commit offset", "topic", req.Topic, "group", req.GroupId, "offset", req.Offset)
}

func (s *Server) handleUnsubscribe(client *grpcClientConn, req *proto.UnsubscribeRequest) {
	client.mu.Lock()
	delete(client.subscriptions, req.Topic)
	client.mu.Unlock()
}

// --- Helpers ---

type subscription struct {
	topic      string
	groupID    string
	nextOffset uint64
	partition  interface {
		Read(offset uint64) ([]byte, error)
	}
}

type grpcClientConn struct {
	clientID      string
	stream        proto.ChimeraService_ConsumeServer
	mu            sync.RWMutex
	subscriptions map[string]subscription
}

func envelopeToProto(env *message.Envelope) *proto.Message {
	msgID := make([]byte, 16)
	copy(msgID, env.MessageID[:])

	headers := make(map[string][]byte)
	for k, v := range env.Headers {
		headers[k] = v
	}

	return &proto.Message{
		Topic:       env.Topic,
		PartitionId: env.PartitionID,
		Offset:      env.Sequence,
		MessageId:   msgID,
		TimestampMs: env.Timestamp / 1e6, // ns -> ms
		RoutingKey:  env.RoutingKey,
		Priority:    int32(env.Priority),
		Headers:     headers,
		ContentType: env.ContentType,
		Payload:     env.Payload,
		SchemaId:    env.SchemaID,
	}
}
