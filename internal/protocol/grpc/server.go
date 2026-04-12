// Package grpc implements a gRPC protocol adapter for ChimeraMQ.
// This adapter provides Protocol Buffers over HTTP/2 streaming support.
package grpc

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Server implements the gRPC protocol adapter.
type Server struct {
	broker     *broker.Broker
	grpcServer *grpc.Server
	listener   net.Listener
	mu         sync.RWMutex
	stopped    bool
}

// Config holds gRPC server configuration.
type Config struct {
	Enabled    bool
	Bind       string
	Port       int
	TLSEnabled bool
	CertFile   string
	KeyFile    string
}

// NewServer creates a new gRPC server.
func NewServer(b *broker.Broker, cfg Config) (*Server, error) {
	var opts []grpc.ServerOption

	// Add interceptors
	opts = append(opts, grpc.UnaryInterceptor(unaryInterceptor(b)))
	opts = append(opts, grpc.StreamInterceptor(streamInterceptor(b)))

	s := &Server{
		broker:     b,
		grpcServer: grpc.NewServer(opts...),
	}

	// Register services
	RegisterChimeraServer(s.grpcServer, &chimeraService{server: s})

	return s, nil
}

// Start starts the gRPC server.
func (s *Server) Start() error {
	cfg := s.broker.Config()
	addr := fmt.Sprintf("%s:%d", cfg.Listener.Bind, 50051) // Default gRPC port

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	s.broker.Logger().Info("gRPC server listening", "addr", addr)

	return s.grpcServer.Serve(listener)
}

// Stop stops the gRPC server.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return
	}
	s.stopped = true

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

// HandleConnection is not used for gRPC - it has its own listener.
func (s *Server) HandleConnection(conn net.Conn, _ []byte) error {
	// gRPC doesn't use the protocol mux - it has its own listener
	return fmt.Errorf("gRPC uses dedicated listener, not protocol mux")
}

// chimeraService implements the Chimera gRPC service.
type chimeraService struct {
	server *Server
}

// Publish publishes a single message.
func (c *chimeraService) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
	env := &message.Envelope{
		Topic:       req.Topic,
		Payload:     req.Payload,
		ContentType: req.ContentType,
		SourceProto: message.ProtoHTTP, // Use HTTP as closest match
	}

	offset, err := c.server.broker.Publish(env)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "publish failed: %v", err)
	}

	return &PublishResponse{
		Offset:    offset,
		Partition: env.PartitionID,
		Success:   true,
	}, nil
}

// Subscribe streams messages from a topic.
func (c *chimeraService) Subscribe(req *SubscribeRequest, stream Chimera_SubscribeServer) error {
	topic := req.Topic
	partition := req.Partition
	offset := req.Offset

	if offset == 0 && req.StartFrom == SubscribeRequest_LATEST {
		// Start from latest offset
		part, err := c.server.broker.Storage().GetOrCreatePartition(topic, partition)
		if err == nil {
			offset = part.HighWatermark()
		}
	}

	// Stream messages
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		msgs, nextOffset, err := c.server.broker.StreamEngine().Fetch(
			topic, partition, offset, int(req.BatchSize),
			time.Duration(req.TimeoutMs)*time.Millisecond,
		)
		if err != nil {
			return status.Errorf(codes.Internal, "fetch failed: %v", err)
		}

		for _, msg := range msgs {
			resp := &MessageResponse{
				Topic:       msg.Topic,
				Partition:   msg.PartitionID,
				Offset:      msg.Sequence,
				Payload:     msg.Payload,
				ContentType: msg.ContentType,
				Timestamp:   uint64(msg.Timestamp),
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}

		offset = nextOffset

		// If no messages, wait a bit before next fetch
		if len(msgs) == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// StreamPublish handles bidirectional streaming publish.
func (c *chimeraService) StreamPublish(stream Chimera_StreamPublishServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		env := &message.Envelope{
			Topic:       req.Topic,
			Payload:     req.Payload,
			ContentType: req.ContentType,
			SourceProto: message.ProtoHTTP,
		}

		offset, err := c.server.broker.Publish(env)
		if err != nil {
			if err := stream.Send(&PublishResponse{
				Success: false,
				Error:   err.Error(),
			}); err != nil {
				return err
			}
			continue
		}

		if err := stream.Send(&PublishResponse{
			Offset:    offset,
			Partition: env.PartitionID,
			Success:   true,
		}); err != nil {
			return err
		}
	}
}

// CreateTopic creates a new topic.
func (c *chimeraService) CreateTopic(ctx context.Context, req *CreateTopicRequest) (*TopicResponse, error) {
	cfg := broker.TopicConfig{
		Name:       req.Name,
		Partitions: req.Partitions,
	}

	switch req.Mode {
	case TopicMode_STREAM:
		cfg.Mode = broker.ModeStream
	case TopicMode_QUEUE:
		cfg.Mode = broker.ModeQueue
	default:
		cfg.Mode = broker.ModeUnified
	}

	if err := c.server.broker.Topics().CreateTopic(cfg); err != nil {
		return nil, status.Errorf(codes.AlreadyExists, "topic exists: %v", err)
	}

	return &TopicResponse{
		Name:       req.Name,
		Partitions: req.Partitions,
		Mode:       req.Mode,
		Success:    true,
	}, nil
}

// DeleteTopic deletes a topic.
func (c *chimeraService) DeleteTopic(ctx context.Context, req *DeleteTopicRequest) (*TopicResponse, error) {
	if err := c.server.broker.Topics().DeleteTopic(req.Name); err != nil {
		return nil, status.Errorf(codes.NotFound, "topic not found: %v", err)
	}

	return &TopicResponse{
		Name:    req.Name,
		Success: true,
	}, nil
}

// ListTopics lists all topics.
func (c *chimeraService) ListTopics(ctx context.Context, req *ListTopicsRequest) (*ListTopicsResponse, error) {
	topics := c.server.broker.Topics().ListTopics()

	resp := &ListTopicsResponse{
		Topics: make([]*TopicInfo, 0, len(topics)),
	}

	for _, t := range topics {
		info := &TopicInfo{
			Name:       t.Name,
			Partitions: t.Partitions,
		}
		switch t.Mode {
		case broker.ModeStream:
			info.Mode = TopicMode_STREAM
		case broker.ModeQueue:
			info.Mode = TopicMode_QUEUE
		default:
			info.Mode = TopicMode_UNIFIED
		}
		resp.Topics = append(resp.Topics, info)
	}

	return resp, nil
}

// Health returns the health status.
func (c *chimeraService) Health(ctx context.Context, req *HealthRequest) (*HealthResponse, error) {
	return &HealthResponse{
		Status:    HealthStatus_HEALTHY,
		Timestamp: uint64(time.Now().Unix()),
	}, nil
}

// unaryInterceptor provides unary RPC interception.
func unaryInterceptor(b *broker.Broker) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Check authentication if enabled
		if provider := b.AuthProvider(); provider != nil {
			md, ok := metadata.FromIncomingContext(ctx)
			if !ok {
				return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
			}

			authHeader := md.Get("authorization")
			if len(authHeader) == 0 {
				return nil, status.Errorf(codes.Unauthenticated, "missing authorization")
			}

			token := authHeader[0]
			if len(token) > 7 && token[:7] == "Bearer " {
				token = token[7:]
			}

			creds := auth.Credentials{Token: token}
			_, err := provider.Authenticate(ctx, creds)
			if err != nil {
				return nil, status.Errorf(codes.Unauthenticated, "invalid credentials")
			}
		}

		return handler(ctx, req)
	}
}

// streamInterceptor provides streaming RPC interception.
func streamInterceptor(b *broker.Broker) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Check authentication if enabled
		if provider := b.AuthProvider(); provider != nil {
			md, ok := metadata.FromIncomingContext(stream.Context())
			if !ok {
				return status.Errorf(codes.Unauthenticated, "missing metadata")
			}

			authHeader := md.Get("authorization")
			if len(authHeader) == 0 {
				return status.Errorf(codes.Unauthenticated, "missing authorization")
			}

			token := authHeader[0]
			if len(token) > 7 && token[:7] == "Bearer " {
				token = token[7:]
			}

			creds := auth.Credentials{Token: token}
			_, err := provider.Authenticate(stream.Context(), creds)
			if err != nil {
				return status.Errorf(codes.Unauthenticated, "invalid credentials")
			}
		}

		return handler(srv, stream)
	}
}

// Detector detects gRPC protocol by checking for HTTP/2 connection preface.
type Detector struct{}

// Detect checks if the peeked bytes match the gRPC/HTTP2 connection preface.
func (d *Detector) Detect(peek []byte) bool {
	// HTTP/2 connection preface: "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	// Or just check for common gRPC patterns
	if len(peek) < 4 {
		return false
	}
	// Check for HTTP/2 preface start: "PRI "
	if string(peek[:4]) == "PRI " {
		return true
	}
	return false
}

// BytesNeeded returns the minimum bytes needed for detection.
func (d *Detector) BytesNeeded() int { return 24 }

// Protocol returns the protocol name.
func (s *Server) Protocol() string {
	return "grpc"
}

// ChimeraServer is the server API for Chimera service.
type ChimeraServer interface {
	Publish(context.Context, *PublishRequest) (*PublishResponse, error)
	Subscribe(*SubscribeRequest, Chimera_SubscribeServer) error
	StreamPublish(Chimera_StreamPublishServer) error
	CreateTopic(context.Context, *CreateTopicRequest) (*TopicResponse, error)
	DeleteTopic(context.Context, *DeleteTopicRequest) (*TopicResponse, error)
	ListTopics(context.Context, *ListTopicsRequest) (*ListTopicsResponse, error)
	Health(context.Context, *HealthRequest) (*HealthResponse, error)
}

// Chimera_SubscribeServer is the server API for Subscribe RPC.
type Chimera_SubscribeServer interface {
	Send(*MessageResponse) error
	grpc.ServerStream
}

// Chimera_StreamPublishServer is the server API for StreamPublish RPC.
type Chimera_StreamPublishServer interface {
	Send(*PublishResponse) error
	Recv() (*PublishRequest, error)
	grpc.ServerStream
}

// RegisterChimeraServer registers the ChimeraServer implementation.
func RegisterChimeraServer(s *grpc.Server, srv ChimeraServer) {
	// Register unary methods
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "chimera.Chimera",
		HandlerType: (*ChimeraServer)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Publish",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(PublishRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return srv.(ChimeraServer).Publish(ctx, in)
					}
					info := &grpc.UnaryServerInfo{
						Server:     srv,
						FullMethod: "/chimera.Chimera/Publish",
					}
					handler := func(ctx context.Context, req interface{}) (interface{}, error) {
						return srv.(ChimeraServer).Publish(ctx, req.(*PublishRequest))
					}
					return interceptor(ctx, in, info, handler)
				},
			},
			{
				MethodName: "CreateTopic",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(CreateTopicRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return srv.(ChimeraServer).CreateTopic(ctx, in)
					}
					info := &grpc.UnaryServerInfo{
						Server:     srv,
						FullMethod: "/chimera.Chimera/CreateTopic",
					}
					handler := func(ctx context.Context, req interface{}) (interface{}, error) {
						return srv.(ChimeraServer).CreateTopic(ctx, req.(*CreateTopicRequest))
					}
					return interceptor(ctx, in, info, handler)
				},
			},
			{
				MethodName: "DeleteTopic",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(DeleteTopicRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return srv.(ChimeraServer).DeleteTopic(ctx, in)
					}
					info := &grpc.UnaryServerInfo{
						Server:     srv,
						FullMethod: "/chimera.Chimera/DeleteTopic",
					}
					handler := func(ctx context.Context, req interface{}) (interface{}, error) {
						return srv.(ChimeraServer).DeleteTopic(ctx, req.(*DeleteTopicRequest))
					}
					return interceptor(ctx, in, info, handler)
				},
			},
			{
				MethodName: "ListTopics",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(ListTopicsRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return srv.(ChimeraServer).ListTopics(ctx, in)
					}
					info := &grpc.UnaryServerInfo{
						Server:     srv,
						FullMethod: "/chimera.Chimera/ListTopics",
					}
					handler := func(ctx context.Context, req interface{}) (interface{}, error) {
						return srv.(ChimeraServer).ListTopics(ctx, req.(*ListTopicsRequest))
					}
					return interceptor(ctx, in, info, handler)
				},
			},
			{
				MethodName: "Health",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					in := new(HealthRequest)
					if err := dec(in); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return srv.(ChimeraServer).Health(ctx, in)
					}
					info := &grpc.UnaryServerInfo{
						Server:     srv,
						FullMethod: "/chimera.Chimera/Health",
					}
					handler := func(ctx context.Context, req interface{}) (interface{}, error) {
						return srv.(ChimeraServer).Health(ctx, req.(*HealthRequest))
					}
					return interceptor(ctx, in, info, handler)
				},
			},
		},
		Streams: []grpc.StreamDesc{
			{
				StreamName: "Subscribe",
				Handler: func(srv interface{}, stream grpc.ServerStream) error {
					return srv.(ChimeraServer).Subscribe(&SubscribeRequest{}, &subscribeServer{stream})
				},
				ServerStreams: true,
			},
			{
				StreamName: "StreamPublish",
				Handler: func(srv interface{}, stream grpc.ServerStream) error {
					return srv.(ChimeraServer).StreamPublish(&streamPublishServer{stream})
				},
				ClientStreams: true,
			},
		},
		Metadata: "chimera.proto",
	}, srv)
}

// Message types
type PublishRequest struct {
	Topic       string
	Payload     []byte
	ContentType string
	Headers     map[string]string
}

type PublishResponse struct {
	Offset    uint64
	Partition uint32
	Success   bool
	Error     string
}

type SubscribeRequest struct {
	Topic     string
	Partition uint32
	Offset    uint64
	BatchSize uint32
	TimeoutMs uint32
	StartFrom SubscribeRequest_StartFrom
}

type SubscribeRequest_StartFrom int32

const (
	SubscribeRequest_EARLIEST SubscribeRequest_StartFrom = 0
	SubscribeRequest_LATEST   SubscribeRequest_StartFrom = 1
)

type MessageResponse struct {
	Topic       string
	Partition   uint32
	Offset      uint64
	Payload     []byte
	ContentType string
	Timestamp   uint64
	Headers     map[string]string
}

type CreateTopicRequest struct {
	Name       string
	Partitions uint32
	Mode       TopicMode
}

type DeleteTopicRequest struct {
	Name string
}

type TopicResponse struct {
	Name       string
	Partitions uint32
	Mode       TopicMode
	Success    bool
	Error      string
}

type ListTopicsRequest struct{}

type ListTopicsResponse struct {
	Topics []*TopicInfo
}

type TopicInfo struct {
	Name       string
	Partitions uint32
	Mode       TopicMode
}

type HealthRequest struct{}

type HealthResponse struct {
	Status    HealthStatus
	Timestamp uint64
}

// TopicMode represents the topic mode.
type TopicMode int32

const (
	TopicMode_UNIFIED TopicMode = 0
	TopicMode_STREAM  TopicMode = 1
	TopicMode_QUEUE   TopicMode = 2
)

// HealthStatus represents the health status.
type HealthStatus int32

const (
	HealthStatus_HEALTHY   HealthStatus = 0
	HealthStatus_UNHEALTHY HealthStatus = 1
	HealthStatus_DEGRADED  HealthStatus = 2
)

// subscribeServer wraps grpc.ServerStream for Subscribe.
type subscribeServer struct {
	grpc.ServerStream
}

func (s *subscribeServer) Send(m *MessageResponse) error {
	return s.ServerStream.SendMsg(m)
}

// streamPublishServer wraps grpc.ServerStream for StreamPublish.
type streamPublishServer struct {
	grpc.ServerStream
}

func (s *streamPublishServer) Send(m *PublishResponse) error {
	return s.ServerStream.SendMsg(m)
}

func (s *streamPublishServer) Recv() (*PublishRequest, error) {
	m := new(PublishRequest)
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}
