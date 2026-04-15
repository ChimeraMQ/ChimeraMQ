package chimera

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/message"
)

// Server handles Chimera native protocol connections.
type Server struct {
	listener  net.Listener
	broker    *broker.Broker
	clients   sync.Map
	clientSeq atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Detector detects the Chimera native protocol by its magic bytes "CHMR".
type Detector struct{}

// Detect checks if the peeked bytes start with the Chimera magic "CHMR".
func (d *Detector) Detect(peek []byte) bool {
	return len(peek) >= 4 &&
		peek[0] == FrameMagic0 &&
		peek[1] == FrameMagic1 &&
		peek[2] == FrameMagic2 &&
		peek[3] == FrameMagic3
}

// BytesNeeded returns 4 (the length of the "CHMR" magic).
func (d *Detector) BytesNeeded() int { return 4 }

// NewServer creates a new Chimera protocol server.
func NewServer(b *broker.Broker) (*Server, error) {
	addr := fmt.Sprintf("%s:%d", b.Config().Listener.Bind, b.Config().Listener.Port)

	var listener net.Listener
	if b.Config().TLS.Enabled {
		// TLS support would require tls.NewListener here
		return nil, fmt.Errorf("TLS for Chimera protocol not yet implemented, use HTTP admin API with TLS")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		listener: listener,
		broker:   b,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// Serve starts accepting connections.
func (s *Server) Serve() error {
	maxConns := s.broker.Config().Limits.MaxConnections
	sem := make(chan struct{}, maxConns)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return nil
			default:
				continue
			}
		}

		select {
		case sem <- struct{}{}:
		default:
			conn.Close()
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() {
				if r := recover(); r != nil {
					s.broker.Logger().Error("chimera handler panic", "recover", r)
				}
			}()
			defer func() { <-sem }()
			s.handleConnection(conn)
		}()
	}
}

// StopAccepting stops accepting new connections.
func (s *Server) StopAccepting() {
	s.cancel()
	s.listener.Close()
}

// DisconnectAll closes all client connections.
func (s *Server) DisconnectAll() {
	s.clients.Range(func(key, value any) bool {
		client := value.(*ClientConn)
		client.conn.Close()
		return true
	})
}

// StopAll performs a full graceful shutdown.
func (s *Server) StopAll() {
	s.StopAccepting()
	s.DisconnectAll()
	s.wg.Wait()
}

// HandleConnection implements ProtocolHandler for use with the multiplexer.
func (s *Server) HandleConnection(conn net.Conn, _ []byte) error {
	s.handleConnection(conn)
	return nil
}

// Stop implements ProtocolHandler.
func (s *Server) Stop() {
	s.StopAll()
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	client := &ClientConn{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 64*1024),
		writer: bufio.NewWriterSize(conn, 64*1024),
		subs:   make(map[string]*Subscription),
	}

	// First frame must be CONNECT
	frame, err := DecodeFrame(client.reader)
	if err != nil {
		return
	}
	if frame.OpCode != OpConnect {
		return
	}

	payload := decodeConnect(frame.Payload)

	// V-02: Authentication check
	if s.broker.Config().Auth.Enabled {
		// Rate limit check
		if lim := s.broker.AuthLimiter(); lim != nil {
			clientIP := auth.ExtractIP(conn)
			if !lim.IsAllowed(clientIP) {
				connackPayload := encodeConnAck("", 1)
				connackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnAck, Payload: connackPayload})
				_ = client.writeFrame(connackFrame)
				return
			}
		}
		identity, authErr := s.authenticate(payload.Username, payload.Password)
		if authErr != nil {
			if lim := s.broker.AuthLimiter(); lim != nil {
				lim.RecordFailed(auth.ExtractIP(conn))
			}
			connackPayload := encodeConnAck("", 1) // status 1 = auth failed
			connackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnAck, Payload: connackPayload})
			_ = client.writeFrame(connackFrame)
			return
		}
		if lim := s.broker.AuthLimiter(); lim != nil {
			lim.RecordSuccess(auth.ExtractIP(conn))
		}
		client.identity = identity
	}

	if payload.ClientID == "" {
		client.clientID = fmt.Sprintf("client-%d", s.clientSeq.Add(1))
	} else {
		client.clientID = payload.ClientID
	}

	// V-09: ClientID collision detection — kick old connection
	if old, loaded := s.clients.LoadOrStore(client.clientID, client); loaded {
		oldClient := old.(*ClientConn)
		oldClient.conn.Close()
		s.clients.Store(client.clientID, client)
	}

	defer s.clients.Delete(client.clientID)

	// Send CONNACK
	connackPayload := encodeConnAck(client.clientID, 0)
	connackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnAck, Payload: connackPayload})
	if err := client.writeFrame(connackFrame); err != nil {
		return
	}

	// V-13: Keepalive with server-side bounds
	keepalive := time.Duration(payload.Keepalive) * time.Second
	if keepalive > 0 {
		if keepalive < 5*time.Second {
			keepalive = 5 * time.Second
		}
		if keepalive > 10*time.Minute {
			keepalive = 10 * time.Minute
		}
		_ = conn.SetReadDeadline(time.Now().Add(keepalive * 2))
	}

	// Main read loop
	for {
		frame, err := DecodeFrame(client.reader)
		if err != nil {
			return
		}

		if keepalive > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(keepalive * 2))
		}

		switch frame.OpCode {
		case OpConnect:
			newPayload := decodeConnect(frame.Payload)
			if s.broker.Config().Auth.Enabled {
				identity, authErr := s.authenticate(newPayload.Username, newPayload.Password)
				if authErr != nil {
					connackPayload := encodeConnAck("", 1)
					connackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnAck, Payload: connackPayload})
					_ = client.writeFrame(connackFrame)
					return
				}
				client.identity = identity
			}
			s.clients.Delete(client.clientID)
			if newPayload.ClientID == "" {
				client.clientID = fmt.Sprintf("client-%d", s.clientSeq.Add(1))
			} else {
				client.clientID = newPayload.ClientID
			}
			if old, loaded := s.clients.LoadOrStore(client.clientID, client); loaded {
				oldClient := old.(*ClientConn)
				oldClient.conn.Close()
				s.clients.Store(client.clientID, client)
			}
			connackPayload := encodeConnAck(client.clientID, 0)
			connackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnAck, Payload: connackPayload})
			_ = client.writeFrame(connackFrame)
		case OpPublish:
			s.handlePublish(client, frame)
		case OpBatchPublish:
			s.handleBatchPublish(client, frame)
		case OpSubscribe:
			s.handleSubscribe(client, frame)
		case OpFetch:
			s.handleFetch(client, frame)
		case OpAck:
			s.handleAck(client, frame)
		case OpNack:
			s.handleNack(client, frame)
		case OpCommitOffset:
			s.handleCommitOffset(client, frame)
		case OpCreateTopic:
			s.handleCreateTopic(client, frame)
		case OpDeleteTopic:
			s.handleDeleteTopic(client, frame)
		case OpPing:
			pong, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPong})
			_ = client.writeFrame(pong)
		case OpDisconnect:
			return
		}
	}
}

// authenticate validates username/password using the broker's auth provider.
func (s *Server) authenticate(username, password string) (*auth.Identity, error) {
	provider := s.broker.AuthProvider()
	if provider == nil {
		return nil, fmt.Errorf("no auth provider")
	}

	creds := auth.Credentials{
		Username: username,
		Password: password,
		Token:    password, // Chimera protocol may send token as password field
	}

	return provider.Authenticate(context.Background(), creds)
}

func (s *Server) handlePublish(client *ClientConn, frame *Frame) {
	payload := decodePublish(frame.Payload)

	// ACL check: write permission on topic
	if acl := s.broker.ACLEngine(); acl != nil {
		if !acl.Check(client.identity, auth.ResourceTopic, payload.Topic, auth.OpWrite) {
			s.sendError(client, "publish denied by ACL")
			return
		}
	}

	env := &message.Envelope{
		Topic:       payload.Topic,
		RoutingKey:  payload.RoutingKey,
		Priority:    payload.Priority,
		TTL:         payload.TTL,
		DeliverAt:   payload.DeliverAt,
		Headers:     payload.Headers,
		Payload:     payload.Body,
		SourceProto: message.ProtoChimera,
	}

	offset, err := s.broker.Publish(env)
	if err != nil {
		s.sendError(client, "publish failed")
		return
	}

	ackPayload := encodePubAck(env.Topic, env.PartitionID, offset)
	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPubAck, Payload: ackPayload})
	_ = client.writeFrame(ackFrame)
}

func (s *Server) handleBatchPublish(client *ClientConn, frame *Frame) {
	batch := decodeBatchPublish(frame.Payload)

	results := make([]BatchPubAckResult, len(batch.Messages))
	var okCount int

	for i, msg := range batch.Messages {
		// ACL check: write permission on topic
		if acl := s.broker.ACLEngine(); acl != nil {
			if !acl.Check(client.identity, auth.ResourceTopic, msg.Topic, auth.OpWrite) {
				results[i] = BatchPubAckResult{OK: false}
				continue
			}
		}

		env := &message.Envelope{
			Topic:       msg.Topic,
			RoutingKey:  msg.RoutingKey,
			Priority:    msg.Priority,
			TTL:         msg.TTL,
			DeliverAt:   msg.DeliverAt,
			Headers:     nil, // headers not supported in batch wire format
			Payload:     msg.Body,
			SourceProto: message.ProtoChimera,
		}

		offset, err := s.broker.Publish(env)
		if err != nil {
			s.broker.Logger().Error("batch publish failed", "index", i, "error", err)
			results[i] = BatchPubAckResult{OK: false}
		} else {
			results[i] = BatchPubAckResult{
				PartitionID: env.PartitionID,
				Offset:      offset,
				OK:          true,
			}
			okCount++
		}
	}

	var buf []byte
	buf = appendUint32(buf, uint32(len(results)))
	for _, r := range results {
		buf = appendUint32(buf, r.PartitionID)
		buf = appendUint64(buf, r.Offset)
		if r.OK {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	buf = appendUint32(buf, uint32(okCount))

	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpBatchPubAck, Payload: buf})
	_ = client.writeFrame(ackFrame)
}

func (s *Server) handleSubscribe(client *ClientConn, frame *Frame) {
	payload := decodeSubscribe(frame.Payload)

	// ACL check: read permission on topic
	if acl := s.broker.ACLEngine(); acl != nil {
		if !acl.Check(client.identity, auth.ResourceTopic, payload.Topic, auth.OpRead) {
			ackPayload := encodeSubAck(payload.Topic, false)
			ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubAck, Payload: ackPayload})
			_ = client.writeFrame(ackFrame)
			return
		}
	}

	sub := &Subscription{topic: payload.Topic, mode: payload.Mode}
	client.subsMu.Lock()
	client.subs[payload.Topic] = sub
	client.subsMu.Unlock()

	ackPayload := encodeSubAck(payload.Topic, true)
	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubAck, Payload: ackPayload})
	_ = client.writeFrame(ackFrame)
}

func (s *Server) handleFetch(client *ClientConn, frame *Frame) {
	r := newReader(frame.Payload)
	topic, _ := r.readString()

	// ACL check: read permission on topic
	if acl := s.broker.ACLEngine(); acl != nil {
		if !acl.Check(client.identity, auth.ResourceTopic, topic, auth.OpRead) {
			s.sendError(client, "fetch denied by ACL")
			return
		}
	}

	var partitionID uint32
	var fromOffset uint64
	var maxMessages uint32 = 100
	if r.len() >= 4 {
		partitionID = binary.BigEndian.Uint32(r.read(4))
	}
	if r.len() >= 8 {
		fromOffset = binary.BigEndian.Uint64(r.read(8))
	}
	if r.len() >= 4 {
		maxMessages = binary.BigEndian.Uint32(r.read(4))
	}

	// V-08: Clamp maxMessages to server limit
	maxFetch := uint32(s.broker.Config().Limits.MaxFetchMessages)
	if maxFetch <= 0 {
		maxFetch = 10000
	}
	if maxMessages == 0 {
		maxMessages = 100
	}
	if maxMessages > maxFetch {
		maxMessages = maxFetch
	}

	msgs, _, err := s.broker.StreamEngine().Fetch(topic, partitionID, fromOffset, int(maxMessages), 5*time.Second)
	if err != nil {
		s.sendError(client, "fetch failed")
		return
	}

	// Encode response: [count:uint32] [for each: length:uint32 + data]
	var resp []byte
	resp = binary.BigEndian.AppendUint32(resp, uint32(len(msgs)))
	for _, env := range msgs {
		data, err := message.Marshal(env)
		if err != nil {
			continue // skip malformed messages
		}
		resp = binary.BigEndian.AppendUint32(resp, uint32(len(data)))
		resp = append(resp, data...)
	}

	respFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpFetchResp, Payload: resp})
	_ = client.writeFrame(respFrame)
}

func (s *Server) handleAck(client *ClientConn, frame *Frame) {
	payload := decodeAck(frame.Payload)
	for _, offset := range payload.Offsets {
		s.broker.QueueEngine().HandleAck(payload.Topic, offset)
	}
}

func (s *Server) handleNack(client *ClientConn, frame *Frame) {
	payload := decodeAck(frame.Payload)
	for _, offset := range payload.Offsets {
		shouldDLQ, _ := s.broker.QueueEngine().HandleNack(payload.Topic, offset)
		if shouldDLQ {
			part, err := s.broker.Storage().GetOrCreatePartition(payload.Topic, payload.PartitionID)
			if err == nil {
				data, err := part.Read(offset)
				if err == nil {
					env, err := message.Unmarshal(data)
					if err == nil { // V-19: check unmarshal error
						topicCfg, ok := s.broker.Topics().GetTopic(payload.Topic)
						if ok && topicCfg.DLQTopic != "" {
							dlqMgr := queue.NewDLQManager(topicCfg.DLQTopic)
							dlqEnv, _ := dlqMgr.Route(env, "max-retries-exceeded", env.DeliverCount)
							if dlqEnv != nil {
								_, _ = s.broker.Publish(dlqEnv)
							}
						}
					}
				}
			}
		}
	}
}

func (s *Server) handleCommitOffset(client *ClientConn, frame *Frame) {
	r := newReader(frame.Payload)
	group, _ := r.readString()
	topic, _ := r.readString()
	var partitionID uint32
	var offset uint64
	if r.len() >= 4 {
		partitionID = binary.BigEndian.Uint32(r.read(4))
	}
	if r.len() >= 8 {
		offset = binary.BigEndian.Uint64(r.read(8))
	}

	topicCfg, ok := s.broker.Topics().GetTopic(topic)
	if ok {
		s.broker.StreamEngine().JoinGroup(group, topic, client.clientID, topicCfg.Partitions, 0)
	}
	_ = s.broker.StreamEngine().CommitOffset(group, partitionID, offset)

	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCommitAck})
	_ = client.writeFrame(ackFrame)
}

func (s *Server) handleCreateTopic(client *ClientConn, frame *Frame) {
	payload := decodeCreateTopic(frame.Payload)

	// ACL check: create permission on topic
	if acl := s.broker.ACLEngine(); acl != nil {
		if !acl.Check(client.identity, auth.ResourceTopic, payload.Name, auth.OpCreate) {
			s.sendError(client, "create topic denied by ACL")
			return
		}
	}

	mode := broker.ModeUnified
	switch payload.Mode {
	case "stream":
		mode = broker.ModeStream
	case "queue":
		mode = broker.ModeQueue
	}

	cfg := broker.TopicConfig{
		Name:       payload.Name,
		Mode:       mode,
		Partitions: payload.Partitions,
	}

	if err := s.broker.Topics().CreateTopic(cfg); err != nil {
		s.sendError(client, err.Error())
		return
	}

	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubAck})
	_ = client.writeFrame(ackFrame)
}

func (s *Server) handleDeleteTopic(client *ClientConn, frame *Frame) {
	r := newReader(frame.Payload)
	name, _ := r.readString()

	// ACL check: delete permission on topic
	if acl := s.broker.ACLEngine(); acl != nil {
		if !acl.Check(client.identity, auth.ResourceTopic, name, auth.OpDelete) {
			s.sendError(client, "delete topic denied by ACL")
			return
		}
	}

	if err := s.broker.Topics().DeleteTopic(name); err != nil {
		s.sendError(client, err.Error())
		return
	}

	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubAck})
	_ = client.writeFrame(ackFrame)
}

func (s *Server) sendError(client *ClientConn, msg string) {
	errPayload := encodeError(0, msg)
	errFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpError, Payload: errPayload})
	_ = client.writeFrame(errFrame)
}
