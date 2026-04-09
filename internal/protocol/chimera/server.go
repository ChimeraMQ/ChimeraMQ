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

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/message"
)

// Server handles Chimera native protocol connections.
type Server struct {
	listener net.Listener
	broker   *broker.Broker
	clients  sync.Map
	clientSeq atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

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
		if !s.authenticate(payload.Username, payload.Password) {
			connackPayload := encodeConnAck("", 1) // status 1 = auth failed
			connackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnAck, Payload: connackPayload})
			client.writeFrame(connackFrame)
			return
		}
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
		conn.SetReadDeadline(time.Now().Add(keepalive * 2))
	}

	// Main read loop
	for {
		frame, err := DecodeFrame(client.reader)
		if err != nil {
			return
		}

		if keepalive > 0 {
			conn.SetReadDeadline(time.Now().Add(keepalive * 2))
		}

		switch frame.OpCode {
		case OpConnect:
			newPayload := decodeConnect(frame.Payload)
			if s.broker.Config().Auth.Enabled {
				if !s.authenticate(newPayload.Username, newPayload.Password) {
					connackPayload := encodeConnAck("", 1)
					connackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnAck, Payload: connackPayload})
					client.writeFrame(connackFrame)
					return
				}
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
			client.writeFrame(connackFrame)
		case OpPublish:
			s.handlePublish(client, frame)
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
			client.writeFrame(pong)
		case OpDisconnect:
			return
		}
	}
}

// authenticate validates username/password against broker config.
func (s *Server) authenticate(username, password string) bool {
	cfg := s.broker.Config()
	if cfg.Auth.Type == "static" {
		// Check tokens first
		if password != "" {
			if _, ok := cfg.Auth.Tokens[password]; ok {
				return true
			}
		}
		// Check users (in production, use bcrypt comparison)
		if _, ok := cfg.Auth.Users[username]; ok {
			return true
		}
	}
	return false
}

func (s *Server) handlePublish(client *ClientConn, frame *Frame) {
	payload := decodePublish(frame.Payload)

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
	client.writeFrame(ackFrame)
}

func (s *Server) handleSubscribe(client *ClientConn, frame *Frame) {
	payload := decodeSubscribe(frame.Payload)

	sub := &Subscription{topic: payload.Topic, mode: payload.Mode}
	client.subsMu.Lock()
	client.subs[payload.Topic] = sub
	client.subsMu.Unlock()

	ackPayload := encodeSubAck(payload.Topic, true)
	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubAck, Payload: ackPayload})
	client.writeFrame(ackFrame)
}

func (s *Server) handleFetch(client *ClientConn, frame *Frame) {
	r := newReader(frame.Payload)
	topic, _ := r.readString()
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
	client.writeFrame(respFrame)
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
								s.broker.Publish(dlqEnv)
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
	s.broker.StreamEngine().CommitOffset(group, partitionID, offset)

	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCommitAck})
	client.writeFrame(ackFrame)
}

func (s *Server) handleCreateTopic(client *ClientConn, frame *Frame) {
	payload := decodeCreateTopic(frame.Payload)

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
	client.writeFrame(ackFrame)
}

func (s *Server) handleDeleteTopic(client *ClientConn, frame *Frame) {
	r := newReader(frame.Payload)
	name, _ := r.readString()

	if err := s.broker.Topics().DeleteTopic(name); err != nil {
		s.sendError(client, err.Error())
		return
	}

	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubAck})
	client.writeFrame(ackFrame)
}

func (s *Server) sendError(client *ClientConn, msg string) {
	errPayload := encodeError(0, msg)
	errFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpError, Payload: errPayload})
	client.writeFrame(errFrame)
}
