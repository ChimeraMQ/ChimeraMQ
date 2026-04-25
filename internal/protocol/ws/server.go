package ws

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/engine/stream"
	"github.com/chimeramq/chimera/internal/message"

	"github.com/coder/websocket"
)

// extractRealIP resolves the real client IP, trusting X-Forwarded-For only when
// the direct peer is within the trusted CIDR range.
func extractRealIP(r *http.Request, trustedCIDR string) string {
	if trustedCIDR != "" {
		proxy := net.ParseIP(strings.TrimSpace(strings.Split(r.RemoteAddr, ":")[0]))
		_, ipNet, err := net.ParseCIDR(trustedCIDR)
		if err == nil && ipNet.Contains(proxy) {
			if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
				ips := strings.Split(fwd, ",")
				if len(ips) == 1 {
					trimmed := strings.TrimSpace(ips[0])
					if clientIP := net.ParseIP(trimmed); clientIP != nil {
						return trimmed
					}
				}
			}
		}
	}
	addr := r.RemoteAddr
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

const (
	wsDefaultRateLimit = 100 // messages per second per connection
	wsRateBurst        = 50  // burst capacity above steady rate
)

// Server implements the WebSocket protocol handler.
type Server struct {
	broker   *broker.Broker
	sessions sync.Map // conn → *wsSession
}

// Detector detects WebSocket via HTTP upgrade (delegated from HTTP handler).
type Detector struct{}

// Detect always returns false — WebSocket is detected inside the HTTP handler
// by checking the Upgrade header, not by first bytes.
func (d *Detector) Detect(_ []byte) bool { return false }

// BytesNeeded returns 0 — WebSocket detection happens inside HTTP handler.
func (d *Detector) BytesNeeded() int { return 0 }

// NewServer creates a new WebSocket protocol server.
func NewServer(b *broker.Broker) *Server {
	return &Server{
		broker: b,
	}
}

// HandleConnection implements ProtocolHandler.
// Not used for WebSocket — connections come through HTTP upgrade.
func (s *Server) HandleConnection(_ net.Conn, _ []byte) error {
	return nil
}

// Stop implements ProtocolHandler.
func (s *Server) Stop() {
	s.sessions.Range(func(key, value any) bool {
		sess := value.(*wsSession)
		sess.close()
		return true
	})
}

// ServeHTTP handles the WebSocket upgrade on the configured path.
// This is mounted on the HTTP mux for the WebSocket path.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Auth check when enabled
	var authIdentity *auth.Identity
	cfg := s.broker.Config()
	if cfg.Auth.Enabled {
		provider := s.broker.AuthProvider()
		if provider != nil {
			// Rate limit check
			clientIP := extractRealIP(r, cfg.Listener.TrustedProxyCIDR)
			if lim := s.broker.AuthLimiter(); lim != nil {
				if !lim.IsAllowed(clientIP) {
					http.Error(w, "authentication rate limited", http.StatusTooManyRequests)
					return
				}
			}

			authHeader := r.Header.Get("Authorization")
			var creds auth.Credentials
			if strings.HasPrefix(authHeader, "Bearer ") {
				creds.Token = strings.TrimPrefix(authHeader, "Bearer ")
			} else if strings.HasPrefix(authHeader, "Basic ") {
				// Decode Basic auth: base64(username:password)
				decoded, err := decodeBasicAuth(authHeader)
				if err != nil {
					http.Error(w, "invalid basic auth", http.StatusUnauthorized)
					return
				}
				creds.Username = decoded.username
				creds.Password = decoded.password
			}
			identity, err := provider.Authenticate(r.Context(), creds)
			if err != nil {
				if lim := s.broker.AuthLimiter(); lim != nil {
					lim.RecordFailed(clientIP)
				}
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			if lim := s.broker.AuthLimiter(); lim != nil {
				lim.RecordSuccess(clientIP)
			}
			authIdentity = identity
		}
	}

	opts := &websocket.AcceptOptions{
		Subprotocols: []string{"chimera-json-v1", "chimera-binary-v1"},
		OriginPatterns: []string{"localhost", "127.0.0.1", "[::1]"},
	}

	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		return
	}

	// Limit message size to prevent oversized frame attacks (16 MB default)
	conn.SetReadLimit(16 << 20)

	subproto := conn.Subprotocol()
	sess := &wsSession{
		conn:       conn,
		broker:     s.broker,
		subproto:   subproto,
		identity:   authIdentity,
		rateTokens: wsRateBurst, // start with full burst capacity
		rateLast:   time.Now(),
	}

	s.sessions.Store(conn, sess)
	defer s.sessions.Delete(conn)
	defer s.broker.RecordDisconnection("ws")
	s.broker.RecordConnection("ws")

	sess.serve()
}

// wsSession represents a single WebSocket connection.
type wsSession struct {
	conn       *websocket.Conn
	broker     *broker.Broker
	subproto   string
	mu         sync.Mutex
	consumerID string
	subTopic   string
	cancelSub  context.CancelFunc
	identity   *auth.Identity // authenticated identity for ACL checks
	rateTokens int64          // message rate limiter tokens
	rateLast   time.Time      // last refill timestamp
}

// wsMessage is the JSON message format for chimera-json-v1.
type wsMessage struct {
	Op          string            `json:"op"`
	Topic       string            `json:"topic,omitempty"`
	Payload     string            `json:"payload,omitempty"` // base64 encoded
	RoutingKey  string            `json:"routing_key,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	PacketID    uint16            `json:"packet_id,omitempty"`
	Offset      uint64            `json:"offset,omitempty"`
	Partition   uint32            `json:"partition,omitempty"`
	Count       int               `json:"count,omitempty"`
	Mode        string            `json:"mode,omitempty"`
	Partitions  uint32            `json:"partitions,omitempty"`
	QoS         byte              `json:"qos,omitempty"`
	Error       string            `json:"error,omitempty"`
	Status      string            `json:"status,omitempty"`
	Group       string            `json:"group,omitempty"`        // consumer group for subscribe
	AutoCommit  bool              `json:"auto_commit,omitempty"`  // auto commit offset
	MaxWait     int               `json:"max_wait_ms,omitempty"`  // max wait time for fetch (ms)
	MaxMessages int               `json:"max_messages,omitempty"` // max messages to fetch
}

func (s *wsSession) serve() {
	defer s.conn.Close(websocket.StatusNormalClosure, "")

	ctx := context.Background()
	_ = ctx

	for {
		msgType, data, err := s.conn.Read(ctx)
		if err != nil {
			return
		}

		switch s.subproto {
		case "chimera-json-v1":
			s.handleJSON(ctx, data)
		case "chimera-binary-v1":
			s.handleBinary(data)
		default:
			// Default to JSON
			s.handleJSON(ctx, data)
		}
		_ = msgType
	}
}

func (s *wsSession) handleJSON(ctx interface{}, data []byte) {
	if !s.allowMessage() {
		s.sendError("rate limited")
		return
	}

	var msg wsMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.sendError("invalid json")
		return
	}

	switch msg.Op {
	case "publish":
		s.handlePublishJSON(&msg)
	case "subscribe":
		s.handleSubscribeJSON(&msg)
	case "unsubscribe":
		s.handleUnsubscribeJSON(&msg)
	case "fetch":
		s.handleFetchJSON(&msg)
	case "ack":
		s.handleAckJSON(&msg)
	case "nack":
		s.handleNackJSON(&msg)
	case "commit":
		s.handleCommitJSON(&msg)
	case "create_topic":
		s.handleCreateTopicJSON(&msg)
	case "delete_topic":
		s.handleDeleteTopicJSON(&msg)
	case "ping":
		s.sendJSON(&wsMessage{Op: "pong", Status: "ok"})
	default:
		s.sendError(fmt.Sprintf("unknown op: %s", msg.Op))
	}
}

func (s *wsSession) handleBinary(data []byte) {
	if !s.allowMessage() {
		s.sendError("rate limited")
		return
	}
	// Binary sub-protocol: try Chimera native frame format (magic "CHMR")
	if len(data) >= 12 && data[0] == 'C' && data[1] == 'H' && data[2] == 'M' && data[3] == 'R' {
		s.handleChimeraFrame(data)
		return
	}
	// Fallback: treat as JSON
	var msg wsMessage
	if err := json.Unmarshal(data, &msg); err == nil {
		s.handleJSON(nil, data)
	}
}

func (s *wsSession) handleChimeraFrame(data []byte) {
	if len(data) < 12 {
		s.sendError("frame too short")
		return
	}
	opcode := data[5]
	payloadLen := int(data[8])<<24 | int(data[9])<<16 | int(data[10])<<8 | int(data[11])
	if 12+payloadLen > len(data) {
		s.sendError("truncated frame payload")
		return
	}
	payload := data[12 : 12+payloadLen]

	switch opcode {
	case 0x03: // OpPublish - treat payload as raw data, use default topic
		msg := &wsMessage{Op: "publish", Topic: "default", Payload: string(payload)}
		s.handlePublishJSON(msg)
	default:
		s.sendError(fmt.Sprintf("unsupported opcode: 0x%02x", opcode))
	}
}

func (s *wsSession) handlePublishJSON(msg *wsMessage) {
	if msg.Topic == "" {
		s.sendError("topic is required")
		return
	}

	// ACL check: write permission on topic
	if acl := s.broker.ACLEngine(); acl != nil {
		if !acl.Check(s.identity, auth.ResourceTopic, msg.Topic, auth.OpWrite) {
			s.sendError("publish denied by ACL")
			return
		}
	}

	payload := []byte(msg.Payload)

	env := &message.Envelope{
		Topic:       msg.Topic,
		Payload:     payload,
		RoutingKey:  msg.RoutingKey,
		SourceProto: message.ProtoWS,
	}

	offset, err := s.broker.Publish(env)
	if err != nil {
		s.sendError(wsSanitizeError(err))
		return
	}

	s.sendJSON(&wsMessage{
		Op:        "puback",
		Status:    "ok",
		Offset:    offset,
		Topic:     msg.Topic,
		Partition: env.PartitionID,
	})
}

func (s *wsSession) handleCreateTopicJSON(msg *wsMessage) {
	if msg.Topic == "" {
		s.sendError("topic name is required")
		return
	}

	// ACL check: create permission on topic
	if acl := s.broker.ACLEngine(); acl != nil {
		if !acl.Check(s.identity, auth.ResourceTopic, msg.Topic, auth.OpCreate) {
			s.sendError("create topic denied by ACL")
			return
		}
	}

	partitions := msg.Partitions
	if partitions == 0 {
		partitions = 8
	}

	var mode broker.TopicMode
	switch msg.Mode {
	case "stream":
		mode = broker.ModeStream
	case "queue":
		mode = broker.ModeQueue
	default:
		mode = broker.ModeUnified
	}

	err := s.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       msg.Topic,
		Mode:       mode,
		Partitions: partitions,
	})
	if err != nil {
		s.sendError(wsSanitizeError(err))
		return
	}

	// Wire per-tenant rate limit to flow controller
	s.broker.WireTopicRateLimit(msg.Topic)

	s.sendJSON(&wsMessage{Op: "create_topic_ack", Status: "ok", Topic: msg.Topic})
}

func (s *wsSession) handleDeleteTopicJSON(msg *wsMessage) {
	if msg.Topic == "" {
		s.sendError("topic name is required")
		return
	}

	// ACL check: delete permission on topic
	if acl := s.broker.ACLEngine(); acl != nil {
		if !acl.Check(s.identity, auth.ResourceTopic, msg.Topic, auth.OpDelete) {
			s.sendError("delete topic denied by ACL")
			return
		}
	}

	err := s.broker.Topics().DeleteTopic(msg.Topic)
	if err != nil {
		s.sendError(wsSanitizeError(err))
		return
	}

	s.sendJSON(&wsMessage{Op: "delete_topic_ack", Status: "ok", Topic: msg.Topic})
}

func (s *wsSession) sendJSON(msg *wsMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	ctx := context.Background()
	_ = s.conn.Write(ctx, websocket.MessageText, data)
}

func (s *wsSession) sendError(msg string) {
	s.sendJSON(&wsMessage{Op: "error", Error: msg})
}

// allowMessage checks the token bucket rate limiter before processing a message.
// Returns false if the connection has exceeded its rate limit.
func (s *wsSession) allowMessage() bool {
	now := time.Now()
	elapsed := now.Sub(s.rateLast).Seconds()
	s.rateLast = now

	// Refill tokens based on elapsed time
	s.rateTokens += int64(elapsed * float64(wsDefaultRateLimit))
	if s.rateTokens > wsRateBurst {
		s.rateTokens = wsRateBurst
	}

	if s.rateTokens <= 0 {
		return false
	}
	s.rateTokens--
	return true
}

// wsSanitizeError returns a safe error message for WebSocket clients.
// Internal errors are replaced with a generic message.
func wsSanitizeError(err error) string {
	msg := err.Error()
	// Strip file paths and internal details
	if strings.Contains(msg, "data") || strings.Contains(msg, "/") || strings.Contains(msg, "goroutine") {
		return "internal error"
	}
	return msg
}

func (s *wsSession) close() {
	s.stopSubscription()
	s.conn.Close(websocket.StatusNormalClosure, "server shutting down")
}

// EvictConsumer closes the WebSocket connection for a given consumer ID.
// Called by the flow controller's eviction callback when a consumer is slow.
func (s *Server) EvictConsumer(consumerID string) {
	s.sessions.Range(func(key, value any) bool {
		sess := value.(*wsSession)
		sess.mu.Lock()
		if sess.consumerID == consumerID {
			sess.mu.Unlock()
			sess.close()
			return false // stop ranging, we found it
		}
		sess.mu.Unlock()
		return true // continue ranging
	})
}

// handleSubscribeJSON handles subscribe operation for both queue and stream modes.
func (s *wsSession) handleSubscribeJSON(msg *wsMessage) {
	if msg.Topic == "" {
		s.sendError("topic is required for subscribe")
		return
	}

	// Check if already subscribed
	s.mu.Lock()
	if s.subTopic != "" {
		s.mu.Unlock()
		s.sendError("already subscribed to a topic, unsubscribe first")
		return
	}
	s.mu.Unlock()

	// Get topic info
	topicInfo, ok := s.broker.Topics().GetTopic(msg.Topic)
	if !ok || topicInfo == nil {
		s.sendError("topic not found")
		return
	}

	// Generate unique consumer ID
	consumerID := fmt.Sprintf("ws-%s-%d", msg.Topic, time.Now().UnixNano())

	// ACL check: read permission on topic
	if acl := s.broker.ACLEngine(); acl != nil {
		if !acl.Check(s.identity, auth.ResourceTopic, msg.Topic, auth.OpRead) {
			s.sendError("subscribe denied by ACL")
			return
		}
	}

	s.mu.Lock()
	s.consumerID = consumerID
	s.subTopic = msg.Topic
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelSub = cancel
	s.mu.Unlock()

	// Send subscribe ack
	s.sendJSON(&wsMessage{
		Op:     "suback",
		Status: "ok",
		Topic:  msg.Topic,
	})

	// Start subscription based on mode
	switch topicInfo.Mode {
	case broker.ModeQueue, broker.ModeUnified:
		s.runQueueSubscription(ctx, msg.Topic, consumerID)
	case broker.ModeStream:
		s.runStreamSubscription(ctx, msg.Topic, msg.Group, msg.AutoCommit)
	}
}

// handleUnsubscribeJSON handles unsubscribe operation.
func (s *wsSession) handleUnsubscribeJSON(msg *wsMessage) {
	s.stopSubscription()
	s.sendJSON(&wsMessage{Op: "unsuback", Status: "ok"})
}

// stopSubscription stops the current subscription.
func (s *wsSession) stopSubscription() {
	s.mu.Lock()
	if s.cancelSub != nil {
		s.cancelSub()
	}
	topic := s.subTopic
	consumerID := s.consumerID
	s.subTopic = ""
	s.consumerID = ""
	s.cancelSub = nil
	s.mu.Unlock()

	// Remove from queue engine if queue mode
	if topic != "" && consumerID != "" {
		s.broker.QueueEngine().RemoveConsumer(topic, consumerID)
	}
}

// runQueueSubscription runs a queue mode subscription.
func (s *wsSession) runQueueSubscription(ctx context.Context, topic, consumerID string) {
	// Register consumer with queue engine
	qc := &queue.QueueConsumer{
		ID:       consumerID,
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	s.broker.QueueEngine().AddConsumer(topic, qc)

	// Message delivery loop
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.broker.Logger().Error("ws queue subscription panic", "topic", topic, "recover", r)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Wait for messages via queue engine dispatch
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

// runStreamSubscription runs a stream mode subscription with long-poll.
func (s *wsSession) runStreamSubscription(ctx context.Context, topic, group string, autoCommit bool) {
	// Join consumer group if specified
	partitionCount := uint32(1)
	if topicInfo, ok := s.broker.Topics().GetTopic(topic); ok && topicInfo != nil {
		partitionCount = topicInfo.Partitions
	}

	if group != "" {
		s.broker.StreamEngine().JoinGroup(group, topic, s.consumerID, partitionCount, stream.StrategyRoundRobin)
	}

	// Fetch loop
	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.broker.Logger().Error("ws stream subscription panic", "topic", topic, "group", group, "recover", r)
			}
		}()
		partitionID := uint32(0)
		offset := uint64(0)

		for {
			select {
			case <-ctx.Done():
				if group != "" {
					s.broker.StreamEngine().LeaveGroup(group, s.consumerID)
				}
				return
			default:
			}

			// Fetch messages
			msgs, newOffset, err := s.broker.StreamEngine().Fetch(topic, partitionID, offset, 10, 5*time.Second)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			for _, env := range msgs {
				s.sendJSON(&wsMessage{
					Op:        "message",
					Topic:     topic,
					Payload:   base64.StdEncoding.EncodeToString(env.Payload),
					Offset:    env.Sequence,
					Partition: env.PartitionID,
					Headers:   bytesHeadersToString(env.Headers),
				})
				s.broker.Metrics().MessageOut(env.Topic, env.PartitionID, group)
				offset = env.Sequence + 1
			}

			// Auto-commit if enabled
			if autoCommit && group != "" && newOffset > offset {
				_ = s.broker.StreamEngine().CommitOffset(group, partitionID, newOffset)
			}

			if len(msgs) == 0 {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

// handleFetchJSON handles on-demand fetch operation.
func (s *wsSession) handleFetchJSON(msg *wsMessage) {
	if msg.Topic == "" {
		s.sendError("topic is required for fetch")
		return
	}

	// ACL check: read permission on topic
	if acl := s.broker.ACLEngine(); acl != nil {
		if !acl.Check(s.identity, auth.ResourceTopic, msg.Topic, auth.OpRead) {
			s.sendError("fetch denied by ACL")
			return
		}
	}

	partitionID := msg.Partition
	offset := msg.Offset
	maxMessages := msg.MaxMessages
	if maxMessages <= 0 {
		maxMessages = 10
	}
	maxWait := time.Duration(msg.MaxWait) * time.Millisecond
	if maxWait <= 0 {
		maxWait = 5 * time.Second
	}

	// Fetch from stream engine
	msgs, newOffset, err := s.broker.StreamEngine().Fetch(msg.Topic, partitionID, offset, maxMessages, maxWait)
	if err != nil {
		s.sendError(wsSanitizeError(err))
		return
	}

	// Send messages
	for _, env := range msgs {
		s.sendJSON(&wsMessage{
			Op:        "message",
			Topic:     msg.Topic,
			Payload:   base64.StdEncoding.EncodeToString(env.Payload),
			Offset:    env.Sequence,
			Partition: env.PartitionID,
			Headers:   bytesHeadersToString(env.Headers),
		})
		s.broker.Metrics().MessageOut(env.Topic, env.PartitionID, msg.Group)
	}

	// Send fetch complete
	s.sendJSON(&wsMessage{
		Op:     "fetch_complete",
		Status: "ok",
		Topic:  msg.Topic,
		Offset: newOffset,
		Count:  len(msgs),
	})
}

// handleAckJSON handles ack operation for queue mode.
func (s *wsSession) handleAckJSON(msg *wsMessage) {
	s.broker.QueueEngine().HandleAck(msg.Topic, msg.Offset)
	s.sendJSON(&wsMessage{Op: "ackack", Status: "ok", Offset: msg.Offset})
}

// handleNackJSON handles nack operation for queue mode.
func (s *wsSession) handleNackJSON(msg *wsMessage) {
	s.broker.QueueEngine().HandleNack(msg.Topic, msg.Offset)
	s.sendJSON(&wsMessage{Op: "nackack", Status: "ok", Offset: msg.Offset})
}

// handleCommitJSON handles offset commit for stream mode.
func (s *wsSession) handleCommitJSON(msg *wsMessage) {
	if msg.Group == "" {
		s.sendError("group is required for commit")
		return
	}
	err := s.broker.StreamEngine().CommitOffset(msg.Group, msg.Partition, msg.Offset)
	if err != nil {
		s.sendError(wsSanitizeError(err))
		return
	}
	s.sendJSON(&wsMessage{Op: "commitack", Status: "ok", Offset: msg.Offset})
}

// bytesHeadersToString converts map[string][]byte headers to map[string]string.
func bytesHeadersToString(headers map[string][]byte) map[string]string {
	result := make(map[string]string, len(headers))
	for k, v := range headers {
		result[k] = string(v)
	}
	return result
}

type basicAuthCreds struct {
	username string
	password string
}

func decodeBasicAuth(header string) (basicAuthCreds, error) {
	encoded := strings.TrimPrefix(header, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return basicAuthCreds{}, err
	}
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return basicAuthCreds{}, fmt.Errorf("invalid basic auth format")
	}
	return basicAuthCreds{username: parts[0], password: parts[1]}, nil
}
