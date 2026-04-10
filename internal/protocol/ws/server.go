package ws

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"

	"context"

	"nhooyr.io/websocket"
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
	cfg := s.broker.Config()
	if cfg.Auth.Enabled {
		provider := s.broker.AuthProvider()
		if provider != nil {
			authHeader := r.Header.Get("Authorization")
			var creds auth.Credentials
			if strings.HasPrefix(authHeader, "Bearer ") {
				creds.Token = strings.TrimPrefix(authHeader, "Bearer ")
			} else if strings.HasPrefix(authHeader, "Basic ") {
				// Basic auth handled inline for WebSocket
				creds.Username = authHeader
				creds.Password = authHeader
			}
			if _, err := provider.Authenticate(r.Context(), creds); err != nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
	}

	opts := &websocket.AcceptOptions{
		Subprotocols: []string{"chimera-json-v1", "chimera-binary-v1"},
	}

	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		return
	}

	subproto := conn.Subprotocol()
	sess := &wsSession{
		conn:     conn,
		broker:   s.broker,
		subproto: subproto,
	}

	s.sessions.Store(conn, sess)
	defer s.sessions.Delete(conn)

	sess.serve()
}

// wsSession represents a single WebSocket connection.
type wsSession struct {
	conn     *websocket.Conn
	broker   *broker.Broker
	subproto string
	mu       sync.Mutex
}

// wsMessage is the JSON message format for chimera-json-v1.
type wsMessage struct {
	Op         string            `json:"op"`
	Topic      string            `json:"topic,omitempty"`
	Payload    string            `json:"payload,omitempty"` // base64 encoded
	RoutingKey string            `json:"routing_key,omitempty"`
	Headers    map[string]string `json:"headers,omitempty"`
	PacketID   uint16            `json:"packet_id,omitempty"`
	Offset     uint64            `json:"offset,omitempty"`
	Partition  uint32            `json:"partition,omitempty"`
	Count      int               `json:"count,omitempty"`
	Mode       string            `json:"mode,omitempty"`
	Partitions uint32            `json:"partitions,omitempty"`
	QoS        byte              `json:"qos,omitempty"`
	Error      string            `json:"error,omitempty"`
	Status     string            `json:"status,omitempty"`
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
	var msg wsMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		s.sendError("invalid json")
		return
	}

	switch msg.Op {
	case "publish":
		s.handlePublishJSON(&msg)
	case "subscribe":
		s.sendError("subscribe not yet supported via WebSocket")
	case "fetch":
		s.sendError("fetch not yet supported via WebSocket")
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

	payload := []byte(msg.Payload)

	env := &message.Envelope{
		Topic:       msg.Topic,
		Payload:     payload,
		RoutingKey:  msg.RoutingKey,
		SourceProto: message.ProtoWS,
	}

	offset, err := s.broker.Publish(env)
	if err != nil {
		s.sendError(err.Error())
		return
	}

	s.sendJSON(&wsMessage{
		Op:       "puback",
		Status:   "ok",
		Offset:   offset,
		Topic:    msg.Topic,
		Partition: env.PartitionID,
	})
}

func (s *wsSession) handleCreateTopicJSON(msg *wsMessage) {
	if msg.Topic == "" {
		s.sendError("topic name is required")
		return
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
		s.sendError(err.Error())
		return
	}

	s.sendJSON(&wsMessage{Op: "create_topic_ack", Status: "ok", Topic: msg.Topic})
}

func (s *wsSession) handleDeleteTopicJSON(msg *wsMessage) {
	if msg.Topic == "" {
		s.sendError("topic name is required")
		return
	}

	err := s.broker.Topics().DeleteTopic(msg.Topic)
	if err != nil {
		s.sendError(err.Error())
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
	s.conn.Write(ctx, websocket.MessageText, data)
}

func (s *wsSession) sendError(msg string) {
	s.sendJSON(&wsMessage{Op: "error", Error: msg})
}

func (s *wsSession) close() {
	s.conn.Close(websocket.StatusNormalClosure, "server shutting down")
}
