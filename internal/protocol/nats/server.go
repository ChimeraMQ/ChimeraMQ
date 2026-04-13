package nats

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
)

// Server implements a NATS protocol server.
type Server struct {
	b        *broker.Broker
	sessions sync.Map // map[string]*Session
}

// Session represents a NATS client session.
type Session struct {
	id            string
	conn          net.Conn
	reader        *bufio.Reader
	writer        *bufio.Writer
	b             *broker.Broker
	subscriptions map[string]*Subscription // subject -> subscription
	mu            sync.RWMutex
	closed        bool
	connected     bool
	info          ClientInfo
}

// Subscription represents a NATS subscription.
type Subscription struct {
	Subject string
	SID     string
	Queue   string
	Group   string
}

// ClientInfo holds client connection information.
type ClientInfo struct {
	Name      string `json:"name,omitempty"`
	Lang      string `json:"lang,omitempty"`
	Version   string `json:"version,omitempty"`
	Protocol  int    `json:"protocol,omitempty"`
	TLS       bool   `json:"tls_required,omitempty"`
	AuthToken string `json:"auth_token,omitempty"`
	User      string `json:"user,omitempty"`
	Pass      string `json:"pass,omitempty"`
}

// NewServer creates a new NATS server.
func NewServer(b *broker.Broker) *Server {
	return &Server{b: b}
}

// HandleConnection handles a NATS client connection.
func (s *Server) HandleConnection(conn net.Conn, _ []byte) error {
	session := &Session{
		id:            generateSessionID(),
		conn:          conn,
		reader:        bufio.NewReader(conn),
		writer:        bufio.NewWriter(conn),
		b:             s.b,
		subscriptions: make(map[string]*Subscription),
		connected:     false,
	}

	s.sessions.Store(session.id, session)
	defer s.sessions.Delete(session.id)

	// Send INFO message first
	if err := session.sendInfo(); err != nil {
		return err
	}

	return session.handle()
}

// Stop stops the NATS server and all sessions.
func (s *Server) Stop() {
	s.sessions.Range(func(key, value interface{}) bool {
		if session, ok := value.(*Session); ok {
			session.close()
		}
		return true
	})
}

func (s *Session) handle() error {
	defer s.close()

	// Read and process messages
	for {
		_ = s.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		msg, err := ReadMessage(s.reader)
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}

		if err := s.processMessage(msg); err != nil {
			_ = s.sendError(err.Error())
			return err
		}
	}
}

func (s *Session) processMessage(msg *Message) error {
	switch msg.Command {
	case CmdConnect:
		return s.handleConnect(msg)
	case CmdPub:
		return s.handlePub(msg)
	case CmdSub:
		return s.handleSub(msg)
	case CmdUnsub:
		return s.handleUnsub(msg)
	case CmdPing:
		return s.handlePing()
	case CmdPong:
		return s.handlePong()
	case "HPUB":
		// NATS 2.0+ headers support (simplified)
		return s.handlePub(msg)
	default:
		return s.sendError(fmt.Sprintf("Unknown command: %s", msg.Command))
	}
}

func (s *Session) handleConnect(msg *Message) error {
	if len(msg.Args) > 0 {
		// JSON encoded connection info
		jsonData := msg.Args[0]
		if err := json.Unmarshal([]byte(jsonData), &s.info); err != nil {
			return s.sendError("Invalid CONNECT JSON")
		}
	}

	// Authentication
	if s.b.Config().Auth.Enabled {
		if !s.authenticate(s.info.User, s.info.Pass, s.info.AuthToken) {
			return s.sendError("Authorization Violation")
		}
	}

	s.connected = true

	// Send +OK for verbose mode if enabled
	if s.b.Config().Logging.Level == "debug" {
		return s.sendOk()
	}
	return nil
}

func (s *Session) authenticate(username, password, token string) bool {
	provider := s.b.AuthProvider()
	if provider == nil {
		return false
	}
	_, err := provider.Authenticate(context.Background(), auth.Credentials{
		Username: username,
		Password: password,
		Token:    token,
	})
	return err == nil
}

func (s *Session) handlePub(msg *Message) error {
	if !s.connected {
		return s.sendError("Not connected")
	}

	if len(msg.Args) < 1 {
		return s.sendError("PUB requires subject")
	}

	subject := msg.Args[0]

	// Remove NATS-specific prefixes
	subject = strings.TrimPrefix(subject, "foo.") // NATS demo subjects
	topic := strings.ReplaceAll(subject, ".", "/")

	// Create message envelope
	env := &message.Envelope{
		Topic:       topic,
		Payload:     msg.Payload,
		ContentType: "application/octet-stream",
		SourceProto: message.ProtoNATS,
	}

	// Handle reply-to if present
	if len(msg.Args) >= 2 {
		// Check if second arg is not payload length
		if _, err := strconv.Atoi(msg.Args[1]); err != nil {
			env.RoutingKey = msg.Args[1]
		}
	}

	// Publish message
	_, err := s.b.Publish(env)
	if err != nil {
		return s.sendError(fmt.Sprintf("Publish failed: %v", err))
	}

	return s.sendOk()
}

func (s *Session) handleSub(msg *Message) error {
	if !s.connected {
		return s.sendError("Not connected")
	}

	if len(msg.Args) < 2 {
		return s.sendError("SUB requires subject and sid")
	}

	subject := msg.Args[0]
	sid := msg.Args[1]

	// Optional queue group
	queue := ""
	if len(msg.Args) >= 3 {
		queue = msg.Args[2]
	}

	// Convert NATS subject to topic
	topic := strings.ReplaceAll(subject, ".", "/")

	sub := &Subscription{
		Subject: subject,
		SID:     sid,
		Queue:   queue,
		Group:   s.id + "-" + sid,
	}

	s.mu.Lock()
	s.subscriptions[sid] = sub
	s.mu.Unlock()

	// Start consuming messages
	go s.runSubscription(sub, topic)

	return nil
}

func (s *Session) handleUnsub(msg *Message) error {
	if !s.connected {
		return s.sendError("Not connected")
	}

	if len(msg.Args) < 1 {
		return s.sendError("UNSUB requires sid")
	}

	sid := msg.Args[0]

	s.mu.Lock()
	delete(s.subscriptions, sid)
	s.mu.Unlock()

	return nil
}

func (s *Session) handlePing() error {
	return s.writeString("PONG\r\n")
}

func (s *Session) handlePong() error {
	// Client responded to our PING, keep connection alive
	return nil
}

func (s *Session) runSubscription(sub *Subscription, topic string) {
	// Create consumer group
	s.b.StreamEngine().JoinGroup(sub.Group, topic, sub.SID, 1, 0)

	for {
		s.mu.RLock()
		if s.closed {
			s.mu.RUnlock()
			return
		}
		s.mu.RUnlock()

		// Fetch messages
		msgs, _, err := s.b.StreamEngine().Fetch(topic, 0, 0, 1, 5*time.Second)
		if err != nil || len(msgs) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, env := range msgs {
			if err := s.sendMessage(sub, env); err != nil {
				return
			}
		}
	}
}

func (s *Session) sendMessage(sub *Subscription, env *message.Envelope) error {
	// Convert topic to NATS subject
	subject := strings.ReplaceAll(env.Topic, "/", ".")

	// MSG <subject> <sid> <#bytes>\r\n[payload]\r\n
	msg := fmt.Sprintf("MSG %s %s %d\r\n", subject, sub.SID, len(env.Payload))

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("session closed")
	}

	if _, err := s.writer.WriteString(msg); err != nil {
		return err
	}
	if _, err := s.writer.Write(env.Payload); err != nil {
		return err
	}
	if _, err := s.writer.WriteString("\r\n"); err != nil {
		return err
	}

	return s.writer.Flush()
}

func (s *Session) sendInfo() error {
	// INFO {"server_id":"...","version":"2.0","go":"...","host":"...","port":...,...}
	info := map[string]interface{}{
		"server_id":   s.b.Config().Node.Name,
		"version":     "2.0.0",
		"go":          "go1.21",
		"host":        s.b.Config().Listener.Bind,
		"port":        s.b.Config().Listener.Port,
		"max_payload": 1048576,
		"headers":     true,
	}

	infoJSON, _ := json.Marshal(info)
	return s.writeString(fmt.Sprintf("INFO %s\r\n", string(infoJSON)))
}

func (s *Session) sendOk() error {
	return s.writeString("+OK\r\n")
}

func (s *Session) sendError(msg string) error {
	return s.writeString(fmt.Sprintf("-ERR '%s'\r\n", msg))
}

func (s *Session) writeString(str string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("session closed")
	}

	if _, err := s.writer.WriteString(str); err != nil {
		return err
	}
	return s.writer.Flush()
}

func (s *Session) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	s.conn.Close()
}

func generateSessionID() string {
	return fmt.Sprintf("nats-%d", time.Now().UnixNano())
}

// Detector detects NATS protocol by its INFO/CONNECT messages.
type Detector struct{}

// Detect checks if the peeked bytes match NATS protocol.
func (d *Detector) Detect(peek []byte) bool {
	if len(peek) < 4 {
		return false
	}
	// NATS clients typically start with CONNECT, PUB, SUB, UNSUB, or respond to INFO
	prefix := string(peek[:4])
	switch prefix {
	case "CONN", "PUB ", "SUB ", "UNSU", "PING", "PONG", "HPUB":
		return true
	}
	return false
}

// BytesNeeded returns the minimum bytes needed for detection.
func (d *Detector) BytesNeeded() int { return 4 }
