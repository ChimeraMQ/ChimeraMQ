package stomp

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
)

// Server implements a STOMP protocol server.
type Server struct {
	b        *broker.Broker
	sessions sync.Map // map[string]*Session
}

// Session represents a STOMP client session.
type Session struct {
	id            string
	conn          net.Conn
	reader        *bufio.Reader
	writer        *bufio.Writer
	b             *broker.Broker
	subscriptions map[string]*Subscription // destination -> subscription
	mu            sync.RWMutex
	closed        bool
	version       string
}

// Subscription represents a STOMP subscription.
type Subscription struct {
	ID          string
	Destination string
	Ack         string
	ConsumerID  string
}

// NewServer creates a new STOMP server.
func NewServer(b *broker.Broker) *Server {
	return &Server{b: b}
}

// HandleConnection handles a STOMP client connection.
func (s *Server) HandleConnection(conn net.Conn, _ []byte) error {
	session := &Session{
		id:            generateSessionID(),
		conn:          conn,
		reader:        bufio.NewReader(conn),
		writer:        bufio.NewWriter(conn),
		b:             s.b,
		subscriptions: make(map[string]*Subscription),
		version:       "1.2",
	}

	s.sessions.Store(session.id, session)
	defer s.sessions.Delete(session.id)

	return session.handle()
}

// Stop stops the STOMP server and all sessions.
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

	// Read and process frames
	for {
		_ = s.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		frame, err := ReadFrame(s.reader)
		if err != nil {
			return err
		}

		if err := s.processFrame(frame); err != nil {
			_ = s.sendError("Processing error", err.Error())
			return err
		}
	}
}

func (s *Session) processFrame(frame *Frame) error {
	switch frame.Command {
	case CmdConnect, CmdStomp:
		return s.handleConnect(frame)
	case CmdSend:
		return s.handleSend(frame)
	case CmdSubscribe:
		return s.handleSubscribe(frame)
	case CmdUnsubscribe:
		return s.handleUnsubscribe(frame)
	case CmdAck:
		return s.handleAck(frame)
	case CmdNack:
		return s.handleNack(frame)
	case CmdBegin:
		return s.handleBegin(frame)
	case CmdCommit:
		return s.handleCommit(frame)
	case CmdAbort:
		return s.handleAbort(frame)
	case CmdDisconnect:
		return s.handleDisconnect(frame)
	default:
		return s.sendError("Invalid command", fmt.Sprintf("Unknown command: %s", frame.Command))
	}
}

func (s *Session) handleConnect(frame *Frame) error {
	// Parse version header
	acceptVersion := frame.Get("accept-version")
	if acceptVersion == "" {
		acceptVersion = "1.0,1.1,1.2"
	}

	// Negotiate version
	versions := strings.Split(acceptVersion, ",")
	s.version = "1.2"
	supported := false
	for _, v := range versions {
		if strings.TrimSpace(v) == "1.2" {
			supported = true
			break
		}
	}
	if !supported {
		s.version = "1.1"
		for _, v := range versions {
			if strings.TrimSpace(v) == "1.1" {
				supported = true
				break
			}
		}
	}
	if !supported {
		s.version = "1.0"
	}

	// Send CONNECTED frame
	response := NewFrame(CmdConnected)
	response.Set("version", s.version)
	response.Set("session", s.id)
	response.Set("server", "ChimeraMQ/0.9.0")
	response.Set("heart-beat", "0,0")

	return s.writeFrame(response)
}

func (s *Session) handleSend(frame *Frame) error {
	destination := frame.Get("destination")
	if destination == "" {
		return s.sendError("Missing destination", "SEND frame must include destination header")
	}

	// Remove leading slash if present
	destination = strings.TrimPrefix(destination, "/")
	destination = strings.TrimPrefix(destination, "topic/")
	destination = strings.TrimPrefix(destination, "queue/")

	// Create message envelope
	env := &message.Envelope{
		Topic:       destination,
		Payload:     frame.Body,
		ContentType: frame.Get("content-type"),
		SourceProto: message.ProtoSTOMP,
	}

	// Handle transaction if present
	if tx := frame.Get("transaction"); tx != "" {
		// Transaction handling would go here
		_ = tx
	}

	// Publish message
	_, err := s.b.Publish(env)
	if err != nil {
		return s.sendError("Publish failed", err.Error())
	}

	// Send receipt if requested
	if receipt := frame.Get("receipt"); receipt != "" {
		response := NewFrame(CmdReceipt)
		response.Set("receipt-id", receipt)
		return s.writeFrame(response)
	}

	return nil
}

func (s *Session) handleSubscribe(frame *Frame) error {
	destination := frame.Get("destination")
	if destination == "" {
		return s.sendError("Missing destination", "SUBSCRIBE frame must include destination header")
	}

	subID := frame.Get("id")
	if subID == "" {
		// Generate subscription ID
		subID = generateSubscriptionID()
	}

	ack := frame.Get("ack")
	if ack == "" {
		ack = "auto"
	}

	// Remove leading slash
	destination = strings.TrimPrefix(destination, "/")
	destination = strings.TrimPrefix(destination, "topic/")
	destination = strings.TrimPrefix(destination, "queue/")

	sub := &Subscription{
		ID:          subID,
		Destination: destination,
		Ack:         ack,
		ConsumerID:  s.id + "-" + subID,
	}

	s.mu.Lock()
	s.subscriptions[subID] = sub
	s.mu.Unlock()

	// Start consuming messages
	go s.runSubscription(sub)

	// Send receipt if requested
	if receipt := frame.Get("receipt"); receipt != "" {
		response := NewFrame(CmdReceipt)
		response.Set("receipt-id", receipt)
		return s.writeFrame(response)
	}

	return nil
}

func (s *Session) handleUnsubscribe(frame *Frame) error {
	subID := frame.Get("id")
	if subID == "" {
		return s.sendError("Missing id", "UNSUBSCRIBE frame must include id header")
	}

	s.mu.Lock()
	delete(s.subscriptions, subID)
	s.mu.Unlock()

	// Send receipt if requested
	if receipt := frame.Get("receipt"); receipt != "" {
		response := NewFrame(CmdReceipt)
		response.Set("receipt-id", receipt)
		return s.writeFrame(response)
	}

	return nil
}

func (s *Session) handleAck(frame *Frame) error {
	// ACK handling for client-individual or client mode
	// In auto mode, messages are auto-acknowledged

	if receipt := frame.Get("receipt"); receipt != "" {
		response := NewFrame(CmdReceipt)
		response.Set("receipt-id", receipt)
		return s.writeFrame(response)
	}
	return nil
}

func (s *Session) handleNack(frame *Frame) error {
	// NACK handling - could route to DLQ

	if receipt := frame.Get("receipt"); receipt != "" {
		response := NewFrame(CmdReceipt)
		response.Set("receipt-id", receipt)
		return s.writeFrame(response)
	}
	return nil
}

func (s *Session) handleBegin(frame *Frame) error {
	// Transaction begin
	if receipt := frame.Get("receipt"); receipt != "" {
		response := NewFrame(CmdReceipt)
		response.Set("receipt-id", receipt)
		return s.writeFrame(response)
	}
	return nil
}

func (s *Session) handleCommit(frame *Frame) error {
	// Transaction commit
	if receipt := frame.Get("receipt"); receipt != "" {
		response := NewFrame(CmdReceipt)
		response.Set("receipt-id", receipt)
		return s.writeFrame(response)
	}
	return nil
}

func (s *Session) handleAbort(frame *Frame) error {
	// Transaction abort
	if receipt := frame.Get("receipt"); receipt != "" {
		response := NewFrame(CmdReceipt)
		response.Set("receipt-id", receipt)
		return s.writeFrame(response)
	}
	return nil
}

func (s *Session) handleDisconnect(frame *Frame) error {
	// Send receipt if requested
	if receipt := frame.Get("receipt"); receipt != "" {
		response := NewFrame(CmdReceipt)
		response.Set("receipt-id", receipt)
		_ = s.writeFrame(response)
	}
	return nil
}

func (s *Session) runSubscription(sub *Subscription) {
	// Create a consumer group for this subscription
	groupName := sub.ConsumerID
	s.b.StreamEngine().JoinGroup(groupName, sub.Destination, sub.ConsumerID, 1, 0)

	for {
		s.mu.RLock()
		if s.closed {
			s.mu.RUnlock()
			return
		}
		s.mu.RUnlock()

		// Fetch messages
		msgs, _, err := s.b.StreamEngine().Fetch(sub.Destination, 0, 0, 1, 5*time.Second)
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
	frame := NewFrame(CmdMessage)
	frame.Set("destination", "/topic/"+env.Topic)
	frame.Set("subscription", sub.ID)
	frame.Set("message-id", strconv.FormatUint(env.Sequence, 10))
	frame.Set("content-type", env.ContentType)

	if sub.Ack != "auto" {
		frame.Set("ack", strconv.FormatUint(env.Sequence, 10))
	}

	frame.Body = env.Payload

	return s.writeFrame(frame)
}

func (s *Session) sendError(message, detail string) error {
	frame := NewFrame(CmdError)
	frame.Set("message", message)
	frame.Body = []byte(detail)
	return s.writeFrame(frame)
}

func (s *Session) writeFrame(frame *Frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return fmt.Errorf("session closed")
	}

	data := frame.Encode()
	if _, err := s.writer.Write(data); err != nil {
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
	return fmt.Sprintf("stomp-%d", time.Now().UnixNano())
}

func generateSubscriptionID() string {
	return fmt.Sprintf("sub-%d", time.Now().UnixNano())
}

// Detector detects STOMP protocol by its CONNECT/STOMP frame.
type Detector struct{}

// Detect checks if the peeked bytes start with a STOMP command.
func (d *Detector) Detect(peek []byte) bool {
	if len(peek) < 4 {
		return false
	}
	// STOMP frames start with commands like "CONNECT", "STOMP", "SEND"
	prefix := string(peek[:4])
	switch prefix {
	case "CONN", "STOM", "SEND", "SUBS", "UNSU", "BEGI", "COMM", "ABOR", "ACK ", "NACK", "DISC":
		return true
	}
	return false
}

// BytesNeeded returns the minimum bytes needed for detection.
func (d *Detector) BytesNeeded() int { return 4 }
