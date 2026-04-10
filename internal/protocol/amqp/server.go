package amqp

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
)

// Server implements the AMQP 1.0 protocol handler.
type Server struct {
	broker    *broker.Broker
	sessions  sync.Map // conn → *amqpConn
	clientSeq atomic.Uint64
}

// Detector detects AMQP 1.0 by its protocol header "AMQP\x00\x01\x00\x00".
type Detector struct{}

// Detect checks for the AMQP 1.0 protocol header.
func (d *Detector) Detect(peek []byte) bool {
	return len(peek) >= 8 && string(peek[:8]) == protocolHeader
}

// BytesNeeded returns 8 (the length of the AMQP protocol header).
func (d *Detector) BytesNeeded() int { return 8 }

// NewServer creates a new AMQP 1.0 protocol server.
func NewServer(b *broker.Broker) *Server {
	return &Server{
		broker: b,
	}
}

// HandleConnection implements ProtocolHandler.
func (s *Server) HandleConnection(conn net.Conn, _ []byte) error {
	s.handleConnection(conn)
	return nil
}

// Stop implements ProtocolHandler.
func (s *Server) Stop() {
	s.sessions.Range(func(key, value any) bool {
		ac := value.(*amqpConn)
		ac.close()
		return true
	})
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	maxFrameSize := uint32(s.broker.Config().Protocols.AMQP.MaxFrameSize)
	if maxFrameSize == 0 {
		maxFrameSize = defaultMaxFrameSize
	}

	// Read client's protocol header
	if err := ReadProtocolHeader(reader); err != nil {
		return
	}

	// Send our protocol header
	if err := WriteProtocolHeader(writer); err != nil {
		return
	}
	writer.Flush()

	// SASL negotiation
	if s.broker.Config().Auth.Enabled {
		if !s.negotiateSASL(reader, writer, maxFrameSize) {
			return
		}
	}

	ac := &amqpConn{
		server:  s,
		conn:    conn,
		reader:  reader,
		writer:  writer,
		maxSize: maxFrameSize,
		channels: make(map[uint16]*amqpChannel),
	}
	defer ac.close()

	s.sessions.Store(conn, ac)
	defer s.sessions.Delete(conn)

	// Main frame loop
	for {
		frame, err := ReadFrame(reader, maxFrameSize)
		if err != nil {
			return
		}

		if frame.Type == frameTypeAMQP && len(frame.Body) > 0 {
			if err := ac.handleFrame(frame); err != nil {
				return
			}
			writer.Flush()
		}
	}
}

func (s *Server) negotiateSASL(reader *bufio.Reader, writer *bufio.Writer, maxSize uint32) bool {
	// Send SASL mechanisms
	saslFrame := BuildSASLMechanisms()
	WriteFrame(writer, frameTypeSASL, 0, saslFrame)
	writer.Flush()

	// Wait for SASL INIT
	frame, err := ReadFrame(reader, maxSize)
	if err != nil {
		return false
	}

	if frame.Type != frameTypeSASL {
		return false
	}

	desc, value, err := ParseDescribedType(frame.Body)
	if err != nil {
		return false
	}

	_ = desc // Should be descSASLInit

	// Parse the SASL INIT: fields are [mechanism, initial-response]
	tr := newTypeReader(value)
	any, err := tr.readAny()
	if err != nil {
		return false
	}
	items, ok := any.([]interface{})
	if !ok {
		return false
	}

	var username, password string
	if len(items) >= 2 {
		if resp, ok := items[1].([]byte); ok {
			// PLAIN: \x00username\x00password
			decoded := string(resp)
			parts := splitNull(decoded)
			if len(parts) >= 3 {
				username = parts[1]
				password = parts[2]
			}
		}
	}

	// Authenticate
	if !s.authenticate(username, password) {
		outcome := BuildSASLOutcome(1) // auth failed
		WriteFrame(writer, frameTypeSASL, 0, outcome)
		writer.Flush()
		return false
	}

	// Success
	outcome := BuildSASLOutcome(0) // OK
	WriteFrame(writer, frameTypeSASL, 0, outcome)
	writer.Flush()
	return true
}

func (s *Server) authenticate(username, password string) bool {
	provider := s.broker.AuthProvider()
	if provider == nil {
		return false
	}
	_, err := provider.Authenticate(context.Background(), auth.Credentials{
		Username: username,
		Password: password,
		Token:    password,
	})
	return err == nil
}

func splitNull(s string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// --- Connection and channel state ---

type amqpConn struct {
	server   *Server
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	maxSize  uint32
	channels map[uint16]*amqpChannel
	mu       sync.Mutex
	containerID string
}

type amqpChannel struct {
	remoteChannel uint16
	nextOutID     uint32
	links         map[uint32]*amqpLink
}

type amqpLink struct {
	name    string
	handle  uint32
	role    byte // 0=sender, 1=receiver
	addr    string
	credit  uint32 // available delivery credits
	delivered uint32 // deliveries sent
}

func (ac *amqpConn) handleFrame(frame *Frame) error {
	desc, value, err := ParseDescribedType(frame.Body)
	if err != nil {
		return err
	}

	switch desc {
	case descOpen:
		return ac.handleOpen(value, frame.Channel)
	case descBegin:
		return ac.handleBegin(value, frame.Channel)
	case descAttach:
		return ac.handleAttach(value, frame.Channel)
	case descTransfer:
		return ac.handleTransfer(value, frame.Channel)
	case descFlow:
		return ac.handleFlow(value, frame.Channel)
	case descDisposition:
		return ac.handleDisposition(value, frame.Channel)
	case descDetach:
		return ac.handleDetach(value, frame.Channel)
	case descEnd:
		return ac.handleEnd(frame.Channel)
	case descClose:
		return ac.handleClose()
	default:
		return fmt.Errorf("unknown descriptor: 0x%x", desc)
	}
}

func (ac *amqpConn) handleOpen(value []byte, channel uint16) error {
	// Parse client's OPEN to get container-id
	ac.containerID = fmt.Sprintf("chimera-amqp-%d", ac.server.clientSeq.Add(1))

	// Send OPEN back
	openBody := BuildOpen(ac.containerID, "chimera")
	return WriteFrame(ac.writer, frameTypeAMQP, channel, openBody)
}

func (ac *amqpConn) handleBegin(value []byte, channel uint16) error {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	ch := &amqpChannel{
		remoteChannel: channel,
		nextOutID:     0,
		links:         make(map[uint32]*amqpLink),
	}
	ac.channels[channel] = ch

	// Send BEGIN back
	beginBody := BuildBegin(0, 0, 65535, 65535, 4294967295)
	return WriteFrame(ac.writer, frameTypeAMQP, channel, beginBody)
}

func (ac *amqpConn) handleAttach(value []byte, channel uint16) error {
	tr := newTypeReader(value)
	any, err := tr.readAny()
	if err != nil {
		return err
	}
	items, ok := any.([]interface{})
	if !ok {
		return fmt.Errorf("expected list in ATTACH")
	}

	var name string
	var handle uint32
	var role byte
	var addr string

	if len(items) > 0 {
		if b, ok := items[0].([]byte); ok {
			name = string(b)
		}
	}
	if len(items) > 1 {
		if v, ok := items[1].(uint32); ok {
			handle = v
		}
	}
	if len(items) > 2 {
		if v, ok := items[2].(byte); ok {
			role = v
		}
	}
	if len(items) > 5 {
		if v, ok := items[5].([]byte); ok {
			addr = string(v)
		} else if v, ok := items[5].(string); ok {
			addr = v
		}
	}

	ac.mu.Lock()
	ch, ok := ac.channels[channel]
	if !ok {
		ac.mu.Unlock()
		return fmt.Errorf("channel %d not found", channel)
	}
	ch.links[handle] = &amqpLink{
		name:   name,
		handle: handle,
		role:   role,
		addr:   addr,
	}
	ac.mu.Unlock()

	// Send ATTACH back
	attachBody := BuildAttach(name, handle, role, addr)
	return WriteFrame(ac.writer, frameTypeAMQP, channel, attachBody)
}

func (ac *amqpConn) handleTransfer(value []byte, channel uint16) error {
	// Parse TRANSFER: [handle, delivery-id, delivery-tag, message-format, settled, more, ...]
	tr := newTypeReader(value)
	any, err := tr.readAny()
	if err != nil {
		return err
	}
	items, ok := any.([]interface{})
	if !ok {
		return fmt.Errorf("expected list in TRANSFER")
	}

	_ = items // Fields parsed as needed

	// The actual message payload follows the performative in the frame body.
	// For now, handle as a simple publish to the address from the link.

	ac.mu.Lock()
	ch, ok := ac.channels[channel]
	if !ok {
		ac.mu.Unlock()
		return nil
	}
	// Find the first sender link on this channel
	var linkAddr string
	for _, link := range ch.links {
		if link.role == 0 { // sender
			linkAddr = link.addr
			break
		}
	}
	ac.mu.Unlock()

	if linkAddr == "" {
		return nil
	}

	// The message body is the raw bytes after the performative in the frame
	// This is a simplified handler — a full implementation would parse AMQP message sections
	payload := extractPayload(value)

	env := &message.Envelope{
		Topic:       linkAddr,
		Payload:     payload,
		SourceProto: message.ProtoAMQP,
	}

	_, err = ac.server.broker.Publish(env)
	return err
}

func (ac *amqpConn) handleFlow(value []byte, channel uint16) error {
	// FLOW: [handle, delivery-count, link-credit, available]
	tr := newTypeReader(value)
	any, err := tr.readAny()
	if err != nil {
		return err
	}
	items, ok := any.([]interface{})
	if !ok {
		return fmt.Errorf("expected list in FLOW")
	}

	var handle uint32
	if len(items) > 0 {
		if v, ok := items[0].(uint32); ok {
			handle = v
		}
	}

	var credit uint32
	if len(items) > 2 {
		if v, ok := items[2].(uint32); ok {
			credit = v
		}
	}

	ac.mu.Lock()
	ch := ac.channels[channel]
	ac.mu.Unlock()

	if ch != nil {
		if link, ok := ch.links[handle]; ok {
			link.credit = credit
		}
	}

	return nil
}

func (ac *amqpConn) handleDisposition(value []byte, channel uint16) error {
	// DISPOSITION: [role, first, last, settled, state, ...]
	tr := newTypeReader(value)
	any, err := tr.readAny()
	if err != nil {
		return err
	}
	items, ok := any.([]interface{})
	if !ok {
		return fmt.Errorf("expected list in DISPOSITION")
	}

	var first, last uint64
	if len(items) > 1 {
		if v, ok := items[1].(uint64); ok {
			first = v
		}
	}
	if len(items) > 2 {
		if v, ok := items[2].(uint64); ok {
			last = v
		} else {
			last = first
		}
	}

	if last > 0 {
		ac.mu.Lock()
		ch := ac.channels[channel]
		ac.mu.Unlock()
		if ch != nil {
			for _, link := range ch.links {
				if link.role == 0 && last >= uint64(link.delivered) {
					link.delivered = uint32(last)
				}
			}
		}
	}
	return nil
}

func (ac *amqpConn) handleDetach(value []byte, channel uint16) error {
	tr := newTypeReader(value)
	any, err := tr.readAny()
	if err != nil {
		return err
	}
	items, ok := any.([]interface{})
	if !ok {
		return fmt.Errorf("expected list in DETACH")
	}

	var handle uint32
	if len(items) > 0 {
		if v, ok := items[0].(uint32); ok {
			handle = v
		}
	}

	ac.mu.Lock()
	if ch, ok := ac.channels[channel]; ok {
		delete(ch.links, handle)
	}
	ac.mu.Unlock()

	// Send DETACH back
	detachBody := BuildDetach(handle, true)
	return WriteFrame(ac.writer, frameTypeAMQP, channel, detachBody)
}

func (ac *amqpConn) handleEnd(channel uint16) error {
	ac.mu.Lock()
	delete(ac.channels, channel)
	ac.mu.Unlock()

	// Send END back
	endBody := BuildEnd()
	return WriteFrame(ac.writer, frameTypeAMQP, channel, endBody)
}

func (ac *amqpConn) handleClose() error {
	// Send CLOSE back
	closeBody := BuildClose()
	WriteFrame(ac.writer, frameTypeAMQP, 0, closeBody)
	ac.writer.Flush()
	return fmt.Errorf("connection closed")
}

func (ac *amqpConn) close() {
	ac.conn.Close()
}

// extractPayload attempts to extract message payload from a TRANSFER frame body.
// This is simplified — a full implementation would parse AMQP message sections.
func extractPayload(performative []byte) []byte {
	// The performative is a described list. After it, the payload follows.
	// For simplicity, try to find a reasonable payload after the performative.
	// Skip the described type by reading past it.
	tr := newTypeReader(performative)
	_, _ = tr.readAny() // skip the described type

	remaining := tr.data[tr.pos:]
	if len(remaining) > 0 {
		return remaining
	}
	return []byte(fmt.Sprintf("amqp-payload-%d", len(performative)))
}

// EncodePlainSASL encodes a PLAIN SASL response.
func EncodePlainSASL(username, password string) []byte {
	encoded := fmt.Sprintf("\x00%s\x00%s", username, password)
	result := make([]byte, base64.StdEncoding.EncodedLen(len(encoded)))
	base64.StdEncoding.Encode(result, []byte(encoded))
	return result
}
