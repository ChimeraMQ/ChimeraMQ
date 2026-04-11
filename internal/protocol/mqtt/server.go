package mqtt

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
)

// Server implements the MQTT protocol handler.
type Server struct {
	broker   *broker.Broker
	topics   *TopicMapper
	retained *RetainedStore
	sessions sync.Map // clientID → *Session
}

// Detector detects the MQTT protocol.
type Detector struct{}

// Detect checks for MQTT CONNECT packet (type=1, flags=0 → byte 0x10).
func (d *Detector) Detect(peek []byte) bool {
	return len(peek) >= 1 && peek[0] == 0x10
}

// BytesNeeded returns 1 (first byte identifies MQTT CONNECT).
func (d *Detector) BytesNeeded() int { return 1 }

// NewServer creates a new MQTT protocol server.
func NewServer(b *broker.Broker) *Server {
	cfg := b.Config()
	return &Server{
		broker:   b,
		topics:   NewTopicMapper(cfg.Protocols.MQTT.TopicSeparator),
		retained: NewRetainedStore(cfg.Protocols.MQTT.RetainedMax),
	}
}

// HandleConnection implements ProtocolHandler.
func (s *Server) HandleConnection(conn net.Conn, _ []byte) error {
	s.handleConnection(conn)
	return nil
}

// Stop implements ProtocolHandler.
func (s *Server) Stop() {
	// Close all sessions
	s.sessions.Range(func(key, value any) bool {
		sess := value.(*Session)
		will := sess.TakeWill()
		if will != nil {
			s.publishWill(will)
		}
		return true
	})
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	// Read CONNECT
	pkt, err := ReadPacket(reader)
	if err != nil {
		return
	}
	if pkt.Type != PacketConnect {
		return // First packet must be CONNECT
	}

	connect, err := ParseConnect(pkt.Remaining)
	if err != nil {
		return
	}

	// Authentication
	if s.broker.Config().Auth.Enabled {
		if !s.authenticate(connect.Username, connect.Password) {
			s.writePacket(writer, PacketConnAck, 0, BuildConnAck(false, ConnAckBadCredentials))
			writer.Flush()
			return
		}
	}

	// Client ID
	clientID := connect.ClientID
	if clientID == "" {
		if connect.ProtocolLevel >= ProtocolLevel50 {
			// MQTT 5.0 allows empty client ID (server assigns)
			clientID = fmt.Sprintf("mqtt-auto-%d", time.Now().UnixNano())
		} else {
			s.writePacket(writer, PacketConnAck, 0, BuildConnAck(false, ConnAckBadClientID))
			writer.Flush()
			return
		}
	}

	// Session
	session := NewSession(clientID, connect.CleanSession, connect.KeepAlive)
	if connect.WillTopic != "" {
		session.SetWill(connect.WillTopic, connect.WillPayload, connect.WillQoS, connect.WillRetain)
	}

	// Handle existing session with same client ID
	if old, loaded := s.sessions.LoadOrStore(clientID, session); loaded {
		oldSess := old.(*Session)
		// Disconnect old session
		s.sessions.Store(clientID, session)
		_ = oldSess // old session is abandoned
	}

	// Send CONNACK
	s.writePacket(writer, PacketConnAck, 0, BuildConnAck(false, ConnAckAccepted))
	if err := writer.Flush(); err != nil {
		return
	}

	// Keepalive
	keepalive := session.KeepAliveDuration()
	if keepalive > 0 {
		if keepalive < 5*time.Second {
			keepalive = 5 * time.Second
		}
		if keepalive > 10*time.Minute {
			keepalive = 10 * time.Minute
		}
	}

	// Main read loop
	defer func() {
		s.sessions.Delete(clientID)
		// Will message on ungraceful disconnect
		if will := session.TakeWill(); will != nil {
			s.publishWill(will)
		}
	}()

	for {
		if keepalive > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(keepalive * 2))
		}

		pkt, err := ReadPacket(reader)
		if err != nil {
			return
		}
		session.Touch()

		switch pkt.Type {
		case PacketConnect:
			// Re-CONNECT on same connection — handle like initial CONNECT
			newConnect, err := ParseConnect(pkt.Remaining)
			if err != nil {
				return
			}
			if s.broker.Config().Auth.Enabled && !s.authenticate(newConnect.Username, newConnect.Password) {
				s.writePacket(writer, PacketConnAck, 0, BuildConnAck(false, ConnAckBadCredentials))
				writer.Flush()
				return
			}

		case PacketPublish:
			s.handlePublish(writer, session, pkt)

		case PacketPubAck:
			if len(pkt.Remaining) >= 2 {
				pid := uint16(pkt.Remaining[0])<<8 | uint16(pkt.Remaining[1])
				session.AckInflight(pid)
			}

		case PacketPubRec:
			if len(pkt.Remaining) >= 2 {
				pid := uint16(pkt.Remaining[0])<<8 | uint16(pkt.Remaining[1])
				session.SetInflightState(pid, statePubRel)
				// Send PUBREL
				s.writePacket(writer, PacketPubRel, 0x02, []byte{pkt.Remaining[0], pkt.Remaining[1]})
				writer.Flush()
			}

		case PacketPubRel:
			if len(pkt.Remaining) >= 2 {
				pid := uint16(pkt.Remaining[0])<<8 | uint16(pkt.Remaining[1])
				session.AckInflight(pid)
				// Send PUBCOMP
				s.writePacket(writer, PacketPubComp, 0, []byte{pkt.Remaining[0], pkt.Remaining[1]})
				writer.Flush()
			}

		case PacketPubComp:
			if len(pkt.Remaining) >= 2 {
				pid := uint16(pkt.Remaining[0])<<8 | uint16(pkt.Remaining[1])
				session.AckInflight(pid)
			}

		case PacketSubscribe:
			s.handleSubscribe(writer, session, pkt)

		case PacketUnsubscribe:
			s.handleUnsubscribe(writer, session, pkt)

		case PacketPingReq:
			s.writePacket(writer, PacketPingResp, 0, nil)
			writer.Flush()

		case PacketDisconnect:
			// Graceful disconnect — clear will
			session.TakeWill()
			return

		case PacketAuth:
			// MQTT 5.0 AUTH — enhanced auth, not implemented yet
		}
	}
}

func (s *Server) handlePublish(writer *bufio.Writer, session *Session, pkt *Packet) {
	pub, err := ParsePublish(pkt)
	if err != nil {
		return
	}

	// Convert MQTT topic to ChimeraMQ topic
	chimeraTopic := s.topics.MQTTToChimera(pub.Topic)

	// Build envelope
	env := &message.Envelope{
		Topic:       chimeraTopic,
		Payload:     pub.Payload,
		SourceProto: message.ProtoMQTT,
	}

	// Publish via broker
	_, pubErr := s.broker.Publish(env)

	// Handle retained messages
	if pub.Retain {
		s.retained.Store(pub.Topic, pub.Payload, pub.QoS)
	}

	// QoS handling
	switch pub.QoS {
	case QoS0:
		// Fire and forget — nothing to send back
	case QoS1:
		if pub.PacketID != 0 {
			s.writePacket(writer, PacketPubAck, 0, BuildPubAck(pub.PacketID))
			writer.Flush()
		}
	case QoS2:
		if pub.PacketID != 0 {
			// Store in inflight to deduplicate until PUBREL arrives
			session.AddInflight(pub.PacketID, chimeraTopic, pub.Payload, QoS2)
			// Send PUBREC
			pid := make([]byte, 2)
			pid[0] = byte(pub.PacketID >> 8)
			pid[1] = byte(pub.PacketID)
			s.writePacket(writer, PacketPubRec, 0, pid)
			writer.Flush()
		}
	}

	_ = pubErr
}

func (s *Server) handleSubscribe(writer *bufio.Writer, session *Session, pkt *Packet) {
	sub, err := ParseSubscribe(pkt.Remaining)
	if err != nil {
		return
	}

	returnCodes := make([]byte, len(sub.Topics))
	for i, topic := range sub.Topics {
		chimeraTopic := s.topics.MQTTToChimera(topic.Filter)

		// Verify topic exists (or allow auto-create)
		_, exists := s.broker.Topics().GetTopic(chimeraTopic)
		if !exists {
			returnCodes[i] = 0x80 // Failure
			continue
		}

		// Register subscription
		grantedQoS := topic.QoS
		if maxQoS := s.broker.Config().Protocols.MQTT.MaxQoS; grantedQoS > maxQoS {
			grantedQoS = maxQoS
		}
		session.AddSub(topic.Filter, grantedQoS)
		returnCodes[i] = grantedQoS

		// Send retained messages for this filter
		retained := s.retained.Matching(topic.Filter)
		for _, rm := range retained {
			pid := session.NextPacketID()
			flags, data := BuildPublish(rm.Topic, rm.Payload, grantedQoS, true, pid)
			s.writePacket(writer, PacketPublish, flags, data)
		}
	}

	s.writePacket(writer, PacketSubAck, 0, BuildSubAck(sub.PacketID, returnCodes))
	writer.Flush()
}

func (s *Server) handleUnsubscribe(writer *bufio.Writer, session *Session, pkt *Packet) {
	packetID, topics, err := ParseUnsubscribe(pkt.Remaining)
	if err != nil {
		return
	}

	for _, topic := range topics {
		session.RemoveSub(topic)
	}

	s.writePacket(writer, PacketUnsubAck, 0, BuildUnsubAck(packetID))
	writer.Flush()
}

func (s *Server) publishWill(will *willMessage) {
	chimeraTopic := s.topics.MQTTToChimera(will.topic)
	env := &message.Envelope{
		Topic:       chimeraTopic,
		Payload:     will.payload,
		SourceProto: message.ProtoMQTT,
	}
	_, _ = s.broker.Publish(env)

	if will.retain {
		s.retained.Store(will.topic, will.payload, will.qos)
	}
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

func (s *Server) writePacket(w *bufio.Writer, pktType byte, flags byte, data []byte) {
	_ = WritePacket(w, pktType, flags, data)
}
