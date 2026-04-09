package mqtt

import (
	"sync"
	"sync/atomic"
	"time"
)

// Session represents an MQTT client session.
type Session struct {
	mu         sync.RWMutex
	clientID   string
	clean      bool
	keepAlive  uint16
	subs       map[string]byte // topic filter → granted QoS
	inflight   map[uint16]*inflightMessage
	nextPID    atomic.Uint32
	will       *willMessage
	connected  bool
	lastActive time.Time
}

type inflightMessage struct {
	topic   string
	payload []byte
	qos     byte
	sent    time.Time
	state   inflightState
}

type inflightState int

const (
	statePubSent  inflightState = iota // QoS 1: waiting PUBACK
	statePubRec                        // QoS 2: waiting PUBREL
	statePubRel                        // QoS 2: waiting PUBCOMP
)

type willMessage struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
}

// NewSession creates a new MQTT session.
func NewSession(clientID string, clean bool, keepAlive uint16) *Session {
	s := &Session{
		clientID:  clientID,
		clean:     clean,
		keepAlive: keepAlive,
		subs:      make(map[string]byte),
		inflight:  make(map[uint16]*inflightMessage),
	}
	s.nextPID.Store(1)
	s.lastActive = time.Now()
	return s
}

// ClientID returns the session's client ID.
func (s *Session) ClientID() string {
	return s.clientID
}

// NextPacketID returns the next available packet identifier.
func (s *Session) NextPacketID() uint16 {
	for {
		id := uint16(s.nextPID.Add(1))
		if id == 0 {
			continue // skip 0
		}
		s.mu.Lock()
		_, exists := s.inflight[id]
		s.mu.Unlock()
		if !exists {
			return id
		}
	}
}

// AddInflight stores a message waiting for acknowledgement.
func (s *Session) AddInflight(packetID uint16, topic string, payload []byte, qos byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight[packetID] = &inflightMessage{
		topic:   topic,
		payload: payload,
		qos:     qos,
		sent:    time.Now(),
		state:   statePubSent,
	}
}

// AckInflight removes a message from the inflight map (PUBACK received).
func (s *Session) AckInflight(packetID uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, packetID)
}

// SetInflightState updates the inflight message state for QoS 2.
func (s *Session) SetInflightState(packetID uint16, state inflightState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if msg, ok := s.inflight[packetID]; ok {
		msg.state = state
	}
}

// Subscriptions returns a copy of the current subscriptions.
func (s *Session) Subscriptions() map[string]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]byte, len(s.subs))
	for k, v := range s.subs {
		out[k] = v
	}
	return out
}

// AddSub adds a subscription.
func (s *Session) AddSub(filter string, qos byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs[filter] = qos
}

// RemoveSub removes a subscription.
func (s *Session) RemoveSub(filter string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subs, filter)
}

// SetWill stores the will message from CONNECT.
func (s *Session) SetWill(topic string, payload []byte, qos byte, retain bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.will = &willMessage{
		topic:   topic,
		payload: payload,
		qos:     qos,
		retain:  retain,
	}
}

// TakeWill returns and clears the will message.
func (s *Session) TakeWill() *willMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.will
	s.will = nil
	return w
}

// Touch updates the last active time.
func (s *Session) Touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActive = time.Now()
}

// LastActive returns when the session was last active.
func (s *Session) LastActive() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastActive
}

// KeepAliveDuration returns the keepalive interval.
func (s *Session) KeepAliveDuration() time.Duration {
	if s.keepAlive == 0 {
		return 0
	}
	return time.Duration(s.keepAlive) * time.Second
}
