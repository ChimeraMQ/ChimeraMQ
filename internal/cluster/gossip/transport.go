package gossip

import (
	"encoding/binary"
	"encoding/json"
	"net"
	"time"
)

// MessageType identifies a gossip protocol message.
type MessageType uint8

const (
	MsgPing MessageType = iota
	MsgAck
	MsgPingReq
	MsgSuspect
	MsgAlive
	MsgDead
	MsgSync // Full state sync
)

// Message is a gossip protocol message.
type Message struct {
	Type        MessageType `json:"t"`
	SenderID    NodeID      `json:"s"`
	Incarnation uint64      `json:"i"`
	TargetID    NodeID      `json:"ti,omitempty"` // For PingReq: who to ping
	TargetAddr  string      `json:"ta,omitempty"` // For PingReq: target address
	Members     []MemberMsg `json:"m,omitempty"`  // Piggybacked member updates
}

// MemberMsg is a serialized member for gossip messages.
type MemberMsg struct {
	ID          NodeID      `json:"id"`
	Addr        string      `json:"addr"`
	Port        int         `json:"port"`
	State       MemberState `json:"state"`
	Incarnation uint64      `json:"inc"`
}

// UDPTransport handles gossip message transport over UDP.
type UDPTransport struct {
	conn *net.UDPConn
}

// NewUDPTransport creates a new UDP transport bound to addr.
func NewUDPTransport(addr string) (*UDPTransport, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}
	return &UDPTransport{conn: conn}, nil
}

// Send sends a message to the given address.
func (t *UDPTransport) Send(addr string, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	// Prefix with length (4 bytes)
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(data)))
	copy(buf[4:], data)

	_ = t.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err = t.conn.WriteToUDP(buf, udpAddr)
	return err
}

// Receive receives a message.
func (t *UDPTransport) Receive() (*Message, *net.UDPAddr, error) {
	buf := make([]byte, 64*1024)
	_ = t.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, addr, err := t.conn.ReadFromUDP(buf)
	if err != nil {
		return nil, nil, err
	}
	if n < 4 {
		return nil, nil, nil
	}

	length := int(binary.BigEndian.Uint32(buf[:4]))
	if length > n-4 {
		return nil, nil, nil
	}

	var msg Message
	if err := json.Unmarshal(buf[4:4+length], &msg); err != nil {
		return nil, nil, err
	}
	return &msg, addr, nil
}

// Close closes the transport.
func (t *UDPTransport) Close() error {
	if t.conn != nil {
		return t.conn.Close()
	}
	return nil
}

// LocalAddr returns the local address.
func (t *UDPTransport) LocalAddr() string {
	if t.conn != nil {
		return t.conn.LocalAddr().String()
	}
	return ""
}
