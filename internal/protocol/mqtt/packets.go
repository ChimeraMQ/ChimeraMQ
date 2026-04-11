package mqtt

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Packet types (MQTT 3.1.1 / 5.0).
const (
	PacketConnect     byte = 1
	PacketConnAck     byte = 2
	PacketPublish     byte = 3
	PacketPubAck      byte = 4
	PacketPubRec      byte = 5
	PacketPubRel      byte = 6
	PacketPubComp     byte = 7
	PacketSubscribe   byte = 8
	PacketSubAck      byte = 9
	PacketUnsubscribe byte = 10
	PacketUnsubAck    byte = 11
	PacketPingReq     byte = 12
	PacketPingResp    byte = 13
	PacketDisconnect  byte = 14
	PacketAuth        byte = 15 // MQTT 5.0 only
)

// Protocol levels.
const (
	ProtocolLevel311 byte = 4 // MQTT 3.1.1
	ProtocolLevel50  byte = 5 // MQTT 5.0
)

// Connect flags.
const (
	connectFlagCleanSession byte = 0x02
	connectFlagWillFlag     byte = 0x04
	connectFlagWillQoS      byte = 0x18
	connectFlagWillRetain   byte = 0x20
	connectFlagPassword     byte = 0x40
	connectFlagUsername     byte = 0x80
)

// ConnAck return codes.
const (
	ConnAckAccepted          byte = 0
	ConnAckBadProtocol       byte = 1
	ConnAckBadClientID       byte = 2
	ConnAckServerUnavailable byte = 3
	ConnAckBadCredentials    byte = 4
	ConnAckUnauthorized      byte = 5
)

// QoS levels.
const (
	QoS0 byte = 0 // At most once
	QoS1 byte = 1 // At least once
	QoS2 byte = 2 // Exactly once
)

// Packet represents a decoded MQTT packet.
type Packet struct {
	Type      byte
	Flags     byte
	Remaining []byte
	PacketID  uint16 // populated for packets that carry one
}

// ConnectPayload holds the parsed CONNECT packet data.
type ConnectPayload struct {
	ProtocolLevel byte
	ClientID      string
	CleanSession  bool
	KeepAlive     uint16
	Username      string
	Password      string
	WillTopic     string
	WillPayload   []byte
	WillQoS       byte
	WillRetain    bool
}

// SubscribePayload holds parsed SUBSCRIBE data.
type SubscribePayload struct {
	PacketID uint16
	Topics   []SubTopic
}

// SubTopic is a single topic in a SUBSCRIBE packet.
type SubTopic struct {
	Filter string
	QoS    byte
}

// PublishPayload holds parsed PUBLISH data.
type PublishPayload struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Retain   bool
	Dup      bool
	PacketID uint16
}

// ---------- Packet reading ----------

// ReadPacket reads one MQTT packet from the reader.
func ReadPacket(r io.Reader) (*Packet, error) {
	// Fixed header: byte 1 = type + flags
	var header [1]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	pktType := header[0] >> 4
	pktFlags := header[0] & 0x0F

	// Remaining length (variable-length encoding, up to 4 bytes)
	remaining, err := readRemainingLength(r)
	if err != nil {
		return nil, err
	}
	if remaining > 16*1024*1024 { // 16MB safety cap (configurable)
		return nil, fmt.Errorf("remaining length too large: %d", remaining)
	}

	data := make([]byte, remaining)
	if remaining > 0 {
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, err
		}
	}

	return &Packet{
		Type:      pktType,
		Flags:     pktFlags,
		Remaining: data,
	}, nil
}

func readRemainingLength(r io.Reader) (int, error) {
	var value int
	var multiplier int = 1
	var b [1]byte

	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
		value += int(b[0]&0x7F) * multiplier
		if b[0]&0x80 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, fmt.Errorf("remaining length exceeds 4 bytes")
}

// ---------- Packet writing ----------

// WritePacket writes an MQTT packet to the writer.
func WritePacket(w io.Writer, pktType byte, flags byte, data []byte) error {
	// Fixed header
	if _, err := w.Write([]byte{pktType<<4 | flags}); err != nil {
		return err
	}
	// Remaining length
	if err := writeRemainingLength(w, len(data)); err != nil {
		return err
	}
	// Payload
	if len(data) > 0 {
		_, err := w.Write(data)
		return err
	}
	return nil
}

func writeRemainingLength(w io.Writer, length int) error {
	var encoded [4]byte
	i := 0
	for {
		encoded[i] = byte(length % 128)
		length /= 128
		if length > 0 {
			encoded[i] |= 0x80
		}
		i++
		if length == 0 {
			break
		}
	}
	_, err := w.Write(encoded[:i])
	return err
}

// ---------- Payload parsing ----------

// ParseConnect parses a CONNECT packet's remaining bytes.
func ParseConnect(data []byte) (*ConnectPayload, error) {
	r := newBytesReader(data)

	// Protocol name
	protoName, err := r.readString()
	if err != nil {
		return nil, fmt.Errorf("read protocol name: %w", err)
	}
	if protoName != "MQTT" && protoName != "MQIsdp" {
		return nil, fmt.Errorf("unexpected protocol name: %s", protoName)
	}

	// Protocol level
	protoLevel, err := r.readByte()
	if err != nil {
		return nil, err
	}

	// Connect flags
	flags, err := r.readByte()
	if err != nil {
		return nil, err
	}

	// Keep alive
	keepAlive, err := r.readUint16()
	if err != nil {
		return nil, err
	}

	cp := &ConnectPayload{
		ProtocolLevel: protoLevel,
		CleanSession:  flags&connectFlagCleanSession != 0,
		KeepAlive:     keepAlive,
	}

	// Client ID
	clientID, err := r.readString()
	if err != nil {
		return nil, err
	}
	cp.ClientID = clientID

	// Will
	if flags&connectFlagWillFlag != 0 {
		cp.WillQoS = (flags & connectFlagWillQoS) >> 3
		cp.WillRetain = flags&connectFlagWillRetain != 0
		cp.WillTopic, err = r.readString()
		if err != nil {
			return nil, err
		}
		cp.WillPayload, err = r.readBytes()
		if err != nil {
			return nil, err
		}
	}

	// Username
	if flags&connectFlagUsername != 0 {
		cp.Username, err = r.readString()
		if err != nil {
			return nil, err
		}
	}

	// Password
	if flags&connectFlagPassword != 0 {
		cp.Password, err = r.readString()
		if err != nil {
			return nil, err
		}
	}

	return cp, nil
}

// ParsePublish parses a PUBLISH packet.
func ParsePublish(pkt *Packet) (*PublishPayload, error) {
	r := newBytesReader(pkt.Remaining)

	topic, err := r.readString()
	if err != nil {
		return nil, err
	}

	pp := &PublishPayload{
		Topic:  topic,
		QoS:    (pkt.Flags >> 1) & 0x03,
		Retain: pkt.Flags&0x01 != 0,
		Dup:    pkt.Flags&0x08 != 0,
	}

	if pp.QoS > 0 {
		pp.PacketID, err = r.readUint16()
		if err != nil {
			return nil, err
		}
	}

	pp.Payload = r.remaining()
	return pp, nil
}

// ParseSubscribe parses a SUBSCRIBE packet.
func ParseSubscribe(data []byte) (*SubscribePayload, error) {
	r := newBytesReader(data)
	packetID, err := r.readUint16()
	if err != nil {
		return nil, err
	}

	sp := &SubscribePayload{PacketID: packetID}
	for r.len() > 0 {
		filter, err := r.readString()
		if err != nil {
			return nil, err
		}
		qos, err := r.readByte()
		if err != nil {
			return nil, err
		}
		sp.Topics = append(sp.Topics, SubTopic{Filter: filter, QoS: qos})
	}

	return sp, nil
}

// ParseUnsubscribe parses an UNSUBSCRIBE packet.
func ParseUnsubscribe(data []byte) (uint16, []string, error) {
	r := newBytesReader(data)
	packetID, err := r.readUint16()
	if err != nil {
		return 0, nil, err
	}

	var topics []string
	for r.len() > 0 {
		filter, err := r.readString()
		if err != nil {
			return 0, nil, err
		}
		topics = append(topics, filter)
	}

	return packetID, topics, nil
}

// ParsePacketID extracts the 2-byte packet ID from ack packets.
func ParsePacketID(data []byte) (uint16, error) {
	if len(data) < 2 {
		return 0, fmt.Errorf("packet too short for packet ID")
	}
	return binary.BigEndian.Uint16(data[:2]), nil
}

// ---------- Packet building ----------

// BuildConnAck builds a CONNACK packet.
func BuildConnAck(sessionPresent bool, returnCode byte) []byte {
	flags := byte(0)
	if sessionPresent {
		flags = 0x01
	}
	return []byte{flags, returnCode}
}

// BuildSubAck builds a SUBACK packet.
func BuildSubAck(packetID uint16, returnCodes []byte) []byte {
	buf := make([]byte, 2+len(returnCodes))
	binary.BigEndian.PutUint16(buf[:2], packetID)
	copy(buf[2:], returnCodes)
	return buf
}

// BuildUnsubAck builds an UNSUBACK packet.
func BuildUnsubAck(packetID uint16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, packetID)
	return buf
}

// BuildPubAck builds a PUBACK packet.
func BuildPubAck(packetID uint16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, packetID)
	return buf
}

// BuildPublish builds a PUBLISH packet's remaining bytes.
func BuildPublish(topic string, payload []byte, qos byte, retain bool, packetID uint16) (flags byte, data []byte) {
	flags = qos << 1
	if retain {
		flags |= 0x01
	}

	// Topic
	buf := encodeString(topic)

	// Packet ID for QoS > 0
	if qos > 0 {
		pid := make([]byte, 2)
		binary.BigEndian.PutUint16(pid, packetID)
		buf = append(buf, pid...)
	}

	buf = append(buf, payload...)
	return flags, buf
}

// ---------- Helpers ----------

type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) len() int {
	return len(r.data) - r.pos
}

func (r *bytesReader) readByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *bytesReader) readUint16() (uint16, error) {
	if r.pos+2 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint16(r.data[r.pos:])
	r.pos += 2
	return v, nil
}

func (r *bytesReader) readString() (string, error) {
	length, err := r.readUint16()
	if err != nil {
		return "", err
	}
	if r.pos+int(length) > len(r.data) {
		return "", io.ErrUnexpectedEOF
	}
	s := string(r.data[r.pos : r.pos+int(length)])
	r.pos += int(length)
	return s, nil
}

func (r *bytesReader) readBytes() ([]byte, error) {
	length, err := r.readUint16()
	if err != nil {
		return nil, err
	}
	if r.pos+int(length) > len(r.data) {
		return nil, io.ErrUnexpectedEOF
	}
	b := make([]byte, length)
	copy(b, r.data[r.pos:r.pos+int(length)])
	r.pos += int(length)
	return b, nil
}

func (r *bytesReader) remaining() []byte {
	return r.data[r.pos:]
}

func encodeString(s string) []byte {
	buf := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(s)))
	copy(buf[2:], s)
	return buf
}
