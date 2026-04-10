package mqtt

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestReadPacketZeroRemaining(t *testing.T) {
	// Packet type=CONNECT (0x10), remaining length=0
	data := []byte{0x10, 0x00}
	pkt, err := ReadPacket(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Type != PacketConnect {
		t.Errorf("type = %d, want %d", pkt.Type, PacketConnect)
	}
	if len(pkt.Remaining) != 0 {
		t.Errorf("remaining = %d bytes, want 0", len(pkt.Remaining))
	}
}

func TestReadPacketTooLarge(t *testing.T) {
	// Craft a packet with remaining length > 256MB
	data := []byte{0x10, 0x80, 0x80, 0x80, 0x08} // 268435456 > 256MB
	_, err := ReadPacket(bytes.NewReader(data))
	if err == nil {
		t.Error("expected error for too-large remaining length")
	}
}

func TestReadRemainingLengthOverflow(t *testing.T) {
	// 5 continuation bytes — should error
	data := []byte{0x80, 0x80, 0x80, 0x80, 0x01}
	_, err := readRemainingLength(bytes.NewReader(data))
	if err == nil {
		t.Error("expected error for remaining length > 4 bytes")
	}
}

func TestParseConnectWithWill(t *testing.T) {
	// Build CONNECT with will flag set
	var buf bytes.Buffer
	// Protocol name "MQTT"
	binary.Write(&buf, binary.BigEndian, uint16(4))
	buf.WriteString("MQTT")
	buf.WriteByte(4) // Protocol level 4 (MQTT 3.1.1)
	// Flags: CleanSession | WillFlag | WillQoS(1) | WillRetain | Username | Password
	flags := byte(0xC0 | 0x04 | 0x20 | 0x40 | 0x02 | 0x01) // will flag + username + password
	buf.WriteByte(flags)
	binary.Write(&buf, binary.BigEndian, uint16(60)) // KeepAlive
	// Client ID
	binary.Write(&buf, binary.BigEndian, uint16(5))
	buf.WriteString("clnt1")
	// Will topic
	binary.Write(&buf, binary.BigEndian, uint16(9))
	buf.WriteString("will/tpic")
	// Will payload
	binary.Write(&buf, binary.BigEndian, uint16(5))
	buf.WriteString("will!")
	// Username
	binary.Write(&buf, binary.BigEndian, uint16(4))
	buf.WriteString("user")
	// Password
	binary.Write(&buf, binary.BigEndian, uint16(4))
	buf.WriteString("pass")

	cp, err := ParseConnect(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if cp.ClientID != "clnt1" {
		t.Errorf("ClientID = %q, want clnt1", cp.ClientID)
	}
	if cp.WillTopic != "will/tpic" {
		t.Errorf("WillTopic = %q", cp.WillTopic)
	}
	if string(cp.WillPayload) != "will!" {
		t.Errorf("WillPayload = %q", cp.WillPayload)
	}
	if cp.Username != "user" {
		t.Errorf("Username = %q", cp.Username)
	}
	if cp.Password != "pass" {
		t.Errorf("Password = %q", cp.Password)
	}
	if cp.KeepAlive != 60 {
		t.Errorf("KeepAlive = %d, want 60", cp.KeepAlive)
	}
}

func TestParseConnectBadProtocol(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(3))
	buf.WriteString("FTP")
	cp, err := ParseConnect(buf.Bytes())
	if err == nil {
		t.Error("expected error for bad protocol")
		_ = cp
	}
}

func TestParseConnectTruncated(t *testing.T) {
	// Only protocol name, nothing else
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(4))
	buf.WriteString("MQTT")
	// Missing: proto level, flags, keepalive, clientID
	_, err := ParseConnect(buf.Bytes())
	if err == nil {
		t.Error("expected error for truncated connect")
	}
}

func TestParsePublishQoS0(t *testing.T) {
	flags, data := BuildPublish("test/topic", []byte("hello"), 0, false, 0)
	pkt := &Packet{Type: PacketPublish, Flags: flags, Remaining: data}
	pp, err := ParsePublish(pkt)
	if err != nil {
		t.Fatal(err)
	}
	if pp.Topic != "test/topic" {
		t.Errorf("Topic = %q", pp.Topic)
	}
	if pp.QoS != 0 {
		t.Errorf("QoS = %d, want 0", pp.QoS)
	}
	if string(pp.Payload) != "hello" {
		t.Errorf("Payload = %q", pp.Payload)
	}
}

func TestParsePublishQoS1(t *testing.T) {
	flags, data := BuildPublish("test/topic", []byte("hello"), 1, true, 42)
	pkt := &Packet{Type: PacketPublish, Flags: flags, Remaining: data}
	pp, err := ParsePublish(pkt)
	if err != nil {
		t.Fatal(err)
	}
	if pp.QoS != 1 {
		t.Errorf("QoS = %d, want 1", pp.QoS)
	}
	if pp.PacketID != 42 {
		t.Errorf("PacketID = %d, want 42", pp.PacketID)
	}
	if !pp.Retain {
		t.Error("Retain should be true")
	}
}

func TestParsePublishTruncated(t *testing.T) {
	pkt := &Packet{Type: PacketPublish, Flags: 0, Remaining: []byte{0x00}}
	_, err := ParsePublish(pkt)
	if err == nil {
		t.Error("expected error for truncated publish")
	}
}

func TestParseSubscribeMultipleTopics(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(1)) // Packet ID
	// Topic 1
	binary.Write(&buf, binary.BigEndian, uint16(7))
	buf.WriteString("topic/a")
	buf.WriteByte(0) // QoS 0
	// Topic 2
	binary.Write(&buf, binary.BigEndian, uint16(7))
	buf.WriteString("topic/b")
	buf.WriteByte(1) // QoS 1

	sp, err := ParseSubscribe(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if sp.PacketID != 1 {
		t.Errorf("PacketID = %d, want 1", sp.PacketID)
	}
	if len(sp.Topics) != 2 {
		t.Fatalf("Topics count = %d, want 2", len(sp.Topics))
	}
	if sp.Topics[0].Filter != "topic/a" || sp.Topics[0].QoS != 0 {
		t.Errorf("topic[0] = %+v", sp.Topics[0])
	}
	if sp.Topics[1].Filter != "topic/b" || sp.Topics[1].QoS != 1 {
		t.Errorf("topic[1] = %+v", sp.Topics[1])
	}
}

func TestParseSubscribeTruncated(t *testing.T) {
	_, err := ParseSubscribe([]byte{0x00})
	if err == nil {
		t.Error("expected error for truncated subscribe")
	}
}

func TestParseUnsubscribeMultiple(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(2)) // Packet ID
	binary.Write(&buf, binary.BigEndian, uint16(5))
	buf.WriteString("abc/d")
	binary.Write(&buf, binary.BigEndian, uint16(5))
	buf.WriteString("efg/h")

	pid, topics, err := ParseUnsubscribe(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if pid != 2 {
		t.Errorf("PacketID = %d, want 2", pid)
	}
	if len(topics) != 2 {
		t.Fatalf("topics count = %d, want 2", len(topics))
	}
	if topics[0] != "abc/d" {
		t.Errorf("topics[0] = %q", topics[0])
	}
}

func TestParsePacketIDTooShort(t *testing.T) {
	_, err := ParsePacketID([]byte{0x00})
	if err == nil {
		t.Error("expected error for short packet")
	}
}

func TestParsePacketIDValid(t *testing.T) {
	pid, err := ParsePacketID([]byte{0x00, 0x2A})
	if err != nil {
		t.Fatal(err)
	}
	if pid != 42 {
		t.Errorf("PacketID = %d, want 42", pid)
	}
}

func TestBuildConnAckSessionPresent(t *testing.T) {
	data := BuildConnAck(true, 0)
	if data[0] != 0x01 {
		t.Errorf("flags = %d, want 1", data[0])
	}
}

func TestWritePacketEmpty(t *testing.T) {
	var buf bytes.Buffer
	err := WritePacket(&buf, PacketPingReq, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 2 {
		t.Errorf("wrote %d bytes, want 2", buf.Len())
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	payload := []byte("test-payload-data")
	var buf bytes.Buffer
	if err := WritePacket(&buf, PacketPublish, 0x02, payload); err != nil {
		t.Fatal(err)
	}

	pkt, err := ReadPacket(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.Type != PacketPublish {
		t.Errorf("type = %d, want %d", pkt.Type, PacketPublish)
	}
	if pkt.Flags != 0x02 {
		t.Errorf("flags = %d, want 2", pkt.Flags)
	}
	if string(pkt.Remaining) != string(payload) {
		t.Errorf("payload mismatch")
	}
}

func TestReadRemainingLengthMultiByte(t *testing.T) {
	// 2-byte encoding: value = 128 (0x80, 0x01)
	data := []byte{0x80, 0x01} // 0 + 128 = 128
	val, err := readRemainingLength(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if val != 128 {
		t.Errorf("value = %d, want 128", val)
	}
}

func TestWriteRemainingLengthLarge(t *testing.T) {
	var buf bytes.Buffer
	if err := writeRemainingLength(&buf, 16384); err != nil {
		t.Fatal(err)
	}
	// Read it back
	val, err := readRemainingLength(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if val != 16384 {
		t.Errorf("roundtrip: got %d, want 16384", val)
	}
}

func TestParseSubscribeBadTopic(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint16(1))
	// Invalid: string length claims 100 bytes but only 2 available
	buf.Write([]byte{0x00, 0x64, 0x41})
	_, err := ParseSubscribe(buf.Bytes())
	if err == nil {
		t.Error("expected error for bad topic string")
	}
}

func TestParseUnsubscribeTruncated(t *testing.T) {
	_, _, err := ParseUnsubscribe([]byte{0x00})
	if err == nil {
		t.Error("expected error for truncated unsubscribe")
	}
}

func TestBytesReaderReadByteEOF(t *testing.T) {
	r := newBytesReader(nil)
	_, err := r.readByte()
	if err == nil {
		t.Error("expected EOF")
	}
}

func TestBytesReaderReadUint16EOF(t *testing.T) {
	r := newBytesReader([]byte{0x00})
	_, err := r.readUint16()
	if err == nil {
		t.Error("expected error")
	}
}

func TestBytesReaderReadStringTruncated(t *testing.T) {
	r := newBytesReader([]byte{0x00, 0x10}) // claims 16 bytes but only header
	_, err := r.readString()
	if err == nil {
		t.Error("expected error")
	}
}

func TestBytesReaderReadBytesTruncated(t *testing.T) {
	r := newBytesReader([]byte{0x00, 0x10}) // claims 16 bytes
	_, err := r.readBytes()
	if err == nil {
		t.Error("expected error")
	}
}
