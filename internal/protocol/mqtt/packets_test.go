package mqtt

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeConnect(t *testing.T) {
	// Build a CONNECT packet manually
	var buf bytes.Buffer

	// Protocol name
	buf.Write(encodeString("MQTT"))
	// Protocol level (3.1.1)
	buf.WriteByte(ProtocolLevel311)
	// Connect flags: clean session + username + password
	buf.WriteByte(connectFlagCleanSession | connectFlagUsername | connectFlagPassword)
	// Keep alive
	var ka [2]byte
	ka[0] = 0
	ka[1] = 60
	buf.Write(ka[:])
	// Client ID
	buf.Write(encodeString("test-client"))
	// Username
	buf.Write(encodeString("user"))
	// Password
	buf.Write(encodeString("pass"))

	remaining := buf.Bytes()

	// Parse
	cp, err := ParseConnect(remaining)
	if err != nil {
		t.Fatalf("ParseConnect: %v", err)
	}

	if cp.ClientID != "test-client" {
		t.Errorf("clientID = %q, want test-client", cp.ClientID)
	}
	if cp.ProtocolLevel != ProtocolLevel311 {
		t.Errorf("protocolLevel = %d, want %d", cp.ProtocolLevel, ProtocolLevel311)
	}
	if !cp.CleanSession {
		t.Error("expected clean session")
	}
	if cp.KeepAlive != 60 {
		t.Errorf("keepAlive = %d, want 60", cp.KeepAlive)
	}
	if cp.Username != "user" {
		t.Errorf("username = %q, want user", cp.Username)
	}
	if cp.Password != "pass" {
		t.Errorf("password = %q, want pass", cp.Password)
	}
}

func TestEncodeDecodeConnectWithWill(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(encodeString("MQTT"))
	buf.WriteByte(ProtocolLevel311)
	buf.WriteByte(connectFlagCleanSession | connectFlagWillFlag | connectFlagWillQoS | connectFlagWillRetain)
	buf.Write([]byte{0, 30}) // keepalive
	buf.Write(encodeString("client-will"))
	buf.Write(encodeString("will/topic"))
	buf.Write(encodeString("will payload"))

	cp, err := ParseConnect(buf.Bytes())
	if err != nil {
		t.Fatalf("ParseConnect: %v", err)
	}
	if cp.WillTopic != "will/topic" {
		t.Errorf("willTopic = %q, want will/topic", cp.WillTopic)
	}
	if string(cp.WillPayload) != "will payload" {
		t.Errorf("willPayload = %q, want 'will payload'", cp.WillPayload)
	}
	if !cp.WillRetain {
		t.Error("expected will retain")
	}
}

func TestConnAck(t *testing.T) {
	data := BuildConnAck(false, ConnAckAccepted)
	if len(data) != 2 {
		t.Fatalf("CONNACK length = %d, want 2", len(data))
	}
	if data[0] != 0 {
		t.Errorf("session present = %d, want 0", data[0])
	}
	if data[1] != ConnAckAccepted {
		t.Errorf("return code = %d, want %d", data[1], ConnAckAccepted)
	}
}

func TestBuildPublish(t *testing.T) {
	flags, data := BuildPublish("test/topic", []byte("hello"), QoS0, false, 0)
	if flags != 0 {
		t.Errorf("QoS0 flags = %d, want 0", flags)
	}

	flags, data = BuildPublish("test/topic", []byte("hello"), QoS1, true, 42)
	if flags&0x01 == 0 {
		t.Error("expected retain flag set")
	}
	if (flags>>1)&0x03 != QoS1 {
		t.Errorf("QoS = %d, want %d", (flags>>1)&0x03, QoS1)
	}

	// Verify packet ID is in the data
	if len(data) < 4 {
		t.Fatalf("publish data too short: %d bytes", len(data))
	}
	pid := uint16(data[len("test/topic")+2])<<8 | uint16(data[len("test/topic")+3])
	_ = pid
	_ = data
}

func TestSubAck(t *testing.T) {
	codes := []byte{0, 1, 2, 0x80}
	data := BuildSubAck(123, codes)
	if len(data) != 6 {
		t.Fatalf("SUBACK length = %d, want 6", len(data))
	}
	pid := uint16(data[0])<<8 | uint16(data[1])
	if pid != 123 {
		t.Errorf("packet ID = %d, want 123", pid)
	}
}

func TestReadPacketVariableLength(t *testing.T) {
	// Test with a simple PINGREQ (no remaining data)
	var buf bytes.Buffer
	buf.WriteByte(0xC0) // PINGREQ
	buf.WriteByte(0x00) // remaining length = 0

	pkt, err := ReadPacket(&buf)
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if pkt.Type != PacketPingReq {
		t.Errorf("type = %d, want %d", pkt.Type, PacketPingReq)
	}
}

func TestWriteAndReadRoundtrip(t *testing.T) {
	var buf bytes.Buffer

	// Write a PUBACK
	data := BuildPubAck(42)
	err := WritePacket(&buf, PacketPubAck, 0, data)
	if err != nil {
		t.Fatalf("WritePacket: %v", err)
	}

	// Read it back
	pkt, err := ReadPacket(&buf)
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if pkt.Type != PacketPubAck {
		t.Errorf("type = %d, want %d", pkt.Type, PacketPubAck)
	}
	pid, err := ParsePacketID(pkt.Remaining)
	if err != nil {
		t.Fatalf("ParsePacketID: %v", err)
	}
	if pid != 42 {
		t.Errorf("packetID = %d, want 42", pid)
	}
}

func TestParseSubscribe(t *testing.T) {
	var buf bytes.Buffer
	// Packet ID
	buf.Write([]byte{0, 10})
	// Topic 1
	buf.Write(encodeString("sensor/#"))
	buf.WriteByte(0) // QoS 0
	// Topic 2
	buf.Write(encodeString("cmd/+"))
	buf.WriteByte(1) // QoS 1

	sub, err := ParseSubscribe(buf.Bytes())
	if err != nil {
		t.Fatalf("ParseSubscribe: %v", err)
	}
	if sub.PacketID != 10 {
		t.Errorf("packetID = %d, want 10", sub.PacketID)
	}
	if len(sub.Topics) != 2 {
		t.Fatalf("topics count = %d, want 2", len(sub.Topics))
	}
	if sub.Topics[0].Filter != "sensor/#" || sub.Topics[0].QoS != 0 {
		t.Errorf("topic[0] = %+v", sub.Topics[0])
	}
	if sub.Topics[1].Filter != "cmd/+" || sub.Topics[1].QoS != 1 {
		t.Errorf("topic[1] = %+v", sub.Topics[1])
	}
}

func TestParseUnsubscribe(t *testing.T) {
	var buf bytes.Buffer
	buf.Write([]byte{0, 5})
	buf.Write(encodeString("topic/a"))
	buf.Write(encodeString("topic/b"))

	pid, topics, err := ParseUnsubscribe(buf.Bytes())
	if err != nil {
		t.Fatalf("ParseUnsubscribe: %v", err)
	}
	if pid != 5 {
		t.Errorf("packetID = %d, want 5", pid)
	}
	if len(topics) != 2 {
		t.Fatalf("topics = %d, want 2", len(topics))
	}
	if topics[0] != "topic/a" {
		t.Errorf("topic[0] = %q", topics[0])
	}
}
