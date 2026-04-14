package mqtt

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
)

func TestParseAuth(t *testing.T) {
	pkt := &Packet{Remaining: []byte{0x00}} // Success reason code
	auth, err := ParseAuth(pkt)
	if err != nil {
		t.Fatalf("ParseAuth: %v", err)
	}
	if auth.ReasonCode != 0x00 {
		t.Errorf("reasonCode = %d, want 0", auth.ReasonCode)
	}

	// Empty remaining
	auth2, err := ParseAuth(&Packet{Remaining: nil})
	if err != nil {
		t.Fatalf("ParseAuth empty: %v", err)
	}
	if auth2.ReasonCode != 0x00 {
		t.Errorf("reasonCode = %d, want 0 for empty", auth2.ReasonCode)
	}
}

func TestBuildAuth(t *testing.T) {
	data := BuildAuth(0x00, "SCRAM-SHA-1", []byte("challenge"))
	if len(data) == 0 {
		t.Fatal("BuildAuth returned empty data")
	}
	// Fixed header: packet type 15 (AUTH) << 4 = 0xF0
	if data[0] != 0xF0 {
		t.Errorf("fixed header = 0x%02X, want 0xF0", data[0])
	}
}

func TestBuildAuthNoProps(t *testing.T) {
	data := BuildAuth(0x00, "", nil)
	if len(data) == 0 {
		t.Fatal("BuildAuth returned empty data")
	}
}

func TestEncodeBytesAndVariableLength(t *testing.T) {
	b := encodeBytes([]byte("hello"))
	if len(b) != 7 { // 2 bytes length + 5 bytes payload
		t.Errorf("encodeBytes len = %d, want 7", len(b))
	}

	vl := encodeVariableLength(128)
	if len(vl) != 2 {
		t.Errorf("encodeVariableLength(128) len = %d, want 2", len(vl))
	}

	vl = encodeVariableLength(16384)
	if len(vl) != 3 {
		t.Errorf("encodeVariableLength(16384) len = %d, want 3", len(vl))
	}

	vl = encodeVariableLength(0)
	if len(vl) != 1 {
		t.Errorf("encodeVariableLength(0) len = %d, want 1", len(vl))
	}
}

func TestProtocolLevel(t *testing.T) {
	sess := NewSession("pl-test", true, 60, ProtocolLevel50)
	if sess.ProtocolLevel() != ProtocolLevel50 {
		t.Errorf("protocolLevel = %d, want %d", sess.ProtocolLevel(), ProtocolLevel50)
	}

	sess2 := NewSession("pl-test2", true, 60, ProtocolLevel311)
	if sess2.ProtocolLevel() != ProtocolLevel311 {
		t.Errorf("protocolLevel = %d, want %d", sess2.ProtocolLevel(), ProtocolLevel311)
	}
}

func TestAuthenticateWithAuthFailure(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	b.Config().Auth.Enabled = true
	b.Config().Auth.Type = "static"
	b.Start()

	srv := NewServer(b)
	if _, ok := srv.authenticate("admin", "wrong"); ok {
		t.Error("expected false for wrong password")
	}
}

func TestHandleConnectionReconnect(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	connectPkt := buildConnect("re-client", true, 60)

	server, client := mqttPipe(
		connectPkt,
		connectPkt, // Re-CONNECT (server processes silently on success)
		buildMQTTDisconnect(),
	)
	defer server.Close()
	defer client.Close()

	done := runHandler(srv, server)

	readPacketFrom(t, client) // CONNACK for first CONNECT
	// Successful re-CONNECT does not send another CONNACK

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConnection did not finish")
	}
}

func TestHandleConnectionParseConnectError(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	// Invalid CONNECT: claims protocol name length 4 but gives "MQ" (2 bytes)
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, uint16(4))
	payload.WriteString("MQ") // truncated

	var pkt bytes.Buffer
	pkt.WriteByte(0x10)
	writeRL(&pkt, payload.Len())
	pkt.Write(payload.Bytes())

	server, client := mqttPipe(pkt.Bytes())
	defer server.Close()
	defer client.Close()

	done := runHandler(srv, server)

	select {
	case <-done:
		// Expected: connection closed due to parse error
	case <-time.After(2 * time.Second):
		t.Error("should exit on parse connect error")
	}
}

func TestHandleConnectionEmptyClientIDOldProtocol(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	server, client := mqttPipe(buildConnect("", true, 60))
	defer server.Close()
	defer client.Close()

	done := runHandler(srv, server)

	resp := readPacketFrom(t, client)
	if resp.Type != PacketConnAck {
		t.Fatalf("expected CONNACK, got %d", resp.Type)
	}
	if resp.Remaining[1] != ConnAckBadClientID {
		t.Errorf("reason = %d, want %d", resp.Remaining[1], ConnAckBadClientID)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConnection did not finish")
	}
}

func TestHandleSubscribeTopicNotFound(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	// Don't create topic — let it fail
	srv := NewServer(b)

	server, client := mqttPipe(
		buildConnect("sub-nf", true, 60),
		buildMQTTSubscribe(1, []SubTopic{{Filter: "nonexistent/topic", QoS: 1}}),
		buildMQTTDisconnect(),
	)
	defer server.Close()
	defer client.Close()

	done := runHandler(srv, server)
	readPacketFrom(t, client) // CONNACK

	resp := readPacketFrom(t, client)
	if resp.Type != PacketSubAck {
		t.Fatalf("expected SUBACK, got %d", resp.Type)
	}
	// Return code should be 0x80 (failure)
	if len(resp.Remaining) >= 3 && resp.Remaining[2] != 0x80 {
		t.Errorf("return code = 0x%02X, want 0x80", resp.Remaining[2])
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConnection did not finish")
	}
}

func TestHandleSubscribeParseError(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	// Invalid SUBSCRIBE packet
	badSub := []byte{0x82, 0x01, 0x00}

	server, client := mqttPipe(
		buildConnect("sub-bad", true, 60),
		badSub,
		buildMQTTDisconnect(),
	)
	defer server.Close()
	defer client.Close()

	done := runHandler(srv, server)
	readPacketFrom(t, client) // CONNACK

	// After bad subscribe, connection may or may not close immediately.
	// Just wait for handler to finish.
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConnection did not finish")
	}
}

func TestRetainedStoreEviction(t *testing.T) {
	rs := NewRetainedStore(2)
	rs.Store("a", []byte("1"), 0)
	rs.Store("b", []byte("2"), 0)
	rs.Store("c", []byte("3"), 0) // should evict one

	count := 0
	count += len(rs.Matching("a"))
	count += len(rs.Matching("b"))
	count += len(rs.Matching("c"))
	if count != 2 {
		t.Errorf("expected 2 retained after eviction, got %d", count)
	}
}

func TestRetainedStoreDeleteEmptyPayload(t *testing.T) {
	rs := NewRetainedStore(10)
	rs.Store("del/topic", []byte("data"), 0)
	if len(rs.Matching("del/topic")) != 1 {
		t.Fatal("expected 1 retained")
	}
	rs.Store("del/topic", []byte{}, 0) // empty payload deletes
	if len(rs.Matching("del/topic")) != 0 {
		t.Error("expected 0 retained after empty payload store")
	}
}

func TestHandleAuthMQTT5(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	// Build MQTT 5.0 CONNECT packet
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, uint16(4))
	payload.WriteString("MQTT")
	payload.WriteByte(5) // Protocol level 5
	payload.WriteByte(0x02)
	binary.Write(&payload, binary.BigEndian, uint16(60))
	binary.Write(&payload, binary.BigEndian, uint16(5))
	payload.WriteString("auth5")

	var connectPkt bytes.Buffer
	connectPkt.WriteByte(0x10)
	writeRL(&connectPkt, payload.Len())
	connectPkt.Write(payload.Bytes())

	// Build AUTH packet (reason code 0x00 = Continue authentication)
	authData := BuildAuth(0x00, "", nil)

	server, client := mqttPipe(
		connectPkt.Bytes(),
		authData,
		buildMQTTDisconnect(),
	)
	defer server.Close()
	defer client.Close()

	done := runHandler(srv, server)
	readPacketFrom(t, client) // CONNACK

	// Should get AUTH response with success
	resp := readPacketFrom(t, client)
	if resp.Type != PacketAuth {
		t.Fatalf("expected AUTH, got %d", resp.Type)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConnection did not finish")
	}
}

func TestHandleAuthParseError(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	// Build MQTT 5.0 CONNECT
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, uint16(4))
	payload.WriteString("MQTT")
	payload.WriteByte(5)
	payload.WriteByte(0x02)
	binary.Write(&payload, binary.BigEndian, uint16(60))
	binary.Write(&payload, binary.BigEndian, uint16(6))
	payload.WriteString("auth5e")

	var connectPkt bytes.Buffer
	connectPkt.WriteByte(0x10)
	writeRL(&connectPkt, payload.Len())
	connectPkt.Write(payload.Bytes())

	// Invalid AUTH packet — just type and flags, no valid remaining
	badAuth := []byte{0xF0, 0x02, 0x00, 0x00}

	server, client := mqttPipe(
		connectPkt.Bytes(),
		badAuth,
		buildMQTTDisconnect(),
	)
	defer server.Close()
	defer client.Close()

	done := runHandler(srv, server)
	readPacketFrom(t, client) // CONNACK

	// Should get AUTH response with error
	resp := readPacketFrom(t, client)
	if resp.Type != PacketAuth {
		t.Fatalf("expected AUTH, got %d", resp.Type)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConnection did not finish")
	}
}

func TestHandleAuthNonMQTT5(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	// Build MQTT 3.1.1 CONNECT
	connectPkt := buildConnect("auth31", true, 60)

	// AUTH packet (0xF0) sent on MQTT 3.1.1 connection should get protocol error
	authData := BuildAuth(0x00, "", nil)

	server, client := mqttPipe(
		connectPkt,
		authData,
		buildMQTTDisconnect(),
	)
	defer server.Close()
	defer client.Close()

	done := runHandler(srv, server)
	readPacketFrom(t, client) // CONNACK

	// Should get AUTH response with protocol error (0x8C)
	resp := readPacketFrom(t, client)
	if resp.Type != PacketAuth {
		t.Fatalf("expected AUTH, got %d", resp.Type)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConnection did not finish")
	}
}

func TestHandleConnectionWithWillOnClose(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	b.Topics().CreateTopic(broker.TopicConfig{Name: "will.topic", Mode: broker.ModeUnified, Partitions: 1})
	srv := NewServer(b)

	server, client := net.Pipe()
	go func() {
		client.Write(buildConnectWithWill("will-client", "will/topic", []byte("bye"), 0, false))
		// Don't send DISCONNECT — ungraceful close
		client.Close()
	}()

	done := runHandler(srv, server)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConnection did not finish")
	}
	server.Close()
}

func TestHandleAuthReauthWithAuthEnabled(t *testing.T) {
	dir := t.TempDir()
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 0, AdminPort: 0},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "error", Format: "text", Output: "stdout"},
		Auth:     broker.AuthConfig{Enabled: true, Type: "static", Users: map[string]string{"admin": "secret"}},
	}
	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bkr.Start(); err != nil {
		t.Fatal(err)
	}
	defer bkr.Stop()

	srv := NewServer(bkr)
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	sess := NewSession("auth-test", true, 60, ProtocolLevel50)

	authPkt := &Packet{Remaining: []byte{0x18}} // Re-authentication
	srv.handleAuth(writer, sess, authPkt)
	writer.Flush()

	resp, err := ReadPacket(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("read auth response: %v", err)
	}
	if resp.Type != PacketAuth {
		t.Fatalf("expected AUTH, got %d", resp.Type)
	}
	if len(resp.Remaining) == 0 || resp.Remaining[0] != 0x00 {
		t.Errorf("reason code = %v, want 0x00", resp.Remaining)
	}
}

func TestHandleAuthReauthWithAuthDisabled(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	sess := NewSession("auth-test", true, 60, ProtocolLevel50)

	authPkt := &Packet{Remaining: []byte{0x18}} // Re-authentication
	srv.handleAuth(writer, sess, authPkt)
	writer.Flush()

	resp, err := ReadPacket(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("read auth response: %v", err)
	}
	if resp.Type != PacketAuth {
		t.Fatalf("expected AUTH, got %d", resp.Type)
	}
	if len(resp.Remaining) == 0 || resp.Remaining[0] != 0x00 {
		t.Errorf("reason code = %v, want 0x00", resp.Remaining)
	}
}

func TestHandleAuthUnknownReasonCode(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	sess := NewSession("auth-test", true, 60, ProtocolLevel50)

	authPkt := &Packet{Remaining: []byte{0x99}} // Unknown reason code
	srv.handleAuth(writer, sess, authPkt)
	writer.Flush()

	resp, err := ReadPacket(bufio.NewReader(&buf))
	if err != nil {
		t.Fatalf("read auth response: %v", err)
	}
	if resp.Type != PacketAuth {
		t.Fatalf("expected AUTH, got %d", resp.Type)
	}
	if len(resp.Remaining) == 0 || resp.Remaining[0] != 0x80 {
		t.Errorf("reason code = %v, want 0x80", resp.Remaining)
	}
}

func TestHandleConnectionReconnectAuthFailure(t *testing.T) {
	dir := t.TempDir()
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 0, AdminPort: 0},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "error", Format: "text", Output: "stdout"},
		Auth:     broker.AuthConfig{Enabled: true, Type: "static", Users: map[string]string{"admin": "secret"}},
	}
	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bkr.Start(); err != nil {
		t.Fatal(err)
	}
	defer bkr.Stop()

	srv := NewServer(bkr)

	buildAuthConnect := func(password string) []byte {
		var payload bytes.Buffer
		binary.Write(&payload, binary.BigEndian, uint16(4))
		payload.WriteString("MQTT")
		payload.WriteByte(4)
		payload.WriteByte(0xC2) // Clean session + Username + Password
		binary.Write(&payload, binary.BigEndian, uint16(60))
		binary.Write(&payload, binary.BigEndian, uint16(7))
		payload.WriteString("re-auth")
		binary.Write(&payload, binary.BigEndian, uint16(5))
		payload.WriteString("admin")
		binary.Write(&payload, binary.BigEndian, uint16(len(password)))
		payload.WriteString(password)

		var pkt bytes.Buffer
		pkt.WriteByte(0x10)
		writeRL(&pkt, payload.Len())
		pkt.Write(payload.Bytes())
		return pkt.Bytes()
	}

	server, client := mqttPipe(
		buildAuthConnect("secret"),
		buildAuthConnect("wrong"),
	)
	defer server.Close()
	defer client.Close()

	done := runHandler(srv, server)
	readPacketFrom(t, client) // First CONNACK (success)

	// On Windows the close may race with the read, so accept either outcome.
	client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	resp, err := ReadPacket(bufio.NewReader(client))
	if err != nil {
		// Connection closed before/instead of CONNACK — valid behavior
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("handleConnection did not finish")
		}
		return
	}
	if resp.Type != PacketConnAck {
		t.Fatalf("expected CONNACK, got %d", resp.Type)
	}
	if len(resp.Remaining) >= 2 && resp.Remaining[1] != ConnAckBadCredentials {
		t.Errorf("reason = %d, want %d", resp.Remaining[1], ConnAckBadCredentials)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConnection did not finish")
	}
}

func TestHandleConnectionKeepaliveClampLow(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	server, client := net.Pipe()
	go func() {
		client.Write(buildConnect("ka-low", true, 1)) // 1s keepalive
		time.Sleep(2 * time.Second)                   // Would timeout if not clamped to 5s
		client.Write(buildMQTTPingReq())
		time.Sleep(100 * time.Millisecond)
		client.Write(buildMQTTDisconnect())
		client.Close()
	}()

	done := runHandler(srv, server)

	resp := readPacketFrom(t, client) // CONNACK
	if resp.Type != PacketConnAck {
		t.Fatalf("expected CONNACK, got %d", resp.Type)
	}

	resp = readPacketFrom(t, client) // PINGRESP
	if resp.Type != PacketPingResp {
		t.Errorf("got type %d, want PINGRESP", resp.Type)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("handleConnection did not finish")
	}
	server.Close()
}

func TestHandleConnectionPubAck(t *testing.T) {
	b, cleanup := newMQTTTestBroker(t)
	defer cleanup()
	srv := NewServer(b)

	server, client := mqttPipe(
		buildConnect("puback-client", true, 60),
		[]byte{0x40, 0x02, 0x00, 0x01}, // PUBACK packet ID 1
		buildMQTTDisconnect(),
	)
	defer server.Close()
	defer client.Close()

	done := runHandler(srv, server)
	readPacketFrom(t, client) // CONNACK

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConnection did not finish")
	}
}
