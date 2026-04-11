package gossip

import (
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestHMACTransportRoundTrip(t *testing.T) {
	// Start two transports
	ta, err := NewUDPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tb, err := NewUDPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ta.Close()
	defer tb.Close()

	secret := []byte("test-secret-key-1234567890123456")
	ha := NewHMACTransport(ta, secret)
	hb := NewHMACTransport(tb, secret)

	msg := &Message{
		Type:        MsgPing,
		SenderID:    "node-a",
		Incarnation: 1,
	}

	// A sends to B
	addrB := tb.LocalAddr()
	if err := ha.Send(addrB, msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// B receives
	received, _, err := hb.Receive()
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if received.Type != MsgPing {
		t.Errorf("type = %d, want MsgPing", received.Type)
	}
	if received.SenderID != "node-a" {
		t.Errorf("sender = %q, want node-a", received.SenderID)
	}
}

func TestHMACTransportWrongKey(t *testing.T) {
	ta, err := NewUDPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tb, err := NewUDPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ta.Close()
	defer tb.Close()

	ha := NewHMACTransport(ta, []byte("correct-secret-key-1234567890"))
	hb := NewHMACTransport(tb, []byte("wrong-secret-key-12345678900"))

	msg := &Message{Type: MsgPing, SenderID: "attacker"}
	if err := ha.Send(tb.LocalAddr(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	_, _, err = hb.Receive()
	if err == nil {
		t.Error("expected HMAC verification error")
	}
}

func TestHMACTransportKeyRotation(t *testing.T) {
	ta, err := NewUDPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tb, err := NewUDPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ta.Close()
	defer tb.Close()

	oldKey := []byte("old-key-123456789012345678901234")
	newKey := []byte("new-key-123456789012345678901234")

	ha := NewHMACTransport(ta, oldKey)
	hb := NewHMACTransport(tb, oldKey)

	// B adds the new key but still has the old one
	hb.AddKey(newKey)

	// A sends signed with old key
	msg := &Message{Type: MsgAlive, SenderID: "node-a"}
	if err := ha.Send(tb.LocalAddr(), msg); err != nil {
		t.Fatalf("Send with old key: %v", err)
	}

	received, _, err := hb.Receive()
	if err != nil {
		t.Fatalf("Receive old key: %v", err)
	}
	if received.SenderID != "node-a" {
		t.Errorf("sender = %q", received.SenderID)
	}

	// Now A rotates to new key
	ha.AddKey(newKey)

	msg2 := &Message{Type: MsgAlive, SenderID: "node-a", Incarnation: 2}
	if err := ha.Send(tb.LocalAddr(), msg2); err != nil {
		t.Fatalf("Send with new key: %v", err)
	}

	received2, _, err := hb.Receive()
	if err != nil {
		t.Fatalf("Receive new key: %v", err)
	}
	if received2.Incarnation != 2 {
		t.Errorf("incarnation = %d, want 2", received2.Incarnation)
	}
}

func TestHMACTransportTamperedPayload(t *testing.T) {
	ta, err := NewUDPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tb, err := NewUDPTransport("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ta.Close()
	defer tb.Close()

	secret := []byte("shared-secret-key-12345678901234")
	_ = NewHMACTransport(ta, secret)
	hb := NewHMACTransport(tb, secret)

	// Manually craft a message with a tampered payload
	msg := &Message{Type: MsgPing, SenderID: "honest"}
	data, _ := json.Marshal(msg)
	mac := computeHMAC(secret, data)

	// Tamper: change sender ID
	tampered := &Message{Type: MsgPing, SenderID: "attacker"}
	tamperedData, _ := json.Marshal(tampered)

	// Send with valid HMAC but tampered payload
	totalPayload := hmacSize + len(tamperedData)
	buf := make([]byte, 4+totalPayload)
	encodeUint32(buf[:4], uint32(totalPayload))
	copy(buf[4:4+hmacSize], mac) // valid MAC for original data
	copy(buf[4+hmacSize:], tamperedData)

	udpAddr, _ := net.ResolveUDPAddr("udp", tb.LocalAddr())
	ta.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	ta.conn.WriteToUDP(buf, udpAddr)

	_, _, err = hb.Receive()
	if err == nil {
		t.Error("expected HMAC verification error for tampered payload")
	}
}

func TestGenerateHMACKey(t *testing.T) {
	key, err := GenerateHMACKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != 32 {
		t.Errorf("key length = %d, want 32", len(key))
	}

	// Should generate different keys
	key2, _ := GenerateHMACKey()
	if string(key) == string(key2) {
		t.Error("keys should be different")
	}
}

func encodeUint32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}
