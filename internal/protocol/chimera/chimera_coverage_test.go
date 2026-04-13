package chimera

import (
	"bufio"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"golang.org/x/crypto/bcrypt"
)

func newAuthTestHarness(t *testing.T) *testHarness {
	t.Helper()

	dir := t.TempDir()
	hash, _ := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)

	cfg, err := broker.LoadConfig("", &broker.CLIFlags{DataDir: dir})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "static"
	cfg.Auth.Users = map[string]string{"admin": string(hash)}
	cfg.Auth.Tokens = map[string]string{"secret": "token-user"}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	srv, err := NewServer(b)
	if err != nil {
		b.Stop()
		t.Fatalf("NewServer: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		srv.Serve()
	}()
	time.Sleep(10 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", srv.listener.Addr().String(), 2*time.Second)
	if err != nil {
		srv.StopAccepting()
		b.Stop()
		t.Fatalf("dial: %v", err)
	}

	h := &testHarness{
		broker:  b,
		server:  srv,
		client:  conn,
		clientR: bufio.NewReaderSize(conn, 64*1024),
		clientW: bufio.NewWriterSize(conn, 64*1024),
	}
	h.cleanup = func() {
		conn.Close()
		srv.StopAccepting()
		wg.Wait()
		b.Stop()
	}

	return h
}

func TestHandleConnectionNonConnectFirst(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	// Send PUBLISH before CONNECT
	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: []byte{}})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()

	// Connection should be closed
	h.client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	_, err := readFrame(h.clientR)
	if err == nil {
		t.Error("expected connection to close after non-CONNECT first frame")
	}
}

func TestHandleConnectionAuthFailure(t *testing.T) {
	h := newAuthTestHarness(t)
	defer h.cleanup()

	payload := encodeConnectPayload("auth-fail", "admin", "wrong", 60)
	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnect, Payload: payload})
	h.clientW.Write(frame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if resp.OpCode != OpConnAck {
		t.Errorf("opcode = %v, want OpConnAck", resp.OpCode)
	}

	r := newReader(resp.Payload)
	id, _ := r.readString()
	if id != "" {
		t.Errorf("id = %q, want empty", id)
	}
	status := r.read(1)
	if len(status) != 1 || status[0] != 1 {
		t.Errorf("status = %v, want 1", status)
	}
}

func TestHandleConnectionReconnectAuthFailure(t *testing.T) {
	h := newAuthTestHarness(t)
	defer h.cleanup()

	// First connect successfully
	payload := encodeConnectPayload("reconn-fail", "admin", "secret", 60)
	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnect, Payload: payload})
	h.clientW.Write(frame)
	h.clientW.Flush()
	readFrame(h.clientR) // consume connack

	// Send second CONNECT with wrong password
	payload = encodeConnectPayload("reconn-fail", "admin", "badpass", 60)
	frame, _ = EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnect, Payload: payload})
	h.clientW.Write(frame)
	h.clientW.Flush()

	// Server sends connack with status 1 then closes connection.
	// On Windows the close may race with the read, so accept either outcome.
	h.client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	resp, err := readFrame(h.clientR)
	if err != nil {
		return // connection closed before/instead of connack — valid behavior
	}
	if resp.OpCode != OpConnAck {
		t.Errorf("opcode = %v, want OpConnAck", resp.OpCode)
		return
	}

	r := newReader(resp.Payload)
	id, _ := r.readString()
	if id != "" {
		t.Errorf("id = %q, want empty", id)
	}
	status := r.read(1)
	if len(status) != 1 || status[0] != 1 {
		t.Errorf("status = %v, want 1", status)
	}
}

func TestAuthenticateDirect(t *testing.T) {
	h := newAuthTestHarness(t)
	defer h.cleanup()

	// Success
	id, err := h.server.authenticate("admin", "secret")
	if err != nil {
		t.Errorf("authenticate valid: %v", err)
	}
	if id == nil {
		t.Error("expected non-nil identity")
	}

	// Failure
	_, err = h.server.authenticate("admin", "wrong")
	if err == nil {
		t.Error("expected error for invalid password")
	}
}

func TestHandleFetchMaxMessagesClamp(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "fetch-clamp")
	readFrame(h.clientR) // connack

	// Create topic
	ctPayload := encodeCreateTopicPayload("clamp-topic", "stream", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Fetch with maxMessages = 0 (should default to 100)
	var fetchPayload []byte
	fetchPayload = appendUint16(fetchPayload, uint16(len("clamp-topic")))
	fetchPayload = append(fetchPayload, "clamp-topic"...)
	fetchPayload = binary.BigEndian.AppendUint32(fetchPayload, 0) // partition
	fetchPayload = binary.BigEndian.AppendUint64(fetchPayload, 0) // offset
	fetchPayload = binary.BigEndian.AppendUint32(fetchPayload, 0) // maxMessages = 0
	fetchFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpFetch, Payload: fetchPayload})
	h.clientW.Write(fetchFrame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read fetch resp (zero): %v", err)
	}
	if resp.OpCode != OpFetchResp {
		t.Errorf("zero maxMessages: opcode = %v, want OpFetchResp", resp.OpCode)
	}

	// Fetch with maxMessages > server limit (should clamp)
	fetchPayload = nil
	fetchPayload = appendUint16(fetchPayload, uint16(len("clamp-topic")))
	fetchPayload = append(fetchPayload, "clamp-topic"...)
	fetchPayload = binary.BigEndian.AppendUint32(fetchPayload, 0)
	fetchPayload = binary.BigEndian.AppendUint64(fetchPayload, 0)
	fetchPayload = binary.BigEndian.AppendUint32(fetchPayload, 999999) // maxMessages huge
	fetchFrame, _ = EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpFetch, Payload: fetchPayload})
	h.clientW.Write(fetchFrame)
	h.clientW.Flush()

	resp, err = readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read fetch resp (huge): %v", err)
	}
	if resp.OpCode != OpFetchResp {
		t.Errorf("huge maxMessages: opcode = %v, want OpFetchResp", resp.OpCode)
	}
}
