package stomp

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
	"golang.org/x/crypto/bcrypt"
)

func hashPassword(t *testing.T, password string) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(hash)
}

func newSTOMPTestBroker(t *testing.T, authEnabled bool) (*broker.Broker, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "stomp-server-*")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 0, AdminPort: 0},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "error", Format: "text", Output: "stdout"},
		Auth: broker.AuthConfig{
			Enabled: authEnabled,
			Type:    "static",
			Users:   map[string]string{"admin": hashPassword(t, "secret")},
			Tokens:  map[string]string{"my-token": "admin"},
		},
	}
	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	if err := bkr.Start(); err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	return bkr, func() {
		bkr.Stop()
		os.RemoveAll(dir)
	}
}

func readFrame(t *testing.T, r *bufio.Reader) *Frame {
	t.Helper()
	f, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	return f
}

func TestHandleConnectionNoAuth(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, false)
	defer cleanup()
	srv := NewServer(b)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		srv.HandleConnection(server, nil)
		close(done)
	}()

	reader := bufio.NewReader(client)
	// Send CONNECT without credentials
	_, _ = client.Write([]byte("CONNECT\naccept-version:1.2\nhost:localhost\n\n\x00"))

	// Should get CONNECTED
	frame := readFrame(t, reader)
	if frame.Command != CmdConnected {
		t.Fatalf("expected CONNECTED, got %s", frame.Command)
	}
	if frame.Get("version") != "1.2" {
		t.Errorf("version = %s", frame.Get("version"))
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleConnectionAuthSuccess(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, true)
	defer cleanup()
	srv := NewServer(b)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		srv.HandleConnection(server, nil)
		close(done)
	}()

	reader := bufio.NewReader(client)
	// Send CONNECT with valid credentials
	_, _ = client.Write([]byte("CONNECT\nlogin:admin\npasscode:secret\naccept-version:1.2\nhost:localhost\n\n\x00"))

	frame := readFrame(t, reader)
	if frame.Command != CmdConnected {
		t.Fatalf("expected CONNECTED, got %s", frame.Command)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleConnectionAuthFailure(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, true)
	defer cleanup()
	srv := NewServer(b)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		srv.HandleConnection(server, nil)
		close(done)
	}()

	reader := bufio.NewReader(client)
	// Send CONNECT with invalid credentials
	_, _ = client.Write([]byte("CONNECT\nlogin:admin\npasscode:wrong\naccept-version:1.2\nhost:localhost\n\n\x00"))

	frame := readFrame(t, reader)
	if frame.Command != CmdError {
		t.Fatalf("expected ERROR, got %s", frame.Command)
	}
	if !strings.Contains(frame.Get("message"), "Authentication failed") {
		t.Fatalf("expected auth failure message, got: %s", frame.Get("message"))
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleConnectionStompCommand(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, false)
	defer cleanup()
	srv := NewServer(b)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		srv.HandleConnection(server, nil)
		close(done)
	}()

	reader := bufio.NewReader(client)
	// Use STOMP command instead of CONNECT
	_, _ = client.Write([]byte("STOMP\naccept-version:1.2\nhost:localhost\n\n\x00"))

	frame := readFrame(t, reader)
	if frame.Command != CmdConnected {
		t.Fatalf("expected CONNECTED, got %s", frame.Command)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleSendAndSubscribe(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, false)
	defer cleanup()
	b.Topics().CreateTopic(broker.TopicConfig{Name: "orders", Mode: broker.ModeUnified, Partitions: 1})
	srv := NewServer(b)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		srv.HandleConnection(server, nil)
		close(done)
	}()

	reader := bufio.NewReader(client)
	// CONNECT
	_, _ = client.Write([]byte("CONNECT\naccept-version:1.2\nhost:localhost\n\n\x00"))
	_ = readFrame(t, reader) // CONNECTED

	// SUBSCRIBE
	_, _ = client.Write([]byte("SUBSCRIBE\ndestination:/topic/orders\nid:sub-1\nack:auto\n\n\x00"))

	// SEND
	_, _ = client.Write([]byte("SEND\ndestination:/topic/orders\ncontent-type:text/plain\n\nhello\x00"))

	// Give server time to process and route message
	time.Sleep(200 * time.Millisecond)

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleUnsubscribe(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, false)
	defer cleanup()
	srv := NewServer(b)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		srv.HandleConnection(server, nil)
		close(done)
	}()

	reader := bufio.NewReader(client)
	// CONNECT
	_, _ = client.Write([]byte("CONNECT\naccept-version:1.2\nhost:localhost\n\n\x00"))
	_ = readFrame(t, reader) // CONNECTED

	// SUBSCRIBE
	_, _ = client.Write([]byte("SUBSCRIBE\ndestination:/topic/orders\nid:sub-1\nack:auto\n\n\x00"))
	time.Sleep(100 * time.Millisecond)

	// UNSUBSCRIBE
	_, _ = client.Write([]byte("UNSUBSCRIBE\nid:sub-1\n\n\x00"))

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestSessionAuthenticateWithProvider(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, true)
	defer cleanup()

	sess := &Session{b: b}
	if _, ok := sess.authenticate("admin", "secret"); !ok {
		t.Error("expected authenticate to succeed")
	}
	if _, ok := sess.authenticate("admin", "wrong"); ok {
		t.Error("expected authenticate to fail")
	}
}

func TestSessionAuthenticateNoProvider(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, false)
	defer cleanup()

	sess := &Session{b: b}
	if _, ok := sess.authenticate("admin", "secret"); ok {
		t.Error("expected authenticate to fail with no provider")
	}
}

func TestServerStop(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, false)
	defer cleanup()
	srv := NewServer(b)

	// Add a session with a safe mock conn
	sess := &Session{id: "test", conn: &mockConn{}, closed: false}
	srv.sessions.Store("test", sess)

	srv.Stop()
}

func TestWriteFrameClosedSession(t *testing.T) {
	sess := &Session{
		id:     "test",
		conn:   &mockConn{},
		writer: bufio.NewWriter(&bytes.Buffer{}),
	}
	sess.close()
	if err := sess.writeFrame(NewFrame(CmdReceipt)); err == nil {
		t.Error("expected error for closed session")
	}
}

func TestSendMessageClosedSession(t *testing.T) {
	sess := &Session{
		id:     "test",
		conn:   &mockConn{},
		writer: bufio.NewWriter(&bytes.Buffer{}),
	}
	sess.close()
	if err := sess.sendMessage(&Subscription{ID: "1"}, &message.Envelope{}); err == nil {
		t.Error("expected error for closed session")
	}
}

func TestHandleAckNackReceipt(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, false)
	defer cleanup()
	srv := NewServer(b)

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		srv.HandleConnection(server, nil)
		close(done)
	}()

	reader := bufio.NewReader(client)
	// CONNECT
	_, _ = client.Write([]byte("CONNECT\naccept-version:1.2\nhost:localhost\n\n\x00"))
	_ = readFrame(t, reader) // CONNECTED

	// ACK with receipt
	_, _ = client.Write([]byte("ACK\nreceipt:r1\n\n\x00"))
	frame := readFrame(t, reader)
	if frame.Command != CmdReceipt {
		t.Fatalf("expected RECEIPT, got %s", frame.Command)
	}
	if frame.Get("receipt-id") != "r1" {
		t.Errorf("receipt-id = %s", frame.Get("receipt-id"))
	}

	// NACK with receipt
	_, _ = client.Write([]byte("NACK\nreceipt:r2\n\n\x00"))
	frame = readFrame(t, reader)
	if frame.Command != CmdReceipt {
		t.Fatalf("expected RECEIPT, got %s", frame.Command)
	}
	if frame.Get("receipt-id") != "r2" {
		t.Errorf("receipt-id = %s", frame.Get("receipt-id"))
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestAuthProviderIntegration(t *testing.T) {
	b, cleanup := newSTOMPTestBroker(t, true)
	defer cleanup()

	provider := b.AuthProvider()
	if provider == nil {
		t.Fatal("expected auth provider")
	}

	_, err := provider.Authenticate(nil, auth.Credentials{Username: "admin", Password: "secret"})
	if err != nil {
		t.Errorf("expected auth success, got: %v", err)
	}
}

type mockConn struct{}

func (m *mockConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (m *mockConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (m *mockConn) Close() error                       { return nil }
func (m *mockConn) LocalAddr() net.Addr                { return nil }
func (m *mockConn) RemoteAddr() net.Addr               { return nil }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }
