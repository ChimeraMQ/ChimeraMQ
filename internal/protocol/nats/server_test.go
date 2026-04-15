package nats

import (
	"bufio"
	"bytes"
	"context"
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

func newNATSTestBroker(t *testing.T, authEnabled bool) (*broker.Broker, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "nats-server-*")
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

func TestHandleConnectionNoAuth(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	// Read INFO
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read INFO: %v", err)
	}
	if !strings.HasPrefix(line, "INFO ") {
		t.Fatalf("expected INFO, got: %s", line)
	}

	// Send CONNECT without credentials (auth disabled)
	_, _ = client.Write([]byte(`CONNECT {"name":"test"}` + "\r\n"))
	// Send PING to verify connection is alive
	_, _ = client.Write([]byte("PING\r\n"))

	// Should get PONG
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err = reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read PONG: %v", err)
	}
	if !strings.HasPrefix(line, "PONG") {
		t.Fatalf("expected PONG, got: %s", line)
	}

	client.Close()
	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleConnectionAuthSuccess(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, true)
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
	// Read INFO
	_, _ = reader.ReadString('\n')

	// Send CONNECT with valid credentials
	connect := `CONNECT {"user":"admin","pass":"secret","name":"test"}` + "\r\n"
	_, _ = client.Write([]byte(connect))
	_, _ = client.Write([]byte("PING\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if !strings.HasPrefix(line, "PONG") {
		t.Fatalf("expected PONG after auth, got: %s", line)
	}

	client.Close()
	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleConnectionAuthFailure(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, true)
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
	// Read INFO
	_, _ = reader.ReadString('\n')

	// Send CONNECT with invalid credentials
	connect := `CONNECT {"user":"admin","pass":"wrong","name":"test"}` + "\r\n"
	_, _ = client.Write([]byte(connect))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.Contains(line, "Authorization Violation") {
		t.Fatalf("expected auth error, got: %s", line)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleConnectionInvalidJSON(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte("CONNECT {invalid\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.Contains(line, "Invalid CONNECT JSON") {
		t.Fatalf("expected invalid JSON error, got: %s", line)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestSessionAuthenticateWithProvider(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, true)
	defer cleanup()

	sess := &Session{b: b}
	if _, ok := sess.authenticate("admin", "secret", ""); !ok {
		t.Error("expected authenticate to succeed with valid credentials")
	}
	if _, ok := sess.authenticate("admin", "wrong", ""); ok {
		t.Error("expected authenticate to fail with invalid password")
	}
}

func TestSessionAuthenticateWithToken(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, true)
	defer cleanup()

	sess := &Session{b: b}
	if _, ok := sess.authenticate("", "", "my-token"); !ok {
		t.Error("expected authenticate to succeed with valid token")
	}
	if _, ok := sess.authenticate("", "", "bad-token"); ok {
		t.Error("expected authenticate to fail with invalid token")
	}
}

func TestSessionAuthenticateNoProvider(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
	defer cleanup()

	sess := &Session{b: b}
	if _, ok := sess.authenticate("admin", "secret", ""); ok {
		t.Error("expected authenticate to fail when no provider")
	}
}

func TestServerStop(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
	defer cleanup()
	srv := NewServer(b)

	// Add a dummy session
	srv.sessions.Store("test", &Session{id: "test", conn: &net.TCPConn{}})

	// Stop should not panic even with nil conn in session
	srv.Stop()
}

func TestSendMessageClosedSession(t *testing.T) {
	sess := &Session{
		id:     "test",
		conn:   &mockConn{},
		writer: bufio.NewWriter(&bytes.Buffer{}),
	}
	sess.close()
	if err := sess.sendMessage(&Subscription{SID: "1", Subject: "test"}, &message.Envelope{}); err == nil {
		t.Error("expected error for closed session")
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

func TestAuthProviderIntegration(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, true)
	defer cleanup()

	provider := b.AuthProvider()
	if provider == nil {
		t.Fatal("expected auth provider")
	}

	_, err := provider.Authenticate(context.TODO(), auth.Credentials{Username: "admin", Password: "secret"})
	if err != nil {
		t.Errorf("expected auth success, got: %v", err)
	}
}

func TestHandlePubNotConnected(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte("PUB orders 5\r\nhello\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.Contains(line, "Not connected") {
		t.Fatalf("expected Not connected error, got: %s", line)
	}

	client.Close()
	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleSubNotConnected(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte("SUB orders 1\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.Contains(line, "Not connected") {
		t.Fatalf("expected Not connected error, got: %s", line)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleUnsubNotConnected(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte("UNSUB 1\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.Contains(line, "Not connected") {
		t.Fatalf("expected Not connected error, got: %s", line)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandlePubMissingSubject(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte(`CONNECT {"name":"test"}` + "\r\n"))
	_, _ = client.Write([]byte("PUB\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.Contains(line, "PUB requires subject") {
		t.Fatalf("expected PUB requires subject error, got: %s", line)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleSubMissingArgs(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte(`CONNECT {"name":"test"}` + "\r\n"))
	_, _ = client.Write([]byte("SUB\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.Contains(line, "SUB requires subject and sid") {
		t.Fatalf("expected SUB requires error, got: %s", line)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleUnsubMissingSID(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte(`CONNECT {"name":"test"}` + "\r\n"))
	_, _ = client.Write([]byte("UNSUB\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.Contains(line, "UNSUB requires sid") {
		t.Fatalf("expected UNSUB requires error, got: %s", line)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandlePingPong(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte(`CONNECT {"name":"test"}` + "\r\n"))
	_, _ = client.Write([]byte("PING\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.HasPrefix(line, "PONG") {
		t.Fatalf("expected PONG, got: %s", line)
	}

	client.Close()
	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleUnknownCommand(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte(`CONNECT {"name":"test"}` + "\r\n"))
	_, _ = client.Write([]byte("FOOBAR\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.Contains(line, "Unknown command") {
		t.Fatalf("expected unknown command error, got: %s", line)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleHPUB(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte(`CONNECT {"name":"test"}` + "\r\n"))
	// HPUB subject headers_len total_len
	_, _ = client.Write([]byte("HPUB orders 0 5\r\nhello\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.HasPrefix(line, "+OK") {
		t.Fatalf("expected +OK, got: %s", line)
	}

	client.Close()
	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleSubAndUnsub(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte(`CONNECT {"name":"test"}` + "\r\n"))
	_, _ = client.Write([]byte("SUB orders 1\r\n"))
	time.Sleep(100 * time.Millisecond)
	_, _ = client.Write([]byte("UNSUB 1\r\n"))

	client.Close()
	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandlePubWithReplyTo(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte(`CONNECT {"name":"test"}` + "\r\n"))
	// PUB subject reply-to payload_len
	_, _ = client.Write([]byte("PUB orders reply.me 5\r\nhello\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.HasPrefix(line, "+OK") {
		t.Fatalf("expected +OK, got: %s", line)
	}

	client.Close()
	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestSendInfo(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
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
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read INFO: %v", err)
	}
	if !strings.HasPrefix(line, "INFO ") {
		t.Fatalf("expected INFO, got: %s", line)
	}
	if !strings.Contains(line, `"server_id"`) {
		t.Fatalf("expected server_id in INFO, got: %s", line)
	}

	client.Close()
	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestWriteStringClosedSession(t *testing.T) {
	sess := &Session{
		id:     "test",
		conn:   &mockConn{},
		writer: bufio.NewWriter(&bytes.Buffer{}),
	}
	sess.close()
	if err := sess.writeString("hello"); err == nil {
		t.Error("expected error for closed session")
	}
}

func TestSendOkClosedSession(t *testing.T) {
	sess := &Session{
		id:     "test",
		conn:   &mockConn{},
		writer: bufio.NewWriter(&bytes.Buffer{}),
	}
	sess.close()
	if err := sess.sendOk(); err == nil {
		t.Error("expected error for closed session")
	}
}

func TestSendErrorClosedSession(t *testing.T) {
	sess := &Session{
		id:     "test",
		conn:   &mockConn{},
		writer: bufio.NewWriter(&bytes.Buffer{}),
	}
	sess.close()
	if err := sess.sendError("boom"); err == nil {
		t.Error("expected error for closed session")
	}
}

func TestSendInfoClosedSession(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
	defer cleanup()
	sess := &Session{
		id:     "test",
		b:      b,
		conn:   &mockConn{},
		writer: bufio.NewWriter(&bytes.Buffer{}),
	}
	sess.close()
	if err := sess.sendInfo(); err == nil {
		t.Error("expected error for closed session")
	}
}
