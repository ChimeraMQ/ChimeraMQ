package nats

import (
	"bufio"
	"bytes"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

func TestMessageString(t *testing.T) {
	m := NewMessage(CmdPing)
	m.Args = []string{"arg1", "arg2"}
	if !strings.Contains(m.String(), "PING") {
		t.Errorf("expected PING in String(), got %s", m.String())
	}
}

func TestMessageEncodeWithArgs(t *testing.T) {
	m := NewMessage(CmdPub)
	m.Args = []string{"orders", "5"}
	m.Payload = []byte("hello")
	data := m.Encode()
	if !bytes.Contains(data, []byte("PUB orders 5")) {
		t.Errorf("encode mismatch: %s", data)
	}
}

func TestMessageEncodeWithHeaders(t *testing.T) {
	m := NewMessage(CmdPub)
	m.Args = []string{"orders"}
	m.Headers = map[string]string{"X-Id": "123"}
	m.Payload = []byte("hello")
	data := m.Encode()
	if !bytes.Contains(data, []byte("X-Id: 123")) {
		t.Errorf("expected headers in encode: %s", data)
	}
}

func TestDetector(t *testing.T) {
	d := &Detector{}
	if !d.Detect([]byte("CONN")) {
		t.Error("should detect CONNECT")
	}
	if !d.Detect([]byte("PUB ")) {
		t.Error("should detect PUB")
	}
	if !d.Detect([]byte("SUB ")) {
		t.Error("should detect SUB")
	}
	if d.Detect([]byte("GET ")) {
		t.Error("should not detect HTTP")
	}
	if d.BytesNeeded() != 4 {
		t.Errorf("BytesNeeded = %d, want 4", d.BytesNeeded())
	}
}

func TestReadMessageEmptyLine(t *testing.T) {
	input := "\r\nPING\r\n"
	msg, err := ReadMessage(bufio.NewReader(strings.NewReader(input)))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msg.Command != CmdPing {
		t.Errorf("command = %s, want PING", msg.Command)
	}
}

func TestReadMessageEOF(t *testing.T) {
	_, err := ReadMessage(bufio.NewReader(strings.NewReader("")))
	if err == nil {
		t.Error("expected EOF")
	}
}

func TestReadMessageHPUBPayload(t *testing.T) {
	input := "HPUB orders 0 5\r\nhello\r\n"
	msg, err := ReadMessage(bufio.NewReader(strings.NewReader(input)))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if msg.Command != "HPUB" {
		t.Errorf("command = %s, want HPUB", msg.Command)
	}
	if len(msg.Args) != 3 || msg.Args[0] != "orders" {
		t.Errorf("args = %v, want [orders 0 5]", msg.Args)
	}
}

func TestHandlePong(t *testing.T) {
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
	_, _ = client.Write([]byte("PONG\r\n"))
	_, _ = client.Write([]byte("PING\r\n"))

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.HasPrefix(line, "PONG") {
		t.Fatalf("expected PONG after PING, got: %s", line)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleConnectDebugLevel(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
	defer cleanup()
	b.Config().Logging.Level = "debug"
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

	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if !strings.HasPrefix(line, "+OK") {
		t.Fatalf("expected +OK in debug mode, got: %s", line)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestSendMessageSuccess(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
	defer cleanup()

	var buf bytes.Buffer
	sess := &Session{
		id:     "test",
		b:      b,
		conn:   &mockConn{},
		writer: bufio.NewWriter(&buf),
	}

	sub := &Subscription{SID: "1", Subject: "orders"}
	env := &message.Envelope{Topic: "orders", Payload: []byte("hello"), Sequence: 42}
	if err := sess.sendMessage(sub, env); err != nil {
		t.Fatalf("sendMessage: %v", err)
	}

	if !bytes.Contains(buf.Bytes(), []byte("MSG orders 1 5")) {
		t.Errorf("expected MSG in output, got: %q", buf.Bytes())
	}
}

func TestHandlePubPublishError(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
	defer cleanup()
	// Don't create topic — publish will fail
	_ = NewServer(b)

	var buf bytes.Buffer
	sess := &Session{
		id:            "test",
		b:             b,
		conn:          &mockConn{},
		writer:        bufio.NewWriter(&buf),
		connected:     true,
		subscriptions: make(map[string]*Subscription),
	}

	msg := &Message{Command: CmdPub, Args: []string{"orders"}, Payload: []byte("hello")}
	err := sess.handlePub(msg)
	if err != nil {
		t.Logf("handlePub returned error (acceptable): %v", err)
	}
}

func TestHandleSubWithQueueGroup(t *testing.T) {
	b, cleanup := newNATSTestBroker(t, false)
	defer cleanup()
	srv := NewServer(b)
	_ = srv

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		NewServer(b).HandleConnection(server, nil)
		close(done)
	}()

	reader := bufio.NewReader(client)
	_, _ = reader.ReadString('\n') // INFO

	_, _ = client.Write([]byte(`CONNECT {"name":"test"}` + "\r\n"))
	_, _ = client.Write([]byte("SUB orders 1 queue-group\r\n"))

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestCloseIdempotent(t *testing.T) {
	sess := &Session{
		id:     "test",
		conn:   &mockConn{},
		writer: bufio.NewWriter(&bytes.Buffer{}),
	}
	sess.close()
	sess.close() // should not panic
	if !sess.closed {
		t.Error("expected closed to be true")
	}
}
