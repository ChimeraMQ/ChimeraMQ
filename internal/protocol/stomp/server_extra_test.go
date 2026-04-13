package stomp

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
)

func TestIsServerCommand(t *testing.T) {
	if !IsServerCommand(CmdConnected) {
		t.Error("CmdConnected should be a server command")
	}
	if !IsServerCommand(CmdMessage) {
		t.Error("CmdMessage should be a server command")
	}
	if !IsServerCommand(CmdReceipt) {
		t.Error("CmdReceipt should be a server command")
	}
	if !IsServerCommand(CmdError) {
		t.Error("CmdError should be a server command")
	}
	if IsServerCommand(CmdSend) {
		t.Error("CmdSend should not be a server command")
	}
}

func TestGenerateSubscriptionID(t *testing.T) {
	id1 := generateSubscriptionID()
	if !strings.HasPrefix(id1, "sub-") {
		t.Errorf("expected prefix 'sub-', got %s", id1)
	}
}

func TestHandleBeginCommitAbort(t *testing.T) {
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

	// BEGIN with receipt
	_, _ = client.Write([]byte("BEGIN\nreceipt:r1\n\n\x00"))
	frame := readFrame(t, reader)
	if frame.Command != CmdReceipt {
		t.Fatalf("expected RECEIPT for BEGIN, got %s", frame.Command)
	}

	// COMMIT with receipt
	_, _ = client.Write([]byte("COMMIT\nreceipt:r2\n\n\x00"))
	frame = readFrame(t, reader)
	if frame.Command != CmdReceipt {
		t.Fatalf("expected RECEIPT for COMMIT, got %s", frame.Command)
	}

	// ABORT with receipt
	_, _ = client.Write([]byte("ABORT\nreceipt:r3\n\n\x00"))
	frame = readFrame(t, reader)
	if frame.Command != CmdReceipt {
		t.Fatalf("expected RECEIPT for ABORT, got %s", frame.Command)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleDisconnect(t *testing.T) {
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

	// DISCONNECT with receipt
	_, _ = client.Write([]byte("DISCONNECT\nreceipt:r1\n\n\x00"))
	frame := readFrame(t, reader)
	if frame.Command != CmdReceipt {
		t.Fatalf("expected RECEIPT for DISCONNECT, got %s", frame.Command)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleSendNoDestination(t *testing.T) {
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

	// SEND without destination
	_, _ = client.Write([]byte("SEND\n\nhello\x00"))
	frame := readFrame(t, reader)
	if frame.Command != CmdError {
		t.Fatalf("expected ERROR, got %s", frame.Command)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleSubscribeNoDestination(t *testing.T) {
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

	// SUBSCRIBE without destination
	_, _ = client.Write([]byte("SUBSCRIBE\nid:sub-1\n\n\x00"))
	frame := readFrame(t, reader)
	if frame.Command != CmdError {
		t.Fatalf("expected ERROR, got %s", frame.Command)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleUnsubscribeNoID(t *testing.T) {
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

	// UNSUBSCRIBE without id
	_, _ = client.Write([]byte("UNSUBSCRIBE\n\n\x00"))
	frame := readFrame(t, reader)
	if frame.Command != CmdError {
		t.Fatalf("expected ERROR, got %s", frame.Command)
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestProcessFrameUnknownCommand(t *testing.T) {
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

	// Unknown command
	_, _ = client.Write([]byte("FOOBAR\n\n\x00"))
	frame := readFrame(t, reader)
	if frame.Command != CmdError {
		t.Fatalf("expected ERROR, got %s", frame.Command)
	}
	if !strings.Contains(frame.Get("message"), "Invalid command") {
		t.Errorf("expected 'Invalid command' message, got: %s", frame.Get("message"))
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleSendWithReceipt(t *testing.T) {
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

	// SEND with receipt
	_, _ = client.Write([]byte("SEND\ndestination:/topic/orders\nreceipt:r1\n\nhello\x00"))
	frame := readFrame(t, reader)
	if frame.Command != CmdReceipt {
		t.Fatalf("expected RECEIPT, got %s", frame.Command)
	}
	if frame.Get("receipt-id") != "r1" {
		t.Errorf("receipt-id = %s", frame.Get("receipt-id"))
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleSubscribeWithReceipt(t *testing.T) {
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

	// SUBSCRIBE with receipt
	_, _ = client.Write([]byte("SUBSCRIBE\ndestination:/topic/orders\nid:sub-1\nack:auto\nreceipt:r1\n\n\x00"))
	frame := readFrame(t, reader)
	if frame.Command != CmdReceipt {
		t.Fatalf("expected RECEIPT, got %s", frame.Command)
	}
	if frame.Get("receipt-id") != "r1" {
		t.Errorf("receipt-id = %s", frame.Get("receipt-id"))
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}

func TestHandleUnsubscribeWithReceipt(t *testing.T) {
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
	time.Sleep(100 * time.Millisecond)

	// UNSUBSCRIBE with receipt
	_, _ = client.Write([]byte("UNSUBSCRIBE\nid:sub-1\nreceipt:r1\n\n\x00"))
	frame := readFrame(t, reader)
	if frame.Command != CmdReceipt {
		t.Fatalf("expected RECEIPT, got %s", frame.Command)
	}
	if frame.Get("receipt-id") != "r1" {
		t.Errorf("receipt-id = %s", frame.Get("receipt-id"))
	}

	client.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
}
