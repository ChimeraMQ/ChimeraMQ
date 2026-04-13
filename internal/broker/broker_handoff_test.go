package broker

import (
	"net"
	"os"
	"testing"
	"time"
)

type mockStopper struct{}

func (m *mockStopper) Stop() {}

type mockListener struct {
	closed bool
}

func (m *mockListener) Accept() (net.Conn, error) {
	return nil, nil
}

func (m *mockListener) Close() error {
	m.closed = true
	return nil
}

func (m *mockListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234}
}

func TestHandoffManagerLifecycle(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	h := NewHandoffManager(b)
	if h == nil {
		t.Fatal("NewHandoffManager returned nil")
	}

	if err := h.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if _, err := os.Stat(h.handoffSock); err != nil {
		t.Errorf("socket not created: %v", err)
	}

	// Test idempotent start (removes old socket)
	if err := h.Start(); err != nil {
		t.Fatalf("second Start: %v", err)
	}

	h.Stop()
	if _, err := os.Stat(h.handoffSock); !os.IsNotExist(err) {
		t.Error("socket should be removed after Stop")
	}
}

func TestHandoffManagerDrainConnections(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	b.protocolMux = &mockStopper{}
	b.mainListener = &mockListener{}

	h := NewHandoffManager(b)
	if err := h.Start(); err != nil {
		t.Fatal(err)
	}
	defer h.Stop()

	if h.Status() != "ACTIVE" {
		t.Errorf("status = %q, want ACTIVE", h.Status())
	}

	if err := h.DrainConnections(); err != nil {
		t.Fatalf("DrainConnections: %v", err)
	}

	if h.Status() != "DRAINING" {
		t.Errorf("status = %q, want DRAINING", h.Status())
	}

	// Second drain should fail
	if err := h.DrainConnections(); err == nil {
		t.Error("expected error for second drain")
	}
}

func TestHandoffManagerHandleDrainRequest(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()
	b.protocolMux = &mockStopper{}
	b.mainListener = &mockListener{}

	h := NewHandoffManager(b)
	if err := h.Start(); err != nil {
		t.Fatal(err)
	}
	defer h.Stop()

	client, err := net.Dial("unix", h.handoffSock)
	if err != nil {
		t.Fatalf("dial handoff socket: %v", err)
	}
	defer client.Close()

	if _, err := client.Write([]byte("DRAI")); err != nil {
		t.Fatalf("write DRAI: %v", err)
	}

	buf := make([]byte, 4)
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client.Read(buf); err != nil {
		t.Fatalf("read response: %v", err)
	}

	if string(buf) != "OK  " {
		t.Errorf("response = %q, want OK  ", string(buf))
	}
}

func TestHandoffManagerHandleStatusRequest(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	h := NewHandoffManager(b)
	if err := h.Start(); err != nil {
		t.Fatal(err)
	}
	defer h.Stop()

	client, err := net.Dial("unix", h.handoffSock)
	if err != nil {
		t.Fatalf("dial handoff socket: %v", err)
	}
	defer client.Close()

	if _, err := client.Write([]byte("STAT")); err != nil {
		t.Fatalf("write STAT: %v", err)
	}

	buf := make([]byte, 8)
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := client.Read(buf)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if string(buf[:n]) != "ACTIVE" {
		t.Errorf("response = %q, want ACTIVE", string(buf[:n]))
	}
}

func TestHandoffManagerHandleUnknownCommand(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	h := NewHandoffManager(b)
	if err := h.Start(); err != nil {
		t.Fatal(err)
	}
	defer h.Stop()

	client, err := net.Dial("unix", h.handoffSock)
	if err != nil {
		t.Fatalf("dial handoff socket: %v", err)
	}
	defer client.Close()

	if _, err := client.Write([]byte("PING")); err != nil {
		t.Fatalf("write PING: %v", err)
	}

	buf := make([]byte, 4)
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client.Read(buf); err != nil {
		t.Fatalf("read response: %v", err)
	}

	if string(buf) != "UNK " {
		t.Errorf("response = %q, want UNK ", string(buf))
	}
}

func TestHandoffManagerHandleShortRead(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	h := NewHandoffManager(b)
	if err := h.Start(); err != nil {
		t.Fatal(err)
	}
	defer h.Stop()

	client, err := net.Dial("unix", h.handoffSock)
	if err != nil {
		t.Fatalf("dial handoff socket: %v", err)
	}
	defer client.Close()

	// Write less than 4 bytes - connection should close without response
	if _, err := client.Write([]byte("AB")); err != nil {
		t.Fatalf("write: %v", err)
	}

	client.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	buf := make([]byte, 1)
	_, err = client.Read(buf)
	if err == nil {
		t.Error("expected connection to close after short read")
	}
}
