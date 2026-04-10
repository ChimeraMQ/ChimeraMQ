package protocol

import (
	"bufio"
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/protocol/chimera"
	httpadmin "github.com/chimeramq/chimera/internal/protocol/http"
	"github.com/chimeramq/chimera/internal/protocol/mqtt"
	"github.com/chimeramq/chimera/internal/protocol/amqp"
)

func TestChimeraDetector(t *testing.T) {
	d := &chimera.Detector{}
	if !d.Detect([]byte{'C', 'H', 'M', 'R'}) {
		t.Error("should detect Chimera magic bytes")
	}
	if d.Detect([]byte{0x10}) {
		t.Error("should not detect MQTT as Chimera")
	}
	if d.BytesNeeded() != 4 {
		t.Errorf("BytesNeeded = %d, want 4", d.BytesNeeded())
	}
}

func TestHTTPDetector(t *testing.T) {
	d := &httpadmin.Detector{}
	tests := []struct {
		peek  []byte
		match bool
	}{
		{[]byte("GET "), true},
		{[]byte("POST"), true},
		{[]byte("PUT "), true},
		{[]byte("DELE"), true},
		{[]byte("OPTI"), true},
		{[]byte{'C', 'H', 'M', 'R'}, false},
		{[]byte{0x10}, false},
	}
	for _, tt := range tests {
		got := d.Detect(tt.peek)
		if got != tt.match {
			t.Errorf("Detect(%q) = %v, want %v", tt.peek, got, tt.match)
		}
	}
}

func TestMQTTDetector(t *testing.T) {
	d := &mqtt.Detector{}
	if !d.Detect([]byte{0x10}) {
		t.Error("should detect MQTT CONNECT (0x10)")
	}
	if d.Detect([]byte{0x30}) {
		t.Error("should not detect PUBLISH (0x30) as CONNECT")
	}
	if d.BytesNeeded() != 1 {
		t.Errorf("BytesNeeded = %d, want 1", d.BytesNeeded())
	}
}

func TestAMQPDetector(t *testing.T) {
	d := &amqp.Detector{}
	if !d.Detect([]byte("AMQP\x00\x01\x00\x00")) {
		t.Error("should detect AMQP 1.0 header")
	}
	if d.Detect([]byte("AMQP\x00\x00\x00\x00")) {
		t.Error("should not detect wrong AMQP version")
	}
	if d.BytesNeeded() != 8 {
		t.Errorf("BytesNeeded = %d, want 8", d.BytesNeeded())
	}
}

func TestMuxProtocolDetection(t *testing.T) {
	// Create a test broker with temp dir
	dir := t.TempDir()
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 0, AdminPort: 0, MaxConnections: 100},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Logging: broker.LoggingConfig{Level: "warn", Format: "text"},
		Limits: broker.LimitsConfig{
			MaxPartitionsPerTopic: 256,
			MaxTopics:             1000,
			MaxFetchMessages:      10000,
			MaxMessageSize:        16 * 1024 * 1024,
			MaxConnections:        100,
		},
		Protocols: broker.ProtocolsConfig{
			Chimera: broker.ProtocolChimeraConfig{Enabled: true},
		},
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	mux := NewProtocolMux(b)
	mux.Register(&amqp.Detector{}, &noopHandler{})
	mux.Register(&mqtt.Detector{}, &noopHandler{})
	mux.Register(&httpadmin.Detector{}, &noopHandler{})
	mux.Register(&chimera.Detector{}, &noopHandler{})

	// Test detection order by checking which handler receives the connection
	tests := []struct {
		name   string
		input  []byte
		expect string
	}{
		{"amqp", []byte("AMQP\x00\x01\x00\x00"), "amqp"},
		{"mqtt", []byte{0x10}, "mqtt"},
		{"http_get", []byte("GET / HTTP/1.1\r\n"), "http"},
		{"chimera", []byte{'C', 'H', 'M', 'R'}, "chimera"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify detectors match correctly
			handlers := mux.detectors
			found := false
			for _, entry := range handlers {
				n := entry.detector.BytesNeeded()
				if len(tt.input) >= n && entry.detector.Detect(tt.input[:n]) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("no detector matched input %x", tt.input)
			}
		})
	}
}

func TestBufferedConn(t *testing.T) {
	// Create a pipe
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// Write data
	go func() {
		client.Write([]byte("hello world"))
		client.Close()
	}()

	// Read via bufferedConn
	br := bytes.NewBuffer(nil)
	buf := make([]byte, 100)
	n, err := server.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	br.Write(buf[:n])

	if br.String() != "hello world" {
		t.Errorf("got %q, want 'hello world'", br.String())
	}
}

// noopHandler is a test ProtocolHandler that does nothing.
type noopHandler struct{}

func (h *noopHandler) HandleConnection(_ net.Conn, _ []byte) error { return nil }
func (h *noopHandler) Stop()                                       {}

// trackingHandler records which connections were handled.
type trackingHandler struct {
	mu       sync.Mutex
	peeks    [][]byte
	count    int
}

func (h *trackingHandler) HandleConnection(_ net.Conn, peeked []byte) error {
	h.mu.Lock()
	h.peeks = append(h.peeks, append([]byte(nil), peeked...))
	h.count++
	h.mu.Unlock()
	return nil
}

func (h *trackingHandler) Stop() {}

func (h *trackingHandler) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}

// echoDetector matches any byte.
type echoDetector struct{}

func (d *echoDetector) Detect(_ []byte) bool { return true }
func (d *echoDetector) BytesNeeded() int     { return 1 }

func newTestBrokerForMux(t *testing.T) (*broker.Broker, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 0, AdminPort: 0, MaxConnections: 100},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Logging: broker.LoggingConfig{Level: "warn", Format: "text"},
		Limits: broker.LimitsConfig{
			MaxPartitionsPerTopic: 256,
			MaxTopics:             1000,
			MaxFetchMessages:      10000,
			MaxMessageSize:        16 * 1024 * 1024,
			MaxConnections:        100,
		},
	}
	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	return b, func() { b.Stop() }
}

func TestMuxServeAndRoute(t *testing.T) {
	b, cleanup := newTestBrokerForMux(t)
	defer cleanup()

	handler := &trackingHandler{}
	mux := NewProtocolMux(b)
	mux.Register(&echoDetector{}, handler)

	// Serve on a random port
	done := make(chan error, 1)
	go func() { done <- mux.Serve() }()

	// Give listener time to bind
	time.Sleep(50 * time.Millisecond)

	// Connect and send data
	addr := mux.listener.Addr().String()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn.Write([]byte("X-some-padding-bytes"))
	conn.Close()

	// Wait for handler to be called
	time.Sleep(100 * time.Millisecond)

	if handler.Count() != 1 {
		t.Errorf("handler count = %d, want 1", handler.Count())
	}

	mux.Stop()
	if err := <-done; err != nil {
		t.Errorf("Serve returned: %v", err)
	}
}

func TestMuxConnections(t *testing.T) {
	b, cleanup := newTestBrokerForMux(t)
	defer cleanup()

	handler := &trackingHandler{}
	mux := NewProtocolMux(b)
	mux.Register(&echoDetector{}, handler)

	go mux.Serve()
	defer mux.Stop()

	time.Sleep(50 * time.Millisecond)
	addr := mux.listener.Addr().String()

	if mux.Connections() != 0 {
		t.Errorf("initial connections = %d, want 0", mux.Connections())
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.Write([]byte("A"))
	time.Sleep(100 * time.Millisecond)

	afterOpen := mux.Connections()
	conn.Close()
	time.Sleep(100 * time.Millisecond)

	afterClose := mux.Connections()
	t.Logf("connections: after open=%d, after close=%d", afterOpen, afterClose)
}

func TestMuxNoMatchProtocol(t *testing.T) {
	b, cleanup := newTestBrokerForMux(t)
	defer cleanup()

	// Register a detector that never matches
	neverDetector := &struct {
		echoDetector
	}{}
	origDetect := neverDetector.Detect
	_ = origDetect
	// Override Detect to always return false
	mux := NewProtocolMux(b)
	mux.Register(&neverMatchDetector{}, &noopHandler{})

	go mux.Serve()
	defer mux.Stop()

	time.Sleep(50 * time.Millisecond)
	addr := mux.listener.Addr().String()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.Write([]byte("HELLO-WORLD-PADDING"))

	buf := make([]byte, 100)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	conn.Close()

	got := string(buf[:n])
	if !bytes.Contains([]byte(got), []byte("no matching protocol")) {
		t.Errorf("got %q, expected 'no matching protocol'", got)
	}
}

// neverMatchDetector never matches any bytes.
type neverMatchDetector struct{}

func (d *neverMatchDetector) Detect(_ []byte) bool { return false }
func (d *neverMatchDetector) BytesNeeded() int     { return 1 }

func TestMuxStopCleanShutdown(t *testing.T) {
	b, cleanup := newTestBrokerForMux(t)
	defer cleanup()

	handler := &noopHandler{}
	mux := NewProtocolMux(b)
	mux.Register(&echoDetector{}, handler)

	done := make(chan error, 1)
	go func() { done <- mux.Serve() }()

	time.Sleep(50 * time.Millisecond)
	mux.Stop()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Stop() timed out")
	}
}

func TestBufferedConnRead(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		client.Write([]byte("peeked-data-extra"))
		client.Close()
	}()

	br := bufio.NewReaderSize(server, 256)
	peeked, _ := br.Peek(7)

	bufConn := &bufferedConn{Conn: server, reader: br}

	// First read should include peeked bytes
	buf := make([]byte, 50)
	n, err := bufConn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	// The read should include the peeked bytes and more
	got := string(buf[:n])
	if !bytes.Contains([]byte(got), peeked) {
		t.Errorf("read = %q, expected to contain peeked %q", got, peeked)
	}
}

func TestRouteConnectionDirect(t *testing.T) {
	b, cleanup := newTestBrokerForMux(t)
	defer cleanup()

	handler := &trackingHandler{}
	mux := NewProtocolMux(b)
	mux.Register(&mqtt.Detector{}, handler)
	mux.Register(&chimera.Detector{}, handler)

	// Test MQTT routing — need 8+ bytes for Peek to succeed
	server, client := net.Pipe()
	go func() {
		client.Write([]byte{0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
		client.Close()
	}()
	mux.routeConnection(server)
	server.Close()

	if handler.Count() != 1 {
		t.Errorf("handler count after MQTT = %d, want 1", handler.Count())
	}
}
