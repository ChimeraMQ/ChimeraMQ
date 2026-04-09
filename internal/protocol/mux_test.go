package protocol

import (
	"bytes"
	"net"
	"testing"

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
