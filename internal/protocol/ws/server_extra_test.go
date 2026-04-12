package ws

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/coder/websocket"
)

func setupWSTestServer(t *testing.T) (*Server, *broker.Broker, *httptest.Server, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "chimera-ws-test-*")
	if err != nil {
		t.Fatal(err)
	}

	cfg := &broker.Config{
		Node: broker.NodeConfig{
			ID:      1,
			Name:    "test-node",
			DataDir: dir,
		},
		Listener: broker.ListenerConfig{
			Bind:           "127.0.0.1",
			Port:           0,
			AdminPort:      0,
			MaxConnections: 100,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{Partitions: 4, RetentionTime: "1h", Mode: "unified"},
		},
		Logging: broker.LoggingConfig{Level: "warn", Format: "text"},
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("NewBroker: %v", err)
	}
	if err := b.Start(); err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Start: %v", err)
	}

	wsSrv := NewServer(b)
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsSrv.ServeHTTP(w, r)
	}))

	cleanup := func() {
		httpSrv.Close()
		wsSrv.Stop()
		b.Stop()
		os.RemoveAll(dir)
	}

	return wsSrv, b, httpSrv, cleanup
}

func connectWS(t *testing.T, url string, subproto string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := &websocket.DialOptions{Subprotocols: []string{subproto}}
	conn, _, err := websocket.Dial(ctx, url, opts)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	return conn
}

func TestWSServeHTTPJSON(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send ping
	pingMsg, _ := json.Marshal(wsMessage{Op: "ping"})
	err := conn.Write(ctx, websocket.MessageText, pingMsg)
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	// Read response
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "pong" {
		t.Errorf("expected pong, got %q", resp.Op)
	}
}

func TestWSServeHTTPPublish(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	// Create topic first
	b.Topics().CreateTopic(broker.TopicConfig{Name: "ws-test", Mode: broker.ModeStream, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish
	pubMsg, _ := json.Marshal(wsMessage{Op: "publish", Topic: "ws-test", Payload: "aGVsbG8="})
	conn.Write(ctx, websocket.MessageText, pubMsg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "puback" {
		t.Errorf("expected puback, got %q", resp.Op)
	}
}

func TestWSServeHTTPCreateTopic(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createMsg, _ := json.Marshal(wsMessage{Op: "create_topic", Topic: "new-topic", Mode: "stream", Partitions: 4})
	conn.Write(ctx, websocket.MessageText, createMsg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "create_topic_ack" {
		t.Errorf("expected create_topic_ack, got %q", resp.Op)
	}
}

func TestWSServeHTTPDeleteTopic(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "del-topic", Mode: broker.ModeStream, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	delMsg, _ := json.Marshal(wsMessage{Op: "delete_topic", Topic: "del-topic"})
	conn.Write(ctx, websocket.MessageText, delMsg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "delete_topic_ack" {
		t.Errorf("expected delete_topic_ack, got %q", resp.Op)
	}
}

func TestWSServeHTTPInvalidJSON(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn.Write(ctx, websocket.MessageText, []byte("not json"))

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "error" {
		t.Errorf("expected error, got %q", resp.Op)
	}
}

func TestWSServeHTTPUnknownOp(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "unknown"})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "error" {
		t.Errorf("expected error, got %q", resp.Op)
	}
}

func TestWSServeHTTPSubscribeNoTopic(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "subscribe"})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "error" {
		t.Errorf("expected error for missing topic, got %q", resp.Op)
	}
}

func TestWSServeHTTPPublishNoTopic(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "publish"})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "error" {
		t.Errorf("expected error for missing topic, got %q", resp.Op)
	}
}

func TestWSServeHTTPBinaryJSONFallback(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-binary-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Send JSON as binary — should fall through to JSON handler
	msg, _ := json.Marshal(wsMessage{Op: "ping"})
	conn.Write(ctx, websocket.MessageBinary, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "pong" {
		t.Errorf("expected pong, got %q", resp.Op)
	}
}

func TestWSStopWithSessions(t *testing.T) {
	wsSrv, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Stop should close all sessions
	wsSrv.Stop()
}

func TestWSServeHTTPCreateTopicNoName(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "create_topic"})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "error" {
		t.Errorf("expected error for missing name, got %q", resp.Op)
	}
}

func TestWSServeHTTPDeleteTopicNoName(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "delete_topic"})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "error" {
		t.Errorf("expected error for missing name, got %q", resp.Op)
	}
}

func TestWSServeHTTPChimeraBinaryFrame(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "default", Mode: broker.ModeStream, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-binary-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Build a Chimera frame: magic "CHMR" + 1 byte version + 1 byte opcode + 2 bytes reserved + 4 bytes payload length + payload
	payload := []byte("hello")
	frame := make([]byte, 12+len(payload))
	copy(frame[0:4], []byte("CHMR"))
	frame[4] = 1    // version
	frame[5] = 0x03 // opcode = publish
	frame[6] = 0    // reserved
	frame[7] = 0    // reserved
	frame[8] = byte(len(payload) >> 24)
	frame[9] = byte(len(payload) >> 16)
	frame[10] = byte(len(payload) >> 8)
	frame[11] = byte(len(payload))
	copy(frame[12:], payload)

	conn.Write(ctx, websocket.MessageBinary, frame)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "puback" {
		t.Errorf("expected puback, got %q", resp.Op)
	}
}

func TestWSServeHTTPChimeraFrameTruncated(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-binary-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Frame with truncated payload (declares 100 bytes but only provides 1)
	frame := make([]byte, 13)
	copy(frame[0:4], []byte("CHMR"))
	frame[4] = 1
	frame[5] = 0x03
	frame[6] = 0
	frame[7] = 0
	frame[8] = 0
	frame[9] = 0
	frame[10] = 0
	frame[11] = 100 // payload length = 100
	frame[12] = 'x'

	conn.Write(ctx, websocket.MessageBinary, frame)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "error" {
		t.Errorf("expected error for truncated frame, got %q", resp.Op)
	}
}

func TestWSServeHTTPChimeraFrameShort(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-binary-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Frame that starts with CHMR but is < 12 bytes (e.g. 11 bytes)
	frame := []byte("CHMR\x01\x03\x00\x00\x00\x00\x00")
	conn.Write(ctx, websocket.MessageBinary, frame)

	// The handleBinary check is len(data) >= 12, so 11 bytes won't enter handleChimeraFrame
	// It falls through to JSON fallback which will fail
	_, data, err := conn.Read(ctx)
	if err != nil {
		// Connection may close — that's acceptable
		return
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	// Either error response or nothing — just verify no panic
	_ = resp
}

func TestWSServeHTTPChimeraUnsupportedOpcode(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-binary-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	frame := make([]byte, 12)
	copy(frame[0:4], []byte("CHMR"))
	frame[4] = 1
	frame[5] = 0xFF // unsupported opcode
	frame[6] = 0
	frame[7] = 0
	frame[8] = 0
	frame[9] = 0
	frame[10] = 0
	frame[11] = 0

	conn.Write(ctx, websocket.MessageBinary, frame)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "error" {
		t.Errorf("expected error for unsupported opcode, got %q", resp.Op)
	}
}

func TestWSServeHTTPCreateTopicDefaultPartitions(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create topic without specifying partitions — should default to 8
	msg, _ := json.Marshal(wsMessage{Op: "create_topic", Topic: "default-part", Partitions: 0})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "create_topic_ack" {
		t.Errorf("expected create_topic_ack, got %q", resp.Op)
	}
}

func TestWSServeHTTPCreateTopicDuplicate(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "dup-topic", Mode: broker.ModeStream, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "create_topic", Topic: "dup-topic"})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "error" {
		t.Errorf("expected error for duplicate topic, got %q", resp.Op)
	}
}

func TestWSServeHTTPDeleteTopicNonexistent(t *testing.T) {
	_, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "delete_topic", Topic: "no-exist"})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "error" {
		t.Errorf("expected error for nonexistent topic, got %q", resp.Op)
	}
}
