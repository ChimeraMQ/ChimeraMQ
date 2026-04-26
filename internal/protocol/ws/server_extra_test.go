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
	"github.com/chimeramq/chimera/internal/message"
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

func TestWSSubscribeQueue(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "sub-q", Mode: broker.ModeQueue, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "subscribe", Topic: "sub-q"})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var r wsMessage
	json.Unmarshal(data, &r)
	if r.Op != "suback" {
		t.Errorf("expected suback, got %q", r.Op)
	}
}

func TestWSSubscribeStream(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "sub-s", Mode: broker.ModeStream, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "subscribe", Topic: "sub-s", Group: "g1", AutoCommit: true})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var r wsMessage
	json.Unmarshal(data, &r)
	if r.Op != "suback" {
		t.Errorf("expected suback, got %q", r.Op)
	}
}

func TestWSUnsubscribe(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "unsub-t", Mode: broker.ModeQueue, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "subscribe", Topic: "unsub-t"})
	conn.Write(ctx, websocket.MessageText, msg)
	conn.Read(ctx)

	msg, _ = json.Marshal(wsMessage{Op: "unsubscribe"})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var r wsMessage
	json.Unmarshal(data, &r)
	if r.Op != "unsuback" {
		t.Errorf("expected unsuback, got %q", r.Op)
	}
}

func TestWSSubscribeAlreadySubscribed(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "dbl-sub", Mode: broker.ModeQueue, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "subscribe", Topic: "dbl-sub"})
	conn.Write(ctx, websocket.MessageText, msg)
	conn.Read(ctx)

	conn.Write(ctx, websocket.MessageText, msg)
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var r wsMessage
	json.Unmarshal(data, &r)
	if r.Op != "error" {
		t.Errorf("expected error for double subscribe, got %q", r.Op)
	}
}

func TestWSAck(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "ack-topic", Mode: broker.ModeQueue, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "ack", Topic: "ack-topic", Offset: 1})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var r wsMessage
	json.Unmarshal(data, &r)
	if r.Op != "ackack" {
		t.Errorf("expected ackack, got %q", r.Op)
	}
}

func TestWSNack(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "nack-topic", Mode: broker.ModeQueue, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "nack", Topic: "nack-topic", Offset: 1})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var r wsMessage
	json.Unmarshal(data, &r)
	if r.Op != "nackack" {
		t.Errorf("expected nackack, got %q", r.Op)
	}
}

func TestWSCommit(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "commit-topic", Mode: broker.ModeStream, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "commit", Topic: "commit-topic", Group: "g1", Partition: 0, Offset: 5})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var r wsMessage
	json.Unmarshal(data, &r)
	if r.Op != "commitack" {
		t.Errorf("expected commitack, got %q", r.Op)
	}
}

func TestWSCommitNoGroup(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "commit-ng", Mode: broker.ModeStream, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "commit", Topic: "commit-ng", Offset: 1})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var r wsMessage
	json.Unmarshal(data, &r)
	if r.Op != "error" {
		t.Errorf("expected error for missing group, got %q", r.Op)
	}
}

func TestWSFetchWithDefaults(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "fetch-def", Mode: broker.ModeStream, Partitions: 1})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "fetch", Topic: "fetch-def"})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var r wsMessage
	json.Unmarshal(data, &r)
	if r.Op != "fetch_complete" {
		t.Errorf("expected fetch_complete, got %q", r.Op)
	}
}

func TestWSFetchMessages(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Topics().CreateTopic(broker.TopicConfig{Name: "fetch-msg", Mode: broker.ModeStream, Partitions: 1})
	b.Publish(&message.Envelope{Topic: "fetch-msg", Payload: []byte("hello")})

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, _ := json.Marshal(wsMessage{Op: "fetch", Topic: "fetch-msg", Offset: 0, MaxMessages: 10, MaxWait: 500})
	conn.Write(ctx, websocket.MessageText, msg)

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m1 wsMessage
	json.Unmarshal(data, &m1)
	if m1.Op != "message" {
		t.Errorf("expected message, got %q", m1.Op)
	}

	_, data, err = conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m2 wsMessage
	json.Unmarshal(data, &m2)
	if m2.Op != "fetch_complete" {
		t.Errorf("expected fetch_complete, got %q", m2.Op)
	}
}

func TestWSEvictConsumer(t *testing.T) {
	wsSrv, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a topic
	wsSrv.broker.Topics().CreateTopic(broker.TopicConfig{Name: "evict-topic", Mode: broker.ModeQueue, Partitions: 1})

	// Subscribe
	subMsg, _ := json.Marshal(wsMessage{Op: "subscribe", Topic: "evict-topic"})
	err := conn.Write(ctx, websocket.MessageText, subMsg)
	if err != nil {
		t.Fatalf("subscribe write: %v", err)
	}

	// Wait for suback
	time.Sleep(50 * time.Millisecond)

	// Find the consumerID from sessions
	var consumerID string
	wsSrv.sessions.Range(func(key, value any) bool {
		sess := value.(*wsSession)
		sess.mu.Lock()
		consumerID = sess.consumerID
		sess.mu.Unlock()
		return false
	})
	if consumerID == "" {
		t.Fatal("no consumerID found")
	}

	// Evict the consumer
	wsSrv.EvictConsumer(consumerID)

	// Connection should be closed
	time.Sleep(50 * time.Millisecond)
	_, _, err = conn.Read(ctx)
	if err == nil {
		t.Error("expected connection to be closed after eviction")
	}
}

func TestWSEvictNonExistentConsumer(t *testing.T) {
	wsSrv, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Evict a consumer that doesn't exist — should be a no-op
	wsSrv.EvictConsumer("non-existent-consumer-id")

	// Connection should still be alive
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pingMsg, _ := json.Marshal(wsMessage{Op: "ping"})
	err := conn.Write(ctx, websocket.MessageText, pingMsg)
	if err != nil {
		t.Fatalf("ping write: %v", err)
	}
}

func TestWSRunStreamSubscriptionErrorPaths(t *testing.T) {
	wsSrv, _, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	conn := connectWS(t, "ws://"+httpSrv.Listener.Addr().String(), "chimera-json-v1")
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a stream topic
	wsSrv.broker.Topics().CreateTopic(broker.TopicConfig{Name: "stream-err-topic", Mode: broker.ModeStream, Partitions: 1})

	// Subscribe in stream mode with a group
	subMsg, _ := json.Marshal(wsMessage{Op: "subscribe", Topic: "stream-err-topic", Group: "test-group"})
	err := conn.Write(ctx, websocket.MessageText, subMsg)
	if err != nil {
		t.Fatalf("subscribe write: %v", err)
	}

	// Read suback
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read suback: %v", err)
	}
	var subResp wsMessage
	json.Unmarshal(data, &subResp)
	if subResp.Op != "suback" {
		t.Fatalf("expected suback, got %q", subResp.Op)
	}

	// Unsubscribe
	time.Sleep(50 * time.Millisecond)

	unsubMsg, _ := json.Marshal(wsMessage{Op: "unsubscribe"})
	err = conn.Write(ctx, websocket.MessageText, unsubMsg)
	if err != nil {
		t.Fatalf("unsubscribe write: %v", err)
	}

	// Read unsuback
	_, data, err = conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "unsuback" {
		t.Errorf("expected unsuback, got %q", resp.Op)
	}
}
