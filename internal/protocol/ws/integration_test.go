package ws

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"nhooyr.io/websocket"
)

func newWSTestBroker(t *testing.T) *broker.Broker {
	t.Helper()
	dir, err := os.MkdirTemp("", "ws-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "ws-test", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 0, AdminPort: 0},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "error", Format: "text", Output: "stdout"},
	}

	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bkr.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bkr.Stop() })
	return bkr
}

func wsDial(t *testing.T, server *httptest.Server) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	return conn
}

func wsRead(t *testing.T, conn *websocket.Conn) wsMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	return resp
}

func wsWrite(t *testing.T, conn *websocket.Conn, msg string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestWSServeHTTPUpgrade(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")
}

func TestWSPingPong(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `{"op":"ping"}`)
	resp := wsRead(t, conn)

	if resp.Op != "pong" {
		t.Errorf("op = %q, want %q", resp.Op, "pong")
	}
	if resp.Status != "ok" {
		t.Errorf("status = %q, want %q", resp.Status, "ok")
	}
}

func TestWSPublish(t *testing.T) {
	bkr := newWSTestBroker(t)
	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name: "ws-pub", Mode: broker.ModeUnified, Partitions: 1,
	})

	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `{"op":"publish","topic":"ws-pub","payload":"aGVsbG8="}`)
	resp := wsRead(t, conn)

	if resp.Op != "puback" {
		t.Errorf("op = %q, want %q", resp.Op, "puback")
	}
	if resp.Status != "ok" {
		t.Errorf("status = %q", resp.Status)
	}
}

func TestWSPublishNoTopic(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `{"op":"publish","payload":"aGVsbG8="}`)
	resp := wsRead(t, conn)

	if resp.Op != "error" {
		t.Errorf("op = %q, want error", resp.Op)
	}
	if resp.Error != "topic is required" {
		t.Errorf("error = %q, want %q", resp.Error, "topic is required")
	}
}

func TestWSCreateTopic(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `{"op":"create_topic","topic":"ws-new","partitions":4}`)
	resp := wsRead(t, conn)

	if resp.Op != "create_topic_ack" {
		t.Errorf("op = %q", resp.Op)
	}
	if resp.Status != "ok" {
		t.Errorf("status = %q", resp.Status)
	}

	tc, ok := bkr.Topics().GetTopic("ws-new")
	if !ok {
		t.Fatal("topic should exist")
	}
	if tc.Partitions != 4 {
		t.Errorf("partitions = %d, want 4", tc.Partitions)
	}
}

func TestWSDeleteTopic(t *testing.T) {
	bkr := newWSTestBroker(t)
	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name: "ws-del", Mode: broker.ModeUnified, Partitions: 1,
	})

	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `{"op":"delete_topic","topic":"ws-del"}`)
	resp := wsRead(t, conn)

	if resp.Op != "delete_topic_ack" {
		t.Errorf("op = %q", resp.Op)
	}
	if _, ok := bkr.Topics().GetTopic("ws-del"); ok {
		t.Error("topic should be deleted")
	}
}

func TestWSUnknownOp(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `{"op":"nonexistent"}`)
	resp := wsRead(t, conn)

	if resp.Op != "error" {
		t.Errorf("op = %q, want error", resp.Op)
	}
	if !strings.Contains(resp.Error, "unknown op") {
		t.Errorf("error = %q, should contain 'unknown op'", resp.Error)
	}
}

func TestWSInvalidJSON(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `not json`)
	resp := wsRead(t, conn)

	if resp.Op != "error" {
		t.Errorf("op = %q, want error", resp.Op)
	}
}

func TestWSSubscribeUnsupported(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `{"op":"subscribe","topic":"test"}`)
	resp := wsRead(t, conn)

	if resp.Op != "error" {
		t.Errorf("op = %q, want error", resp.Op)
	}
	if !strings.Contains(resp.Error, "not yet supported") {
		t.Errorf("error = %q", resp.Error)
	}
}

func TestWSStop(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	srv.Stop()
}

func TestWSPublishWithRoutingKey(t *testing.T) {
	bkr := newWSTestBroker(t)
	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name: "rk-topic", Mode: broker.ModeUnified, Partitions: 1,
	})

	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `{"op":"publish","topic":"rk-topic","payload":"aGVsbG8=","routing_key":"order-1"}`)
	resp := wsRead(t, conn)

	if resp.Op != "puback" {
		t.Errorf("op = %q", resp.Op)
	}
	if resp.Status != "ok" {
		t.Errorf("status = %q", resp.Status)
	}
}

func TestWSCreateTopicWithMode(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Create stream topic
	wsWrite(t, conn, `{"op":"create_topic","topic":"stream-t","partitions":2,"mode":"stream"}`)
	resp := wsRead(t, conn)
	if resp.Op != "create_topic_ack" {
		t.Errorf("op = %q", resp.Op)
	}

	tc, ok := bkr.Topics().GetTopic("stream-t")
	if !ok {
		t.Fatal("topic should exist")
	}
	if tc.Mode != broker.ModeStream {
		t.Errorf("mode = %v, want stream", tc.Mode)
	}

	// Create queue topic
	wsWrite(t, conn, `{"op":"create_topic","topic":"queue-t","partitions":1,"mode":"queue"}`)
	resp = wsRead(t, conn)
	if resp.Op != "create_topic_ack" {
		t.Errorf("op = %q", resp.Op)
	}

	// Create unified (default) topic
	wsWrite(t, conn, `{"op":"create_topic","topic":"uni-t","partitions":4}`)
	resp = wsRead(t, conn)
	if resp.Op != "create_topic_ack" {
		t.Errorf("op = %q", resp.Op)
	}
}

func TestWSDeleteTopicNoName(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `{"op":"delete_topic"}`)
	resp := wsRead(t, conn)

	if resp.Op != "error" {
		t.Errorf("op = %q, want error", resp.Op)
	}
}

func TestWSFetchUnsupported(t *testing.T) {
	bkr := newWSTestBroker(t)
	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	conn := wsDial(t, server)
	defer conn.Close(websocket.StatusNormalClosure, "")

	wsWrite(t, conn, `{"op":"fetch","topic":"test"}`)
	resp := wsRead(t, conn)

	if resp.Op != "error" {
		t.Errorf("op = %q, want error", resp.Op)
	}
}

func TestWSBinaryJSONFallback(t *testing.T) {
	bkr := newWSTestBroker(t)
	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name: "bin-json", Mode: broker.ModeUnified, Partitions: 1,
	})

	srv := NewServer(bkr)
	server := httptest.NewServer(srv)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"chimera-binary-v1"},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Send JSON message via binary subprotocol — should fall back to JSON handler
	conn.Write(ctx, websocket.MessageText, []byte(`{"op":"ping"}`))
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var resp wsMessage
	json.Unmarshal(data, &resp)
	if resp.Op != "pong" {
		t.Errorf("op = %q, want pong", resp.Op)
	}
}
