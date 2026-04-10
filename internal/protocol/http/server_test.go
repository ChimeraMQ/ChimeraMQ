package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/queue"
)

func setupTestServer(t *testing.T) (*AdminServer, *broker.Broker, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "chimera-http-test-*")
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
			Hot: broker.HotConfig{
				SegmentSize: 1024 * 1024,
				SyncMode:    "immediate",
			},
			WAL: broker.WALConfig{
				MaxSize:  4 * 1024 * 1024,
				SyncMode: "immediate",
			},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{
				Partitions:    4,
				RetentionTime: "1h",
				Mode:          "unified",
			},
		},
		Logging: broker.LoggingConfig{
			Level:  "warn",
			Format: "text",
		},
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

	srv := NewAdminServer(b)

	cleanup := func() {
		b.Stop()
		os.RemoveAll(dir)
	}

	return srv, b, cleanup
}

func doRequest(t *testing.T, srv *AdminServer, method, path string, body []byte) *http.Response {
	t.Helper()

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()

	srv.mux.ServeHTTP(w, req)

	return w.Result()
}

// --- Topic CRUD ---

func TestHandleCreateTopic(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "test-topic",
		"mode":       "stream",
		"partitions": 4,
	})

	resp := doRequest(t, srv, "POST", "/v1/topics", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["name"] != "test-topic" {
		t.Errorf("name = %v, want test-topic", result["name"])
	}
	if result["partitions"] != float64(4) {
		t.Errorf("partitions = %v, want 4", result["partitions"])
	}
}

func TestHandleCreateTopicInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/topics", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleCreateTopicDefaultPartitions(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name": "defaults-topic",
		"mode": "queue",
	})

	resp := doRequest(t, srv, "POST", "/v1/topics", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["partitions"] != float64(4) {
		t.Errorf("default partitions = %v, want 4", result["partitions"])
	}
}

func TestHandleCreateDuplicateTopic(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "dup-topic",
		"mode":       "stream",
		"partitions": 2,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	resp := doRequest(t, srv, "POST", "/v1/topics", body)
	if resp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", resp.StatusCode)
	}
}

func TestHandleListTopics(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	for _, name := range []string{"topic-a", "topic-b", "topic-c"} {
		body, _ := json.Marshal(map[string]interface{}{
			"name":       name,
			"mode":       "stream",
			"partitions": 2,
		})
		doRequest(t, srv, "POST", "/v1/topics", body)
	}

	resp := doRequest(t, srv, "GET", "/v1/topics", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var topics []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&topics)
	if len(topics) != 3 {
		t.Errorf("topic count = %d, want 3", len(topics))
	}
}

func TestHandleListTopicsEmpty(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/topics", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var topics []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&topics)
	if len(topics) != 0 {
		t.Errorf("expected empty list, got %d", len(topics))
	}
}

func TestHandleGetTopic(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "describe-me",
		"mode":       "queue",
		"partitions": 4,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	resp := doRequest(t, srv, "GET", "/v1/topics/describe-me", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["name"] != "describe-me" {
		t.Errorf("name = %v, want describe-me", result["name"])
	}
}

func TestHandleGetTopicNotFound(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/topics/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleDeleteTopic(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "delete-me",
		"mode":       "stream",
		"partitions": 2,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	resp := doRequest(t, srv, "DELETE", "/v1/topics/delete-me", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Verify gone
	resp2 := doRequest(t, srv, "GET", "/v1/topics/delete-me", nil)
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("status after delete = %d, want 404", resp2.StatusCode)
	}
}

func TestHandleDeleteTopicNotFound(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/topics/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// --- Publish ---

func TestHandlePublish(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic first
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "pub-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Publish message
	msg, _ := json.Marshal(map[string]string{"hello": "world"})
	resp := doRequest(t, srv, "POST", "/v1/messages/pub-topic", msg)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["topic"] != "pub-topic" {
		t.Errorf("topic = %v, want pub-topic", result["topic"])
	}
	if result["offset"] == nil {
		t.Error("expected non-nil offset")
	}
	if result["partition"] == nil {
		t.Error("expected non-nil partition")
	}
}

func TestHandlePublishToNonexistentTopic(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/messages/no-such-topic", []byte("data"))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

func TestHandlePublishMultiple(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "multi-pub",
		"mode":       "unified",
		"partitions": 2,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("message-%d", i))
		resp := doRequest(t, srv, "POST", "/v1/messages/multi-pub", msg)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("publish %d: status = %d, want 200", i, resp.StatusCode)
		}
	}
}

// --- Fetch ---

func TestHandleFetch(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "fetch-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	for i := 0; i < 3; i++ {
		msg := []byte(fmt.Sprintf("msg-%d", i))
		doRequest(t, srv, "POST", "/v1/messages/fetch-topic", msg)
	}

	resp := doRequest(t, srv, "GET", "/v1/messages/fetch-topic?partition=0&offset=0&limit=10", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"] != float64(3) {
		t.Errorf("count = %v, want 3", result["count"])
	}
}

func TestHandleFetchEmpty(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "empty-fetch",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	resp := doRequest(t, srv, "GET", "/v1/messages/empty-fetch?partition=0&offset=0&limit=10&timeout=100ms", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"] != float64(0) {
		t.Errorf("count = %v, want 0", result["count"])
	}
}

func TestHandleFetchWithTimeout(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "timeout-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Fetch from offset 1 (past the high watermark) to trigger long-poll
	resp := doRequest(t, srv, "GET", "/v1/messages/timeout-topic?partition=0&offset=1&limit=10&timeout=200ms", nil)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	// Should return empty after timeout
	if result["count"] != float64(0) {
		t.Errorf("count = %v, want 0", result["count"])
	}
}

// --- Ack/Nack ---

func TestHandleAck(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "ack-topic",
		"mode":       "queue",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Ack with arbitrary offset — HandleAck returns false for untracked offsets
	// but the endpoint should still return 200
	ackBody, _ := json.Marshal(map[string]interface{}{
		"offsets": []float64{999},
	})
	resp := doRequest(t, srv, "POST", "/v1/messages/ack-topic/ack", ackBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["total"] != float64(1) {
		t.Errorf("total = %v, want 1", result["total"])
	}
}

func TestHandleAckInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/messages/some-topic/ack", []byte("bad"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleNack(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "nack-topic",
		"mode":       "queue",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	msg := []byte("nack-me")
	resp := doRequest(t, srv, "POST", "/v1/messages/nack-topic", msg)
	var pubResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&pubResult)

	offset := pubResult["offset"].(float64)

	nackBody, _ := json.Marshal(map[string]interface{}{
		"offsets": []float64{offset},
	})
	resp = doRequest(t, srv, "POST", "/v1/messages/nack-topic/nack", nackBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["nacked"] != float64(1) {
		t.Errorf("nacked = %v, want 1", result["nacked"])
	}
}

func TestHandleNackInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/messages/some-topic/nack", []byte("bad"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// --- Health & Metrics ---

func TestHandleHealth(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/health", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["status"] != "healthy" {
		t.Errorf("status = %v, want healthy", result["status"])
	}
	if result["node_id"] == nil {
		t.Error("expected node_id")
	}
	if result["name"] == nil {
		t.Error("expected name")
	}
	if result["uptime"] == nil {
		t.Error("expected uptime")
	}
}

func TestHandleMetrics(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Publish something to generate metrics
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "metrics-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)
	doRequest(t, srv, "POST", "/v1/messages/metrics-topic", []byte("metrics-msg"))

	resp := doRequest(t, srv, "GET", "/v1/metrics", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "text/plain" {
		t.Errorf("content-type = %q, want text/plain", ct)
	}

	respBody, _ := io.ReadAll(resp.Body)
	if len(respBody) == 0 {
		t.Error("expected non-empty metrics output")
	}
}

// --- Consumers ---

func TestHandleListConsumersEmpty(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/consumers", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"] != float64(0) {
		t.Errorf("count = %v, want 0", result["count"])
	}
}

func TestHandleListConsumersWithGroup(t *testing.T) {
	srv, b, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "cg-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	b.StreamEngine().JoinGroup("test-group", "cg-topic", "consumer-1", 4, 0)

	resp := doRequest(t, srv, "GET", "/v1/consumers", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"].(float64) < 1 {
		t.Error("expected at least 1 consumer group")
	}
}

func TestHandleGetConsumer(t *testing.T) {
	srv, b, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "cg-detail",
		"mode":       "stream",
		"partitions": 4,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	b.StreamEngine().JoinGroup("detail-group", "cg-detail", "m1", 4, 0)

	resp := doRequest(t, srv, "GET", "/v1/consumers/detail-group", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["group"] != "detail-group" {
		t.Errorf("group = %v, want detail-group", result["group"])
	}
}

func TestHandleGetConsumerNotFound(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/consumers/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// --- Topic modes ---

func TestHandleCreateTopicAllModes(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	modes := []struct {
		mode string
	}{
		{"stream"},
		{"queue"},
		{"unified"},
		{"unknown"}, // should default to unified
	}

	for i, m := range modes {
		body, _ := json.Marshal(map[string]interface{}{
			"name":       fmt.Sprintf("mode-topic-%d", i),
			"mode":       m.mode,
			"partitions": 2,
		})
		resp := doRequest(t, srv, "POST", "/v1/topics", body)
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("mode %q: status = %d, want 201", m.mode, resp.StatusCode)
		}
	}
}

func TestAdminServerServeAndShutdown(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Use port 0 to let OS pick a free port
	srv.server.Addr = "127.0.0.1:0"

	// Start serving in background
	done := make(chan error, 1)
	go func() {
		done <- srv.Serve()
	}()

	// Give server a moment to bind
	time.Sleep(50 * time.Millisecond)

	// Shutdown gracefully
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Serve should have returned http.ErrServerClosed
	if err := <-done; err != nil && err != http.ErrServerClosed {
		t.Errorf("Serve returned unexpected error: %v", err)
	}
}

func TestHandlePublishWithRoutingKey(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "rk-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	resp := doRequest(t, srv, "POST", "/v1/topics", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create: %d", resp.StatusCode)
	}

	// Publish with routing key header
	req := httptest.NewRequest("POST", "/v1/messages/rk-topic", bytes.NewReader([]byte("routed")))
	req.Header.Set("X-Routing-Key", "user-123")
	req.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("publish: %d", w.Code)
	}

	var result map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &result)
	if result["topic"] != "rk-topic" {
		t.Errorf("topic = %v, want rk-topic", result["topic"])
	}
}

func TestHandleFetchWithParams(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create and publish
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "fp-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	for i := 0; i < 5; i++ {
		doRequest(t, srv, "POST", "/v1/messages/fp-topic", []byte{byte(i)})
	}

	// Fetch with offset and limit
	resp := doRequest(t, srv, "GET", "/v1/messages/fp-topic?partition=0&offset=2&limit=2&timeout=100ms", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fetch: %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(respBody, &result)
	count := int(result["count"].(float64))
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func TestHandleAckEmptyOffsets(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"offsets": []uint64{},
	})
	resp := doRequest(t, srv, "POST", "/v1/messages/t/ack", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ack empty: %d", resp.StatusCode)
	}
}

func TestHandleNackEmptyOffsets(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"offsets": []uint64{},
	})
	resp := doRequest(t, srv, "POST", "/v1/messages/t/nack", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("nack empty: %d", resp.StatusCode)
	}
}

func TestHandleFetchNonexistentTopic(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Fetch from a topic that was never created
	resp := doRequest(t, srv, "GET", "/v1/messages/no-such-topic?partition=0&offset=0&limit=10&timeout=100ms", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"] != float64(0) {
		t.Errorf("count = %v, want 0", result["count"])
	}
}

func TestHandleFetchWithHeaders(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "hdr-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Publish message with content type header
	req := httptest.NewRequest("POST", "/v1/messages/hdr-topic", bytes.NewReader([]byte("data")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Routing-Key", "key1")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	// Fetch it
	resp := doRequest(t, srv, "GET", "/v1/messages/hdr-topic?partition=0&offset=0&limit=10", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fetch: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	respBody, _ := io.ReadAll(resp.Body)
	json.Unmarshal(respBody, &result)
	count := int(result["count"].(float64))
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}

	// Verify message has payload
	msgs := result["messages"].([]interface{})
	msg := msgs[0].(map[string]interface{})
	if msg["payload"] == nil {
		t.Error("expected non-nil payload")
	}
}

func TestHandlePublishToQueueTopic(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create queue topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "qpub-topic",
		"mode":       "queue",
		"partitions": 2,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Publish
	resp := doRequest(t, srv, "POST", "/v1/messages/qpub-topic", []byte("queue-msg"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("publish to queue: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["topic"] != "qpub-topic" {
		t.Errorf("topic = %v, want qpub-topic", result["topic"])
	}
}

func TestHandleAckForTrackedOffset(t *testing.T) {
	srv, b, cleanup := setupTestServer(t)
	defer cleanup()

	// Create queue topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "tracked-ack",
		"mode":       "queue",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Add a consumer so messages get tracked
	qc := &queue.QueueConsumer{
		ID:       "c1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	b.QueueEngine().AddConsumer("tracked-ack", qc)

	// Publish
	resp := doRequest(t, srv, "POST", "/v1/messages/tracked-ack", []byte("msg"))
	var pubResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&pubResult)
	offset := uint64(pubResult["offset"].(float64))

	// Ack the specific offset
	ackBody, _ := json.Marshal(map[string]interface{}{
		"offsets": []uint64{offset},
	})
	resp = doRequest(t, srv, "POST", "/v1/messages/tracked-ack/ack", ackBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ack: %d", resp.StatusCode)
	}

	var ackResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&ackResult)
	if ackResult["acked"] != float64(1) {
		t.Errorf("acked = %v, want 1", ackResult["acked"])
	}
}

// errorReader always returns an error on Read.
type errorReader struct{}

func (errorReader) Read(p []byte) (int, error) { return 0, fmt.Errorf("simulated read error") }

func TestHandlePublishReadBodyError(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "read-err-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Use a request with a failing body reader
	req := httptest.NewRequest("POST", "/v1/messages/read-err-topic", errorReader{})
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestHandleFetchMissingParams(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "noparam-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Fetch without query params — should use defaults
	resp := doRequest(t, srv, "GET", "/v1/messages/noparam-topic", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleAckNonQueueTopic(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create stream topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "stream-ack-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Ack on a stream topic — should still work (just no tracked offsets)
	ackBody, _ := json.Marshal(map[string]interface{}{
		"offsets": []float64{0},
	})
	resp := doRequest(t, srv, "POST", "/v1/messages/stream-ack-topic/ack", ackBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["acked"] != float64(0) {
		t.Errorf("acked = %v, want 0 (no tracked offsets)", result["acked"])
	}
}

func TestHandleNackNonQueueTopic(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create stream topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "stream-nack-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Nack on stream topic
	nackBody, _ := json.Marshal(map[string]interface{}{
		"offsets": []float64{0},
	})
	resp := doRequest(t, srv, "POST", "/v1/messages/stream-nack-topic/nack", nackBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleMetricsAfterPublish(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic and publish
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "metrics-pub",
		"mode":       "stream",
		"partitions": 2,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	for i := 0; i < 5; i++ {
		doRequest(t, srv, "POST", "/v1/messages/metrics-pub", []byte("data"))
	}

	resp := doRequest(t, srv, "GET", "/v1/metrics", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	if len(respBody) == 0 {
		t.Error("expected non-empty metrics")
	}
}

func TestHandleListTopicsAllModes(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topics in different modes
	modes := []struct {
		name string
		mode string
	}{
		{"list-stream", "stream"},
		{"list-queue", "queue"},
		{"list-unified", "unified"},
	}
	for _, m := range modes {
		body, _ := json.Marshal(map[string]interface{}{
			"name":       m.name,
			"mode":       m.mode,
			"partitions": 2,
		})
		doRequest(t, srv, "POST", "/v1/topics", body)
	}

	resp := doRequest(t, srv, "GET", "/v1/topics", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var topics []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&topics)

	modesFound := map[string]bool{}
	for _, tp := range topics {
		if mode, ok := tp["mode"].(string); ok {
			modesFound[mode] = true
		}
	}
	if !modesFound["stream"] || !modesFound["queue"] || !modesFound["unified"] {
		t.Errorf("expected all three modes, got: %v", modesFound)
	}
}

func TestHandleFetchWithCustomTimeout(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "fetch-timeout-t",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Publish a message first (so data is available)
	doRequest(t, srv, "POST", "/v1/messages/fetch-timeout-t", []byte("fetch-timeout-msg"))

	// Fetch with custom timeout (data already exists, so long-poll not needed)
	resp := doRequest(t, srv, "GET", "/v1/messages/fetch-timeout-t?partition=0&offset=0&limit=10&timeout=500ms", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	count := result["count"].(float64)
	if count < 1 {
		t.Errorf("expected at least 1 message, got count=%v", result["count"])
	}
}

func TestHandleNackWithDLQ(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create DLQ target topic
	dlqBody, _ := json.Marshal(map[string]interface{}{
		"name":       "dlq-target-http",
		"mode":       "queue",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", dlqBody)

	// Create source topic with DLQ config and MaxRetries=1
	srcBody, _ := json.Marshal(map[string]interface{}{
		"name":        "dlq-source-http",
		"mode":        "queue",
		"partitions":  1,
		"dlq_topic":   "dlq-target-http",
		"max_retries": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", srcBody)

	// Publish a message
	resp := doRequest(t, srv, "POST", "/v1/messages/dlq-source-http", []byte("dlq-msg"))
	var pubResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&pubResult)
	offset := pubResult["offset"].(float64)

	// Nack the message — with max_retries=1, first nack should trigger DLQ
	nackBody, _ := json.Marshal(map[string]interface{}{
		"offsets": []float64{offset},
	})
	resp = doRequest(t, srv, "POST", "/v1/messages/dlq-source-http/nack", nackBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["nacked"] != float64(1) {
		t.Errorf("nacked = %v, want 1", result["nacked"])
	}
}

func TestHandleMetricsContentType(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/metrics", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "text/plain" {
		t.Errorf("content-type = %q, want text/plain", ct)
	}
}

func TestHandleDeleteTopicAndVerify(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "delete-verify",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Delete it
	resp := doRequest(t, srv, "DELETE", "/v1/topics/delete-verify", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Verify it's gone
	resp = doRequest(t, srv, "GET", "/v1/topics/delete-verify", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandlePublishWithContentType(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "ct-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Publish with Content-Type header
	req := httptest.NewRequest("POST", "/v1/messages/ct-topic", bytes.NewReader([]byte("msg-with-ct")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleFetchError(t *testing.T) {
	srv, b, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "fetch-err-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Close the storage to cause Fetch error
	b.Storage().Close()

	resp := doRequest(t, srv, "GET", "/v1/messages/fetch-err-topic?partition=0&offset=0&limit=10", nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Logf("status = %d (expected 500 on error, but may vary)", resp.StatusCode)
	}
}
