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
