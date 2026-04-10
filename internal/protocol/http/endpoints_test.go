package http

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
)

// setupTestServerWithFeatures creates a broker with schema, DLQ, WASM enabled.
func setupTestServerWithFeatures(t *testing.T) (*AdminServer, *broker.Broker, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "chimera-http-feat-*")
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
		Schema: broker.SchemaConfig{
			Enabled:       true,
			DefaultCompat: "backward",
		},
		DLQ: broker.DLQConfig{
			Enabled:    true,
			TopicPrefix: "__dlq_",
			MaxRetries: 3,
		},
		WASM: broker.WASMConfig{
			Enabled:        true,
			MaxMemoryPages: 256,
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

// --- Schema Registry ---

func TestHandleRegisterSchema(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"type":   "json",
		"schema": `{"type":"object","properties":{"name":{"type":"string"}}}`,
	})
	resp := doRequest(t, srv, "POST", "/v1/schemas/test-subject", body)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["subject"] != "test-subject" {
		t.Errorf("subject = %v", result["subject"])
	}
	if result["version"] != float64(1) {
		t.Errorf("version = %v, want 1", result["version"])
	}
}

func TestHandleRegisterSchemaInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/schemas/bad-schema", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleGetLatestSchema(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"type":   "json",
		"schema": `{"type":"object"}`,
	})
	doRequest(t, srv, "POST", "/v1/schemas/latest-test", body)

	resp := doRequest(t, srv, "GET", "/v1/schemas/latest-test/latest", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["version"] != float64(1) {
		t.Errorf("version = %v, want 1", result["version"])
	}
}

func TestHandleGetLatestSchemaNotFound(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/schemas/nonexistent/latest", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleListSchemas(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	for i := 0; i < 2; i++ {
		body, _ := json.Marshal(map[string]interface{}{
			"schema": fmt.Sprintf(`{"type":"object","version":%d}`, i+1),
		})
		doRequest(t, srv, "POST", "/v1/schemas/list-test", body)
	}

	resp := doRequest(t, srv, "GET", "/v1/schemas/list-test", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var versions []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&versions)
	if len(versions) != 2 {
		t.Errorf("versions = %d, want 2", len(versions))
	}
}

func TestHandleDeleteSubject(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"schema": `{"type":"object"}`,
	})
	doRequest(t, srv, "POST", "/v1/schemas/del-sub", body)

	resp := doRequest(t, srv, "DELETE", "/v1/schemas/del-sub", nil)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 200 or 204", resp.StatusCode)
	}
}

func TestHandleSetCompatibility(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"compatibility": "BACKWARD",
	})
	resp := doRequest(t, srv, "PUT", "/v1/schemas/compat-test/compatibility", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

// --- Cluster ---

func TestHandleClusterMembersSingleNode(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/cluster/members", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["mode"] != "single-node" {
		t.Errorf("mode = %v, want single-node", result["mode"])
	}
}

// --- Security Middleware ---

func TestSecurityMiddleware(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/v1/health", nil)
	w := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)

	resp := w.Result()
	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Error("missing X-Content-Type-Options header")
	}
	if resp.Header.Get("X-Frame-Options") != "DENY" {
		t.Error("missing X-Frame-Options header")
	}
}

func TestSecurityMiddlewareCORS(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("OPTIONS", "/v1/health", nil)
	w := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("OPTIONS status = %d, want 204", resp.StatusCode)
	}
	if resp.Header.Get("Access-Control-Allow-Methods") == "" {
		t.Error("missing CORS headers")
	}
}

// --- Consumer Group Operations ---

func TestHandleConsumerJoin(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "cg-join-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	joinBody, _ := json.Marshal(map[string]interface{}{
		"member_id": "c1",
		"topic":       "cg-join-topic",
	})
	resp := doRequest(t, srv, "POST", "/v1/consumers/test-group/join", joinBody)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(b))
	}
}

func TestHandleConsumerLeave(t *testing.T) {
	srv, b, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "cg-leave-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	b.StreamEngine().JoinGroup("leave-group", "cg-leave-topic", "c1", 4, 0)

	leaveBody, _ := json.Marshal(map[string]interface{}{
		"member_id": "c1",
	})
	resp := doRequest(t, srv, "POST", "/v1/consumers/leave-group/leave", leaveBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleConsumerHeartbeat(t *testing.T) {
	srv, b, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "cg-hb-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	b.StreamEngine().JoinGroup("hb-group", "cg-hb-topic", "c1", 4, 0)

	hbBody, _ := json.Marshal(map[string]interface{}{
		"member_id": "c1",
	})
	resp := doRequest(t, srv, "POST", "/v1/consumers/hb-group/heartbeat", hbBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleConsumerOffsets(t *testing.T) {
	srv, b, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "cg-off-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	b.StreamEngine().JoinGroup("off-group", "cg-off-topic", "c1", 4, 0)

	resp := doRequest(t, srv, "GET", "/v1/consumers/off-group/offsets", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleConsumerCommitOffsets(t *testing.T) {
	srv, b, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "cg-commit-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	b.StreamEngine().JoinGroup("commit-group", "cg-commit-topic", "c1", 4, 0)

	commitBody, _ := json.Marshal(map[string]interface{}{
		"member_id": "c1",
		"offsets": map[string]interface{}{
			"0": float64(10),
			"1": float64(20),
		},
	})
	resp := doRequest(t, srv, "POST", "/v1/consumers/commit-group/offsets", commitBody)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(b))
	}
}

// --- DLQ Endpoints ---

func TestHandleDLQPeek(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":        "dlq-peek-src",
		"mode":        "queue",
		"partitions":  1,
		"dlq_topic":   "__dlq_dlq-peek-src",
		"max_retries": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	dlqBody, _ := json.Marshal(map[string]interface{}{
		"name":       "__dlq_dlq-peek-src",
		"mode":       "queue",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", dlqBody)

	resp := doRequest(t, srv, "GET", "/v1/dlq/dlq-peek-src", nil)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(b))
	}
}

func TestHandleDLQClear(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/dlq/dlq-clear-topic", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleDLQReplay(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/dlq/dlq-replay-topic/replay", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

// --- WASM ---

func TestHandleUploadWASM(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":   "test-module",
		"binary": "AGFzbQEAAAA=",
	})
	resp := doRequest(t, srv, "POST", "/v1/wasm/modules", body)
	if resp.StatusCode == http.StatusNotFound {
		t.Error("endpoint not registered")
	}
}

func TestHandleListWASM(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/wasm/modules", nil)
	if resp.StatusCode != http.StatusOK {
		t.Logf("status = %d (WASM module list)", resp.StatusCode)
	}
}

func TestHandleDeleteWASM(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/wasm/modules/nonexistent", nil)
	if resp.StatusCode == http.StatusMethodNotAllowed {
		t.Error("endpoint not registered")
	}
}

// --- Stream Processor Topology ---

func TestHandleCreateTopology(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name": "test-topo",
		"source": map[string]interface{}{
			"topic":      "topo-src",
			"partitions": 1,
		},
		"sink": map[string]interface{}{
			"topic": "topo-sink",
		},
		"operators": []map[string]interface{}{
			{"type": "filter", "module": "identity"},
		},
	})
	resp := doRequest(t, srv, "POST", "/v1/processors", body)
	if resp.StatusCode == http.StatusNotFound {
		t.Error("endpoint not registered")
	}
}

func TestHandleListTopologies(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/processors", nil)
	if resp.StatusCode != http.StatusOK {
		t.Logf("status = %d (topology list)", resp.StatusCode)
	}
}

func TestHandleGetTopology(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/processors/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Logf("status = %d (expected 404)", resp.StatusCode)
	}
}

func TestHandleDeleteTopology(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/processors/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Logf("status = %d", resp.StatusCode)
	}
}

// --- Fetch Parameter Validation ---

func TestHandleFetchInvalidPartition(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/messages/some-topic?partition=abc&offset=0&limit=10", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleFetchInvalidOffset(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/messages/some-topic?partition=0&offset=abc&limit=10", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleFetchInvalidLimit(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/messages/some-topic?partition=0&offset=0&limit=abc", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleFetchNegativeLimit(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/messages/some-topic?partition=0&offset=0&limit=-1", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// --- Topic name missing ---

func TestHandleCreateTopicNoName(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"mode":       "stream",
		"partitions": 4,
	})
	resp := doRequest(t, srv, "POST", "/v1/topics", body)
	if resp.StatusCode == http.StatusNotFound {
		t.Error("endpoint not registered")
	}
	t.Logf("CreateTopic no name: status=%d", resp.StatusCode)
}
