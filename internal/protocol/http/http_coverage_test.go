package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
)

// --- Schema Registry: nil registry paths (503) ---

func TestHandleRegisterSchemaNilRegistry(t *testing.T) {
	srv := &AdminServer{broker: &broker.Broker{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/v1/schemas/test-subject", strings.NewReader(`{"type":"json","schema":"{}"}`))
	srv.handleRegisterSchema(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHandleListSchemasNilRegistry(t *testing.T) {
	srv := &AdminServer{broker: &broker.Broker{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v1/schemas/test-subject", nil)
	srv.handleListSchemas(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHandleGetLatestSchemaNilRegistry(t *testing.T) {
	srv := &AdminServer{broker: &broker.Broker{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v1/schemas/test-subject/latest", nil)
	srv.handleGetLatestSchema(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHandleGetSchemaVersionNilRegistry(t *testing.T) {
	srv := &AdminServer{broker: &broker.Broker{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v1/schemas/test-subject/versions/1", nil)
	srv.handleGetSchemaVersion(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHandleDeleteSubjectNilRegistry(t *testing.T) {
	srv := &AdminServer{broker: &broker.Broker{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("DELETE", "/v1/schemas/test-subject", nil)
	srv.handleDeleteSubject(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHandleSetCompatibilityNilRegistry(t *testing.T) {
	srv := &AdminServer{broker: &broker.Broker{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("PUT", "/v1/schemas/test-subject/compatibility", strings.NewReader(`{"mode":"BACKWARD"}`))
	srv.handleSetCompatibility(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

// --- Schema Registry: error paths ---

func TestHandleRegisterSchemaSubjectTooLong(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	longSubject := strings.Repeat("a", 256)
	body, _ := json.Marshal(map[string]interface{}{
		"type":   "json",
		"schema": `{"type":"object"}`,
	})
	resp := doRequest(t, srv, "POST", "/v1/schemas/"+longSubject, body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleRegisterSchemaConflict(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	body1, _ := json.Marshal(map[string]interface{}{
		"type":   "json",
		"schema": `{"type":"object","properties":{"name":{"type":"string"}}}`,
	})
	resp1 := doRequest(t, srv, "POST", "/v1/schemas/conflict-sub", body1)
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first register status = %d, want 200", resp1.StatusCode)
	}

	body2, _ := json.Marshal(map[string]interface{}{
		"type":   "json",
		"schema": `{"type":"object","properties":{"name":{"type":"integer"}}}`,
	})
	resp2 := doRequest(t, srv, "POST", "/v1/schemas/conflict-sub", body2)
	if resp2.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 409", resp2.StatusCode)
	}
}

func TestHandleRegisterSchemaEmptySubjectDirect(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/v1/schemas/", strings.NewReader(`{"type":"json","schema":"{}"}`))
	srv.handleRegisterSchema(w, r)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestHandleSetCompatibilityRegistryError(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	longSubject := strings.Repeat("a", 256)
	body, _ := json.Marshal(map[string]interface{}{
		"mode": "BACKWARD",
	})
	resp := doRequest(t, srv, "PUT", "/v1/schemas/"+longSubject+"/compatibility", body)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

// --- Consumer Group: nil engine / error paths ---

func TestHandleListConsumersNilEngine(t *testing.T) {
	srv := &AdminServer{broker: &broker.Broker{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v1/consumers", nil)
	srv.handleListConsumers(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHandleConsumerHeartbeatNilEngine(t *testing.T) {
	srv := &AdminServer{broker: &broker.Broker{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/v1/consumers/test-group/heartbeat", strings.NewReader(`{"member_id":"m1"}`))
	srv.handleConsumerHeartbeat(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHandleConsumerHeartbeatNotFound(t *testing.T) {
	srv, b, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic and join group
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "hb-404-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)
	b.StreamEngine().JoinGroup("hb-404-group", "hb-404-topic", "real-member", 1, 0)

	// Heartbeat with wrong member_id
	hbBody, _ := json.Marshal(map[string]interface{}{
		"member_id": "wrong-member",
	})
	resp := doRequest(t, srv, "POST", "/v1/consumers/hb-404-group/heartbeat", hbBody)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

// --- Fetch: error paths ---

func TestHandleFetchNilTopics(t *testing.T) {
	srv := &AdminServer{broker: &broker.Broker{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/v1/messages/test-topic", nil)
	srv.handleFetch(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHandleFetchPartitionOutOfRange(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "fetch-part-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	resp := doRequest(t, srv, "POST", "/v1/topics", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create topic status = %d, want 201", resp.StatusCode)
	}

	resp = doRequest(t, srv, "GET", "/v1/messages/fetch-part-topic?partition=5&offset=0&limit=10", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleFetchTimeoutClamp(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "fetch-to-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Publish a message so fetch succeeds quickly
	pubBody, _ := json.Marshal(map[string]interface{}{"payload": "hello"})
	doRequest(t, srv, "POST", "/v1/topics/fetch-to-topic/publish", pubBody)

	// Timeout larger than maxFetchTimeout should clamp to 30s
	resp := doRequest(t, srv, "GET", "/v1/messages/fetch-to-topic?partition=0&offset=0&limit=10&timeout=60s", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("large timeout: status = %d, want 200", resp.StatusCode)
	}

	// Timeout smaller than 100ms should clamp to 100ms
	resp = doRequest(t, srv, "GET", "/v1/messages/fetch-to-topic?partition=0&offset=0&limit=10&timeout=1ms", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("small timeout: status = %d, want 200", resp.StatusCode)
	}
}

// --- DLQ: nil DLQ / error paths ---

func TestHandleDLQReplayNilDLQ(t *testing.T) {
	srv := &AdminServer{broker: &broker.Broker{}}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/v1/dlq/test-topic/replay", nil)
	srv.handleDLQReplay(w, r)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

func TestHandleDLQReplayInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/dlq/replay-topic/replay", []byte("not json"))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}
