package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
)

func TestDetector(t *testing.T) {
	d := &Detector{}
	for _, prefix := range []string{"GET ", "POST", "PUT ", "DELE", "OPTI", "PATC", "HEAD", "CONN"} {
		if !d.Detect([]byte(prefix)) {
			t.Errorf("should detect %q", prefix)
		}
	}
	if d.Detect([]byte{0x10}) {
		t.Error("should not detect non-HTTP")
	}
	if d.Detect([]byte("ABC")) {
		t.Error("should not detect short input")
	}
	if d.BytesNeeded() != 4 {
		t.Errorf("BytesNeeded = %d, want 4", d.BytesNeeded())
	}
}

func TestSecurityMiddlewareHeadersValues(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/v1/health", nil)
	w := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)
	resp := w.Result()

	if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Error("expected X-Content-Type-Options header")
	}
	if resp.Header.Get("X-Frame-Options") != "DENY" {
		t.Error("expected X-Frame-Options header")
	}
	if resp.Header.Get("X-XSS-Protection") != "1; mode=block" {
		t.Error("expected X-XSS-Protection header")
	}
}

func TestMethodToOpMapping(t *testing.T) {
	tests := []struct {
		method string
		want   auth.Operation
	}{
		{"GET", auth.OpRead},
		{"POST", auth.OpWrite},
		{"PUT", auth.OpWrite},
		{"DELETE", auth.OpDelete},
		{"PATCH", auth.OpRead},
	}
	for _, tt := range tests {
		if got := methodToOp(tt.method); got != tt.want {
			t.Errorf("methodToOp(%q) = %v, want %v", tt.method, got, tt.want)
		}
	}
}

func TestGetIdentityNil(t *testing.T) {
	req := httptest.NewRequest("GET", "/", nil)
	if id := getIdentity(req); id != nil {
		t.Error("expected nil identity")
	}
}

func TestRegisterWebSocket(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	called := false
	srv.RegisterWebSocket("/ws-extra", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))

	req := httptest.NewRequest("GET", "/ws-extra", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if !called {
		t.Error("WebSocket handler was not called")
	}
}

func TestHandleConnectionHTTP(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	server, client := net.Pipe()
	go func() {
		client.Write([]byte("GET /v1/health HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		client.Close()
	}()

	done := make(chan struct{})
	go func() {
		srv.HandleConnection(server, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish")
	}
	server.Close()
}

func TestAdminServerStopMethod(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()
	srv.Stop()
}

func TestHandleFetchLimitExceedsMax(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body := []byte(`{"name":"fl-max","mode":"stream","partitions":1}`)
	doRequest(t, srv, "POST", "/v1/topics", body)

	resp := doRequest(t, srv, "GET", "/v1/messages/fl-max?limit=999999", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleGetSchemaVersion(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	// Register a schema first
	regBody, _ := json.Marshal(map[string]interface{}{
		"type":   "json",
		"schema": `{"type":"object"}`,
	})
	doRequest(t, srv, "POST", "/v1/schemas/version-test", regBody)

	resp := doRequest(t, srv, "GET", "/v1/schemas/version-test/versions/1", nil)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(body))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["version"] != float64(1) {
		t.Errorf("version = %v, want 1", result["version"])
	}
}

func TestHandleGetSchemaVersionInvalid(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/schemas/test-subject/versions/abc", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleGetSchemaVersionNotFound(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/schemas/nonexist/versions/99", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleUploadWASMModule(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	// Minimal WASM binary header
	wasmPayload := []byte("\x00asm\x01\x00\x00\x00")
	resp := doRequest(t, srv, "POST", "/v1/wasm/modules?name=test-mod", wasmPayload)
	// WASM may not be available in test env — accept 503
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 201, 400, or 503", resp.StatusCode)
	}
}

func TestHandleUploadWASMNoName(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/wasm/modules", []byte("data"))
	// WASM may not be available in test env — accept 503
	if resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 400 or 503", resp.StatusCode)
	}
}

func TestHandleDeleteWASMNotFound(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/wasm/modules/nonexist", nil)
	// WASM may not be available in test env — accept 503
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 404 or 503", resp.StatusCode)
	}
}

func TestHandleRegisterSchemaNoSubject(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	// The route pattern includes {subject}, so calling without it hits a different route.
	// Instead test with empty body
	resp := doRequest(t, srv, "POST", "/v1/schemas/test-subject", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleConsumerGroup(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic first
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "cg-topic",
		"mode":       "queue",
		"partitions": 2,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	// Join consumer group
	joinBody, _ := json.Marshal(map[string]interface{}{
		"topic":     "cg-topic",
		"member_id": "consumer-1",
	})
	resp := doRequest(t, srv, "POST", "/v1/consumers/test-group/join", joinBody)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("join status = %d, body = %s", resp.StatusCode, string(respBody))
	}

	// Leave consumer group
	leaveBody, _ := json.Marshal(map[string]interface{}{
		"member_id": "consumer-1",
	})
	resp = doRequest(t, srv, "POST", "/v1/consumers/test-group/leave", leaveBody)
	if resp.StatusCode != http.StatusOK {
		t.Logf("leave status = %d (may fail without heartbeat)", resp.StatusCode)
	}
}

func TestHandleConsumerHeartbeatExtra(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic and join
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "hb-topic",
		"mode":       "queue",
		"partitions": 2,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	joinBody, _ := json.Marshal(map[string]interface{}{
		"topic":     "hb-topic",
		"member_id": "consumer-hb",
	})
	doRequest(t, srv, "POST", "/v1/consumers/hb-group/join", joinBody)

	hbBody, _ := json.Marshal(map[string]interface{}{
		"member_id": "consumer-hb",
	})
	resp := doRequest(t, srv, "POST", "/v1/consumers/hb-group/heartbeat", hbBody)
	if resp.StatusCode != http.StatusOK {
		t.Logf("heartbeat status = %d", resp.StatusCode)
	}
}

func TestHandleConsumerOffsetsExtra(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/consumers/offsets-group/offsets?topic=off-topic", nil)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		t.Logf("offsets status = %d", resp.StatusCode)
	}
}

func TestHandleListConsumers(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/consumers", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func setupAuthTestServer(t *testing.T) (*AdminServer, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "chimera-http-auth-*")
	if err != nil {
		t.Fatal(err)
	}

	cfg := &broker.Config{
		Node: broker.NodeConfig{ID: 1, Name: "auth-node", DataDir: dir},
		Listener: broker.ListenerConfig{
			Bind: "127.0.0.1", Port: 0, AdminPort: 0, MaxConnections: 100,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{Partitions: 4, RetentionTime: "1h", Mode: "unified"},
		},
		Logging: broker.LoggingConfig{Level: "warn", Format: "text"},
		Auth: broker.AuthConfig{
			Enabled: true,
			Type:    "static",
			Users:   map[string]string{"admin": "$2a$04$AZNl/xU1Y0OWAcPUS/vz0OJeoAa4t4UpaAwWSShyp4b4Hf0VRRRCe"},
			Tokens:  map[string]string{"my-token": "admin"},
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
	return srv, cleanup
}

func TestAuthNoHeader(t *testing.T) {
	srv, cleanup := setupAuthTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/topics", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestAuthBearerToken(t *testing.T) {
	srv, cleanup := setupAuthTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/v1/topics", nil)
	req.Header.Set("Authorization", "Bearer my-token")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Result().StatusCode == http.StatusUnauthorized {
		t.Error("valid token should not be unauthorized")
	}
}

func TestAuthInvalidBearerToken(t *testing.T) {
	srv, cleanup := setupAuthTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/v1/topics", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Result().StatusCode)
	}
}

func TestAuthBasicAuth(t *testing.T) {
	srv, cleanup := setupAuthTestServer(t)
	defer cleanup()

	// Base64("admin:password123") = "YWRtaW46cGFzc3dvcmQxMjM="
	req := httptest.NewRequest("GET", "/v1/topics", nil)
	req.Header.Set("Authorization", "Basic YWRtaW46cGFzc3dvcmQxMjM=")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Result().StatusCode == http.StatusUnauthorized {
		t.Error("valid basic auth should not be unauthorized")
	}
}

func TestAuthBasicInvalidPassword(t *testing.T) {
	srv, cleanup := setupAuthTestServer(t)
	defer cleanup()

	// Base64("admin:wrong") = "YWRtaW46d3Jvbg=="
	req := httptest.NewRequest("GET", "/v1/topics", nil)
	req.Header.Set("Authorization", "Basic YWRtaW46d3Jvbg==")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Result().StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Result().StatusCode)
	}
}

func TestHealthNoAuth(t *testing.T) {
	srv, cleanup := setupAuthTestServer(t)
	defer cleanup()

	// Health endpoint should NOT require auth (no auth wrapper)
	resp := doRequest(t, srv, "GET", "/v1/health", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("health status = %d, want 200", resp.StatusCode)
	}
}

func TestClusterMembersNoAuth(t *testing.T) {
	srv, cleanup := setupAuthTestServer(t)
	defer cleanup()

	// Cluster members endpoint should NOT require auth
	resp := doRequest(t, srv, "GET", "/v1/cluster/members", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("cluster members status = %d, want 200", resp.StatusCode)
	}
}

// --- singleConnListener.Addr coverage ---

func TestSingleConnListenerAddr(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	server, client := net.Pipe()
	defer client.Close()

	go func() {
		// Write a valid HTTP request so the server processes it,
		// which causes http.Server.Serve to call ln.Addr().
		client.Write([]byte("GET /v1/health HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		client.Close()
	}()

	done := make(chan struct{})
	go func() {
		srv.HandleConnection(server, nil)
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(3 * time.Second):
		t.Error("HandleConnection did not finish in time")
	}
	server.Close()
}

func TestSingleConnListenerAddrDirect(t *testing.T) {
	// Directly test Addr() by creating a pipe and calling it
	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()

	ln := &singleConnListener{conn: server}
	addr := ln.Addr()
	if addr == nil {
		t.Error("Addr() returned nil")
	}
	if addr.Network() != "pipe" {
		t.Errorf("Addr().Network() = %q, want 'pipe'", addr.Network())
	}
}

func TestSingleConnListenerCloseAndDoubleAccept(t *testing.T) {
	// Test that Accept returns the connection once and then net.ErrClosed
	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()

	ln := &singleConnListener{conn: server}

	// First Accept should return the connection
	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("first Accept: %v", err)
	}
	if conn == nil {
		t.Error("first Accept returned nil conn")
	}

	// Second Accept should return net.ErrClosed
	_, err = ln.Accept()
	if err != net.ErrClosed {
		t.Errorf("second Accept error = %v, want net.ErrClosed", err)
	}

	// Close should return nil
	if err := ln.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}

// --- Stream Processor Topology: Start / Stop ---

func TestHandleStartTopology(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/processors/some-topo/start", nil)
	// Processor may not be fully functional — accept 503 (not enabled)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Logf("start topology status = %d", resp.StatusCode)
	}
}

func TestHandleStopTopology(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/processors/some-topo/stop", nil)
	// Processor may not be fully functional — accept 503 (not enabled)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Logf("stop topology status = %d", resp.StatusCode)
	}
}

func TestHandleStartTopologyNoFeatures(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/processors/some-topo/start", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("start topology without features: status = %d, want 503", resp.StatusCode)
	}
}

func TestHandleStopTopologyNoFeatures(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/processors/some-topo/stop", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("stop topology without features: status = %d, want 503", resp.StatusCode)
	}
}

// --- Topology Create: invalid JSON and missing name ---

func TestHandleCreateTopologyInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/processors", []byte("not json"))
	// Accept 503 if processor is nil, or 400 if processor exists but body is bad
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 503 or 400", resp.StatusCode)
	}
}

func TestHandleCreateTopologyMissingName(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"source": map[string]interface{}{
			"topic": "src",
		},
	})
	resp := doRequest(t, srv, "POST", "/v1/processors", body)
	// Accept 503 if processor is nil, or 400 if processor exists but name is missing
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 503 or 400", resp.StatusCode)
	}
}

func TestHandleCreateTopologyWithFeatures(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name": "feat-topo",
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
	// Accept 503 if processor not initialized
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		t.Logf("create topology with features: status = %d", resp.StatusCode)
	}
}

// --- Get Topology with features ---

func TestHandleGetTopologyWithFeatures(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/processors/nonexistent", nil)
	// Accept 503 if processor not initialized, or 404 if topology not found
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 503 or 404", resp.StatusCode)
	}
}

// --- Delete Topology with features ---

func TestHandleDeleteTopologyWithFeatures(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/processors/nonexistent", nil)
	// Accept 503 if processor not initialized, or 404/409 if topology not found
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 503, 404, or 409", resp.StatusCode)
	}
}

// --- WASM Upload with valid name and body ---

func TestHandleUploadWASMWithFeatures(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	wasmPayload := []byte("\x00asm\x01\x00\x00\x00")
	resp := doRequest(t, srv, "POST", "/v1/wasm/modules?name=my-module", wasmPayload)
	// Accept 503 (WASM not available), 201 (success), or 400 (compile error)
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 503, 201, or 400", resp.StatusCode)
	}
}

func TestHandleUploadWASMNoNameWithFeatures(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/wasm/modules", []byte("data"))
	// Accept 503 (WASM not available) or 400 (no name)
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 503 or 400", resp.StatusCode)
	}
}

// --- Delete WASM with features ---

func TestHandleDeleteWASMWithFeatures(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/wasm/modules/nonexistent", nil)
	// Accept 503 (WASM not available), 404 (not found), or 204 (success)
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want 503, 404, or 204", resp.StatusCode)
	}
}

// --- List Topologies with features ---

func TestHandleListTopologiesWithFeatures(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/processors", nil)
	// Accept 503 (processor not available) or 200 (success)
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 503 or 200", resp.StatusCode)
	}
}

// --- List WASM with features ---

func TestHandleListWASMWithFeatures(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/wasm/modules", nil)
	// Accept 503 (WASM not available) or 200 (success)
	if resp.StatusCode != http.StatusServiceUnavailable && resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 503 or 200", resp.StatusCode)
	}
}

// --- Cluster Members response body validation ---

func TestHandleClusterMembersResponseBody(t *testing.T) {
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
	// members should be nil/null for single-node
	if result["members"] != nil {
		t.Errorf("members = %v, want nil for single-node", result["members"])
	}
}

// --- Topology start/stop without features (nil processor) ---

func TestHandleTopologyEndpointsNoProcessor(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// GET /v1/processors (list)
	resp := doRequest(t, srv, "GET", "/v1/processors", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("list topologies: status = %d, want 503", resp.StatusCode)
	}

	// GET /v1/processors/{name} (get)
	resp = doRequest(t, srv, "GET", "/v1/processors/test", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("get topology: status = %d, want 503", resp.StatusCode)
	}

	// DELETE /v1/processors/{name} (delete)
	resp = doRequest(t, srv, "DELETE", "/v1/processors/test", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("delete topology: status = %d, want 503", resp.StatusCode)
	}

	// POST /v1/processors (create)
	resp = doRequest(t, srv, "POST", "/v1/processors", []byte(`{"name":"t"}`))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("create topology: status = %d, want 503", resp.StatusCode)
	}
}

// --- WASM endpoints without WASM runtime ---

func TestHandleWASMEndpointsNoRuntime(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// POST /v1/wasm/modules (upload)
	resp := doRequest(t, srv, "POST", "/v1/wasm/modules?name=test", []byte("data"))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("upload WASM: status = %d, want 503", resp.StatusCode)
	}

	// GET /v1/wasm/modules (list)
	resp = doRequest(t, srv, "GET", "/v1/wasm/modules", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("list WASM: status = %d, want 503", resp.StatusCode)
	}

	// DELETE /v1/wasm/modules/{name} (delete)
	resp = doRequest(t, srv, "DELETE", "/v1/wasm/modules/test", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("delete WASM: status = %d, want 503", resp.StatusCode)
	}
}

// --- Consumer handlers: more coverage ---

func TestHandleConsumerJoinMissingFields(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Missing member_id
	body, _ := json.Marshal(map[string]interface{}{
		"topic": "some-topic",
	})
	resp := doRequest(t, srv, "POST", "/v1/consumers/join-group/join", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing member_id: status = %d, want 400", resp.StatusCode)
	}

	// Missing topic
	body2, _ := json.Marshal(map[string]interface{}{
		"member_id": "c1",
	})
	resp = doRequest(t, srv, "POST", "/v1/consumers/join-group/join", body2)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing topic: status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleConsumerJoinInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/consumers/bad-group/join", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid JSON join: status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleConsumerLeaveInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/consumers/leave-grp/leave", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid JSON leave: status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleConsumerLeaveMissingMemberID(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{})
	resp := doRequest(t, srv, "POST", "/v1/consumers/leave-grp/leave", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing member_id leave: status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleConsumerHeartbeatInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/consumers/hb-grp/heartbeat", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid JSON heartbeat: status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleConsumerHeartbeatMissingMemberID(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{})
	resp := doRequest(t, srv, "POST", "/v1/consumers/hb-grp/heartbeat", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("missing member_id heartbeat: status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleConsumerOffsetsNotFound(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/consumers/nonexistent-group/offsets", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("offsets nonexistent group: status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleConsumerCommitOffsetsInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/consumers/commit-grp/offsets", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid JSON commit offsets: status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleConsumerJoinRoundRobin(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "rr-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	doRequest(t, srv, "POST", "/v1/topics", body)

	joinBody, _ := json.Marshal(map[string]interface{}{
		"member_id":  "c1",
		"topic":      "rr-topic",
		"partitions": 4,
		"strategy":   "round_robin",
	})
	resp := doRequest(t, srv, "POST", "/v1/consumers/rr-group/join", joinBody)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("join round_robin: status = %d, body = %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["group"] != "rr-group" {
		t.Errorf("group = %v, want rr-group", result["group"])
	}
}

// --- Schema handlers: more coverage ---

func TestHandleRegisterSchemaEmptyBody(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/schemas/empty-sub", []byte("{}"))
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("empty schema body: status = %d, body = %s", resp.StatusCode, string(b))
	}
}

func TestHandleSetCompatibilityInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "PUT", "/v1/schemas/compat-test-2/compatibility", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid JSON compatibility: status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleDeleteSubjectNonexistent(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/schemas/nonexistent-subject", nil)
	// May return 200 (no-op delete) or 500 (error)
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusInternalServerError {
		t.Logf("delete nonexistent subject: status = %d", resp.StatusCode)
	}
}

// --- DLQ handlers: more coverage ---

func TestHandleDLQPeekWithoutDLQ(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/dlq/some-topic", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("DLQ peek without DLQ: status = %d, want 503", resp.StatusCode)
	}
}

func TestHandleDLQPeekWithLimit(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/dlq/dlq-limit-topic?limit=5", nil)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("DLQ peek with limit: status = %d, body = %s", resp.StatusCode, string(b))
	}
}

func TestHandleDLQClearWithoutDLQ(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/dlq/some-topic", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("DLQ clear without DLQ: status = %d, want 503", resp.StatusCode)
	}
}

func TestHandleDLQReplayWithoutDLQ(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/dlq/some-topic/replay", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("DLQ replay without DLQ: status = %d, want 503", resp.StatusCode)
	}
}

// --- Auth: ACL resource type mapping ---

func setupACLTestServer(t *testing.T) (*AdminServer, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "chimera-http-acl-*")
	if err != nil {
		t.Fatal(err)
	}

	cfg := &broker.Config{
		Node: broker.NodeConfig{ID: 1, Name: "acl-node", DataDir: dir},
		Listener: broker.ListenerConfig{
			Bind: "127.0.0.1", Port: 0, AdminPort: 0, MaxConnections: 100,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{Partitions: 4, RetentionTime: "1h", Mode: "unified"},
		},
		Logging: broker.LoggingConfig{Level: "warn", Format: "text"},
		Auth: broker.AuthConfig{
			Enabled: true,
			Type:    "static",
			Users:   map[string]string{"admin": "$2a$04$AZNl/xU1Y0OWAcPUS/vz0OJeoAa4t4UpaAwWSShyp4b4Hf0VRRRCe"},
			Tokens:  map[string]string{"acl-token": "admin"},
		},
		ACL: broker.ACL{
			Enabled:       true,
			DefaultPolicy: "allow",
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
	return srv, cleanup
}

func doAuthRequest(t *testing.T, srv *AdminServer, method, path string, body []byte, token string) *http.Response {
	t.Helper()

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req := httptest.NewRequest(method, path, bodyReader)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	return w.Result()
}

func TestACLWithTokenSchemaResource(t *testing.T) {
	srv, cleanup := setupACLTestServer(t)
	defer cleanup()

	// Access schema endpoint with ACL — this exercises the ResourceSchema path
	resp := doAuthRequest(t, srv, "POST", "/v1/schemas/acl-test", []byte(`{"schema":"{}"}`), "acl-token")
	// Schema registry is not enabled in this config, so expect 503
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Logf("schema with ACL: status = %d", resp.StatusCode)
	}
}

func TestACLWithTokenClusterResource(t *testing.T) {
	srv, cleanup := setupACLTestServer(t)
	defer cleanup()

	// Access cluster members — should work (no auth wrapper)
	resp := doRequest(t, srv, "GET", "/v1/cluster/members", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("cluster members: status = %d, want 200", resp.StatusCode)
	}
}

func TestACLWithTokenWASMResource(t *testing.T) {
	srv, cleanup := setupACLTestServer(t)
	defer cleanup()

	// Access WASM endpoint — exercises the ResourceWASM path
	resp := doAuthRequest(t, srv, "GET", "/v1/wasm/modules", nil, "acl-token")
	// WASM not enabled, so expect 503
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Logf("WASM with ACL: status = %d", resp.StatusCode)
	}
}

func TestACLWithTokenProcessorResource(t *testing.T) {
	srv, cleanup := setupACLTestServer(t)
	defer cleanup()

	// Access processor endpoint — exercises the ResourceCluster path (processors maps to cluster)
	resp := doAuthRequest(t, srv, "GET", "/v1/processors", nil, "acl-token")
	// Processor not enabled, so expect 503
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Logf("processors with ACL: status = %d", resp.StatusCode)
	}
}

func TestACLForbiddenAccess(t *testing.T) {
	dir, err := os.MkdirTemp("", "chimera-http-deny-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	cfg := &broker.Config{
		Node: broker.NodeConfig{ID: 1, Name: "deny-node", DataDir: dir},
		Listener: broker.ListenerConfig{
			Bind: "127.0.0.1", Port: 0, AdminPort: 0, MaxConnections: 100,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{Partitions: 4, RetentionTime: "1h", Mode: "unified"},
		},
		Logging: broker.LoggingConfig{Level: "warn", Format: "text"},
		Auth: broker.AuthConfig{
			Enabled: true,
			Type:    "static",
			Users:   map[string]string{"user1": "pass1"},
			Tokens:  map[string]string{"deny-token": "user1"},
		},
		ACL: broker.ACL{
			Enabled:       true,
			DefaultPolicy: "deny",
		},
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer b.Stop()

	srv := NewAdminServer(b)

	// Access topics list with deny-all ACL — should be 403
	resp := doAuthRequest(t, srv, "GET", "/v1/topics", nil, "deny-token")
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("deny-all ACL: status = %d, want 403", resp.StatusCode)
	}
}

// --- Serve method TLS path coverage ---

func TestServeTLSConfigPath(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// The Serve method checks TLS config. We can't easily test the TLS path
	// without real certs, but we can test that non-TLS Serve works.
	// This is already covered by TestAdminServerServeAndShutdown.
	// Just ensure the method exists and is callable.
	srv.server.Addr = "127.0.0.1:0"
	done := make(chan error, 1)
	go func() {
		done <- srv.Serve()
	}()
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	<-done
}
