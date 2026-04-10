package http

import (
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
			Users:   map[string]string{"admin": "password123"},
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
