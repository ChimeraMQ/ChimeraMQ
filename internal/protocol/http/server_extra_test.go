package http

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
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
