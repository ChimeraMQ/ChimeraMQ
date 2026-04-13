package ws

import (
	"context"
	"encoding/base64"
	"net/http"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/coder/websocket"
	"golang.org/x/crypto/bcrypt"
)

func TestNewServer(t *testing.T) {
	s := NewServer(nil)
	if s == nil {
		t.Fatal("server should not be nil")
	}
}

func TestDetectorDetect(t *testing.T) {
	d := Detector{}
	if d.Detect([]byte("GET /ws HTTP/1.1")) {
		t.Error("detector should always return false")
	}
}

func TestDetectorBytesNeeded(t *testing.T) {
	d := Detector{}
	if d.BytesNeeded() != 0 {
		t.Errorf("BytesNeeded = %d, want 0", d.BytesNeeded())
	}
}

func TestServerHandleConnection(t *testing.T) {
	s := NewServer(nil)
	// HandleConnection should be a no-op for WebSocket
	if err := s.HandleConnection(nil, nil); err != nil {
		t.Errorf("HandleConnection should return nil, got %v", err)
	}
}

func TestServerStop(t *testing.T) {
	s := NewServer(nil)
	// Stop with no sessions should be safe
	s.Stop()
}

func TestWSSessionMessage(t *testing.T) {
	msg := wsMessage{
		Op:         "publish",
		Topic:      "test-topic",
		Payload:    "aGVsbG8=",
		RoutingKey: "key1",
	}
	if msg.Op != "publish" {
		t.Errorf("Op = %q", msg.Op)
	}
	if msg.Topic != "test-topic" {
		t.Errorf("Topic = %q", msg.Topic)
	}
}

func TestWSSessionMessageDefaults(t *testing.T) {
	msg := wsMessage{}
	if msg.Op != "" {
		t.Error("default Op should be empty")
	}
	if msg.Partitions != 0 {
		t.Error("default Partitions should be 0")
	}
}

func TestTopicModeConstants(t *testing.T) {
	if broker.ModeStream != 0 {
		t.Errorf("ModeStream = %d, want 0", broker.ModeStream)
	}
	if broker.ModeQueue != 1 {
		t.Errorf("ModeQueue = %d, want 1", broker.ModeQueue)
	}
	if broker.ModeUnified != 2 {
		t.Errorf("ModeUnified = %d, want 2", broker.ModeUnified)
	}
}

func TestDecodeBasicAuthValid(t *testing.T) {
	creds, err := decodeBasicAuth("Basic " + base64.StdEncoding.EncodeToString([]byte("admin:secret")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds.username != "admin" {
		t.Errorf("username = %q, want admin", creds.username)
	}
	if creds.password != "secret" {
		t.Errorf("password = %q, want secret", creds.password)
	}
}

func TestDecodeBasicAuthInvalidBase64(t *testing.T) {
	_, err := decodeBasicAuth("Basic !!!not-base64!!!")
	if err == nil {
		t.Error("expected error for invalid base64")
	}
}

func TestDecodeBasicAuthInvalidFormat(t *testing.T) {
	_, err := decodeBasicAuth("Basic " + base64.StdEncoding.EncodeToString([]byte("nocolon")))
	if err == nil {
		t.Error("expected error for missing colon")
	}
}

func TestBytesHeadersToString(t *testing.T) {
	input := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}
	result := bytesHeadersToString(input)
	if result["key1"] != "value1" {
		t.Errorf("key1 = %q, want value1", result["key1"])
	}
	if result["key2"] != "value2" {
		t.Errorf("key2 = %q, want value2", result["key2"])
	}
	if len(result) != 2 {
		t.Errorf("len = %d, want 2", len(result))
	}
}

func TestWSServeHTTPAuthBearerSuccess(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Config().Auth.Enabled = true
	b.Config().Auth.Type = "static"
	b.Config().Auth.Tokens = map[string]string{"valid-token": "admin"}
	b.Start() // re-start with new auth config

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws://" + httpSrv.Listener.Addr().String()
	headers := http.Header{}
	headers.Set("Authorization", "Bearer valid-token")
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"chimera-json-v1"},
		HTTPHeader:   headers,
	})
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
}

func TestWSServeHTTPAuthBearerFailure(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Config().Auth.Enabled = true
	b.Config().Auth.Type = "static"
	b.Config().Auth.Tokens = map[string]string{"valid-token": "admin"}
	b.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws://" + httpSrv.Listener.Addr().String()
	headers := http.Header{}
	headers.Set("Authorization", "Bearer invalid-token")
	_, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"chimera-json-v1"},
		HTTPHeader:   headers,
	})
	if err == nil {
		t.Fatal("expected dial to fail with invalid token")
	}
	if resp != nil && resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestWSServeHTTPAuthBasicSuccess(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Config().Auth.Enabled = true
	b.Config().Auth.Type = "static"
	hash, _ := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	b.Config().Auth.Users = map[string]string{"admin": string(hash)}
	b.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws://" + httpSrv.Listener.Addr().String()
	headers := http.Header{}
	headers.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("admin:secret")))
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"chimera-json-v1"},
		HTTPHeader:   headers,
	})
	if err != nil {
		t.Fatalf("websocket dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
}

func TestWSServeHTTPAuthMissingHeader(t *testing.T) {
	_, b, httpSrv, cleanup := setupWSTestServer(t)
	defer cleanup()

	b.Config().Auth.Enabled = true
	b.Config().Auth.Type = "static"
	b.Config().Auth.Users = map[string]string{"admin": "secret"}
	b.Start()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wsURL := "ws://" + httpSrv.Listener.Addr().String()
	_, resp, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"chimera-json-v1"},
	})
	if err == nil {
		t.Fatal("expected dial to fail without auth header")
	}
	if resp != nil && resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}
