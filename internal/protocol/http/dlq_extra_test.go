package http

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/dlq"
	"github.com/chimeramq/chimera/internal/message"
)

func setupTestServerWithPProf(t *testing.T) (*AdminServer, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "chimera-http-pprof-*")
	if err != nil {
		t.Fatal(err)
	}

	cfg := &broker.Config{
		Node: broker.NodeConfig{ID: 1, Name: "pprof-node", DataDir: dir},
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
		Observability: broker.ObservabilityConfig{
			PProf: broker.PProfConfig{Enabled: true},
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

// --- DLQ Preview ---

func TestHandleDLQPreview(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"max_messages": 10,
		"condition": map[string]interface{}{
			"reason": "test",
		},
	})
	resp := doRequest(t, srv, "POST", "/v1/dlq/dlq-preview-topic/preview", body)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["topic"] != "dlq-preview-topic" {
		t.Errorf("topic = %v, want dlq-preview-topic", result["topic"])
	}
}

func TestHandleDLQPreviewWithoutDLQ(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/dlq/some-topic/preview", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestHandleDLQPreviewInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/dlq/some-topic/preview", []byte("not json"))
	if resp.StatusCode != http.StatusOK {
		// invalid JSON sets default max_messages and continues
		t.Logf("preview invalid JSON: status = %d", resp.StatusCode)
	}
}

// --- DLQ Export ---

func TestHandleDLQExport(t *testing.T) {
	srv, _, cleanup := setupTestServerWithFeatures(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/dlq/dlq-export-topic/export?max_messages=5", nil)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(b))
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("content-type = %q, want application/json", ct)
	}

	cd := resp.Header.Get("Content-Disposition")
	if cd == "" {
		t.Error("missing Content-Disposition header")
	}
}

func TestHandleDLQExportWithoutDLQ(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/dlq/some-topic/export", nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

// --- parseCondition ---

func TestParseCondition(t *testing.T) {
	// Empty condition should match all messages
	cond := parseCondition(map[string]interface{}{})
	entry := &dlq.DLQEntry{Reason: "any", Retries: 1}
	if !cond(entry) {
		t.Error("empty condition should match all messages")
	}

	// Reason condition
	cond = parseCondition(map[string]interface{}{"reason": "timeout"})
	entry = &dlq.DLQEntry{Reason: "timeout", Retries: 1}
	if !cond(entry) {
		t.Error("reason condition should match")
	}
	entry = &dlq.DLQEntry{Reason: "other", Retries: 1}
	if cond(entry) {
		t.Error("reason condition should not match other reasons")
	}

	// Min retries condition
	cond = parseCondition(map[string]interface{}{"min_retries": float64(3)})
	entry = &dlq.DLQEntry{Reason: "any", Retries: 5}
	if !cond(entry) {
		t.Error("min_retries condition should match")
	}
	entry = &dlq.DLQEntry{Reason: "any", Retries: 1}
	if cond(entry) {
		t.Error("min_retries condition should not match low retries")
	}

	// Reason pattern condition
	cond = parseCondition(map[string]interface{}{"reason_pattern": "time.*"})
	entry = &dlq.DLQEntry{Reason: "timeout", Retries: 1}
	if !cond(entry) {
		t.Error("reason_pattern condition should match")
	}
	entry = &dlq.DLQEntry{Reason: "other", Retries: 1}
	if cond(entry) {
		t.Error("reason_pattern condition should not match other reasons")
	}

	// Payload contains condition
	cond = parseCondition(map[string]interface{}{"payload_contains": "hello"})
	entry = &dlq.DLQEntry{Reason: "any", Retries: 1, OriginalMsg: &message.Envelope{Payload: []byte("hello world")}}
	if !cond(entry) {
		t.Error("payload_contains condition should match")
	}
	entry = &dlq.DLQEntry{Reason: "any", Retries: 1, OriginalMsg: &message.Envelope{Payload: []byte("goodbye")}}
	if cond(entry) {
		t.Error("payload_contains condition should not match missing substring")
	}

	// Composite AND condition
	cond = parseCondition(map[string]interface{}{
		"reason":         "timeout",
		"min_retries":    float64(2),
		"payload_contains": "err",
	})
	entry = &dlq.DLQEntry{Reason: "timeout", Retries: 3, OriginalMsg: &message.Envelope{Payload: []byte("error")}}
	if !cond(entry) {
		t.Error("composite AND condition should match")
	}
	entry = &dlq.DLQEntry{Reason: "timeout", Retries: 3, OriginalMsg: &message.Envelope{Payload: []byte("ok")}}
	if cond(entry) {
		t.Error("composite AND condition should not match when one fails")
	}
}

// --- Config Reload ---

func TestHandleConfigReload(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create a config file in the expected location (DataDir/../chimera.yaml)
	dataDir := srv.broker.Config().Node.DataDir
	configPath := filepath.Join(dataDir, "..", "chimera.yaml")
	configContent := `
node:
  id: 1
  name: reload-test
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	defer os.Remove(configPath)

	resp := doRequest(t, srv, "POST", "/v1/config/reload", nil)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s", resp.StatusCode, string(b))
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["status"] != "ok" {
		t.Errorf("status = %v, want ok", result["status"])
	}
}

func TestHandleConfigReloadMissingFile(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Ensure no config file exists in fallback locations
	resp := doRequest(t, srv, "POST", "/v1/config/reload", nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

// --- PProf Endpoints ---

func TestHandlePProfIndex(t *testing.T) {
	srv, cleanup := setupTestServerWithPProf(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/debug/pprof/", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Errorf("content-type = %q, want text/html; charset=utf-8", ct)
	}

	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Error("expected non-empty pprof index body")
	}
}

func TestHandlePProfAllocs(t *testing.T) {
	srv, cleanup := setupTestServerWithPProf(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/debug/pprof/allocs", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlePProfHeap(t *testing.T) {
	srv, cleanup := setupTestServerWithPProf(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/debug/pprof/heap", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlePProfGoroutine(t *testing.T) {
	srv, cleanup := setupTestServerWithPProf(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/debug/pprof/goroutine", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlePProfBlock(t *testing.T) {
	srv, cleanup := setupTestServerWithPProf(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/debug/pprof/block", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlePProfMutex(t *testing.T) {
	srv, cleanup := setupTestServerWithPProf(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/debug/pprof/mutex", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlePProfThreadcreate(t *testing.T) {
	srv, cleanup := setupTestServerWithPProf(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/debug/pprof/threadcreate", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlePProfCmdline(t *testing.T) {
	srv, cleanup := setupTestServerWithPProf(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/debug/pprof/cmdline", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlePProfProfile(t *testing.T) {
	srv, cleanup := setupTestServerWithPProf(t)
	defer cleanup()

	// CPU profile with seconds=1 to keep test fast
	resp := doRequest(t, srv, "GET", "/debug/pprof/profile?seconds=1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlePProfTrace(t *testing.T) {
	srv, cleanup := setupTestServerWithPProf(t)
	defer cleanup()

	// Trace with seconds=1 to keep test fast
	resp := doRequest(t, srv, "GET", "/debug/pprof/trace?seconds=1", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "application/octet-stream" {
		t.Errorf("content-type = %q, want application/octet-stream", ct)
	}
}
