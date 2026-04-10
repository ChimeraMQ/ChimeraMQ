package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	adminhttp "github.com/chimeramq/chimera/internal/protocol/http"
)

func TestDLQPeekEndpoint(t *testing.T) {
	tb := newTestBrokerWithConfig(t, func(cfg *broker.Config) {
		cfg.DLQ.Enabled = true
		cfg.DLQ.TopicPrefix = "__dlq_"
		cfg.DLQ.MaxRetries = 3
	})
	defer tb.close()

	// Create topic first
	body, _ := json.Marshal(map[string]interface{}{"name": "orders", "partitions": 1})
	http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))

	// Publish some messages
	for i := 0; i < 3; i++ {
		msg, _ := json.Marshal(map[string]string{"msg": fmt.Sprintf("test-%d", i)})
		http.Post(tb.addr+"/v1/messages/orders", "text/plain", bytes.NewReader(msg))
	}

	// DLQ peek should return empty (nothing failed yet)
	resp, err := http.Get(tb.addr + "/v1/dlq/orders")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("dlq peek: status %d", resp.StatusCode)
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"].(float64) != 0 {
		t.Errorf("DLQ count = %v, want 0", result["count"])
	}
}

func TestDLQClearEndpoint(t *testing.T) {
	tb := newTestBrokerWithConfig(t, func(cfg *broker.Config) {
		cfg.DLQ.Enabled = true
	})
	defer tb.close()

	req, _ := http.NewRequest("DELETE", tb.addr+"/v1/dlq/orders", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("dlq clear: status %d", resp.StatusCode)
	}
}

func TestDLQReplayEndpoint(t *testing.T) {
	tb := newTestBrokerWithConfig(t, func(cfg *broker.Config) {
		cfg.DLQ.Enabled = true
	})
	defer tb.close()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{"name": "orders", "partitions": 1})
	http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))

	// Replay on empty DLQ should succeed
	req, _ := http.NewRequest("POST", tb.addr+"/v1/dlq/orders/replay", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("dlq replay: status %d", resp.StatusCode)
	}
}

func TestFlowControlPublish(t *testing.T) {
	tb := newTestBrokerWithConfig(t, func(cfg *broker.Config) {
		cfg.FlowControl.Enabled = true
		cfg.FlowControl.MaxMemoryBytes = 1024 * 1024 // 1MB
		cfg.FlowControl.HighWatermark = 0.80
		cfg.FlowControl.LowWatermark = 0.60
	})
	defer tb.close()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{"name": "test-topic", "partitions": 1})
	http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))

	// Normal publish should succeed
	msg := []byte("small message")
	resp, err := http.Post(tb.addr+"/v1/messages/test-topic", "text/plain", bytes.NewReader(msg))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("normal publish: status %d", resp.StatusCode)
	}
}

func TestIdempotentPublishWithHeaders(t *testing.T) {
	tb := newTestBrokerWithConfig(t, func(cfg *broker.Config) {
		cfg.Idempotent.Enabled = true
		cfg.Idempotent.WindowSize = "5m"
		cfg.Idempotent.MaxEntries = 1000
	})
	defer tb.close()

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{"name": "test-topic", "partitions": 1})
	http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))

	// Publish with producer headers
	msg := []byte("idempotent message")
	req, _ := http.NewRequest("POST", tb.addr+"/v1/messages/test-topic", bytes.NewReader(msg))
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("idempotent publish: status %d", resp.StatusCode)
	}
}

// Helper to create a test broker with custom config modifications.
func newTestBrokerWithConfig(t *testing.T, modify func(*broker.Config)) *testBroker {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "chimera-dlq-test-*")
	if err != nil {
		t.Fatal(err)
	}

	port := 20000 + rand.Intn(1000)
	adminPort := port + 1000

	cfg := &broker.Config{
		Node: broker.NodeConfig{
			ID:      1,
			Name:    "test-node",
			DataDir: tmpDir,
		},
		Listener: broker.ListenerConfig{
			Bind:           "127.0.0.1",
			Port:           port,
			AdminPort:      adminPort,
			MaxConnections: 100,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{
				SegmentSize: 1024 * 1024,
				SyncMode:    "immediate",
				MaxSegments: 5,
			},
			WAL: broker.WALConfig{
				MaxSize:  4 * 1024 * 1024,
				SyncMode: "immediate",
			},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{
				Partitions:    1,
				RetentionTime: "1h",
				Mode:          "unified",
			},
		},
		Logging: broker.LoggingConfig{
			Level:  "warn",
			Format: "text",
			Output: "stdout",
		},
	}

	modify(cfg)

	b, err := broker.NewBroker(cfg)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("create broker: %v", err)
	}
	if err := b.Start(); err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("start broker: %v", err)
	}

	srv := adminhttp.NewAdminServer(b)
	go srv.Serve()

	addr := fmt.Sprintf("http://127.0.0.1:%d", adminPort)
	waitForServer(t, addr)

	tb := &testBroker{
		broker: b,
		server: srv,
		addr:   addr,
		tmpDir: tmpDir,
	}
	t.Cleanup(tb.close)
	return tb
}
