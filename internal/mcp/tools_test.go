package mcp

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
)

func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()
	dir, err := os.MkdirTemp("", "mcp-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "test", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 0, AdminPort: 0},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "error", Format: "text", Output: "stdout"},
	}

	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bkr.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bkr.Stop() })
	return bkr
}

func TestMCPCreateTopic(t *testing.T) {
	bkr := newTestBroker(t)
	s := NewServer(bkr)

	params, _ := json.Marshal(map[string]interface{}{
		"name": "chimera_create_topic",
		"arguments": map[string]interface{}{
			"name":       "mcp-test-topic",
			"partitions": 4,
		},
	})
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      10,
		Method:  "tools/call",
		Params:  params,
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	// Verify topic was created
	tc, ok := bkr.Topics().GetTopic("mcp-test-topic")
	if !ok {
		t.Fatal("topic should exist")
	}
	if tc.Partitions != 4 {
		t.Errorf("partitions = %d, want 4", tc.Partitions)
	}
}

func TestMCPPublish(t *testing.T) {
	bkr := newTestBroker(t)
	bkr.Topics().CreateTopic(broker.TopicConfig{
		Name:       "pub-test",
		Mode:       broker.ModeUnified,
		Partitions: 1,
	})

	s := NewServer(bkr)

	params, _ := json.Marshal(map[string]interface{}{
		"name": "chimera_publish",
		"arguments": map[string]interface{}{
			"topic": "pub-test",
			"key":   "order-1",
			"value": "hello-world",
		},
	})
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      11,
		Method:  "tools/call",
		Params:  params,
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}

func TestMCPListTopics(t *testing.T) {
	bkr := newTestBroker(t)
	bkr.Topics().CreateTopic(broker.TopicConfig{Name: "t1", Mode: broker.ModeUnified, Partitions: 2})
	bkr.Topics().CreateTopic(broker.TopicConfig{Name: "t2", Mode: broker.ModeStream, Partitions: 4})

	s := NewServer(bkr)

	params, _ := json.Marshal(map[string]interface{}{
		"name":      "chimera_list_topics",
		"arguments": map[string]interface{}{},
	})
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      12,
		Method:  "tools/call",
		Params:  params,
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}

func TestMCPTopicInfo(t *testing.T) {
	bkr := newTestBroker(t)
	bkr.Topics().CreateTopic(broker.TopicConfig{Name: "info-topic", Mode: broker.ModeQueue, Partitions: 8})

	s := NewServer(bkr)

	params, _ := json.Marshal(map[string]interface{}{
		"name":      "chimera_topic_info",
		"arguments": map[string]interface{}{"name": "info-topic"},
	})
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      13,
		Method:  "tools/call",
		Params:  params,
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}

func TestMCPDeleteTopic(t *testing.T) {
	bkr := newTestBroker(t)
	bkr.Topics().CreateTopic(broker.TopicConfig{Name: "del-me", Mode: broker.ModeUnified, Partitions: 2})

	s := NewServer(bkr)

	params, _ := json.Marshal(map[string]interface{}{
		"name":      "chimera_delete_topic",
		"arguments": map[string]interface{}{"name": "del-me"},
	})
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      14,
		Method:  "tools/call",
		Params:  params,
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	if _, ok := bkr.Topics().GetTopic("del-me"); ok {
		t.Error("topic should be deleted")
	}
}

func TestMCPBrokerInfo(t *testing.T) {
	bkr := newTestBroker(t)
	s := NewServer(bkr)

	params, _ := json.Marshal(map[string]interface{}{
		"name":      "chimera_broker_info",
		"arguments": map[string]interface{}{},
	})
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      15,
		Method:  "tools/call",
		Params:  params,
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}

func TestMCPToolsCallInvalidParams(t *testing.T) {
	s := NewServer(nil)
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      16,
		Method:  "tools/call",
		Params:  json.RawMessage(`invalid json`),
	})

	if resp.Error == nil {
		t.Fatal("expected error for invalid params")
	}
}

func TestMCPServeWritesResponse(t *testing.T) {
	bkr := newTestBroker(t)
	s := NewServer(bkr)

	var buf bytes.Buffer
	s.SetWriter(&buf)

	req := JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      20,
		Method:  "initialize",
	}
	s.handleRequest(req)

	if buf.Len() == 0 {
		t.Fatal("expected output written to writer")
	}

	var resp JSONRPCResponse
	if err := json.Unmarshal(buf.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if resp.ID != 20 {
		t.Errorf("response ID = %d, want 20", resp.ID)
	}
}

func TestMCPCreateTopicDefaultPartitions(t *testing.T) {
	bkr := newTestBroker(t)
	s := NewServer(bkr)

	// Create without specifying partitions — should use broker default (4)
	params, _ := json.Marshal(map[string]interface{}{
		"name":      "chimera_create_topic",
		"arguments": map[string]interface{}{"name": "default-parts"},
	})
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      21,
		Method:  "tools/call",
		Params:  params,
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	tc, ok := bkr.Topics().GetTopic("default-parts")
	if !ok {
		t.Fatal("topic should exist")
	}
	if tc.Partitions != 4 {
		t.Errorf("partitions = %d, want 4 (default)", tc.Partitions)
	}
}
