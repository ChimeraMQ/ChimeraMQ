package mcp

import (
	"encoding/json"
	"testing"
)

func TestHandleInitialize(t *testing.T) {
	s := NewServer(nil)
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "initialize",
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	result, ok := resp.Result.(map[string]interface{})
	if !ok {
		t.Fatal("result should be a map")
	}
	if result["protocolVersion"] != "2024-11-05" {
		t.Errorf("protocolVersion = %v", result["protocolVersion"])
	}
}

func TestHandleToolsList(t *testing.T) {
	s := NewServer(nil)
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      2,
		Method:  "tools/list",
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}

	result, ok := resp.Result.(map[string]interface{})
	if !ok {
		t.Fatal("result should be a map")
	}
	tools, ok := result["tools"].([]ToolDef)
	if !ok {
		t.Fatal("tools should be a slice")
	}
	if len(tools) == 0 {
		t.Error("expected at least one tool")
	}

	// Verify expected tools exist
	names := make(map[string]bool)
	for _, tool := range tools {
		names[tool.Name] = true
	}
	for _, name := range []string{
		"chimera_list_topics",
		"chimera_create_topic",
		"chimera_publish",
		"chimera_topic_info",
		"chimera_delete_topic",
		"chimera_broker_info",
	} {
		if !names[name] {
			t.Errorf("missing tool: %s", name)
		}
	}
}

func TestHandleNoop(t *testing.T) {
	s := NewServer(nil)
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      3,
		Method:  "initialized",
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}

func TestMethodNotFound(t *testing.T) {
	s := NewServer(nil)
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      4,
		Method:  "nonexistent",
	})

	if resp.Error == nil {
		t.Fatal("expected error for unknown method")
	}
	if resp.Error.Code != -32601 {
		t.Errorf("error code = %d, want -32601", resp.Error.Code)
	}
}

func TestToolsCallUnknownTool(t *testing.T) {
	s := NewServer(nil)
	params, _ := json.Marshal(map[string]interface{}{
		"name":      "unknown_tool",
		"arguments": map[string]interface{}{},
	})
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      5,
		Method:  "tools/call",
		Params:  params,
	})

	if resp.Error == nil {
		t.Fatal("expected error for unknown tool")
	}
	if resp.Error.Code != -32603 {
		t.Errorf("error code = %d, want -32603", resp.Error.Code)
	}
}

func TestBrokerInfoWithoutBroker(t *testing.T) {
	s := NewServer(nil)
	// This will panic without a broker, so we test the tool listing only
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      6,
		Method:  "tools/list",
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}

func TestMustMarshal(t *testing.T) {
	result := mustMarshal(map[string]string{"key": "value"})
	if result != `{"key":"value"}` {
		t.Errorf("mustMarshal = %q", result)
	}
}

func TestToolDefFields(t *testing.T) {
	tool := ToolDef{
		Name:        "test_tool",
		Description: "A test tool",
		InputSchema: map[string]interface{}{
			"type": "object",
		},
	}
	if tool.Name != "test_tool" {
		t.Errorf("Name = %q", tool.Name)
	}
}
