package mcp

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// toolCreateTopic — error paths
// ---------------------------------------------------------------------------

func TestToolCreateTopic_InvalidJSON(t *testing.T) {
	s := NewServer(nil)
	_, err := s.toolCreateTopic(json.RawMessage(`{invalid`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestToolCreateTopic_DuplicateTopic(t *testing.T) {
	bkr := newTestBroker(t)
	s := NewServer(bkr)

	args, _ := json.Marshal(map[string]interface{}{
		"name":       "dup-topic",
		"partitions": 2,
	})
	_, _ = s.toolCreateTopic(args)

	_, err := s.toolCreateTopic(args)
	if err == nil {
		t.Fatal("expected error for duplicate topic")
	}
}

// ---------------------------------------------------------------------------
// toolPublish — error paths
// ---------------------------------------------------------------------------

func TestToolPublish_InvalidJSON(t *testing.T) {
	s := NewServer(nil)
	_, err := s.toolPublish(json.RawMessage(`not-json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestToolPublish_TopicNotFound(t *testing.T) {
	bkr := newTestBroker(t)
	s := NewServer(bkr)

	args, _ := json.Marshal(map[string]interface{}{
		"topic": "nonexistent",
		"value": "data",
	})
	_, err := s.toolPublish(args)
	if err == nil {
		t.Fatal("expected error when publishing to nonexistent topic")
	}
}

// ---------------------------------------------------------------------------
// toolTopicInfo — error paths
// ---------------------------------------------------------------------------

func TestToolTopicInfo_InvalidJSON(t *testing.T) {
	s := NewServer(nil)
	_, err := s.toolTopicInfo(json.RawMessage(`bad`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestToolTopicInfo_NotFound(t *testing.T) {
	bkr := newTestBroker(t)
	s := NewServer(bkr)

	args, _ := json.Marshal(map[string]interface{}{
		"name": "absent-topic",
	})
	_, err := s.toolTopicInfo(args)
	if err == nil {
		t.Fatal("expected error for missing topic")
	}
}

// ---------------------------------------------------------------------------
// toolDeleteTopic — error paths
// ---------------------------------------------------------------------------

func TestToolDeleteTopic_InvalidJSON(t *testing.T) {
	s := NewServer(nil)
	_, err := s.toolDeleteTopic(json.RawMessage(`{`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestToolDeleteTopic_NotFound(t *testing.T) {
	bkr := newTestBroker(t)
	s := NewServer(bkr)

	args, _ := json.Marshal(map[string]interface{}{
		"name": "ghost-topic",
	})
	_, err := s.toolDeleteTopic(args)
	if err == nil {
		t.Fatal("expected error when deleting nonexistent topic")
	}
}

// ---------------------------------------------------------------------------
// handleRequest — exercised via the unexported handleRequest directly
// ---------------------------------------------------------------------------

func TestHandleRequest_MethodNotFound(t *testing.T) {
	s := NewServer(nil)
	var buf bytes.Buffer
	s.SetWriter(&buf)

	s.handleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      99,
		Method:  "nonexistent_method",
	})

	var resp JSONRPCResponse
	if err := json.Unmarshal(buf.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error for unknown method")
	}
	if resp.Error.Code != -32601 {
		t.Errorf("error code = %d, want -32601", resp.Error.Code)
	}
}

func TestHandleRequest_HandlerError(t *testing.T) {
	s := NewServer(nil)
	var buf bytes.Buffer
	s.SetWriter(&buf)

	// tools/call with invalid JSON params causes handleToolsCall to return
	// an error, which triggers the handler-error branch in handleRequest.
	s.handleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      100,
		Method:  "tools/call",
		Params:  json.RawMessage(`invalid-json`),
	})

	var resp JSONRPCResponse
	if err := json.Unmarshal(buf.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error response")
	}
	if resp.Error.Code != -32603 {
		t.Errorf("error code = %d, want -32603", resp.Error.Code)
	}
}

func TestHandleRequest_Success(t *testing.T) {
	s := NewServer(nil)
	var buf bytes.Buffer
	s.SetWriter(&buf)

	s.handleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      101,
		Method:  "initialize",
	})

	var resp JSONRPCResponse
	if err := json.Unmarshal(buf.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
	if resp.ID != 101 {
		t.Errorf("ID = %d, want 101", resp.ID)
	}
}

// ---------------------------------------------------------------------------
// Serve — JSON-RPC loop over stdio
// ---------------------------------------------------------------------------

func TestServe_EOF(t *testing.T) {
	oldStdin := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdin = r
	defer func() { os.Stdin = oldStdin; r.Close() }()

	w.Close() // immediate EOF

	s := NewServer(nil)
	if err := s.Serve(); err != nil {
		t.Fatalf("Serve() returned error: %v", err)
	}
}

func TestServe_ValidRequest(t *testing.T) {
	oldStdin := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()

	var outBuf bytes.Buffer
	s := NewServer(nil)
	s.SetWriter(&outBuf)

	req := JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      200,
		Method:  "initialize",
	}
	reqBytes, _ := json.Marshal(req)
	w.Write(reqBytes)
	w.Write([]byte("\n"))
	w.Close()

	done := make(chan error, 1)
	go func() { done <- s.Serve() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within timeout")
	}

	if outBuf.Len() == 0 {
		t.Fatal("expected output after Serve")
	}

	var resp JSONRPCResponse
	if err := json.Unmarshal(outBuf.Bytes(), &resp); err != nil {
		t.Fatalf("invalid response JSON: %v", err)
	}
	if resp.ID != 200 {
		t.Errorf("response ID = %d, want 200", resp.ID)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}

func TestServe_MultipleRequests(t *testing.T) {
	oldStdin := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()

	var outBuf bytes.Buffer
	s := NewServer(nil)
	s.SetWriter(&outBuf)

	for i := 300; i < 303; i++ {
		req := JSONRPCRequest{
			JSONRPC: "2.0",
			ID:      i,
			Method:  "initialized",
		}
		b, _ := json.Marshal(req)
		w.Write(b)
		w.Write([]byte("\n"))
	}
	w.Close()

	done := make(chan error, 1)
	go func() { done <- s.Serve() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within timeout")
	}

	decoder := json.NewDecoder(&outBuf)
	count := 0
	for decoder.More() {
		var resp JSONRPCResponse
		if err := decoder.Decode(&resp); err != nil {
			t.Fatalf("failed to decode response %d: %v", count, err)
		}
		count++
	}
	if count != 3 {
		t.Errorf("got %d responses, want 3", count)
	}
}

func TestServe_WithBrokerTool(t *testing.T) {
	bkr := newTestBroker(t)

	oldStdin := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdin = r
	defer func() { os.Stdin = oldStdin }()

	var outBuf bytes.Buffer
	s := NewServer(bkr)
	s.SetWriter(&outBuf)

	params, _ := json.Marshal(map[string]interface{}{
		"name": "chimera_create_topic",
		"arguments": map[string]interface{}{
			"name":       "serve-topic",
			"partitions": 2,
		},
	})
	req := JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      400,
		Method:  "tools/call",
		Params:  params,
	}
	b, _ := json.Marshal(req)
	w.Write(b)
	w.Write([]byte("\n"))
	w.Close()

	done := make(chan error, 1)
	go func() { done <- s.Serve() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within timeout")
	}

	if outBuf.Len() == 0 {
		t.Fatal("expected output")
	}

	var resp JSONRPCResponse
	if err := json.Unmarshal(outBuf.Bytes(), &resp); err != nil {
		t.Fatalf("invalid response: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error in response: %v", resp.Error)
	}

	tc, ok := bkr.Topics().GetTopic("serve-topic")
	if !ok {
		t.Fatal("serve-topic should exist")
	}
	if tc.Partitions != 2 {
		t.Errorf("partitions = %d, want 2", tc.Partitions)
	}
}

// ---------------------------------------------------------------------------
// tools/call — tool error wrapping (isError path)
// ---------------------------------------------------------------------------

func TestToolsCall_ToolReturnsError(t *testing.T) {
	bkr := newTestBroker(t)
	s := NewServer(bkr)

	params, _ := json.Marshal(map[string]interface{}{
		"name":      "chimera_topic_info",
		"arguments": map[string]interface{}{"name": "no-such-topic"},
	})
	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      500,
		Method:  "tools/call",
		Params:  params,
	})

	// tools/call wraps tool errors as success with isError content.
	if resp.Error != nil {
		t.Fatalf("tools/call should not return RPC error: %v", resp.Error)
	}

	result, ok := resp.Result.(map[string]interface{})
	if !ok {
		t.Fatal("result should be a map")
	}

	content, ok := result["content"].([]map[string]interface{})
	if !ok || len(content) == 0 {
		t.Fatal("expected content array with at least one entry")
	}
	if !strings.Contains(content[0]["text"].(string), "Error:") {
		t.Errorf("expected error text, got: %v", content[0]["text"])
	}

	isErr, _ := result["isError"].(bool)
	if !isErr {
		t.Error("expected isError = true")
	}
}

// ---------------------------------------------------------------------------
// tools/call — bad params type
// ---------------------------------------------------------------------------

func TestToolsCall_InvalidParamsParsing(t *testing.T) {
	s := NewServer(nil)

	resp := s.HandleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      501,
		Method:  "tools/call",
		Params:  json.RawMessage(`{"name": 123}`),
	})

	if resp.Error == nil {
		t.Fatal("expected error for bad params type")
	}
}

// ---------------------------------------------------------------------------
// handleRequest — broker info via tools/call
// ---------------------------------------------------------------------------

func TestHandleRequest_BrokerInfo(t *testing.T) {
	bkr := newTestBroker(t)
	s := NewServer(bkr)
	var buf bytes.Buffer
	s.SetWriter(&buf)

	params, _ := json.Marshal(map[string]interface{}{
		"name":      "chimera_broker_info",
		"arguments": map[string]interface{}{},
	})
	s.handleRequest(JSONRPCRequest{
		JSONRPC: "2.0",
		ID:      600,
		Method:  "tools/call",
		Params:  params,
	})

	var resp JSONRPCResponse
	if err := json.Unmarshal(buf.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}
