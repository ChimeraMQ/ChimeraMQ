package http

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandleCreateExchange(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name":    "test-ex",
		"type":    "topic",
		"durable": true,
	})
	resp := doRequest(t, srv, "POST", "/v1/exchanges", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["name"] != "test-ex" {
		t.Errorf("name = %v, want test-ex", result["name"])
	}
}

func TestHandleCreateExchangeInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/exchanges", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleCreateExchangeInvalidName(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name": "",
		"type": "topic",
	})
	resp := doRequest(t, srv, "POST", "/v1/exchanges", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleCreateExchangeDuplicate(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name": "dup-ex",
		"type": "topic",
	})
	doRequest(t, srv, "POST", "/v1/exchanges", body)
	resp := doRequest(t, srv, "POST", "/v1/exchanges", body)
	// Exchange declaration is idempotent; may return 201 or 409
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		t.Errorf("status = %d, want 201 or 409", resp.StatusCode)
	}
}

func TestHandleListExchanges(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name": "list-ex",
		"type": "fanout",
	})
	doRequest(t, srv, "POST", "/v1/exchanges", body)

	resp := doRequest(t, srv, "GET", "/v1/exchanges", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"] == nil {
		t.Error("expected count in response")
	}
}

func TestHandleGetExchange(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name": "get-ex",
		"type": "direct",
	})
	doRequest(t, srv, "POST", "/v1/exchanges", body)

	resp := doRequest(t, srv, "GET", "/v1/exchanges/get-ex", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["name"] != "get-ex" {
		t.Errorf("name = %v, want get-ex", result["name"])
	}
}

func TestHandleGetExchangeNotFound(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/exchanges/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleDeleteExchange(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name": "del-ex",
		"type": "topic",
	})
	doRequest(t, srv, "POST", "/v1/exchanges", body)

	resp := doRequest(t, srv, "DELETE", "/v1/exchanges/del-ex", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	resp2 := doRequest(t, srv, "GET", "/v1/exchanges/del-ex", nil)
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("after delete status = %d, want 404", resp2.StatusCode)
	}
}

func TestHandleDeleteExchangeNotFound(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/exchanges/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleBindExchange(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	// Create exchange and topic
	exBody, _ := json.Marshal(map[string]interface{}{
		"name": "bind-ex",
		"type": "topic",
	})
	doRequest(t, srv, "POST", "/v1/exchanges", exBody)

	topicBody, _ := json.Marshal(map[string]interface{}{
		"name":       "bind-dest",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", topicBody)

	bindBody, _ := json.Marshal(map[string]interface{}{
		"key":         "test.key",
		"destination": "bind-dest",
	})
	resp := doRequest(t, srv, "POST", "/v1/exchanges/bind-ex/bindings", bindBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleBindExchangeInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/exchanges/some-ex/bindings", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleBindExchangeNoDestination(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"key": "test.key",
	})
	resp := doRequest(t, srv, "POST", "/v1/exchanges/some-ex/bindings", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleUnbindExchange(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	exBody, _ := json.Marshal(map[string]interface{}{
		"name": "unbind-ex",
		"type": "topic",
	})
	doRequest(t, srv, "POST", "/v1/exchanges", exBody)

	topicBody, _ := json.Marshal(map[string]interface{}{
		"name":       "unbind-dest",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", topicBody)

	bindBody, _ := json.Marshal(map[string]interface{}{
		"key":         "k",
		"destination": "unbind-dest",
	})
	doRequest(t, srv, "POST", "/v1/exchanges/unbind-ex/bindings", bindBody)

	unbindBody, _ := json.Marshal(map[string]interface{}{
		"key":         "k",
		"destination": "unbind-dest",
	})
	resp := doRequest(t, srv, "DELETE", "/v1/exchanges/unbind-ex/bindings", unbindBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleUnbindExchangeInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/exchanges/some-ex/bindings", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandlePublishToExchange(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	exBody, _ := json.Marshal(map[string]interface{}{
		"name": "pub-ex",
		"type": "fanout",
	})
	doRequest(t, srv, "POST", "/v1/exchanges", exBody)

	topicBody, _ := json.Marshal(map[string]interface{}{
		"name":       "pub-dest",
		"mode":       "stream",
		"partitions": 1,
	})
	doRequest(t, srv, "POST", "/v1/topics", topicBody)

	bindBody, _ := json.Marshal(map[string]interface{}{
		"key":         "",
		"destination": "pub-dest",
	})
	doRequest(t, srv, "POST", "/v1/exchanges/pub-ex/bindings", bindBody)

	pubBody, _ := json.Marshal(map[string]interface{}{
		"routing_key": "rk",
		"payload":     []byte("hello"),
	})
	resp := doRequest(t, srv, "POST", "/v1/exchanges/pub-ex/publish", pubBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["routed"] == nil {
		t.Error("expected routed in response")
	}
}

func TestHandlePublishToExchangeInvalidJSON(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/exchanges/some-ex/publish", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandlePublishToExchangeNotFound(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"routing_key": "rk",
		"payload":     []byte("hello"),
	})
	resp := doRequest(t, srv, "POST", "/v1/exchanges/nonexistent/publish", body)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}
