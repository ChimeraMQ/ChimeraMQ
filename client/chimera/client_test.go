package chimera

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func setupTestServer(handler http.HandlerFunc) *Client {
	s := httptest.NewServer(handler)
	t := &testing.T{}
	t.Cleanup(s.Close)
	_ = t
	return NewClient(s.URL, WithToken("test-token"))
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	json.NewEncoder(w).Encode(v)
}

func TestHealth(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health" {
			t.Errorf("expected /v1/health, got %s", r.URL.Path)
		}
		writeJSON(w, map[string]interface{}{
			"status":  "healthy",
			"node_id": 1,
			"name":    "test-node",
			"uptime":  "1h0m0s",
		})
	})

	h, err := c.Health()
	if err != nil {
		t.Fatalf("Health() error: %v", err)
	}
	if h.Status != "healthy" {
		t.Errorf("expected healthy, got %s", h.Status)
	}
	if h.NodeID != 1 {
		t.Errorf("expected node_id 1, got %d", h.NodeID)
	}
}

func TestCreateTopic(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/v1/topics" {
			t.Errorf("expected /v1/topics, got %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing Authorization header")
		}
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		if body["name"] != "test-topic" {
			t.Errorf("expected name=test-topic, got %v", body["name"])
		}
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, map[string]interface{}{"name": "test-topic"})
	})

	if err := c.CreateTopic("test-topic", "unified", 8); err != nil {
		t.Fatalf("CreateTopic() error: %v", err)
	}
}

func TestPublish(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/octet-stream" {
			t.Errorf("expected octet-stream, got %s", r.Header.Get("Content-Type"))
		}
		writeJSON(w, map[string]interface{}{
			"offset":    float64(42),
			"partition": float64(3),
		})
	})

	result, err := c.Publish("orders", []byte(`{"order":1}`))
	if err != nil {
		t.Fatalf("Publish() error: %v", err)
	}
	if result.Offset != 42 {
		t.Errorf("expected offset 42, got %d", result.Offset)
	}
	if result.Partition != 3 {
		t.Errorf("expected partition 3, got %d", result.Partition)
	}
}

func TestFetch(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/messages/orders" {
			t.Errorf("expected /v1/messages/orders, got %s", r.URL.Path)
		}
		if r.URL.Query().Get("offset") != "0" {
			t.Errorf("expected offset=0, got %s", r.URL.Query().Get("offset"))
		}
		writeJSON(w, map[string]interface{}{
			"count":       float64(1),
			"next_offset": float64(1),
			"messages": []interface{}{
				map[string]interface{}{
					"offset":    float64(0),
					"partition": float64(0),
					"data":      "hello",
				},
			},
		})
	})

	result, err := c.Fetch("orders", 0, 0, 10)
	if err != nil {
		t.Fatalf("Fetch() error: %v", err)
	}
	if result.Count != 1 {
		t.Errorf("expected count 1, got %d", result.Count)
	}
	if len(result.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(result.Messages))
	}
}

func TestAPIError(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]interface{}{
			"error": "topic not found",
		})
	})

	_, err := c.GetTopic("nonexistent")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	apiErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected *Error, got %T", err)
	}
	if apiErr.StatusCode != 404 {
		t.Errorf("expected 404, got %d", apiErr.StatusCode)
	}
	if apiErr.Message != "topic not found" {
		t.Errorf("expected 'topic not found', got %s", apiErr.Message)
	}
}

func TestAuthHeader(t *testing.T) {
	var gotAuth string
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		writeJSON(w, []interface{}{})
	})

	c.ListTopics()
	if gotAuth != "Bearer test-token" {
		t.Errorf("expected Bearer test-token, got %s", gotAuth)
	}
}

func TestNoAuthWhenNoToken(t *testing.T) {
	var gotAuth string
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		writeJSON(w, []interface{}{})
	}))
	defer s.Close()

	c := NewClient(s.URL)
	c.ListTopics()
	if gotAuth != "" {
		t.Errorf("expected no auth header, got %s", gotAuth)
	}
}

func TestJoinGroup(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/consumers/my-group/join" {
			t.Errorf("expected /v1/consumers/my-group/join, got %s", r.URL.Path)
		}
		writeJSON(w, map[string]interface{}{
			"member_id":  "c1",
			"partitions": []interface{}{float64(0), float64(1)},
			"generation": float64(1),
		})
	})

	result, err := c.JoinGroup("my-group", "orders", "c1")
	if err != nil {
		t.Fatalf("JoinGroup() error: %v", err)
	}
	if result.MemberID != "c1" {
		t.Errorf("expected member_id c1, got %s", result.MemberID)
	}
	if len(result.Partitions) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(result.Partitions))
	}
}

func TestDeleteTopic(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path != "/v1/topics/test-topic" {
			t.Errorf("expected /v1/topics/test-topic, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	})

	if err := c.DeleteTopic("test-topic"); err != nil {
		t.Fatalf("DeleteTopic() error: %v", err)
	}
}
