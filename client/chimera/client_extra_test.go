package chimera

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestErrorString(t *testing.T) {
	err := &Error{StatusCode: 404, Message: "not found"}
	if err.Error() != "chimera: not found (HTTP 404)" {
		t.Errorf("Error() = %q, want chimera: not found (HTTP 404)", err.Error())
	}
}

func TestWithHTTPClient(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		writeJSON(w, []interface{}{})
	}))
	defer s.Close()

	customClient := &http.Client{Timeout: 5 * time.Second}
	c := NewClient(s.URL, WithHTTPClient(customClient))
	if c.http != customClient {
		t.Error("WithHTTPClient did not set custom client")
	}
}

func TestAck(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/v1/messages/orders/ack" {
			t.Errorf("expected /v1/messages/orders/ack, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		writeJSON(w, map[string]interface{}{"acked": 1})
	})

	if err := c.Ack("orders", 42); err != nil {
		t.Fatalf("Ack() error: %v", err)
	}
}

func TestNack(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/v1/messages/orders/nack" {
			t.Errorf("expected /v1/messages/orders/nack, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		writeJSON(w, map[string]interface{}{"nacked": 1})
	})

	if err := c.Nack("orders", 42); err != nil {
		t.Fatalf("Nack() error: %v", err)
	}
}

func TestListConsumerGroups(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/consumers" {
			t.Errorf("expected /v1/consumers, got %s", r.URL.Path)
		}
		writeJSON(w, []map[string]interface{}{
			{"group": "g1", "topic": "t1", "members": []string{"m1"}},
		})
	})

	groups, err := c.ListConsumerGroups()
	if err != nil {
		t.Fatalf("ListConsumerGroups() error: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if groups[0].Group != "g1" {
		t.Errorf("expected group g1, got %s", groups[0].Group)
	}
}

func TestLeaveGroup(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/consumers/my-group/leave" {
			t.Errorf("expected /v1/consumers/my-group/leave, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})

	if err := c.LeaveGroup("my-group", "c1"); err != nil {
		t.Fatalf("LeaveGroup() error: %v", err)
	}
}

func TestHeartbeat(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/consumers/my-group/heartbeat" {
			t.Errorf("expected /v1/consumers/my-group/heartbeat, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})

	if err := c.Heartbeat("my-group", "c1"); err != nil {
		t.Fatalf("Heartbeat() error: %v", err)
	}
}

func TestCommitOffsets(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/consumers/my-group/offsets" {
			t.Errorf("expected /v1/consumers/my-group/offsets, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})

	if err := c.CommitOffsets("my-group", map[int]int64{0: 10}); err != nil {
		t.Fatalf("CommitOffsets() error: %v", err)
	}
}

func TestGetOffsets(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/consumers/my-group/offsets" {
			t.Errorf("expected /v1/consumers/my-group/offsets, got %s", r.URL.Path)
		}
		writeJSON(w, map[string]interface{}{"0": float64(10)})
	})

	offsets, err := c.GetOffsets("my-group")
	if err != nil {
		t.Fatalf("GetOffsets() error: %v", err)
	}
	if offsets[0] != 10 {
		t.Errorf("expected offset 10, got %d", offsets[0])
	}
}

func TestRegisterSchema(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/schemas/orders" {
			t.Errorf("expected /v1/schemas/orders, got %s", r.URL.Path)
		}
		writeJSON(w, map[string]interface{}{
			"subject": "orders",
			"version": float64(1),
			"type":    "json",
		})
	})

	schema, err := c.RegisterSchema("orders", "json", `{"type":"object"}`)
	if err != nil {
		t.Fatalf("RegisterSchema() error: %v", err)
	}
	if schema.Subject != "orders" {
		t.Errorf("expected subject orders, got %s", schema.Subject)
	}
}

func TestGetLatestSchema(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/schemas/orders/latest" {
			t.Errorf("expected /v1/schemas/orders/latest, got %s", r.URL.Path)
		}
		writeJSON(w, map[string]interface{}{
			"subject": "orders",
			"version": float64(2),
		})
	})

	schema, err := c.GetLatestSchema("orders")
	if err != nil {
		t.Fatalf("GetLatestSchema() error: %v", err)
	}
	if schema.Version != 2 {
		t.Errorf("expected version 2, got %d", schema.Version)
	}
}

func TestPeekDLQ(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/dlq/orders" {
			t.Errorf("expected /v1/dlq/orders, got %s", r.URL.Path)
		}
		writeJSON(w, []map[string]interface{}{
			{"offset": float64(1), "reason": "timeout"},
		})
	})

	entries, err := c.PeekDLQ("orders")
	if err != nil {
		t.Fatalf("PeekDLQ() error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Reason != "timeout" {
		t.Errorf("expected reason timeout, got %s", entries[0].Reason)
	}
}

func TestClearDLQ(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if r.URL.Path != "/v1/dlq/orders" {
			t.Errorf("expected /v1/dlq/orders, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})

	if err := c.ClearDLQ("orders"); err != nil {
		t.Fatalf("ClearDLQ() error: %v", err)
	}
}

func TestReplayDLQ(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/v1/dlq/orders/replay" {
			t.Errorf("expected /v1/dlq/orders/replay, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	})

	if err := c.ReplayDLQ("orders"); err != nil {
		t.Fatalf("ReplayDLQ() error: %v", err)
	}
}

func TestClusterMembers(t *testing.T) {
	c := setupTestServer(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/cluster/members" {
			t.Errorf("expected /v1/cluster/members, got %s", r.URL.Path)
		}
		writeJSON(w, []map[string]interface{}{
			{"node_id": float64(1), "address": "127.0.0.1:5672", "role": "leader", "status": "alive"},
		})
	})

	members, err := c.ClusterMembers()
	if err != nil {
		t.Fatalf("ClusterMembers() error: %v", err)
	}
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0].Role != "leader" {
		t.Errorf("expected role leader, got %s", members[0].Role)
	}
}
