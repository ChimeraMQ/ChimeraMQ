package chimera

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --- Error path tests for 75% coverage functions ---

func TestHealthError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, map[string]interface{}{"error": "boom"})
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.Health()
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestListTopicsError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.ListTopics()
	if err == nil {
		t.Error("expected error for 503 response")
	}
}

func TestGetTopicError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		writeJSON(w, map[string]interface{}{"error": "topic not found"})
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.GetTopic("missing")
	if err == nil {
		t.Error("expected error for 404 response")
	}
}

func TestListConsumerGroupsError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.ListConsumerGroups()
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestJoinGroupError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.JoinGroup("g", "topic", "m1")
	if err == nil {
		t.Error("expected error for 400 response")
	}
}

func TestGetOffsetsError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.GetOffsets("g")
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestRegisterSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.RegisterSchema("subj", "json", `{}`)
	if err == nil {
		t.Error("expected error for 409 response")
	}
}

func TestGetLatestSchemaError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.GetLatestSchema("subj")
	if err == nil {
		t.Error("expected error for 404 response")
	}
}

func TestPeekDLQError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.PeekDLQ("topic")
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestClusterMembersError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.ClusterMembers()
	if err == nil {
		t.Error("expected error for 503 response")
	}
}

// --- Publish and Fetch error paths ---

func TestPublishError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte("rate limited"))
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.Publish("topic", []byte("data"))
	if err == nil {
		t.Error("expected error for 429 response")
	}
}

func TestPublishInvalidJSON(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.Publish("topic", []byte("data"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestFetchError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.Fetch("topic", 0, 0, 10)
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

// --- doRequest edge cases ---

func TestDoRequestNetworkError(t *testing.T) {
	c := NewClient("http://127.0.0.1:1") // port 1 should refuse connection
	_, err := c.Health()
	if err == nil {
		t.Error("expected network error")
	}
}

func TestDoRequestInvalidJSON(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{invalid"))
	}))
	defer s.Close()

	c := NewClient(s.URL)
	_, err := c.ListTopics()
	if err == nil {
		t.Error("expected JSON parse error")
	}
}

// --- Post/Delete error paths ---

func TestCreateTopicError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	err := c.CreateTopic("t", "stream", 1)
	if err == nil {
		t.Error("expected error for 400 response")
	}
}

func TestDeleteTopicError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	err := c.DeleteTopic("t")
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestAckError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	err := c.Ack("t", 0)
	if err == nil {
		t.Error("expected error for 409 response")
	}
}

func TestNackError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	err := c.Nack("t", 0)
	if err == nil {
		t.Error("expected error for 409 response")
	}
}

func TestLeaveGroupError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	err := c.LeaveGroup("g", "m1")
	if err == nil {
		t.Error("expected error for 400 response")
	}
}

func TestHeartbeatError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	err := c.Heartbeat("g", "m1")
	if err == nil {
		t.Error("expected error for 404 response")
	}
}

func TestCommitOffsetsError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	err := c.CommitOffsets("g", map[int]int64{0: 1})
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestClearDLQError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	err := c.ClearDLQ("t")
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestReplayDLQError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	err := c.ReplayDLQ("t")
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

// --- Success paths for partially-covered functions ---

func TestGetTopicSuccess(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]interface{}{"name": "t1", "mode": "stream", "partitions": 4})
	}))
	defer s.Close()

	c := NewClient(s.URL)
	topic, err := c.GetTopic("t1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topic.Name != "t1" {
		t.Errorf("expected name t1, got %s", topic.Name)
	}
}

func TestGetNilResult(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer s.Close()

	c := NewClient(s.URL)
	if err := c.get("/v1/health", nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDoRequestMarshalError(t *testing.T) {
	c := NewClient("http://127.0.0.1:1")
	_, _, err := c.doRequest("POST", "/v1/topics", make(chan int))
	if err == nil {
		t.Error("expected marshal error")
	}
}

func TestDoRequestNewRequestError(t *testing.T) {
	c := NewClient("http://127.0.0.1:1")
	_, _, err := c.doRequest("BAD\nMETHOD", "/v1/topics", nil)
	if err == nil {
		t.Error("expected new request error")
	}
}

type errorBody struct{}

func (errorBody) Read(p []byte) (int, error) { return 0, errors.New("read error") }
func (errorBody) Close() error               { return nil }

type errorReadTripper struct{}

func (errorReadTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: 200,
		Body:       errorBody{},
		Header:     make(http.Header),
	}, nil
}

func TestDoRequestReadError(t *testing.T) {
	c := NewClient("http://example.com", WithHTTPClient(&http.Client{Transport: errorReadTripper{}}))
	_, _, err := c.doRequest("GET", "/", nil)
	if err == nil {
		t.Error("expected read error")
	}
}

func TestPublishNewRequestError(t *testing.T) {
	c := NewClient("http://127.0.0.1:1")
	_, err := c.Publish("bad\n", []byte("x"))
	if err == nil {
		t.Error("expected new request error")
	}
}

func TestPublishNetworkError(t *testing.T) {
	c := NewClient("http://127.0.0.1:1")
	_, err := c.Publish("topic", []byte("x"))
	if err == nil {
		t.Error("expected network error")
	}
}
