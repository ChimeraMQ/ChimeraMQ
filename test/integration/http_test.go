package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

// --- Topic CRUD ---

func TestHTTPCreateTopic(t *testing.T) {
	tb := newTestBroker(t)

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "test-topic",
		"mode":       "stream",
		"partitions": 4,
	})

	resp, err := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if result["name"] != "test-topic" {
		t.Errorf("expected name=test-topic, got %v", result["name"])
	}
}

func TestHTTPCreateTopicDefaultPartitions(t *testing.T) {
	tb := newTestBroker(t)

	body, _ := json.Marshal(map[string]interface{}{
		"name": "defaults-topic",
		"mode": "queue",
	})

	resp, err := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	// Default partitions from config is 4
	if result["partitions"] != float64(4) {
		t.Errorf("expected default partitions=4, got %v", result["partitions"])
	}
}

func TestHTTPCreateDuplicateTopic(t *testing.T) {
	tb := newTestBroker(t)

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "dup-topic",
		"mode":       "unified",
		"partitions": 2,
	})

	resp1, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp1.Body.Close()

	resp2, err := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("duplicate create: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusConflict {
		t.Errorf("expected 409 for duplicate, got %d", resp2.StatusCode)
	}
}

func TestHTTPListTopics(t *testing.T) {
	tb := newTestBroker(t)

	// Create two topics
	for _, name := range []string{"topic-a", "topic-b"} {
		body, _ := json.Marshal(map[string]interface{}{
			"name":       name,
			"mode":       "stream",
			"partitions": 2,
		})
		resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
		resp.Body.Close()
	}

	resp, err := http.Get(tb.addr + "/v1/topics")
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var topics []map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&topics)

	if len(topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(topics))
	}
}

func TestHTTPGetTopic(t *testing.T) {
	tb := newTestBroker(t)

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "describe-me",
		"mode":       "queue",
		"partitions": 4,
	})
	resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	resp, err := http.Get(tb.addr + "/v1/topics/describe-me")
	if err != nil {
		t.Fatalf("get topic: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if result["name"] != "describe-me" {
		t.Errorf("expected name=describe-me, got %v", result["name"])
	}
}

func TestHTTPGetTopicNotFound(t *testing.T) {
	tb := newTestBroker(t)

	resp, err := http.Get(tb.addr + "/v1/topics/nonexistent")
	if err != nil {
		t.Fatalf("get topic: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestHTTPDeleteTopic(t *testing.T) {
	tb := newTestBroker(t)

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "delete-me",
		"mode":       "stream",
		"partitions": 2,
	})
	resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	req, _ := http.NewRequest("DELETE", tb.addr+"/v1/topics/delete-me", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete topic: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	// Verify it's gone
	resp2, err := http.Get(tb.addr + "/v1/topics/delete-me")
	if err != nil {
		t.Fatalf("get deleted topic: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 after delete, got %d", resp2.StatusCode)
	}
}

// --- Publish ---

func TestHTTPPublish(t *testing.T) {
	tb := newTestBroker(t)

	// Create topic first
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "pub-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	// Publish message
	msg := []byte(`{"hello":"world"}`)
	resp, err := http.Post(tb.addr+"/v1/messages/pub-topic", "application/json", bytes.NewReader(msg))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if result["topic"] != "pub-topic" {
		t.Errorf("expected topic=pub-topic, got %v", result["topic"])
	}
	if result["offset"] == nil {
		t.Error("expected non-nil offset")
	}
	if result["partition"] == nil {
		t.Error("expected non-nil partition")
	}
}

func TestHTTPPublishToNonexistentTopic(t *testing.T) {
	tb := newTestBroker(t)

	msg := []byte("data")
	resp, err := http.Post(tb.addr+"/v1/messages/no-such-topic", "text/plain", bytes.NewReader(msg))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", resp.StatusCode)
	}
}

// --- Health & Metrics ---

func TestHTTPHealth(t *testing.T) {
	tb := newTestBroker(t)

	resp, err := http.Get(tb.addr + "/v1/health")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	if result["status"] != "healthy" {
		t.Errorf("expected status=healthy, got %v", result["status"])
	}
	if result["node_id"] == nil {
		t.Error("expected node_id")
	}
}

func TestHTTPMetrics(t *testing.T) {
	tb := newTestBroker(t)

	// Publish something to generate metrics
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "metrics-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	msg := []byte("metrics-msg")
	resp, _ = http.Post(tb.addr+"/v1/messages/metrics-topic", "text/plain", bytes.NewReader(msg))
	resp.Body.Close()

	resp, err := http.Get(tb.addr + "/v1/metrics")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	ct := resp.Header.Get("Content-Type")
	if ct != "text/plain" {
		t.Errorf("expected text/plain content-type, got %s", ct)
	}

	respBody, _ := io.ReadAll(resp.Body)
	if len(respBody) == 0 {
		t.Error("expected non-empty metrics output")
	}
}

func TestHTTPMultiplePublish(t *testing.T) {
	tb := newTestBroker(t)

	// Create topic
	body, _ := json.Marshal(map[string]interface{}{
		"name":       "multi-pub",
		"mode":       "unified",
		"partitions": 2,
	})
	resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	// Publish 10 messages
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("message-%d", i))
		resp, err := http.Post(tb.addr+"/v1/messages/multi-pub", "text/plain", bytes.NewReader(msg))
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("publish %d: expected 200, got %d", i, resp.StatusCode)
		}
	}
}

// --- Stream Fetch ---

func TestHTTPFetch(t *testing.T) {
	tb := newTestBroker(t)

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "fetch-topic",
		"mode":       "stream",
		"partitions": 1,
	})
	resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	for i := 0; i < 3; i++ {
		msg := []byte(fmt.Sprintf("msg-%d", i))
		resp, _ := http.Post(tb.addr+"/v1/messages/fetch-topic", "text/plain", bytes.NewReader(msg))
		resp.Body.Close()
	}

	resp, err := http.Get(tb.addr + "/v1/messages/fetch-topic?partition=0&offset=0&limit=10")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"] != float64(3) {
		t.Errorf("expected count=3, got %v", result["count"])
	}
}

func TestHTTPQueueAckViaHTTP(t *testing.T) {
	tb := newTestBroker(t)

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "ack-http",
		"mode":       "queue",
		"partitions": 1,
	})
	resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	msg := []byte("ack-me")
	resp, _ = http.Post(tb.addr+"/v1/messages/ack-http", "text/plain", bytes.NewReader(msg))
	var pubResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&pubResult)
	resp.Body.Close()

	offset := pubResult["offset"].(float64)

	ackBody, _ := json.Marshal(map[string]interface{}{
		"offsets": []float64{offset},
	})
	resp, err := http.Post(tb.addr+"/v1/messages/ack-http/ack", "application/json", bytes.NewReader(ackBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPQueueNackViaHTTP(t *testing.T) {
	tb := newTestBroker(t)

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "nack-http",
		"mode":       "queue",
		"partitions": 1,
	})
	resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	msg := []byte("nack-me")
	resp, _ = http.Post(tb.addr+"/v1/messages/nack-http", "text/plain", bytes.NewReader(msg))
	var pubResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&pubResult)
	resp.Body.Close()

	offset := pubResult["offset"].(float64)

	nackBody, _ := json.Marshal(map[string]interface{}{
		"offsets": []float64{offset},
	})
	resp, err := http.Post(tb.addr+"/v1/messages/nack-http/nack", "application/json", bytes.NewReader(nackBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPConsumerGroups(t *testing.T) {
	tb := newTestBroker(t)

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "cg-topic",
		"mode":       "stream",
		"partitions": 4,
	})
	resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	tb.broker.StreamEngine().JoinGroup("test-group", "cg-topic", "m1", 4, 0)

	resp, err := http.Get(tb.addr + "/v1/consumers")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"].(float64) < 1 {
		t.Error("expected at least 1 consumer group")
	}
}

func TestHTTPGetConsumerGroup(t *testing.T) {
	tb := newTestBroker(t)

	body, _ := json.Marshal(map[string]interface{}{
		"name":       "cg-detail",
		"mode":       "stream",
		"partitions": 4,
	})
	resp, _ := http.Post(tb.addr+"/v1/topics", "application/json", bytes.NewReader(body))
	resp.Body.Close()

	tb.broker.StreamEngine().JoinGroup("detail-group", "cg-detail", "m1", 4, 0)

	resp, err := http.Get(tb.addr + "/v1/consumers/detail-group")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHTTPGetConsumerGroupNotFound(t *testing.T) {
	tb := newTestBroker(t)

	resp, err := http.Get(tb.addr + "/v1/consumers/nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}
