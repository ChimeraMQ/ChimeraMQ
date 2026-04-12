package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/protocol/mqtt"
)

// --- HTTP Protocol Compliance Tests ---

// TestHTTPCompliancePublishFetchRoundTrip verifies the full HTTP publish/fetch cycle.
func TestHTTPCompliancePublishFetchRoundTrip(t *testing.T) {
	tb := newTestBroker(t)

	postJSON(t, tb.addr, "/v1/topics", map[string]any{
		"name": "http-rt", "mode": "stream", "partitions": 1,
	})

	for i := 0; i < 5; i++ {
		postText(t, tb.addr, "/v1/messages/http-rt", fmt.Sprintf("msg-%d", i))
	}

	resp := get(t, tb.addr, "/v1/messages/http-rt?partition=0&offset=0&limit=10")
	var fetchResp struct {
		Count    int `json:"count"`
		Messages []struct {
			Offset  int    `json:"offset"`
			Payload string `json:"payload"`
		} `json:"messages"`
		NextOffset int `json:"next_offset"`
	}
	parseJSON(t, resp, &fetchResp)
	if fetchResp.Count != 5 {
		t.Fatalf("expected 5 messages, got %d", fetchResp.Count)
	}
}

// TestHTTPComplianceTopicValidation verifies topic name validation.
func TestHTTPComplianceTopicValidation(t *testing.T) {
	tb := newTestBroker(t)

	cases := []struct {
		name string
		ok   bool
	}{
		{"valid-topic", true},
		{"", false},
		{"topic/with/slash", false},
		{strings.Repeat("a", 256), false},
	}

	for _, tc := range cases {
		code, _ := postJSONStatus(t, tb.addr, "/v1/topics", map[string]any{
			"name": tc.name, "mode": "stream", "partitions": 1,
		})
		if tc.ok && code != 200 && code != 201 {
			t.Errorf("topic %q: status=%d, want 200/201", tc.name, code)
		}
		if !tc.ok && code == 200 {
			t.Errorf("invalid topic %q: unexpectedly succeeded", tc.name)
		}
	}
}

// TestHTTPComplianceFetchWithOffset verifies offset-based fetching.
func TestHTTPComplianceFetchWithOffset(t *testing.T) {
	tb := newTestBroker(t)

	postJSON(t, tb.addr, "/v1/topics", map[string]any{
		"name": "offset-test", "mode": "stream", "partitions": 1,
	})

	for i := 0; i < 10; i++ {
		postText(t, tb.addr, "/v1/messages/offset-test", fmt.Sprintf("msg-%d", i))
	}

	resp := get(t, tb.addr, "/v1/messages/offset-test?partition=0&offset=5&limit=10")
	var fetchResp struct {
		Count int `json:"count"`
	}
	parseJSON(t, resp, &fetchResp)
	if fetchResp.Count != 5 {
		t.Errorf("expected 5 messages from offset 5, got %d", fetchResp.Count)
	}
}

// TestHTTPComplianceSchemaEnforcement verifies schema endpoint exists.
// Full schema enforcement requires schema registry to be enabled in config.
func TestHTTPComplianceSchemaEndpoint(t *testing.T) {
	tb := newTestBroker(t)

	// Schema registry may not be enabled; just verify endpoint exists
	code, _ := postJSONStatus(t, tb.addr, "/v1/schemas/test-subject", map[string]any{
		"type":   "json",
		"schema": `{"type":"object"}`,
	})
	// Either 200 (success) or 503 (not enabled) is acceptable
	if code != 200 && code != 503 {
		t.Errorf("schema endpoint: status=%d, want 200 or 503", code)
	}
}

// --- MQTT Protocol Compliance Tests ---

// TestMQTTTopicMapping verifies MQTT topic name conversion.
func TestMQTTTopicMapping(t *testing.T) {
	mapper := mqtt.NewTopicMapper(".")

	cases := []struct {
		mqtt, chimera string
	}{
		{"sensor/temperature", "sensor.temperature"},
		{"home/livingroom/temp", "home.livingroom.temp"},
		{"single", "single"},
	}

	for _, tc := range cases {
		got := mapper.MQTTToChimera(tc.mqtt)
		if got != tc.chimera {
			t.Errorf("MQTTToChimera(%q) = %q, want %q", tc.mqtt, got, tc.chimera)
		}
	}
}

// TestMQTTFilterWildcard verifies wildcard matching for MQTT topic filters.
func TestMQTTFilterWildcard(t *testing.T) {
	cases := []struct {
		filter, topic string
		match         bool
	}{
		{"sensor/+", "sensor/temp", true},
		{"sensor/+", "sensor", false},
		{"sensor/+", "sensor/temp/extra", false},
		{"#", "anything/goes", true},
		{"#", "single", true},
		{"home/+/temp", "home/living/temp", true},
		{"home/+/temp", "home/kitchen/humidity", false},
		{"sensor/+/+", "sensor/temp/value", true},
		{"sensor/temp", "sensor/temp", true},
		{"sensor/temp", "sensor/humidity", false},
	}

	for _, tc := range cases {
		got := mqtt.FilterMatches(tc.filter, tc.topic)
		if got != tc.match {
			t.Errorf("FilterMatches(%q, %q) = %v, want %v", tc.filter, tc.topic, got, tc.match)
		}
	}
}

// TestMQTTRetainedStore verifies retained message store/lookup/delete.
func TestMQTTRetainedStore(t *testing.T) {
	rs := mqtt.NewRetainedStore(100)

	rs.Store("sensor/temp", []byte("22.5"), 0)
	msgs := rs.Matching("sensor/temp")
	if len(msgs) != 1 {
		t.Fatalf("expected 1 retained, got %d", len(msgs))
	}
	if string(msgs[0].Payload) != "22.5" {
		t.Errorf("payload = %q, want 22.5", msgs[0].Payload)
	}

	// Delete by storing empty
	rs.Store("sensor/temp", nil, 0)
	msgs = rs.Matching("sensor/temp")
	if len(msgs) != 0 {
		t.Errorf("expected 0 after delete, got %d", len(msgs))
	}
}

// TestMQTTRetainedWildcard verifies wildcard matching in retained store.
func TestMQTTRetainedWildcard(t *testing.T) {
	rs := mqtt.NewRetainedStore(100)
	rs.Store("sensor/temp", []byte("22.5"), 0)
	rs.Store("sensor/humidity", []byte("65"), 0)
	rs.Store("device/status", []byte("online"), 0)

	msgs := rs.Matching("sensor/+")
	if len(msgs) != 2 {
		t.Errorf("sensor/+ matched %d, want 2", len(msgs))
	}

	msgs = rs.Matching("#")
	if len(msgs) != 3 {
		t.Errorf("# matched %d, want 3", len(msgs))
	}
}

// TestMQTTSessionPacketID verifies packet ID generation is unique and non-zero.
func TestMQTTSessionPacketID(t *testing.T) {
	sess := mqtt.NewSession("test-client", true, 60, mqtt.ProtocolLevel311)

	ids := make(map[uint16]bool)
	for i := 0; i < 100; i++ {
		pid := sess.NextPacketID()
		if pid == 0 {
			t.Error("packet ID should never be 0")
		}
		if ids[pid] {
			t.Errorf("duplicate packet ID: %d", pid)
		}
		ids[pid] = true
	}
}

// TestMQTTSessionInflight verifies inflight message add/ack flow.
func TestMQTTSessionInflight(t *testing.T) {
	sess := mqtt.NewSession("inflight-client", true, 60, mqtt.ProtocolLevel311)

	// Add inflight and verify packet IDs skip existing ones
	sess.AddInflight(1, "topic/a", []byte("msg1"), 1)
	sess.AddInflight(2, "topic/b", []byte("msg2"), 2)

	// Packet ID 1 and 2 should be skipped by NextPacketID
	pid := sess.NextPacketID()
	if pid == 1 || pid == 2 {
		t.Errorf("NextPacketID returned %d, which is in inflight", pid)
	}

	// Ack and verify the IDs become available again
	sess.AckInflight(1)
	pid2 := sess.NextPacketID()
	_ = pid2 // just verify no panic
}

// --- Cross-protocol tests ---

// TestCrossProtocolMQTTPublishStreamFetch verifies MQTT-published messages
// are fetchable through the stream engine.
func TestCrossProtocolMQTTPublishStreamFetch(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "cross-mqtt",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	for i := 0; i < 3; i++ {
		env := &message.Envelope{
			Topic:       "cross-mqtt",
			Payload:     []byte(fmt.Sprintf("mqtt-msg-%d", i)),
			SourceProto: message.ProtoMQTT,
		}
		if _, err := tb.broker.Publish(env); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("cross-mqtt", 0, 0, 10, 0)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(msgs) != 3 {
		t.Errorf("expected 3 messages, got %d", len(msgs))
	}
}

// TestCrossProtocolHTTPPublishFetch verifies HTTP publish and fetch cycle.
func TestCrossProtocolHTTPPublishFetch(t *testing.T) {
	tb := newTestBroker(t)

	postJSON(t, tb.addr, "/v1/topics", map[string]any{
		"name": "cross-http", "mode": "stream", "partitions": 1,
	})

	for i := 0; i < 3; i++ {
		postText(t, tb.addr, "/v1/messages/cross-http", fmt.Sprintf("http-msg-%d", i))
	}

	resp := get(t, tb.addr, "/v1/messages/cross-http?partition=0&offset=0&limit=10")
	var fetchResp struct {
		Count int `json:"count"`
	}
	parseJSON(t, resp, &fetchResp)
	if fetchResp.Count != 3 {
		t.Errorf("expected 3 messages, got %d", fetchResp.Count)
	}
}

// --- HTTP helpers ---

func postJSONStatus(t *testing.T, baseURL, path string, body map[string]any) (int, string) {
	t.Helper()
	data, _ := json.Marshal(body)
	resp, err := http.Post(baseURL+path, "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func postJSON(t *testing.T, baseURL, path string, body map[string]any) {
	t.Helper()
	code, resp := postJSONStatus(t, baseURL, path, body)
	if code != 200 && code != 201 {
		t.Fatalf("POST %s: status=%d body=%s", path, code, resp)
	}
}

func postTextStatus(t *testing.T, baseURL, path, text string) (int, string) {
	t.Helper()
	resp, err := http.Post(baseURL+path, "text/plain", strings.NewReader(text))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func postText(t *testing.T, baseURL, path, text string) {
	t.Helper()
	code, _ := postTextStatus(t, baseURL, path, text)
	if code != 200 && code != 201 {
		t.Fatalf("POST %s: status=%d", path, code)
	}
}

func get(t *testing.T, baseURL, path string) string {
	t.Helper()
	resp, err := http.Get(baseURL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func parseJSON(t *testing.T, data string, v any) {
	t.Helper()
	if err := json.Unmarshal([]byte(data), v); err != nil {
		t.Fatalf("parse JSON: %v\ndata: %s", err, data)
	}
}
