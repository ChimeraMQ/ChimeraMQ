package grpc

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
)

func setupTestBroker(t *testing.T) (*broker.Broker, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "chimera-grpc-test-*")
	if err != nil {
		t.Fatal(err)
	}

	cfg := &broker.Config{
		Node: broker.NodeConfig{
			ID:      1,
			Name:    "test-node",
			DataDir: dir,
		},
		Listener: broker.ListenerConfig{
			Bind:           "127.0.0.1",
			Port:           0,
			AdminPort:      0,
			MaxConnections: 100,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{
				SegmentSize: 1024 * 1024,
				SyncMode:    "immediate",
			},
			WAL: broker.WALConfig{
				MaxSize:  4 * 1024 * 1024,
				SyncMode: "immediate",
			},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{
				Partitions:    4,
				RetentionTime: "1h",
				Mode:          "unified",
			},
		},
		Logging: broker.LoggingConfig{
			Level:  "warn",
			Format: "text",
		},
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to create broker: %v", err)
	}

	if err := b.Start(); err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to start broker: %v", err)
	}

	cleanup := func() {
		b.Stop()
		os.RemoveAll(dir)
	}

	return b, cleanup
}

func TestNewServer(t *testing.T) {
	b, cleanup := setupTestBroker(t)
	defer cleanup()

	grpcCfg := Config{
		Enabled: true,
		Bind:    "127.0.0.1",
		Port:    0,
	}

	srv, err := NewServer(b, grpcCfg)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	if srv == nil {
		t.Fatal("expected non-nil server")
	}

	if srv.broker != b {
		t.Error("server broker mismatch")
	}

	if srv.grpcServer == nil {
		t.Error("expected non-nil gRPC server")
	}
}

func TestChimeraServicePublish(t *testing.T) {
	b, cleanup := setupTestBroker(t)
	defer cleanup()

	// Create topic first
	if err := b.Topics().CreateTopic(broker.TopicConfig{
		Name:       "test-topic",
		Partitions: 1,
		Mode:       broker.ModeUnified,
	}); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	srv := &chimeraService{server: &Server{broker: b}}

	req := &PublishRequest{
		Topic:       "test-topic",
		Payload:     []byte("test message"),
		ContentType: "text/plain",
		Headers:     map[string]string{"key": "value"},
	}

	resp, err := srv.Publish(context.Background(), req)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	if !resp.Success {
		t.Error("expected success to be true")
	}

	// Offset 0 is valid for first message
	if resp.Partition != 0 {
		t.Errorf("expected partition 0, got %d", resp.Partition)
	}
}

func TestChimeraServicePublishInvalidTopic(t *testing.T) {
	b, cleanup := setupTestBroker(t)
	defer cleanup()

	srv := &chimeraService{server: &Server{broker: b}}

	req := &PublishRequest{
		Topic:       "nonexistent-topic",
		Payload:     []byte("test message"),
		ContentType: "text/plain",
	}

	_, err := srv.Publish(context.Background(), req)
	if err == nil {
		t.Error("expected error for non-existent topic")
	}
}

func TestChimeraServiceCreateTopic(t *testing.T) {
	b, cleanup := setupTestBroker(t)
	defer cleanup()

	srv := &chimeraService{server: &Server{broker: b}}

	req := &CreateTopicRequest{
		Name:       "new-topic",
		Partitions: 3,
		Mode:       TopicMode_STREAM,
	}

	resp, err := srv.CreateTopic(context.Background(), req)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}

	if !resp.Success {
		t.Error("expected success to be true")
	}

	if resp.Name != "new-topic" {
		t.Errorf("expected name 'new-topic', got %s", resp.Name)
	}

	if resp.Partitions != 3 {
		t.Errorf("expected 3 partitions, got %d", resp.Partitions)
	}

	// Try creating duplicate topic - should fail
	_, err = srv.CreateTopic(context.Background(), req)
	if err == nil {
		t.Error("expected error for duplicate topic creation")
	}
}

func TestChimeraServiceCreateTopicAllModes(t *testing.T) {
	b, cleanup := setupTestBroker(t)
	defer cleanup()

	srv := &chimeraService{server: &Server{broker: b}}

	modes := []struct {
		mode     TopicMode
		expected broker.TopicMode
	}{
		{TopicMode_STREAM, broker.ModeStream},
		{TopicMode_QUEUE, broker.ModeQueue},
		{TopicMode_UNIFIED, broker.ModeUnified},
	}

	for i, tc := range modes {
		req := &CreateTopicRequest{
			Name:       "topic-" + string(rune('a'+i)),
			Partitions: 1,
			Mode:       tc.mode,
		}

		resp, err := srv.CreateTopic(context.Background(), req)
		if err != nil {
			t.Fatalf("CreateTopic failed for mode %v: %v", tc.mode, err)
		}

		if !resp.Success {
			t.Errorf("expected success for mode %v", tc.mode)
		}
	}
}

func TestChimeraServiceDeleteTopic(t *testing.T) {
	b, cleanup := setupTestBroker(t)
	defer cleanup()

	// Create topic first
	if err := b.Topics().CreateTopic(broker.TopicConfig{
		Name:       "delete-me",
		Partitions: 1,
		Mode:       broker.ModeUnified,
	}); err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	srv := &chimeraService{server: &Server{broker: b}}

	req := &DeleteTopicRequest{Name: "delete-me"}
	resp, err := srv.DeleteTopic(context.Background(), req)
	if err != nil {
		t.Fatalf("DeleteTopic failed: %v", err)
	}

	if !resp.Success {
		t.Error("expected success to be true")
	}

	// Try deleting non-existent topic
	_, err = srv.DeleteTopic(context.Background(), &DeleteTopicRequest{Name: "nonexistent"})
	if err == nil {
		t.Error("expected error for non-existent topic")
	}
}

func TestChimeraServiceListTopics(t *testing.T) {
	b, cleanup := setupTestBroker(t)
	defer cleanup()

	// Create some topics
	topics := []string{"topic-a", "topic-b", "topic-c"}
	for _, name := range topics {
		if err := b.Topics().CreateTopic(broker.TopicConfig{
			Name:       name,
			Partitions: 1,
			Mode:       broker.ModeUnified,
		}); err != nil {
			t.Fatalf("failed to create topic %s: %v", name, err)
		}
	}

	srv := &chimeraService{server: &Server{broker: b}}

	resp, err := srv.ListTopics(context.Background(), &ListTopicsRequest{})
	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}

	if len(resp.Topics) != len(topics) {
		t.Errorf("expected %d topics, got %d", len(topics), len(resp.Topics))
	}
}

func TestChimeraServiceHealth(t *testing.T) {
	b, cleanup := setupTestBroker(t)
	defer cleanup()

	srv := &chimeraService{server: &Server{broker: b}}

	resp, err := srv.Health(context.Background(), &HealthRequest{})
	if err != nil {
		t.Fatalf("Health failed: %v", err)
	}

	if resp.Status != HealthStatus_HEALTHY {
		t.Errorf("expected HEALTHY status, got %v", resp.Status)
	}

	if resp.Timestamp == 0 {
		t.Error("expected non-zero timestamp")
	}
}

func TestDetector(t *testing.T) {
	d := &Detector{}

	tests := []struct {
		name     string
		peek     []byte
		expected bool
	}{
		{
			name:     "HTTP/2 preface",
			peek:     []byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"),
			expected: true,
		},
		{
			name:     "HTTP/2 preface start",
			peek:     []byte("PRI "),
			expected: true,
		},
		{
			name:     "HTTP request",
			peek:     []byte("GET / HTTP/1.1\r\n"),
			expected: false,
		},
		{
			name:     "MQTT connect",
			peek:     []byte{0x10, 0x0c, 0x00, 0x04},
			expected: false,
		},
		{
			name:     "AMQP header",
			peek:     []byte("AMQP\x00\x00\x09\x01"),
			expected: false,
		},
		{
			name:     "Too short",
			peek:     []byte("PR"),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := d.Detect(tc.peek)
			if result != tc.expected {
				t.Errorf("Detect(%q) = %v, expected %v", tc.peek, result, tc.expected)
			}
		})
	}

	if d.BytesNeeded() != 24 {
		t.Errorf("BytesNeeded() = %d, expected 24", d.BytesNeeded())
	}
}

func TestProtocol(t *testing.T) {
	s := &Server{}
	if s.Protocol() != "grpc" {
		t.Errorf("expected protocol 'grpc', got %s", s.Protocol())
	}
}

func TestHandleConnection(t *testing.T) {
	s := &Server{}
	err := s.HandleConnection(nil, nil)
	if err == nil {
		t.Error("expected error for HandleConnection")
	}
}

func TestMessageTypes(t *testing.T) {
	// Test PublishRequest
	pubReq := &PublishRequest{
		Topic:       "test",
		Payload:     []byte("data"),
		ContentType: "application/json",
		Headers:     map[string]string{"key": "value"},
	}
	if pubReq.Topic != "test" {
		t.Error("PublishRequest topic mismatch")
	}

	// Test PublishResponse
	pubResp := &PublishResponse{
		Offset:    42,
		Partition: 1,
		Success:   true,
		Error:     "",
	}
	if pubResp.Offset != 42 {
		t.Error("PublishResponse offset mismatch")
	}

	// Test SubscribeRequest
	subReq := &SubscribeRequest{
		Topic:     "test",
		Partition: 0,
		Offset:    0,
		BatchSize: 100,
		TimeoutMs: 5000,
		StartFrom: SubscribeRequest_LATEST,
	}
	if subReq.BatchSize != 100 {
		t.Error("SubscribeRequest batch size mismatch")
	}

	// Test MessageResponse
	msgResp := &MessageResponse{
		Topic:       "test",
		Partition:   0,
		Offset:      42,
		Payload:     []byte("data"),
		ContentType: "text/plain",
		Timestamp:   uint64(time.Now().Unix()),
		Headers:     map[string]string{},
	}
	if msgResp.Offset != 42 {
		t.Error("MessageResponse offset mismatch")
	}

	// Test CreateTopicRequest
	createReq := &CreateTopicRequest{
		Name:       "new-topic",
		Partitions: 3,
		Mode:       TopicMode_STREAM,
	}
	if createReq.Mode != TopicMode_STREAM {
		t.Error("CreateTopicRequest mode mismatch")
	}

	// Test TopicResponse
	topicResp := &TopicResponse{
		Name:       "new-topic",
		Partitions: 3,
		Mode:       TopicMode_QUEUE,
		Success:    true,
		Error:      "",
	}
	if !topicResp.Success {
		t.Error("TopicResponse success should be true")
	}

	// Test TopicInfo
	topicInfo := &TopicInfo{
		Name:       "info-topic",
		Partitions: 1,
		Mode:       TopicMode_UNIFIED,
	}
	if topicInfo.Mode != TopicMode_UNIFIED {
		t.Error("TopicInfo mode mismatch")
	}

	// Test ListTopicsResponse
	listResp := &ListTopicsResponse{
		Topics: []*TopicInfo{topicInfo},
	}
	if len(listResp.Topics) != 1 {
		t.Error("ListTopicsResponse topics length mismatch")
	}

	// Test HealthResponse
	healthResp := &HealthResponse{
		Status:    HealthStatus_HEALTHY,
		Timestamp: uint64(time.Now().Unix()),
	}
	if healthResp.Status != HealthStatus_HEALTHY {
		t.Error("HealthResponse status mismatch")
	}
}

func TestTopicModeConstants(t *testing.T) {
	if TopicMode_UNIFIED != 0 {
		t.Error("TopicMode_UNIFIED should be 0")
	}
	if TopicMode_STREAM != 1 {
		t.Error("TopicMode_STREAM should be 1")
	}
	if TopicMode_QUEUE != 2 {
		t.Error("TopicMode_QUEUE should be 2")
	}
}

func TestHealthStatusConstants(t *testing.T) {
	if HealthStatus_HEALTHY != 0 {
		t.Error("HealthStatus_HEALTHY should be 0")
	}
	if HealthStatus_UNHEALTHY != 1 {
		t.Error("HealthStatus_UNHEALTHY should be 1")
	}
	if HealthStatus_DEGRADED != 2 {
		t.Error("HealthStatus_DEGRADED should be 2")
	}
}

func TestSubscribeRequestStartFromConstants(t *testing.T) {
	if SubscribeRequest_EARLIEST != 0 {
		t.Error("SubscribeRequest_EARLIEST should be 0")
	}
	if SubscribeRequest_LATEST != 1 {
		t.Error("SubscribeRequest_LATEST should be 1")
	}
}

func TestServerStop(t *testing.T) {
	b, cleanup := setupTestBroker(t)
	defer cleanup()

	grpcCfg := Config{
		Enabled: true,
		Bind:    "127.0.0.1",
		Port:    0,
	}

	srv, err := NewServer(b, grpcCfg)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	// Stop should not panic
	srv.Stop()

	// Second stop should be no-op
	srv.Stop()
}
