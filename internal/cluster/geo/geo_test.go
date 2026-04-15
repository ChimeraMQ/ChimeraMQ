package geo

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// --- TestBroker ---

type testBroker struct {
	published atomic.Int64
	failNext  atomic.Bool
	logger    *testLogger
}

func (b *testBroker) Publish(env *message.Envelope) (uint64, error) {
	b.published.Add(1)
	if b.failNext.Load() {
		return 0, &publishError{msg: "simulated failure"}
	}
	return uint64(b.published.Load()), nil
}

func (b *testBroker) Logger() LoggerLike {
	return b.logger
}

type publishError struct{ msg string }

func (e *publishError) Error() string { return e.msg }

type testLogger struct{}

func (l *testLogger) Info(msg string, args ...any)  {}
func (l *testLogger) Error(msg string, args ...any) {}
func (l *testLogger) Debug(msg string, args ...any) {}
func (l *testLogger) Warn(msg string, args ...any)  {}

func newTestBroker() *testBroker {
	return &testBroker{logger: &testLogger{}}
}

// --- Config Tests ---

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Enabled {
		t.Error("default config should have Enabled=false")
	}
	if cfg.ReplicationMode != ReplicationAsync {
		t.Error("default replication mode should be async")
	}
	if cfg.BatchSize != 100 {
		t.Errorf("default batch size = %d, want 100", cfg.BatchSize)
	}
}

func TestNewManager(t *testing.T) {
	cfg := Config{Enabled: false}
	m := NewManager(cfg)
	if m == nil {
		t.Fatal("manager is nil")
	}
	if m.cfg.Enabled {
		t.Error("manager should not be enabled")
	}
}

func TestManagerStartStopDisabled(t *testing.T) {
	cfg := Config{Enabled: false}
	m := NewManager(cfg)

	if err := m.Start(); err != nil {
		t.Fatalf("Start disabled: %v", err)
	}

	m.Stop()

	if !m.stopped {
		t.Error("manager should be stopped")
	}
}

func TestManagerStartStopWithReplicas(t *testing.T) {
	// Use a test server that accepts connections
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cfg := Config{
		Enabled: true,
		RemoteDCs: []RemoteDCConfig{
			{ID: "dc1", Address: srv.Listener.Addr().String()},
		},
		BatchSize: 10,
	}
	m := NewManager(cfg)

	if err := m.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if len(m.replicas) != 1 {
		t.Errorf("replicas = %d, want 1", len(m.replicas))
	}

	m.Stop()
}

func TestManagerReplicateDisabled(t *testing.T) {
	cfg := Config{Enabled: false}
	m := NewManager(cfg)

	env := &message.Envelope{Topic: "test", Payload: []byte("data")}
	if err := m.Replicate(env); err != nil {
		t.Fatalf("Replicate disabled: %v", err)
	}
}

func TestManagerReplicateWithReplica(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		} else if r.URL.Path == geoReplicatePath {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"accepted":1,"failed":0}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cfg := Config{
		Enabled:   true,
		BatchSize: 10,
		RemoteDCs: []RemoteDCConfig{
			{ID: "dc1", Address: srv.Listener.Addr().String()},
		},
		FlushInterval: 50 * time.Millisecond,
	}
	m := NewManager(cfg)
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	env := &message.Envelope{
		Topic:       "test",
		PartitionID: 0,
		Sequence:    42,
		Payload:     []byte("data"),
	}
	if err := m.Replicate(env); err != nil {
		t.Fatalf("Replicate: %v", err)
	}

	// Wait for flush
	time.Sleep(100 * time.Millisecond)
}

func TestManagerReplicateBufferFull(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
		} else {
			// Slow down so buffer fills
			time.Sleep(500 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cfg := Config{
		Enabled:   true,
		BatchSize: 1,
		RemoteDCs: []RemoteDCConfig{
			{ID: "dc1", Address: srv.Listener.Addr().String()},
		},
	}
	m := NewManager(cfg)
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	// Fill the buffer (buffer size = batchSize * 10 = 10)
	for i := 0; i < 200; i++ {
		env := &message.Envelope{Topic: "test", Payload: []byte("data")}
		_ = m.Replicate(env)
	}

	// Next replicate should fail with buffer full
	env := &message.Envelope{Topic: "test", Payload: []byte("data")}
	err := m.Replicate(env)
	if err == nil {
		t.Error("expected error when replica buffer is full")
	}
}

func TestManagerDoubleStop(t *testing.T) {
	cfg := Config{Enabled: false}
	m := NewManager(cfg)
	m.Stop()
	m.Stop() // should not panic
}

func TestReplicaStartStop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cfg := RemoteDCConfig{ID: "dc1", Address: srv.Listener.Addr().String()}
	client := &Client{address: cfg.Address}
	replica := &Replica{
		cfg:     cfg,
		client:  client,
		buffer:  make(chan *ReplicationEvent, 100),
		stopCh:  make(chan struct{}),
		lagInfo: make(map[string]*LagInfo),
	}

	if err := replica.Start(); err != nil {
		t.Fatalf("replica.Start: %v", err)
	}

	replica.Stop()
}

func TestClientConnectSendClose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
		} else if r.URL.Path == geoReplicatePath {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := &Client{address: srv.Listener.Addr().String()}
	if err := c.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Send([]byte("test")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	c.Close()
}

func TestClientConnectHealthFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := &Client{address: srv.Listener.Addr().String()}
	err := c.Connect()
	if err == nil {
		t.Error("expected error when health check fails")
	}
}

func TestSendBatchEmpty(t *testing.T) {
	cfg := RemoteDCConfig{ID: "dc1", Address: "localhost:9091"}
	client := &Client{address: cfg.Address}
	replica := &Replica{
		cfg:     cfg,
		client:  client,
		buffer:  make(chan *ReplicationEvent, 100),
		stopCh:  make(chan struct{}),
		lagInfo: make(map[string]*LagInfo),
	}
	replica.sendBatch([]*ReplicationEvent{})
}

func TestUpdateLagInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
		} else if r.URL.Path == geoReplicatePath {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"accepted":2,"failed":0}`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	cfg := RemoteDCConfig{ID: "dc1", Address: srv.Listener.Addr().String()}
	client := &Client{address: srv.Listener.Addr().String()}
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}

	replica := &Replica{
		cfg:     cfg,
		client:  client,
		buffer:  make(chan *ReplicationEvent, 100),
		stopCh:  make(chan struct{}),
		lagInfo: make(map[string]*LagInfo),
	}

	batch := []*ReplicationEvent{
		{Topic: "t1", Partition: 0, Offset: 10, Timestamp: time.Now().Add(-1 * time.Second)},
		{Topic: "t1", Partition: 0, Offset: 11, Timestamp: time.Now().Add(-2 * time.Second)},
	}
	if err := replica.sendBatch(batch); err != nil {
		t.Fatalf("sendBatch: %v", err)
	}

	replica.mu.RLock()
	lag, ok := replica.lagInfo["t1/0"]
	replica.mu.RUnlock()

	if !ok {
		t.Fatal("lag info not found")
	}
	if lag.RemoteOffset != 11 {
		t.Errorf("remote offset = %d, want 11", lag.RemoteOffset)
	}
	if lag.DC != "dc1" {
		t.Errorf("dc = %q, want dc1", lag.DC)
	}
}

func TestCreateReplicaBufferSizes(t *testing.T) {
	tests := []struct {
		batchSize int
		wantMin   int
		wantMax   int
	}{
		{1, 100, 100},
		{10, 100, 100},
		{1000, 1000, 10000},
		{20000, 100000, 100000},
	}

	for _, tt := range tests {
		cfg := Config{
			Enabled:   true,
			BatchSize: tt.batchSize,
		}
		m := NewManager(cfg)
		dcCfg := RemoteDCConfig{ID: "dc1", Address: "localhost:9091"}
		replica, err := m.createReplica(dcCfg)
		if err != nil {
			t.Fatalf("createReplica batchSize=%d: %v", tt.batchSize, err)
		}
		capacity := cap(replica.buffer)
		if capacity < tt.wantMin || capacity > tt.wantMax {
			t.Errorf("batchSize=%d: buffer capacity=%d, want between %d and %d", tt.batchSize, capacity, tt.wantMin, tt.wantMax)
		}
	}
}

func TestManagerReplicateStopped(t *testing.T) {
	m := NewManager(Config{Enabled: true})
	m.replicas["dc1"] = &Replica{
		cfg:     RemoteDCConfig{ID: "dc1"},
		client:  &Client{},
		buffer:  make(chan *ReplicationEvent), // size 0 so send blocks
		stopCh:  make(chan struct{}),
		lagInfo: make(map[string]*LagInfo),
	}
	close(m.stopCh)

	env := &message.Envelope{Topic: "test", Payload: []byte("data")}
	err := m.Replicate(env)
	if err == nil || err.Error() != "manager stopped" {
		t.Fatalf("expected 'manager stopped' error, got: %v", err)
	}
}

func TestReplicateFlushOnStop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := RemoteDCConfig{ID: "dc1", Address: srv.Listener.Addr().String()}
	client := &Client{address: cfg.Address, baseURL: "http://" + srv.Listener.Addr().String()}
	replica := &Replica{
		cfg:     cfg,
		client:  client,
		buffer:  make(chan *ReplicationEvent, 100),
		stopCh:  make(chan struct{}),
		lagInfo: make(map[string]*LagInfo),
	}

	if err := replica.Start(); err != nil {
		t.Fatal(err)
	}

	replica.buffer <- &ReplicationEvent{Topic: "t1", Partition: 0, Offset: 1}
	// Stop immediately to trigger flush of pending batch
	replica.Stop()
}

func TestReplicateTickerFlush(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := RemoteDCConfig{ID: "dc1", Address: srv.Listener.Addr().String()}
	client := &Client{address: cfg.Address, baseURL: "http://" + srv.Listener.Addr().String()}
	replica := &Replica{
		cfg:     cfg,
		client:  client,
		buffer:  make(chan *ReplicationEvent, 100),
		stopCh:  make(chan struct{}),
		lagInfo: make(map[string]*LagInfo),
	}

	if err := replica.Start(); err != nil {
		t.Fatal(err)
	}

	// Send one event and wait for ticker to flush
	replica.buffer <- &ReplicationEvent{Topic: "t1", Partition: 0, Offset: 1}
	time.Sleep(250 * time.Millisecond)

	replica.Stop()
}

// --- Topic Filtering Tests ---

func TestShouldReplicateTopic_AllTopics(t *testing.T) {
	replica := &Replica{
		cfg: RemoteDCConfig{
			Topics:        nil, // empty = all
			ExcludeTopics: nil,
		},
		lagInfo: make(map[string]*LagInfo),
	}

	if !replica.shouldReplicateTopic("any-topic") {
		t.Error("should replicate any topic when whitelist is empty")
	}
}

func TestShouldReplicateTopic_Whitelist(t *testing.T) {
	replica := &Replica{
		cfg: RemoteDCConfig{
			Topics: []string{"orders", "payments"},
		},
		lagInfo: make(map[string]*LagInfo),
	}

	if !replica.shouldReplicateTopic("orders") {
		t.Error("should replicate 'orders'")
	}
	if !replica.shouldReplicateTopic("payments") {
		t.Error("should replicate 'payments'")
	}
	if replica.shouldReplicateTopic("logs") {
		t.Error("should NOT replicate 'logs'")
	}
}

func TestShouldReplicateTopic_Blacklist(t *testing.T) {
	replica := &Replica{
		cfg: RemoteDCConfig{
			ExcludeTopics: []string{"__internal", "__debug"},
		},
		lagInfo: make(map[string]*LagInfo),
	}

	if replica.shouldReplicateTopic("__internal") {
		t.Error("should NOT replicate '__internal'")
	}
	if replica.shouldReplicateTopic("__debug") {
		t.Error("should NOT replicate '__debug'")
	}
	if !replica.shouldReplicateTopic("orders") {
		t.Error("should replicate 'orders'")
	}
}

func TestShouldReplicateTopic_WhitelistAndBlacklist(t *testing.T) {
	replica := &Replica{
		cfg: RemoteDCConfig{
			Topics:        []string{"orders", "payments", "logs"},
			ExcludeTopics: []string{"logs"},
		},
		lagInfo: make(map[string]*LagInfo),
	}

	if !replica.shouldReplicateTopic("orders") {
		t.Error("should replicate 'orders'")
	}
	if replica.shouldReplicateTopic("logs") {
		t.Error("should NOT replicate 'logs' (blacklisted)")
	}
	if replica.shouldReplicateTopic("unknown") {
		t.Error("should NOT replicate 'unknown' (not in whitelist)")
	}
}

func TestManagerReplicateTopicFiltering(t *testing.T) {
	var receivedCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
		} else if r.URL.Path == geoReplicatePath {
			receivedCount.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"accepted":1,"failed":0}`))
		}
	}))
	defer srv.Close()

	cfg := Config{
		Enabled:       true,
		BatchSize:     1,
		FlushInterval: 50 * time.Millisecond,
		RemoteDCs: []RemoteDCConfig{
			{
				ID:            "dc1",
				Address:       srv.Listener.Addr().String(),
				Topics:        []string{"allowed-topic"},
				ExcludeTopics: []string{"excluded-topic"},
			},
		},
	}
	m := NewManager(cfg)
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	// Allowed topic
	m.Replicate(&message.Envelope{Topic: "allowed-topic", Payload: []byte("ok")})

	// Excluded topic (should be skipped)
	m.Replicate(&message.Envelope{Topic: "excluded-topic", Payload: []byte("skip")})

	// Not in whitelist (should be skipped)
	m.Replicate(&message.Envelope{Topic: "other-topic", Payload: []byte("skip")})

	// Wait for flush
	time.Sleep(150 * time.Millisecond)

	// Only the allowed-topic should have been sent
	if got := receivedCount.Load(); got != 1 {
		t.Errorf("received count = %d, want 1", got)
	}
}

// --- Retry Tests ---

func TestSendBatchWithRetry_Success(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
			return
		}
		attempts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := RemoteDCConfig{ID: "dc1", Address: srv.Listener.Addr().String()}
	client := &Client{address: srv.Listener.Addr().String()}
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	replica := &Replica{
		cfg:     cfg,
		client:  client,
		buffer:  make(chan *ReplicationEvent, 100),
		stopCh:  make(chan struct{}),
		lagInfo: make(map[string]*LagInfo),
		retryPolicy: RetryPolicy{
			MaxRetries:     3,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     100 * time.Millisecond,
		},
	}

	batch := []*ReplicationEvent{
		{Topic: "t1", Partition: 0, Offset: 1},
	}
	replica.sendBatchWithRetry(batch)

	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
}

func TestSendBatchWithRetry_EventualSuccess(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
			return
		}
		n := attempts.Add(1)
		if n <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := RemoteDCConfig{ID: "dc1", Address: srv.Listener.Addr().String()}
	client := &Client{address: srv.Listener.Addr().String()}
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	replica := &Replica{
		cfg:     cfg,
		client:  client,
		buffer:  make(chan *ReplicationEvent, 100),
		stopCh:  make(chan struct{}),
		lagInfo: make(map[string]*LagInfo),
		retryPolicy: RetryPolicy{
			MaxRetries:     3,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
		},
	}

	batch := []*ReplicationEvent{
		{Topic: "t1", Partition: 0, Offset: 1},
	}
	replica.sendBatchWithRetry(batch)

	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestSendBatchWithRetry_AllFail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	cfg := RemoteDCConfig{ID: "dc1", Address: srv.Listener.Addr().String()}
	client := &Client{address: srv.Listener.Addr().String()}
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	replica := &Replica{
		cfg:    cfg,
		client: client,
		buffer: make(chan *ReplicationEvent, 100),
		stopCh: make(chan struct{}),
		lagInfo: map[string]*LagInfo{
			"t1/0": {DC: "dc1", Topic: "t1", Partition: 0},
		},
		retryPolicy: RetryPolicy{
			MaxRetries:     2,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
		},
	}

	batch := []*ReplicationEvent{
		{Topic: "t1", Partition: 0, Offset: 1, Timestamp: time.Now().Add(-5 * time.Second)},
	}
	replica.sendBatchWithRetry(batch)

	// Lag info should still be updated (failure tracking)
	replica.mu.RLock()
	lag := replica.lagInfo["t1/0"]
	replica.mu.RUnlock()

	if lag.LagTime < 4*time.Second {
		t.Errorf("lag time should be >= 4s, got %v", lag.LagTime)
	}
}

// --- Sync Mode Tests ---

func TestReplicationSyncMode(t *testing.T) {
	var receivedBatches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoReplicatePath {
			receivedBatches.Add(1)
			// Read and verify body
			body := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(body)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"accepted":1,"failed":0}`))
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	cfg := Config{
		Enabled:         true,
		ReplicationMode: ReplicationSync,
		BatchSize:       1,
		FlushInterval:   50 * time.Millisecond,
		RemoteDCs: []RemoteDCConfig{
			{ID: "dc1", Address: srv.Listener.Addr().String()},
		},
	}
	m := NewManager(cfg)
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	m.Replicate(&message.Envelope{Topic: "t1", PartitionID: 0, Sequence: 1, Payload: []byte("sync-test")})
	time.Sleep(100 * time.Millisecond)

	if got := receivedBatches.Load(); got < 1 {
		t.Errorf("received batches = %d, want >= 1", got)
	}
}

// --- Manager Stats Tests ---

func TestManagerStats(t *testing.T) {
	m := NewManager(Config{Enabled: true})
	replica := &Replica{
		cfg:     RemoteDCConfig{ID: "dc1"},
		stopCh:  make(chan struct{}),
		lagInfo: make(map[string]*LagInfo),
	}
	replica.eventsSent.Store(42)
	replica.eventsFailed.Store(3)
	m.replicas["dc1"] = replica

	replicated, failed := m.Stats()
	if replicated != 42 {
		t.Errorf("replicated = %d, want 42", replicated)
	}
	if failed != 3 {
		t.Errorf("failed = %d, want 3", failed)
	}
}

func TestManagerLagInfos(t *testing.T) {
	m := NewManager(Config{Enabled: true})
	m.replicas["dc1"] = &Replica{
		cfg:    RemoteDCConfig{ID: "dc1"},
		stopCh: make(chan struct{}),
		lagInfo: map[string]*LagInfo{
			"t1/0": {DC: "dc1", Topic: "t1", Partition: 0, RemoteOffset: 100},
		},
	}

	infos := m.LagInfos()
	if len(infos["dc1"]) != 1 {
		t.Errorf("lag infos for dc1 = %d, want 1", len(infos["dc1"]))
	}
	if infos["dc1"][0].RemoteOffset != 100 {
		t.Errorf("remote offset = %d, want 100", infos["dc1"][0].RemoteOffset)
	}
}

// --- Receiver Tests ---

func TestReceiverHealth(t *testing.T) {
	broker := newTestBroker()
	receiver := &Receiver{
		broker:  broker,
		localDC: "us-east-1",
		logger:  broker.logger,
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/geo-health", nil)
	rec := httptest.NewRecorder()
	receiver.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}

	var resp map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp["dc"] != "us-east-1" {
		t.Errorf("dc = %q, want us-east-1", resp["dc"])
	}
}

func TestReceiverReplicateBatch(t *testing.T) {
	broker := newTestBroker()
	receiver := &Receiver{
		broker:  broker,
		localDC: "us-east-1",
		logger:  broker.logger,
	}

	events := []ReplicationEvent{
		{Topic: "t1", Partition: 0, Offset: 1, Timestamp: time.Now(), Message: []byte("msg-1")},
		{Topic: "t1", Partition: 0, Offset: 2, Timestamp: time.Now(), Message: []byte("msg-2")},
	}
	payload, _ := json.Marshal(events)

	// Frame: 4-byte length + payload
	frame := make([]byte, 4+len(payload))
	frame[0] = byte(len(payload) >> 24)
	frame[1] = byte(len(payload) >> 16)
	frame[2] = byte(len(payload) >> 8)
	frame[3] = byte(len(payload))
	copy(frame[4:], payload)

	req := httptest.NewRequest(http.MethodPost, "/v1/geo-replicate", nil)
	req.Body = ioReader(frame)
	req.ContentLength = int64(len(frame))
	req.Header.Set("Content-Type", "application/octet-stream")

	rec := httptest.NewRecorder()
	receiver.handleReplicate(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}

	var resp map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp["accepted"] != 2 {
		t.Errorf("accepted = %d, want 2", resp["accepted"])
	}
	if resp["failed"] != 0 {
		t.Errorf("failed = %d, want 0", resp["failed"])
	}
}

func TestReceiverReplicatePartialFailure(t *testing.T) {
	broker := newTestBroker()
	broker.failNext.Store(true) // First publish fails

	receiver := &Receiver{
		broker:  broker,
		localDC: "us-east-1",
		logger:  broker.logger,
	}

	events := []ReplicationEvent{
		{Topic: "t1", Partition: 0, Offset: 1, Timestamp: time.Now(), Message: []byte("msg-1")},
	}
	payload, _ := json.Marshal(events)

	frame := make([]byte, 4+len(payload))
	frame[0] = byte(len(payload) >> 24)
	frame[1] = byte(len(payload) >> 16)
	frame[2] = byte(len(payload) >> 8)
	frame[3] = byte(len(payload))
	copy(frame[4:], payload)

	req := httptest.NewRequest(http.MethodPost, "/v1/geo-replicate", nil)
	req.Body = ioReader(frame)
	req.ContentLength = int64(len(frame))
	req.Header.Set("Content-Type", "application/octet-stream")

	rec := httptest.NewRecorder()
	receiver.handleReplicate(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202", rec.Code)
	}

	var resp map[string]int
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if resp["failed"] != 1 {
		t.Errorf("failed = %d, want 1", resp["failed"])
	}
}

func TestReceiverReplicateBadRequest(t *testing.T) {
	broker := newTestBroker()
	receiver := &Receiver{
		broker:  broker,
		localDC: "us-east-1",
		logger:  broker.logger,
	}

	// Too short body
	req := httptest.NewRequest(http.MethodPost, "/v1/geo-replicate", nil)
	req.Body = ioReader([]byte{1, 2, 3})
	req.ContentLength = 3

	rec := httptest.NewRecorder()
	receiver.handleReplicate(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}

	// Truncated payload
	req2 := httptest.NewRequest(http.MethodPost, "/v1/geo-replicate", nil)
	bigLen := make([]byte, 4)
	bigLen[3] = 100 // claims 100 bytes payload
	req2.Body = ioReader(append(bigLen, []byte("only10bytes")...))
	req2.ContentLength = 14

	rec2 := httptest.NewRecorder()
	receiver.handleReplicate(rec2, req2)

	if rec2.Code != http.StatusBadRequest {
		t.Errorf("truncated payload status = %d, want 400", rec2.Code)
	}

	// Invalid JSON
	req3 := httptest.NewRequest(http.MethodPost, "/v1/geo-replicate", nil)
	validFrame := make([]byte, 4+5)
	validFrame[3] = 5
	copy(validFrame[4:], []byte("notjson"))
	req3.Body = ioReader(validFrame)
	req3.ContentLength = int64(len(validFrame))

	rec3 := httptest.NewRecorder()
	receiver.handleReplicate(rec3, req3)

	if rec3.Code != http.StatusBadRequest {
		t.Errorf("invalid json status = %d, want 400", rec3.Code)
	}
}

// Helper: convert []byte to io.ReadCloser
type byteReader struct{ *bytes.Reader }

func (b byteReader) Close() error { return nil }

func ioReader(data []byte) byteReader {
	return byteReader{bytes.NewReader(data)}
}

// --- ParseReplicationMode Tests ---

func TestParseReplicationMode(t *testing.T) {
	tests := []struct {
		input string
		want  ReplicationMode
	}{
		{"async", ReplicationAsync},
		{"ASYNC", ReplicationAsync},
		{"sync", ReplicationSync},
		{"SYNC", ReplicationSync},
		{"", ReplicationAsync},
		{"unknown", ReplicationAsync},
	}

	for _, tt := range tests {
		got := ParseReplicationMode(tt.input)
		if got != tt.want {
			t.Errorf("ParseReplicationMode(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

// --- Auth Tests ---

func TestClientApplyAuth_Token(t *testing.T) {
	c := &Client{
		auth: AuthConfig{Type: "token", Token: "my-secret-token"},
	}
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	c.applyAuth(req)

	if got := req.Header.Get("Authorization"); got != "Bearer my-secret-token" {
		t.Errorf("Authorization = %q, want Bearer my-secret-token", got)
	}
}

func TestClientApplyAuth_Basic(t *testing.T) {
	c := &Client{
		auth: AuthConfig{Type: "basic", User: "admin", Pass: "secret"},
	}
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	c.applyAuth(req)

	user, pass, ok := req.BasicAuth()
	if !ok {
		t.Fatal("no basic auth set")
	}
	if user != "admin" || pass != "secret" {
		t.Errorf("basic auth = %s:%s, want admin:secret", user, pass)
	}
}

func TestClientApplyAuth_None(t *testing.T) {
	c := &Client{auth: AuthConfig{Type: ""}}
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	c.applyAuth(req)

	if got := req.Header.Get("Authorization"); got != "" {
		t.Errorf("unexpected Authorization header: %q", got)
	}
}

// --- Receiver Integration Test ---

func TestReceiverServeStop(t *testing.T) {
	broker := newTestBroker()

	receiver, err := NewReceiver(broker, broker.logger, "us-east-1", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Start in background
	go func() {
		_ = receiver.Serve()
	}()

	// Wait for server to be ready
	time.Sleep(50 * time.Millisecond)

	// Test health endpoint
	resp, err := http.Get("http://" + receiver.Addr() + "/v1/geo-health")
	if err != nil {
		t.Fatalf("health request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("health status = %d, want 200", resp.StatusCode)
	}

	// Stop
	receiver.Stop()
}

// --- Client Send Integration ---

func TestClientSendRoundTrip(t *testing.T) {
	var receivedSize atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == geoHealthPath {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == geoReplicatePath {
			body := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(body)
			receivedSize.Store(r.ContentLength)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"accepted":1,"failed":0}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := &Client{address: srv.Listener.Addr().String()}
	if err := c.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	testData := []byte(`[{"topic":"t1","partition":0,"offset":1,"timestamp":"2024-01-01T00:00:00Z","message":"aGVsbG8="}]`)
	if err := c.Send(testData); err != nil {
		t.Fatalf("Send: %v", err)
	}
	c.Close()

	if receivedSize.Load() <= 0 {
		t.Error("expected to receive data on server")
	}
}
