package geo

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

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
	cfg := Config{
		Enabled: true,
		RemoteDCs: []RemoteDCConfig{
			{ID: "dc1", Address: "localhost:9091"},
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
	cfg := Config{
		Enabled:   true,
		BatchSize: 10,
		RemoteDCs: []RemoteDCConfig{
			{ID: "dc1", Address: "localhost:9091"},
		},
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
}

func TestManagerReplicateBufferFull(t *testing.T) {
	cfg := Config{
		Enabled:   true,
		BatchSize: 1,
		RemoteDCs: []RemoteDCConfig{
			{ID: "dc1", Address: "localhost:9091"},
		},
	}
	m := NewManager(cfg)
	if err := m.Start(); err != nil {
		t.Fatal(err)
	}
	defer m.Stop()

	// Fill the buffer
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
	cfg := RemoteDCConfig{ID: "dc1", Address: "localhost:9091"}
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
	c := &Client{address: "localhost:9091"}
	if err := c.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := c.Send([]byte("test")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	c.Close()
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
	cfg := RemoteDCConfig{ID: "dc1"}
	replica := &Replica{
		cfg:     cfg,
		buffer:  make(chan *ReplicationEvent, 100),
		stopCh:  make(chan struct{}),
		lagInfo: make(map[string]*LagInfo),
	}

	batch := []*ReplicationEvent{
		{Topic: "t1", Partition: 0, Offset: 10, Timestamp: time.Now().Add(-1 * time.Second)},
		{Topic: "t1", Partition: 0, Offset: 11, Timestamp: time.Now().Add(-2 * time.Second)},
	}
	replica.sendBatch(batch)

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
		buffer:  make(chan *ReplicationEvent, 0), // size 0 so send blocks
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
	cfg := RemoteDCConfig{ID: "dc1", Address: "localhost:9091"}
	client := &Client{address: cfg.Address}
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
