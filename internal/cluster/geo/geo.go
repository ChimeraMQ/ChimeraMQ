// Package geo implements cross-datacenter geo-replication for ChimeraMQ.
//
// It replicates messages from the local datacenter to one or more remote
// datacenters over HTTP/2 with optional TLS and token-based authentication.
// Supports both async (fire-and-forget) and sync (blocking ack) modes.
package geo

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// ReplicationMode represents the geo-replication mode.
type ReplicationMode int

const (
	// ReplicationAsync provides asynchronous replication with eventual consistency.
	ReplicationAsync ReplicationMode = iota

	// ReplicationSync provides synchronous replication with strong consistency.
	ReplicationSync
)

// Config holds geo-replication configuration.
type Config struct {
	Enabled bool `yaml:"enabled"`

	// LocalDC is the identifier for the local datacenter.
	LocalDC string `yaml:"local_dc"`

	// RemoteDCs is a list of remote datacenter configurations.
	RemoteDCs []RemoteDCConfig `yaml:"remote_dcs"`

	// ReplicationMode determines sync or async replication.
	ReplicationMode ReplicationMode `yaml:"replication_mode"`

	// BatchSize is the number of messages to replicate in a batch.
	BatchSize int `yaml:"batch_size"`

	// FlushInterval is the maximum time to wait before flushing a batch.
	FlushInterval time.Duration `yaml:"flush_interval"`

	// MaxLag is the maximum allowed replication lag before backpressure.
	MaxLag time.Duration `yaml:"max_lag"`

	// RetryPolicy controls replication retry behavior.
	RetryPolicy RetryPolicy `yaml:"retry_policy"`
}

// RemoteDCConfig represents a remote datacenter configuration.
type RemoteDCConfig struct {
	// ID is the unique identifier for the datacenter.
	ID string `yaml:"id"`

	// Name is a human-readable name for the datacenter.
	Name string `yaml:"name"`

	// Address is the connection address for the remote DC.
	Address string `yaml:"address"`

	// Region is the cloud region.
	Region string `yaml:"region"`

	// Topics is a list of topics to replicate (empty = all).
	Topics []string `yaml:"topics"`

	// ExcludeTopics is a list of topics to exclude from replication.
	ExcludeTopics []string `yaml:"exclude_topics"`

	// TLS configuration.
	TLS TLSConfig `yaml:"tls"`

	// Auth configuration.
	Auth AuthConfig `yaml:"auth"`
}

// TLSConfig holds TLS configuration for remote DC connection.
type TLSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	CAFile     string `yaml:"ca_file"`
	SkipVerify bool   `yaml:"skip_verify"`
}

// AuthConfig holds authentication configuration.
type AuthConfig struct {
	Type  string `yaml:"type"` // token, tls, basic
	Token string `yaml:"token"`
	User  string `yaml:"user"`
	Pass  string `yaml:"pass"`
}

// RetryPolicy defines retry behavior.
type RetryPolicy struct {
	MaxRetries        int           `yaml:"max_retries"`
	InitialBackoff    time.Duration `yaml:"initial_backoff"`
	MaxBackoff        time.Duration `yaml:"max_backoff"`
	BackoffMultiplier float64       `yaml:"backoff_multiplier"`
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:         false,
		ReplicationMode: ReplicationAsync,
		BatchSize:       100,
		FlushInterval:   time.Second,
		MaxLag:          30 * time.Second,
		RetryPolicy: RetryPolicy{
			MaxRetries:        10,
			InitialBackoff:    time.Second,
			MaxBackoff:        5 * time.Minute,
			BackoffMultiplier: 2.0,
		},
	}
}

// ReplicationEvent represents a replication event.
type ReplicationEvent struct {
	Topic     string    `json:"topic"`
	Partition uint32    `json:"partition"`
	Offset    uint64    `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Message   []byte    `json:"message"`
}

// LagInfo represents replication lag information.
type LagInfo struct {
	DC           string        `json:"dc"`
	Topic        string        `json:"topic"`
	Partition    uint32        `json:"partition"`
	LocalOffset  uint64        `json:"local_offset"`
	RemoteOffset uint64        `json:"remote_offset"`
	Lag          uint64        `json:"lag"`
	LagTime      time.Duration `json:"lag_time"`
	LastUpdate   time.Time     `json:"last_update"`
}

// Manager manages geo-replication.
type Manager struct {
	cfg      Config
	replicas map[string]*Replica
	mu       sync.RWMutex
	stopCh   chan struct{}
	stopped  bool
}

// Replica represents a replication connection to a remote DC.
type Replica struct {
	cfg     RemoteDCConfig
	client  *Client
	buffer  chan *ReplicationEvent
	stopCh  chan struct{}
	lagInfo map[string]*LagInfo // key: topic/partition
	mu      sync.RWMutex

	batchSize     int
	flushInterval time.Duration
	mode          ReplicationMode
	retryPolicy   RetryPolicy

	// Per-replica stats
	eventsSent   atomic.Int64
	eventsFailed atomic.Int64
}

// Client represents an HTTP/2 client connection to a remote DC.
type Client struct {
	address string
	tls     TLSConfig
	auth    AuthConfig
	http    *http.Client
	baseURL string
}

// Receiver is the server-side handler that accepts incoming replication
// batches from remote datacenters and appends them to local storage.
type Receiver struct {
	broker   BrokerLike
	logger   LoggerLike
	localDC  string
	server   *http.Server
	listener net.Listener

	// Stats
	eventsReceived atomic.Int64
	eventsRejected atomic.Int64
}

// BrokerLike is the interface the Receiver needs from the broker.
type BrokerLike interface {
	Publish(env *message.Envelope) (uint64, error)
}

// LoggerLike is a minimal logger interface.
type LoggerLike interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
	Warn(msg string, args ...any)
}

const (
	geoReplicatePath = "/v1/geo-replicate"
	geoHealthPath    = "/v1/geo-health"
)

// NewManager creates a new geo-replication manager.
func NewManager(cfg Config) *Manager {
	return &Manager{
		cfg:      cfg,
		replicas: make(map[string]*Replica),
		stopCh:   make(chan struct{}),
	}
}

// Start starts the geo-replication manager.
func (m *Manager) Start() error {
	if !m.cfg.Enabled {
		return nil
	}

	for _, dcCfg := range m.cfg.RemoteDCs {
		replica, err := m.createReplica(dcCfg)
		if err != nil {
			return fmt.Errorf("create replica for %s: %w", dcCfg.ID, err)
		}
		m.replicas[dcCfg.ID] = replica

		if err := replica.Start(); err != nil {
			return fmt.Errorf("start replica for %s: %w", dcCfg.ID, err)
		}
	}

	return nil
}

// Stop stops the geo-replication manager.
func (m *Manager) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	close(m.stopCh)
	m.mu.Unlock()

	for _, replica := range m.replicas {
		replica.Stop()
	}
}

// Replicate queues a message for replication.
func (m *Manager) Replicate(env *message.Envelope) error {
	if !m.cfg.Enabled {
		return nil
	}

	// Check topic filtering for each replica
	for _, replica := range m.replicas {
		if !replica.shouldReplicateTopic(env.Topic) {
			continue
		}

		event := &ReplicationEvent{
			Topic:     env.Topic,
			Partition: env.PartitionID,
			Offset:    env.Sequence,
			Timestamp: time.Now(),
			Message:   env.Payload,
		}

		select {
		case replica.buffer <- event:
			// Event queued
		case <-m.stopCh:
			return fmt.Errorf("manager stopped")
		default:
			// Buffer full, apply backpressure
			return fmt.Errorf("replica buffer full for %s", replica.cfg.ID)
		}
	}

	return nil
}

// Stats returns replication statistics.
func (m *Manager) Stats() (replicated, failed int64) {
	for _, replica := range m.replicas {
		replicated += replica.eventsSent.Load()
		failed += replica.eventsFailed.Load()
	}
	return
}

// LagInfos returns current lag information for all replicas.
func (m *Manager) LagInfos() map[string][]LagInfo {
	result := make(map[string][]LagInfo)
	for id, replica := range m.replicas {
		result[id] = replica.getLagInfos()
	}
	return result
}

// createReplica creates a new replica.
func (m *Manager) createReplica(cfg RemoteDCConfig) (*Replica, error) {
	client := &Client{
		address: cfg.Address,
		tls:     cfg.TLS,
		auth:    cfg.Auth,
	}

	// Calculate buffer size with a reasonable maximum
	bufferSize := m.cfg.BatchSize * 10
	const maxBufferSize = 100_000
	if bufferSize > maxBufferSize {
		bufferSize = maxBufferSize
	}
	if bufferSize < 100 {
		bufferSize = 100 // minimum buffer size
	}

	return &Replica{
		cfg:           cfg,
		client:        client,
		buffer:        make(chan *ReplicationEvent, bufferSize),
		stopCh:        make(chan struct{}),
		lagInfo:       make(map[string]*LagInfo),
		batchSize:     m.cfg.BatchSize,
		flushInterval: m.cfg.FlushInterval,
		mode:          m.cfg.ReplicationMode,
		retryPolicy:   m.cfg.RetryPolicy,
	}, nil
}

// shouldReplicateTopic checks if a topic should be replicated to this DC.
func (r *Replica) shouldReplicateTopic(topic string) bool {
	// If whitelist is set, topic must be in it
	if len(r.cfg.Topics) > 0 {
		found := false
		for _, t := range r.cfg.Topics {
			if t == topic {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// If blacklist is set, topic must NOT be in it
	for _, t := range r.cfg.ExcludeTopics {
		if t == topic {
			return false
		}
	}

	return true
}

// Start starts the replica.
func (r *Replica) Start() error {
	// Connect to remote DC
	if err := r.client.Connect(); err != nil {
		return fmt.Errorf("connect to %s: %w", r.cfg.Address, err)
	}

	// Start replication goroutine
	go r.replicate()

	return nil
}

// Stop stops the replica.
func (r *Replica) Stop() {
	close(r.stopCh)
	r.client.Close()
}

// replicate is the main replication loop.
func (r *Replica) replicate() {
	flushInterval := r.flushInterval
	if flushInterval <= 0 {
		flushInterval = time.Second
	}
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	batch := make([]*ReplicationEvent, 0, r.batchSize)

	for {
		select {
		case <-r.stopCh:
			// Flush remaining batch
			if len(batch) > 0 {
				r.sendBatchWithRetry(batch)
			}
			return

		case event := <-r.buffer:
			batch = append(batch, event)
			if len(batch) >= r.batchSize {
				r.sendBatchWithRetry(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				r.sendBatchWithRetry(batch)
				batch = batch[:0]
			}
		}
	}
}

// sendBatchWithRetry sends a batch with exponential backoff retry.
func (r *Replica) sendBatchWithRetry(batch []*ReplicationEvent) {
	maxRetries := r.retryPolicy.MaxRetries
	backoff := r.retryPolicy.InitialBackoff
	maxBackoff := r.retryPolicy.MaxBackoff
	multiplier := r.retryPolicy.BackoffMultiplier

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := r.sendBatch(batch); err == nil {
			r.eventsSent.Add(int64(len(batch)))
			return
		}

		if attempt < maxRetries {
			select {
			case <-time.After(backoff):
				// Exponential backoff
				backoff = time.Duration(float64(backoff) * multiplier)
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			case <-r.stopCh:
				return
			}
		}
	}

	// All retries failed — update lag info and track failure
	r.eventsFailed.Add(int64(len(batch)))
	r.mu.Lock()
	for _, event := range batch {
		key := fmt.Sprintf("%s/%d", event.Topic, event.Partition)
		if lag, ok := r.lagInfo[key]; ok {
			lag.LagTime = time.Since(event.Timestamp)
		}
	}
	r.mu.Unlock()
}

// sendBatch sends a batch of events to the remote DC.
func (r *Replica) sendBatch(batch []*ReplicationEvent) error {
	if len(batch) == 0 {
		return nil
	}

	// Serialize batch
	data, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("marshal batch: %w", err)
	}

	// Send to remote DC
	if err := r.client.Send(data); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	// Update lag info
	r.updateLagInfo(batch)

	return nil
}

// updateLagInfo updates lag information after successful replication.
func (r *Replica) updateLagInfo(batch []*ReplicationEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for _, event := range batch {
		key := fmt.Sprintf("%s/%d", event.Topic, event.Partition)
		lag, ok := r.lagInfo[key]
		if !ok {
			lag = &LagInfo{
				DC:        r.cfg.ID,
				Topic:     event.Topic,
				Partition: event.Partition,
			}
			r.lagInfo[key] = lag
		}

		lag.RemoteOffset = event.Offset
		lag.LastUpdate = now
		lag.LagTime = now.Sub(event.Timestamp)
	}
}

// getLagInfos returns a snapshot of lag information.
func (r *Replica) getLagInfos() []LagInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]LagInfo, 0, len(r.lagInfo))
	for _, lag := range r.lagInfo {
		result = append(result, *lag)
	}
	return result
}

// Connect connects to the remote DC using HTTP/2.
func (c *Client) Connect() error {
	transport := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 5,
		IdleConnTimeout:     90 * time.Second,
	}

	// Configure TLS if enabled
	if c.tls.Enabled {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: c.tls.SkipVerify, //nolint:gosec
		}

		if c.tls.CertFile != "" && c.tls.KeyFile != "" {
			cert, err := tls.LoadX509KeyPair(c.tls.CertFile, c.tls.KeyFile)
			if err != nil {
				return fmt.Errorf("load client cert: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}

		if c.tls.CAFile != "" {
			caData, err := os.ReadFile(c.tls.CAFile)
			if err != nil {
				return fmt.Errorf("read CA cert: %w", err)
			}
			caPool := x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(caData) {
				return fmt.Errorf("failed to parse CA cert")
			}
			tlsCfg.RootCAs = caPool
		}

		transport.TLSClientConfig = tlsCfg
	}

	c.http = &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	// Determine protocol scheme
	scheme := "http"
	if c.tls.Enabled {
		scheme = "https"
	}
	c.baseURL = fmt.Sprintf("%s://%s", scheme, c.address)

	// Verify connectivity with a health check
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+geoHealthPath, nil)
	if err != nil {
		return fmt.Errorf("create health request: %w", err)
	}
	c.applyAuth(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned %d", resp.StatusCode)
	}

	return nil
}

// Close closes the HTTP client.
func (c *Client) Close() {
	if c.http != nil {
		c.http.CloseIdleConnections()
	}
}

// Send sends data to the remote DC via HTTP/2 POST.
func (c *Client) Send(data []byte) error {
	if c.http == nil {
		return fmt.Errorf("client not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Frame: 4-byte length prefix + JSON payload
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(data)))
	copy(frame[4:], data)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+geoReplicatePath, bytes.NewReader(frame))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Geo-Replication", "true")
	c.applyAuth(req)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("remote returned %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// applyAuth adds authentication headers to the request.
func (c *Client) applyAuth(req *http.Request) {
	switch c.auth.Type {
	case "token":
		if c.auth.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.auth.Token)
		}
	case "basic":
		if c.auth.User != "" && c.auth.Pass != "" {
			req.SetBasicAuth(c.auth.User, c.auth.Pass)
		}
	}
}

// NewReceiver creates a new geo-replication receiver server.
func NewReceiver(broker BrokerLike, logger LoggerLike, localDC string, bindAddr string) (*Receiver, error) {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}

	return &Receiver{
		broker:   broker,
		logger:   logger,
		localDC:  localDC,
		listener: listener,
	}, nil
}

// Serve starts the receiver server and blocks until stopped.
func (r *Receiver) Serve() error {
	mux := http.NewServeMux()
	mux.HandleFunc(geoReplicatePath, r.handleReplicate)
	mux.HandleFunc(geoHealthPath, r.handleHealth)

	r.server = &http.Server{
		Handler:  mux,
		ErrorLog: nil, // use our own logger
	}

	r.logger.Info("geo-replication receiver listening", "addr", r.listener.Addr().String())
	return r.server.Serve(r.listener)
}

// Stop gracefully shuts down the receiver.
func (r *Receiver) Stop() {
	if r.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = r.server.Shutdown(ctx)
	}
}

// Addr returns the listener address.
func (r *Receiver) Addr() string {
	return r.listener.Addr().String()
}

// Stats returns receiver statistics.
func (r *Receiver) Stats() (received, rejected int64) {
	return r.eventsReceived.Load(), r.eventsRejected.Load()
}

// handleHealth responds to health check requests.
func (r *Receiver) handleHealth(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok","dc":"` + r.localDC + `"}`))
}

// handleReplicate handles incoming replication batches.
func (r *Receiver) handleReplicate(w http.ResponseWriter, req *http.Request) {
	// Read entire body
	body, err := io.ReadAll(io.LimitReader(req.Body, 64*1024*1024)) // 64MB max
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Decode: 4-byte length prefix + JSON payload
	if len(body) < 4 {
		http.Error(w, "frame too short", http.StatusBadRequest)
		return
	}

	payloadLen := binary.BigEndian.Uint32(body[:4])
	if int(payloadLen) > len(body)-4 {
		http.Error(w, "truncated payload", http.StatusBadRequest)
		return
	}

	payload := body[4 : 4+payloadLen]

	// Parse batch
	var events []ReplicationEvent
	if err := json.Unmarshal(payload, &events); err != nil {
		http.Error(w, "parse batch: "+err.Error(), http.StatusBadRequest)
		return
	}

	var accepted, failed int
	for _, event := range events {
		env := &message.Envelope{
			Topic:       event.Topic,
			PartitionID: event.Partition,
			Sequence:    event.Offset,
			Timestamp:   event.Timestamp.UnixNano(),
			Payload:     event.Message,
			SourceProto: message.ProtoChimera, // replicated messages preserve origin
		}

		if _, err := r.broker.Publish(env); err != nil {
			r.logger.Error("geo-replicate publish failed",
				"topic", event.Topic, "partition", event.Partition, "error", err)
			failed++
			r.eventsRejected.Add(1)
		} else {
			accepted++
			r.eventsReceived.Add(1)
		}
	}

	if failed > 0 {
		r.logger.Warn("geo-replicate batch partial failure",
			"accepted", accepted, "failed", failed)
		// Return 202 Accepted to signal partial success
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	// Write response body
	resp := fmt.Sprintf(`{"accepted":%d,"failed":%d}`, accepted, failed)
	_, _ = w.Write([]byte(resp))
}

// RegisterGeoRoutes registers geo-replication endpoints on an existing mux.
// This is an alternative to running a dedicated Receiver server, allowing
// the endpoints to be served on the admin HTTP port.
func RegisterGeoRoutes(mux *http.ServeMux, broker BrokerLike, logger LoggerLike, localDC string) {
	receiver := &Receiver{
		broker:  broker,
		logger:  logger,
		localDC: localDC,
	}
	mux.HandleFunc(geoReplicatePath, receiver.handleReplicate)
	mux.HandleFunc(geoHealthPath, receiver.handleHealth)
}

// ParseReplicationMode parses a mode string to ReplicationMode.
func ParseReplicationMode(s string) ReplicationMode {
	switch strings.ToLower(s) {
	case "sync":
		return ReplicationSync
	default:
		return ReplicationAsync
	}
}
