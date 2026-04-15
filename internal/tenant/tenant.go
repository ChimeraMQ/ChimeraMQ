package tenant

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Manager handles multi-tenancy isolation and quotas.
type Manager struct {
	mu      sync.RWMutex
	tenants map[string]*Tenant
	enabled atomic.Bool
}

// Tenant represents an isolated namespace.
type Tenant struct {
	ID          string
	Name        string
	Description string
	TopicPrefix string              // e.g., "tenant-1_"
	Quotas      QuotaConfig         // per-tenant limits
	Topics      map[string]struct{} // registered topics
	Metadata    map[string]string
	Labels      map[string]string
	CreatedAt   time.Time
	Enabled     bool

	// Rate tracking
	quotaMu      sync.Mutex   // protects reset+check atomicity
	publishCount atomic.Int64 // current window count
	fetchCount   atomic.Int64
	connCount    atomic.Int64
	windowStart  atomic.Int64 // unix seconds
}

// Quotas holds per-tenant resource limits (alias for QuotaConfig for compatibility).
type Quotas = QuotaConfig

// QuotaConfig holds per-tenant resource limits.
type QuotaConfig struct {
	MaxTopics       int   `yaml:"max_topics"`
	MaxPartitions   int   `yaml:"max_partitions"`
	MaxPublishRate  int64 `yaml:"max_publish_rate"`  // msgs/sec, 0=unlimited
	MaxFetchRate    int64 `yaml:"max_fetch_rate"`    // fetches/sec, 0=unlimited
	MaxConnections  int64 `yaml:"max_connections"`   // 0=unlimited
	MaxStorageBytes int64 `yaml:"max_storage_bytes"` // 0=unlimited
}

// Config holds multi-tenancy configuration.
type Config struct {
	Enabled   bool           `yaml:"enabled"`
	Separator string         `yaml:"separator"` // default: "_"
	Tenants   []TenantConfig `yaml:"tenants"`
}

// TenantConfig is the YAML config for a tenant.
type TenantConfig struct {
	ID          string            `yaml:"id"`
	TopicPrefix string            `yaml:"topic_prefix"`
	Quotas      QuotaConfig       `yaml:"quotas"`
	Metadata    map[string]string `yaml:"metadata"`
}

// NewManager creates a new tenant manager.
func NewManager(cfg Config) *Manager {
	m := &Manager{
		tenants: make(map[string]*Tenant),
	}
	if cfg.Enabled {
		m.enabled.Store(true)
	}
	sep := cfg.Separator
	if sep == "" {
		sep = "_"
	}
	for _, tc := range cfg.Tenants {
		prefix := tc.TopicPrefix
		if prefix == "" {
			prefix = tc.ID + sep
		}
		m.tenants[tc.ID] = &Tenant{
			ID:          tc.ID,
			TopicPrefix: prefix,
			Quotas:      tc.Quotas,
			Topics:      make(map[string]struct{}),
			Metadata:    tc.Metadata,
		}
	}
	return m
}

// Enabled returns whether multi-tenancy is active.
func (m *Manager) Enabled() bool { return m.enabled.Load() }

// Register adds a tenant at runtime.
func (m *Manager) Register(t *Tenant) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tenants[t.ID] = t
}

// GetTenant returns the tenant that owns the given topic.
// If multi-tenancy is disabled, returns nil.
// The tenant is determined by matching the topic prefix.
func (m *Manager) GetTenant(topic string) *Tenant {
	if !m.enabled.Load() {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, t := range m.tenants {
		if strings.HasPrefix(topic, t.TopicPrefix) {
			return t
		}
	}
	return nil
}

// GetTenantByID returns a tenant by its ID.
func (m *Manager) GetTenantByID(id string) *Tenant {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tenants[id]
}

// TenantForTopic resolves the tenant and local topic name from a full topic.
// Returns (tenant, localName, true) if found, or (nil, "", false) otherwise.
func (m *Manager) TenantForTopic(topic string) (*Tenant, string, bool) {
	t := m.GetTenant(topic)
	if t == nil {
		return nil, "", false
	}
	local := strings.TrimPrefix(topic, t.TopicPrefix)
	return t, local, true
}

// RegisterTopic registers a topic under a tenant (for quota tracking).
func (m *Manager) RegisterTopic(tenantID, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tenants[tenantID]
	if !ok {
		return fmt.Errorf("tenant %q not found", tenantID)
	}
	if t.Quotas.MaxTopics > 0 && len(t.Topics) >= t.Quotas.MaxTopics {
		return fmt.Errorf("tenant %q exceeded max topics (%d)", tenantID, t.Quotas.MaxTopics)
	}
	t.Topics[topic] = struct{}{}
	return nil
}

// UnregisterTopic removes a topic from tenant tracking.
func (m *Manager) UnregisterTopic(tenantID, topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.tenants[tenantID]; ok {
		delete(t.Topics, topic)
	}
}

// TopicCount returns the number of topics registered for a tenant.
func (m *Manager) TopicCount(tenantID string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tenants[tenantID]
	if !ok {
		return 0
	}
	return len(t.Topics)
}

// ListTenants returns all tenants.
func (m *Manager) ListTenants() []*Tenant {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tenants := make([]*Tenant, 0, len(m.tenants))
	for _, t := range m.tenants {
		tenants = append(tenants, t)
	}
	return tenants
}

func (m *Manager) CreateTenant(t *Tenant) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.tenants[t.ID]; exists {
		return fmt.Errorf("tenant %q already exists", t.ID)
	}
	if t.Topics == nil {
		t.Topics = make(map[string]struct{})
	}
	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	if t.Labels == nil {
		t.Labels = make(map[string]string)
	}
	if t.TopicPrefix == "" {
		t.TopicPrefix = t.ID + "_"
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	m.tenants[t.ID] = t
	return nil
}

// UpdateTenant updates an existing tenant's quotas only.
func (m *Manager) UpdateTenant(t *Tenant) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	existing, exists := m.tenants[t.ID]
	if !exists {
		return fmt.Errorf("tenant %q not found", t.ID)
	}
	// Only update quota fields; protect internal state
	existing.Quotas = t.Quotas
	if t.Name != "" {
		existing.Name = t.Name
	}
	if t.Description != "" {
		existing.Description = t.Description
	}
	if t.Labels != nil {
		existing.Labels = t.Labels
	}
	return nil
}

// DeleteTenant deletes a tenant by ID.
func (m *Manager) DeleteTenant(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.tenants[id]; !exists {
		return fmt.Errorf("tenant %q not found", id)
	}
	delete(m.tenants, id)
	return nil
}

// ListTopics returns all topics for a tenant.
func (m *Manager) ListTopics(tenantID string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tenants[tenantID]
	if !ok {
		return nil
	}
	topics := make([]string, 0, len(t.Topics))
	for topic := range t.Topics {
		topics = append(topics, topic)
	}
	return topics
}

// CheckQuota checks if a tenant can perform the given operation.
// It tracks actual request rates per second and enforces configured limits.
func (m *Manager) CheckQuota(tenantID string, op string) bool {
	if !m.enabled.Load() {
		return true
	}
	m.mu.RLock()
	t, ok := m.tenants[tenantID]
	m.mu.RUnlock()
	if !ok {
		return false
	}

	switch op {
	case "publish":
		if t.Quotas.MaxPublishRate <= 0 {
			return true // unlimited
		}
		t.quotaMu.Lock()
		t.resetWindowIfNeeded()
		allowed := t.publishCount.Add(1) <= t.Quotas.MaxPublishRate
		t.quotaMu.Unlock()
		return allowed
	case "fetch":
		if t.Quotas.MaxFetchRate <= 0 {
			return true
		}
		t.quotaMu.Lock()
		t.resetWindowIfNeeded()
		allowed := t.fetchCount.Add(1) <= t.Quotas.MaxFetchRate
		t.quotaMu.Unlock()
		return allowed
	case "connect":
		if t.Quotas.MaxConnections <= 0 {
			return true
		}
		return t.connCount.Load() <= t.Quotas.MaxConnections
	case "topic":
		if t.Quotas.MaxTopics <= 0 {
			return true
		}
		m.mu.RLock()
		defer m.mu.RUnlock()
		return len(t.Topics) < t.Quotas.MaxTopics
	default:
		return true
	}
}

// IncrConnection increments the connection counter for a tenant.
func (m *Manager) IncrConnection(tenantID string) {
	if !m.enabled.Load() {
		return
	}
	m.mu.RLock()
	t, ok := m.tenants[tenantID]
	m.mu.RUnlock()
	if ok {
		t.connCount.Add(1)
	}
}

// DecrConnection decrements the connection counter for a tenant.
func (m *Manager) DecrConnection(tenantID string) {
	if !m.enabled.Load() {
		return
	}
	m.mu.RLock()
	t, ok := m.tenants[tenantID]
	m.mu.RUnlock()
	if ok {
		t.connCount.Add(-1)
	}
}

// resetWindowIfNeeded resets rate counters if the current second has elapsed.
// Uses atomic CAS to ensure only one goroutine performs the reset.
func (t *Tenant) resetWindowIfNeeded() {
	now := time.Now().Unix()
	for {
		start := t.windowStart.Load()
		if now <= start {
			return // not yet a new window
		}
		// Atomically claim the window transition
		if t.windowStart.CompareAndSwap(start, now) {
			t.publishCount.Store(0)
			t.fetchCount.Store(0)
			return
		}
		// CAS failed — another goroutine reset; retry to see if we're still past the window
	}
}

// Remove removes a tenant.
func (m *Manager) Remove(tenantID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tenants, tenantID)
}
