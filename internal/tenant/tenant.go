package tenant

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// Manager handles multi-tenancy isolation and quotas.
type Manager struct {
	mu       sync.RWMutex
	tenants  map[string]*Tenant
	enabled  atomic.Bool
}

// Tenant represents an isolated namespace.
type Tenant struct {
	ID          string
	TopicPrefix string            // e.g., "tenant-1_"
	Quotas      QuotaConfig       // per-tenant limits
	Topics      map[string]struct{} // registered topics
	Metadata    map[string]string
}

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
	Enabled  bool            `yaml:"enabled"`
	Separator string         `yaml:"separator"` // default: "_"
	Tenants  []TenantConfig  `yaml:"tenants"`
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

// ListTenants returns all tenant IDs.
func (m *Manager) ListTenants() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.tenants))
	for id := range m.tenants {
		ids = append(ids, id)
	}
	return ids
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
func (m *Manager) CheckQuota(tenantID string, op string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tenants[tenantID]
	if !ok {
		return false
	}
	switch op {
	case "publish":
		return t.Quotas.MaxPublishRate == 0 // 0 = unlimited
	case "fetch":
		return t.Quotas.MaxFetchRate == 0
	case "connect":
		return t.Quotas.MaxConnections == 0
	case "topic":
		if t.Quotas.MaxTopics <= 0 {
			return true
		}
		return len(t.Topics) < t.Quotas.MaxTopics
	default:
		return true
	}
}

// Remove removes a tenant.
func (m *Manager) Remove(tenantID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tenants, tenantID)
}
