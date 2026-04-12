package tenant

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ResourceUsage tracks actual resource consumption for a tenant.
type ResourceUsage struct {
	// Storage
	StorageBytes atomic.Int64
	MessageCount atomic.Int64

	// Network
	BytesIn  atomic.Int64
	BytesOut atomic.Int64

	// Operations
	PublishCount atomic.Int64
	FetchCount   atomic.Int64

	// Timestamps
	LastActivity atomic.Int64 // unix nanoseconds
}

// NamespaceIsolation provides topic and resource isolation between tenants.
type NamespaceIsolation struct {
	mu sync.RWMutex

	// Tenant namespaces
	namespaces map[string]*TenantNamespace
}

// TenantNamespace represents an isolated namespace for a tenant.
type TenantNamespace struct {
	TenantID         string
	TopicPrefix      string
	AllowedTopics    map[string]struct{} // whitelist (empty = all allowed)
	BlockedTopics    map[string]struct{} // blacklist
	AllowedOrigins   []string            // for CORS
	AllowedProducers []string            // client ID patterns
	AllowedConsumers []string            // consumer group patterns
}

// ResourceQuotaEnforcer actively enforces resource quotas.
type ResourceQuotaEnforcer struct {
	mu      sync.RWMutex
	manager *Manager
	usages  map[string]*ResourceUsage // tenantID -> usage

	stopCh chan struct{}
}

// NewResourceQuotaEnforcer creates a new quota enforcer.
func NewResourceQuotaEnforcer(m *Manager) *ResourceQuotaEnforcer {
	return &ResourceQuotaEnforcer{
		manager: m,
		usages:  make(map[string]*ResourceUsage),
		stopCh:  make(chan struct{}),
	}
}

// Start begins periodic quota enforcement.
func (e *ResourceQuotaEnforcer) Start() {
	go e.run()
}

// Stop stops the quota enforcer.
func (e *ResourceQuotaEnforcer) Stop() {
	close(e.stopCh)
}

func (e *ResourceQuotaEnforcer) run() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.enforceQuotas()
		}
	}
}

func (e *ResourceQuotaEnforcer) enforceQuotas() {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for tenantID, usage := range e.usages {
		tenant := e.manager.GetTenantByID(tenantID)
		if tenant == nil {
			continue
		}

		// Check storage quota
		if tenant.Quotas.MaxStorageBytes > 0 {
			if usage.StorageBytes.Load() >= tenant.Quotas.MaxStorageBytes {
				// Storage quota exceeded - could trigger cleanup or alerts
				fmt.Printf("Tenant %s exceeded storage quota: %d >= %d\n",
					tenantID, usage.StorageBytes.Load(), tenant.Quotas.MaxStorageBytes)
			}
		}
	}
}

// GetUsage returns or creates resource usage tracking for a tenant.
func (e *ResourceQuotaEnforcer) GetUsage(tenantID string) *ResourceUsage {
	e.mu.Lock()
	defer e.mu.Unlock()

	usage, ok := e.usages[tenantID]
	if !ok {
		usage = &ResourceUsage{}
		e.usages[tenantID] = usage
	}
	return usage
}

// RecordPublish records a publish operation for quota tracking.
func (e *ResourceQuotaEnforcer) RecordPublish(tenantID string, bytes int64) {
	if !e.manager.Enabled() {
		return
	}

	usage := e.GetUsage(tenantID)
	usage.PublishCount.Add(1)
	usage.BytesIn.Add(bytes)
	usage.StorageBytes.Add(bytes)
	usage.MessageCount.Add(1)
	usage.LastActivity.Store(time.Now().UnixNano())
}

// RecordFetch records a fetch operation for quota tracking.
func (e *ResourceQuotaEnforcer) RecordFetch(tenantID string, bytes int64) {
	if !e.manager.Enabled() {
		return
	}

	usage := e.GetUsage(tenantID)
	usage.FetchCount.Add(1)
	usage.BytesOut.Add(bytes)
	usage.LastActivity.Store(time.Now().UnixNano())
}

// RecordStorageUpdate updates storage tracking (for compaction/deletion).
func (e *ResourceQuotaEnforcer) RecordStorageUpdate(tenantID string, deltaBytes int64) {
	if !e.manager.Enabled() {
		return
	}

	usage := e.GetUsage(tenantID)
	if deltaBytes > 0 {
		usage.StorageBytes.Add(deltaBytes)
	} else {
		// Handle negative delta
		current := usage.StorageBytes.Load()
		newVal := current + deltaBytes
		if newVal < 0 {
			newVal = 0
		}
		usage.StorageBytes.Store(newVal)
	}
}

// NewNamespaceIsolation creates a new namespace isolation manager.
func NewNamespaceIsolation() *NamespaceIsolation {
	return &NamespaceIsolation{
		namespaces: make(map[string]*TenantNamespace),
	}
}

// RegisterNamespace registers a tenant namespace.
func (ni *NamespaceIsolation) RegisterNamespace(ns *TenantNamespace) {
	ni.mu.Lock()
	defer ni.mu.Unlock()
	ni.namespaces[ns.TenantID] = ns
}

// CanAccessTopic checks if a tenant can access a specific topic.
func (ni *NamespaceIsolation) CanAccessTopic(tenantID, topic string) bool {
	ni.mu.RLock()
	defer ni.mu.RUnlock()

	ns, ok := ni.namespaces[tenantID]
	if !ok {
		return false
	}

	// Check blacklist
	if _, blocked := ns.BlockedTopics[topic]; blocked {
		return false
	}

	// Check whitelist (if defined)
	if len(ns.AllowedTopics) > 0 {
		if _, allowed := ns.AllowedTopics[topic]; !allowed {
			return false
		}
	}

	return true
}

// GetNamespace returns a tenant's namespace.
func (ni *NamespaceIsolation) GetNamespace(tenantID string) *TenantNamespace {
	ni.mu.RLock()
	defer ni.mu.RUnlock()
	return ni.namespaces[tenantID]
}

// IsTopicInNamespace checks if a topic belongs to a tenant's namespace.
func (ni *NamespaceIsolation) IsTopicInNamespace(tenantID, topic string) bool {
	ns := ni.GetNamespace(tenantID)
	if ns == nil {
		return false
	}
	return hasPrefix(topic, ns.TopicPrefix)
}

// EnforceTopicIsolation checks if a topic operation violates tenant isolation.
func (ni *NamespaceIsolation) EnforceTopicIsolation(tenantID, topic string) error {
	if !ni.IsTopicInNamespace(tenantID, topic) {
		return fmt.Errorf("topic %q does not belong to tenant %s namespace", topic, tenantID)
	}
	if !ni.CanAccessTopic(tenantID, topic) {
		return fmt.Errorf("tenant %s does not have access to topic %q", tenantID, topic)
	}
	return nil
}

// GetTenantUsageStats returns comprehensive usage statistics for a tenant.
func (e *ResourceQuotaEnforcer) GetTenantUsageStats(tenantID string) map[string]interface{} {
	usage := e.GetUsage(tenantID)
	tenant := e.manager.GetTenantByID(tenantID)

	stats := map[string]interface{}{
		"tenant_id":     tenantID,
		"storage_bytes": usage.StorageBytes.Load(),
		"message_count": usage.MessageCount.Load(),
		"bytes_in":      usage.BytesIn.Load(),
		"bytes_out":     usage.BytesOut.Load(),
		"publish_count": usage.PublishCount.Load(),
		"fetch_count":   usage.FetchCount.Load(),
		"last_activity": time.Unix(0, usage.LastActivity.Load()).Format(time.RFC3339),
	}

	if tenant != nil {
		stats["quota_storage_bytes"] = tenant.Quotas.MaxStorageBytes
		stats["quota_publish_rate"] = tenant.Quotas.MaxPublishRate
		stats["quota_fetch_rate"] = tenant.Quotas.MaxFetchRate
		stats["quota_max_topics"] = tenant.Quotas.MaxTopics
		stats["quota_max_connections"] = tenant.Quotas.MaxConnections

		// Calculate utilization percentages
		if tenant.Quotas.MaxStorageBytes > 0 {
			stats["storage_utilization_pct"] = float64(usage.StorageBytes.Load()) / float64(tenant.Quotas.MaxStorageBytes) * 100
		}
	}

	return stats
}

// CheckRateLimit checks if a tenant has exceeded their rate limit.
func (e *ResourceQuotaEnforcer) CheckRateLimit(tenantID string, op string, windowSize time.Duration, maxRequests int64) bool {
	if !e.manager.Enabled() {
		return true
	}

	usage := e.GetUsage(tenantID)

	switch op {
	case "publish":
		return usage.PublishCount.Load() < maxRequests
	case "fetch":
		return usage.FetchCount.Load() < maxRequests
	default:
		return true
	}
}

// ResetUsage resets resource usage counters for a tenant.
func (e *ResourceQuotaEnforcer) ResetUsage(tenantID string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if usage, ok := e.usages[tenantID]; ok {
		usage.PublishCount.Store(0)
		usage.FetchCount.Store(0)
		usage.BytesIn.Store(0)
		usage.BytesOut.Store(0)
	}
}

// DeleteUsage removes usage tracking for a tenant.
func (e *ResourceQuotaEnforcer) DeleteUsage(tenantID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.usages, tenantID)
}

// hasPrefix checks if s has the given prefix.
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
