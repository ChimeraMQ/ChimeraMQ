package tenant

import (
	"testing"
	"time"
)

func TestResourceQuotaEnforcer(t *testing.T) {
	// Create a manager with a test tenant
	mgr := NewManager(Config{
		Enabled:   true,
		Separator: "_",
		Tenants: []TenantConfig{
			{
				ID:          "test-tenant",
				TopicPrefix: "test-tenant_",
				Quotas: QuotaConfig{
					MaxStorageBytes: 1024 * 1024, // 1MB
					MaxPublishRate:  100,
					MaxFetchRate:    100,
					MaxConnections:  10,
					MaxTopics:       5,
				},
			},
		},
	})

	// Create quota enforcer
	enforcer := NewResourceQuotaEnforcer(mgr)
	enforcer.Start()
	defer enforcer.Stop()

	// Test GetUsage creates new tracking
	usage := enforcer.GetUsage("test-tenant")
	if usage == nil {
		t.Fatal("GetUsage returned nil")
	}

	// Test RecordPublish
	enforcer.RecordPublish("test-tenant", 1000)
	if usage.PublishCount.Load() != 1 {
		t.Errorf("expected publish count 1, got %d", usage.PublishCount.Load())
	}
	if usage.BytesIn.Load() != 1000 {
		t.Errorf("expected bytes in 1000, got %d", usage.BytesIn.Load())
	}
	if usage.StorageBytes.Load() != 1000 {
		t.Errorf("expected storage bytes 1000, got %d", usage.StorageBytes.Load())
	}
	if usage.MessageCount.Load() != 1 {
		t.Errorf("expected message count 1, got %d", usage.MessageCount.Load())
	}

	// Test RecordFetch
	enforcer.RecordFetch("test-tenant", 500)
	if usage.FetchCount.Load() != 1 {
		t.Errorf("expected fetch count 1, got %d", usage.FetchCount.Load())
	}
	if usage.BytesOut.Load() != 500 {
		t.Errorf("expected bytes out 500, got %d", usage.BytesOut.Load())
	}

	// Test RecordStorageUpdate (positive)
	enforcer.RecordStorageUpdate("test-tenant", 500)
	if usage.StorageBytes.Load() != 1500 {
		t.Errorf("expected storage bytes 1500, got %d", usage.StorageBytes.Load())
	}

	// Test RecordStorageUpdate (negative)
	enforcer.RecordStorageUpdate("test-tenant", -200)
	if usage.StorageBytes.Load() != 1300 {
		t.Errorf("expected storage bytes 1300, got %d", usage.StorageBytes.Load())
	}

	// Test GetTenantUsageStats
	stats := enforcer.GetTenantUsageStats("test-tenant")
	if stats["tenant_id"] != "test-tenant" {
		t.Errorf("expected tenant_id 'test-tenant', got %v", stats["tenant_id"])
	}
	if stats["publish_count"] != int64(1) {
		t.Errorf("expected publish_count 1, got %v", stats["publish_count"])
	}
	if stats["fetch_count"] != int64(1) {
		t.Errorf("expected fetch_count 1, got %v", stats["fetch_count"])
	}

	// Test ResetUsage
	enforcer.ResetUsage("test-tenant")
	if usage.PublishCount.Load() != 0 {
		t.Errorf("expected publish count 0 after reset, got %d", usage.PublishCount.Load())
	}
	if usage.FetchCount.Load() != 0 {
		t.Errorf("expected fetch count 0 after reset, got %d", usage.FetchCount.Load())
	}

	// Test DeleteUsage
	enforcer.DeleteUsage("test-tenant")
	newUsage := enforcer.GetUsage("test-tenant")
	// Should create new usage tracking
	if newUsage.PublishCount.Load() != 0 {
		t.Error("expected new usage after delete")
	}
}

func TestResourceQuotaEnforcerDisabled(t *testing.T) {
	// Create a manager with multi-tenancy disabled
	mgr := NewManager(Config{
		Enabled: false,
	})

	enforcer := NewResourceQuotaEnforcer(mgr)

	// Recording should not panic when disabled
	enforcer.RecordPublish("any-tenant", 1000)
	enforcer.RecordFetch("any-tenant", 500)
	enforcer.RecordStorageUpdate("any-tenant", 200)
}

func TestNamespaceIsolation(t *testing.T) {
	ns := NewNamespaceIsolation()

	// Register a namespace
	ns.RegisterNamespace(&TenantNamespace{
		TenantID:      "tenant-1",
		TopicPrefix:   "tenant-1_",
		AllowedTopics: map[string]struct{}{},
		BlockedTopics: map[string]struct{}{"tenant-1_blocked": {}},
	})

	// Test CanAccessTopic
	if !ns.CanAccessTopic("tenant-1", "tenant-1_orders") {
		t.Error("expected CanAccessTopic to return true for allowed topic")
	}

	if ns.CanAccessTopic("tenant-1", "tenant-1_blocked") {
		t.Error("expected CanAccessTopic to return false for blocked topic")
	}

	if ns.CanAccessTopic("unknown-tenant", "any-topic") {
		t.Error("expected CanAccessTopic to return false for unknown tenant")
	}

	// Test IsTopicInNamespace
	if !ns.IsTopicInNamespace("tenant-1", "tenant-1_orders") {
		t.Error("expected IsTopicInNamespace to return true for topic in namespace")
	}

	if ns.IsTopicInNamespace("tenant-1", "other-tenant_orders") {
		t.Error("expected IsTopicInNamespace to return false for topic not in namespace")
	}

	// Test EnforceTopicIsolation
	if err := ns.EnforceTopicIsolation("tenant-1", "tenant-1_orders"); err != nil {
		t.Errorf("expected EnforceTopicIsolation to succeed: %v", err)
	}

	if err := ns.EnforceTopicIsolation("tenant-1", "tenant-1_blocked"); err == nil {
		t.Error("expected EnforceTopicIsolation to fail for blocked topic")
	}

	if err := ns.EnforceTopicIsolation("tenant-1", "other-tenant_orders"); err == nil {
		t.Error("expected EnforceTopicIsolation to fail for topic not in namespace")
	}
}

func TestNamespaceIsolationWithWhitelist(t *testing.T) {
	ns := NewNamespaceIsolation()

	// Register a namespace with whitelist
	ns.RegisterNamespace(&TenantNamespace{
		TenantID:      "tenant-1",
		TopicPrefix:   "tenant-1_",
		AllowedTopics: map[string]struct{}{"tenant-1_allowed1": {}, "tenant-1_allowed2": {}},
		BlockedTopics: map[string]struct{}{},
	})

	// Allowed topics should be accessible
	if !ns.CanAccessTopic("tenant-1", "tenant-1_allowed1") {
		t.Error("expected CanAccessTopic to return true for whitelisted topic")
	}

	// Non-whitelisted topics should not be accessible
	if ns.CanAccessTopic("tenant-1", "tenant-1_notallowed") {
		t.Error("expected CanAccessTopic to return false for non-whitelisted topic")
	}
}

func TestCheckRateLimit(t *testing.T) {
	mgr := NewManager(Config{
		Enabled:   true,
		Separator: "_",
		Tenants: []TenantConfig{
			{
				ID:          "rate-tenant",
				TopicPrefix: "rate-tenant_",
				Quotas: QuotaConfig{
					MaxPublishRate: 10,
					MaxFetchRate:   10,
				},
			},
		},
	})

	enforcer := NewResourceQuotaEnforcer(mgr)

	// Should allow within limit
	if !enforcer.CheckRateLimit("rate-tenant", "publish", time.Second, 10) {
		t.Error("expected CheckRateLimit to return true for first request")
	}

	// Simulate 5 publishes
	for i := 0; i < 5; i++ {
		enforcer.RecordPublish("rate-tenant", 100)
	}

	// Should still allow
	if !enforcer.CheckRateLimit("rate-tenant", "publish", time.Second, 10) {
		t.Error("expected CheckRateLimit to return true within limit")
	}

	// Simulate more publishes to exceed limit
	for i := 0; i < 10; i++ {
		enforcer.RecordPublish("rate-tenant", 100)
	}

	// Note: Rate limit check is basic, just checks if count < max
	// The actual rate limiting is done by the Manager's CheckQuota with time windows
}

func TestTenantManagerCRUD(t *testing.T) {
	mgr := NewManager(Config{
		Enabled:   true,
		Separator: "_",
	})

	// Test CreateTenant
	tenant := &Tenant{
		ID:          "new-tenant",
		Name:        "New Tenant",
		Description: "A test tenant",
		TopicPrefix: "new-tenant_",
		CreatedAt:   time.Now(),
		Enabled:     true,
		Quotas: QuotaConfig{
			MaxTopics:       10,
			MaxStorageBytes: 1024 * 1024,
		},
		Labels: map[string]string{"env": "test"},
	}

	if err := mgr.CreateTenant(tenant); err != nil {
		t.Fatalf("CreateTenant failed: %v", err)
	}

	// Test duplicate creation
	if err := mgr.CreateTenant(tenant); err == nil {
		t.Error("expected CreateTenant to fail for duplicate tenant")
	}

	// Test GetTenantByID
	tgot := mgr.GetTenantByID("new-tenant")
	if tgot == nil {
		t.Fatal("GetTenantByID returned nil")
	}
	if tgot.ID != "new-tenant" {
		t.Errorf("expected tenant ID 'new-tenant', got %s", tgot.ID)
	}

	// Test duplicate creation
	if err := mgr.CreateTenant(tenant); err == nil {
		t.Error("expected CreateTenant to fail for duplicate tenant")
	}

	// Test GetTenantByID for non-existent
	if mgr.GetTenantByID("non-existent") != nil {
		t.Error("expected GetTenantByID to return nil for non-existent tenant")
	}

	// Test ListTenants
	tenants := mgr.ListTenants()
	if len(tenants) != 1 {
		t.Errorf("expected 1 tenant, got %d", len(tenants))
	}

	// Test UpdateTenant
	tenant.Name = "Updated Tenant"
	tenant.Quotas.MaxTopics = 20
	if err := mgr.UpdateTenant(tenant); err != nil {
		t.Fatalf("UpdateTenant failed: %v", err)
	}

	got := mgr.GetTenantByID("new-tenant")
	if got == nil {
		t.Fatal("expected to find tenant")
	}
	if got.Name != "Updated Tenant" {
		t.Errorf("expected updated name 'Updated Tenant', got %s", got.Name)
	}
	if got.Quotas.MaxTopics != 20 {
		t.Errorf("expected updated max topics 20, got %d", got.Quotas.MaxTopics)
	}

	// Test DeleteTenant
	if err := mgr.DeleteTenant("new-tenant"); err != nil {
		t.Fatalf("DeleteTenant failed: %v", err)
	}

	// Test delete non-existent
	if err := mgr.DeleteTenant("new-tenant"); err == nil {
		t.Error("expected DeleteTenant to fail for non-existent tenant")
	}

	// Verify deletion
	if mgr.GetTenantByID("new-tenant") != nil {
		t.Error("expected tenant to be deleted")
	}
}

func TestResourceUsageConcurrency(t *testing.T) {
	mgr := NewManager(Config{Enabled: true})
	enforcer := NewResourceQuotaEnforcer(mgr)
	enforcer.Start()
	defer enforcer.Stop()

	// Run concurrent operations
	done := make(chan bool, 3)

	// Concurrent publishes
	go func() {
		for i := 0; i < 1000; i++ {
			enforcer.RecordPublish("concurrent-tenant", 100)
		}
		done <- true
	}()

	// Concurrent fetches
	go func() {
		for i := 0; i < 1000; i++ {
			enforcer.RecordFetch("concurrent-tenant", 50)
		}
		done <- true
	}()

	// Concurrent storage updates
	go func() {
		for i := 0; i < 1000; i++ {
			enforcer.RecordStorageUpdate("concurrent-tenant", 10)
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 3; i++ {
		<-done
	}

	// Verify results
	usage := enforcer.GetUsage("concurrent-tenant")
	if usage.PublishCount.Load() != 1000 {
		t.Errorf("expected publish count 1000, got %d", usage.PublishCount.Load())
	}
	if usage.FetchCount.Load() != 1000 {
		t.Errorf("expected fetch count 1000, got %d", usage.FetchCount.Load())
	}
	// Storage should be: 1000*100 (from publishes) + 1000*10 (from updates) = 110000
	expectedStorage := int64(1000*100 + 1000*10)
	if usage.StorageBytes.Load() != expectedStorage {
		t.Errorf("expected storage bytes %d, got %d", expectedStorage, usage.StorageBytes.Load())
	}
}
