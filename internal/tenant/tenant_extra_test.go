package tenant

import (
	"testing"
	"time"
)

func TestIncrDecrConnection(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_", Quotas: QuotaConfig{MaxConnections: 5}},
		},
	})

	m.IncrConnection("acme")
	m.IncrConnection("acme")

	if !m.CheckQuota("acme", "connect") {
		t.Error("should allow connect within limit")
	}

	m.DecrConnection("acme")

	// Unknown tenant should not panic
	m.IncrConnection("unknown")
	m.DecrConnection("unknown")
}

func TestIncrDecrConnectionDisabled(t *testing.T) {
	m := NewManager(Config{Enabled: false})
	m.IncrConnection("acme")
	m.DecrConnection("acme")
	// Should not panic
}

func TestCheckQuotaFetch(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_", Quotas: QuotaConfig{MaxFetchRate: 2}},
		},
	})

	if !m.CheckQuota("acme", "fetch") {
		t.Error("first fetch should be allowed")
	}
	if !m.CheckQuota("acme", "fetch") {
		t.Error("second fetch should be allowed")
	}
	if m.CheckQuota("acme", "fetch") {
		t.Error("third fetch should exceed quota")
	}
}

func TestCheckQuotaConnect(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_", Quotas: QuotaConfig{MaxConnections: 1}},
		},
	})

	m.IncrConnection("acme")
	if !m.CheckQuota("acme", "connect") {
		t.Error("connect should be allowed at limit")
	}

	m.IncrConnection("acme")
	if m.CheckQuota("acme", "connect") {
		t.Error("connect should exceed limit")
	}
}

func TestCheckQuotaDisabled(t *testing.T) {
	m := NewManager(Config{Enabled: false})
	if !m.CheckQuota("any", "publish") {
		t.Error("should allow when disabled")
	}
}

func TestCheckQuotaDefaultOp(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_"},
		},
	})
	if !m.CheckQuota("acme", "unknown_op") {
		t.Error("unknown op should be allowed by default")
	}
}

func TestResetWindowIfNeeded(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_", Quotas: QuotaConfig{MaxPublishRate: 2}},
		},
	})

	// Use up quota
	m.CheckQuota("acme", "publish")
	m.CheckQuota("acme", "publish")
	if m.CheckQuota("acme", "publish") {
		t.Error("quota should be exceeded")
	}

	// Manually set window start to past to trigger reset
	tenant := m.GetTenantByID("acme")
	tenant.windowStart.Store(time.Now().Add(-2 * time.Second).Unix())

	if !m.CheckQuota("acme", "publish") {
		t.Error("quota should reset after window elapsed")
	}
}

func TestEnforceQuotas(t *testing.T) {
	mgr := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{
				ID:          "quota-tenant",
				TopicPrefix: "quota-tenant_",
				Quotas: QuotaConfig{
					MaxStorageBytes: 100,
				},
			},
		},
	})

	enforcer := NewResourceQuotaEnforcer(mgr)
	enforcer.Start()
	defer enforcer.Stop()

	// Trigger enforcement by recording enough storage
	enforcer.RecordStorageUpdate("quota-tenant", 150)

	// Give enforceQuotas ticker time to run
	time.Sleep(150 * time.Millisecond)
}

func TestCheckRateLimitDisabledManager(t *testing.T) {
	mgr := NewManager(Config{Enabled: false})
	enforcer := NewResourceQuotaEnforcer(mgr)

	if !enforcer.CheckRateLimit("any", "publish", time.Second, 1) {
		t.Error("should allow when manager disabled")
	}
}

func TestCheckRateLimitFetch(t *testing.T) {
	mgr := NewManager(Config{Enabled: true})
	enforcer := NewResourceQuotaEnforcer(mgr)

	enforcer.RecordFetch("tenant", 100)
	if !enforcer.CheckRateLimit("tenant", "fetch", time.Second, 2) {
		t.Error("should allow within fetch limit")
	}

	enforcer.RecordFetch("tenant", 100)
	enforcer.RecordFetch("tenant", 100)
	if enforcer.CheckRateLimit("tenant", "fetch", time.Second, 2) {
		t.Error("should deny over fetch limit")
	}
}

func TestCheckRateLimitUnknownOp(t *testing.T) {
	mgr := NewManager(Config{Enabled: true})
	enforcer := NewResourceQuotaEnforcer(mgr)

	if !enforcer.CheckRateLimit("tenant", "other", time.Second, 0) {
		t.Error("unknown op should be allowed")
	}
}

func TestIsTopicInNamespaceNoNamespace(t *testing.T) {
	ns := NewNamespaceIsolation()
	if ns.IsTopicInNamespace("unknown", "topic") {
		t.Error("should return false for unknown namespace")
	}
}

func TestGetTenantUsageStatsNoTenant(t *testing.T) {
	mgr := NewManager(Config{Enabled: true})
	enforcer := NewResourceQuotaEnforcer(mgr)

	stats := enforcer.GetTenantUsageStats("nonexistent")
	if stats["tenant_id"] != "nonexistent" {
		t.Errorf("tenant_id = %v", stats["tenant_id"])
	}
	// Should not contain quota keys when tenant is nil
	if _, ok := stats["quota_storage_bytes"]; ok {
		t.Error("should not have quota keys for missing tenant")
	}
}
