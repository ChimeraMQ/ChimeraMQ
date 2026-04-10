package tenant

import (
	"testing"
)

func TestNewManager(t *testing.T) {
	m := NewManager(Config{Enabled: true})
	if !m.Enabled() {
		t.Error("should be enabled")
	}
}

func TestManagerDisabled(t *testing.T) {
	m := NewManager(Config{})
	if m.Enabled() {
		t.Error("should be disabled")
	}
	// GetTenant should return nil when disabled
	if m.GetTenant("any-topic") != nil {
		t.Error("should return nil when disabled")
	}
}

func TestRegisterAndGetTenant(t *testing.T) {
	m := NewManager(Config{
		Enabled:   true,
		Separator: "_",
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_"},
			{ID: "globex", TopicPrefix: "globex_"},
		},
	})

	acme := m.GetTenant("acme_orders")
	if acme == nil {
		t.Fatal("should find tenant for acme_ topic")
	}
	if acme.ID != "acme" {
		t.Errorf("tenant ID = %q, want acme", acme.ID)
	}

	globex := m.GetTenant("globex_events")
	if globex == nil {
		t.Fatal("should find tenant for globex_ topic")
	}

	unknown := m.GetTenant("unknown_topic")
	if unknown != nil {
		t.Error("should not find tenant for unknown prefix")
	}
}

func TestTenantForTopic(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_"},
		},
	})

	tenant, local, ok := m.TenantForTopic("acme_orders")
	if !ok {
		t.Fatal("should find tenant")
	}
	if tenant.ID != "acme" {
		t.Errorf("tenant = %q", tenant.ID)
	}
	if local != "orders" {
		t.Errorf("local = %q, want orders", local)
	}
}

func TestTenantForTopicNotFound(t *testing.T) {
	m := NewManager(Config{Enabled: true})
	_, _, ok := m.TenantForTopic("unknown")
	if ok {
		t.Error("should not find tenant for unknown topic")
	}
}

func TestRegisterTopic(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_", Quotas: QuotaConfig{MaxTopics: 5}},
		},
	})

	if err := m.RegisterTopic("acme", "acme_orders"); err != nil {
		t.Fatal(err)
	}
	if m.TopicCount("acme") != 1 {
		t.Errorf("count = %d, want 1", m.TopicCount("acme"))
	}
}

func TestRegisterTopicQuotaExceeded(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_", Quotas: QuotaConfig{MaxTopics: 2}},
		},
	})

	m.RegisterTopic("acme", "acme_t1")
	m.RegisterTopic("acme", "acme_t2")
	err := m.RegisterTopic("acme", "acme_t3")
	if err == nil {
		t.Error("expected quota exceeded error")
	}
}

func TestRegisterTopicUnknownTenant(t *testing.T) {
	m := NewManager(Config{Enabled: true})
	err := m.RegisterTopic("unknown", "topic")
	if err == nil {
		t.Error("expected error for unknown tenant")
	}
}

func TestUnregisterTopic(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_"},
		},
	})
	m.RegisterTopic("acme", "acme_orders")
	m.UnregisterTopic("acme", "acme_orders")
	if m.TopicCount("acme") != 0 {
		t.Error("should have 0 topics after unregister")
	}
}

func TestListTenants(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "a", TopicPrefix: "a_"},
			{ID: "b", TopicPrefix: "b_"},
		},
	})
	tenants := m.ListTenants()
	if len(tenants) != 2 {
		t.Errorf("tenants = %d, want 2", len(tenants))
	}
}

func TestListTopics(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_"},
		},
	})
	m.RegisterTopic("acme", "acme_orders")
	m.RegisterTopic("acme", "acme_events")
	topics := m.ListTopics("acme")
	if len(topics) != 2 {
		t.Errorf("topics = %d, want 2", len(topics))
	}
}

func TestListTopicsUnknown(t *testing.T) {
	m := NewManager(Config{Enabled: true})
	if topics := m.ListTopics("unknown"); topics != nil {
		t.Error("should be nil for unknown tenant")
	}
}

func TestCheckQuota(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_", Quotas: QuotaConfig{MaxPublishRate: 0}},
		},
	})
	if !m.CheckQuota("acme", "publish") {
		t.Error("should allow publish with rate 0 (unlimited)")
	}
}

func TestCheckQuotaUnknownTenant(t *testing.T) {
	m := NewManager(Config{Enabled: true})
	if m.CheckQuota("unknown", "publish") {
		t.Error("should not allow for unknown tenant")
	}
}

func TestRemoveTenant(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_"},
		},
	})
	m.Remove("acme")
	if m.GetTenantByID("acme") != nil {
		t.Error("tenant should be removed")
	}
}

func TestDefaultPrefix(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme"}, // no TopicPrefix → "acme_"
		},
	})
	t1 := m.GetTenant("acme_orders")
	if t1 == nil {
		t.Fatal("should find tenant with default prefix")
	}
}

func TestRuntimeRegister(t *testing.T) {
	m := NewManager(Config{Enabled: true})
	m.Register(&Tenant{
		ID:          "dynamic",
		TopicPrefix: "dyn_",
		Topics:      make(map[string]struct{}),
	})
	if m.GetTenant("dyn_topic") == nil {
		t.Error("should find runtime-registered tenant")
	}
}

func TestGetTenantByID(t *testing.T) {
	m := NewManager(Config{
		Enabled: true,
		Tenants: []TenantConfig{
			{ID: "acme", TopicPrefix: "acme_"},
		},
	})
	if m.GetTenantByID("acme") == nil {
		t.Error("should find by ID")
	}
	if m.GetTenantByID("unknown") != nil {
		t.Error("should not find unknown ID")
	}
}
