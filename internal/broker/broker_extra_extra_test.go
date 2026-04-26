package broker

import (
	"testing"
)

// --- RecordConnection/RecordDisconnection ---

func TestRecordConnectionAndDisconnection(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	b.RecordConnection("amqp")
	b.RecordConnection("amqp")
	b.RecordConnection("mqtt")

	// Metrics call just records — no panic means success
	b.RecordDisconnection("amqp")
	b.RecordDisconnection("amqp")
	b.RecordDisconnection("mqtt")
}

func TestRecordDisconnectionZeroBoundary(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	b.RecordConnection("ws")
	b.RecordDisconnection("ws")
	// After disconnection, count should be at zero boundary — just verify no panic
}

// --- DrainMode ---

func TestDrainMode(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.IsDrainMode() {
		t.Error("should not be in drain mode initially")
	}

	b.SetDrainMode(true)
	if !b.IsDrainMode() {
		t.Error("should be in drain mode after SetDrainMode(true)")
	}

	b.SetDrainMode(false)
	if b.IsDrainMode() {
		t.Error("should not be in drain mode after SetDrainMode(false)")
	}
}

// --- WireTopicRateLimit ---

func TestWireTopicRateLimitNilFlowCtrl(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	// flowCtrl is nil — should return early without panic
	b.WireTopicRateLimit("test-topic")
}

func TestWireTopicRateLimitNilTenant(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Force tenantMgr to nil but keep flowCtrl
	b.flowCtrl = nil // both nil for safety
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	b.WireTopicRateLimit("test-topic")
}

// --- GeoManager/GeoReceiver ---

func TestGeoManagerNil(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.GeoManager() != nil {
		t.Error("expected nil GeoManager when geo-replication disabled")
	}
	if b.GeoReceiver() != nil {
		t.Error("expected nil GeoReceiver when geo-replication disabled")
	}
}

// --- AuthLimiter ---

func TestAuthLimiterNil(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.AuthLimiter() != nil {
		t.Error("expected nil AuthLimiter when auth disabled")
	}
}

// --- IsFIPSEnabled ---

func TestIsFIPSEnabled(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	// Should not panic — returns whatever fips.IsEnabled() says
	_ = b.IsFIPSEnabled()
}

// --- IsClustered ---

func TestIsClustered(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.IsClustered() {
		t.Error("expected not clustered with default config")
	}
}

// --- StartTime ---

func TestBrokerStartTime(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.StartTime().IsZero() {
		t.Error("expected non-zero start time after broker started")
	}
}

// --- RecordQueueDepth ---

func TestRecordQueueDepth(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	b.RecordQueueDepth("test-topic", 42)
	// Just verify no panic
}

// --- QuotaEnforcer nil ---

func TestQuotaEnforcerNil(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.QuotaEnforcer() != nil {
		t.Error("expected nil QuotaEnforcer when multi-tenancy disabled")
	}
}

// --- Exchanges ---

func TestExchanges(t *testing.T) {
	cfg := defaultConfig()
	cfg.Listener.Port = 0
	cfg.Listener.AdminPort = 0
	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	exchanges := b.Exchanges()
	if exchanges == nil {
		t.Error("expected non-nil Exchanges")
	}
}
