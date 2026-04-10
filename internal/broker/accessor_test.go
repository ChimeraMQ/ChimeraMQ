package broker

import (
	"os"
	"testing"
)

func TestBrokerAccessorsNil(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	// Accessors that return nil when features not enabled
	if b.Cluster() != nil {
		t.Error("Cluster() should be nil without cluster config")
	}
	if b.WarmEngine() != nil {
		t.Error("WarmEngine() should be nil without warm config")
	}
	if b.ColdManager() != nil {
		t.Error("ColdManager() should be nil without cold config")
	}
	if b.Migrator() != nil {
		t.Error("Migrator() should be nil without tier config")
	}
	if b.Encryptor() != nil {
		t.Error("Encryptor() should be nil without encryption config")
	}
	if b.SchemaRegistry() != nil {
		t.Error("SchemaRegistry() should be nil without schema config")
	}
	if b.SchemaEnforcer() != nil {
		t.Error("SchemaEnforcer() should be nil without schema config")
	}
	if b.TTLExpirer() != nil {
		t.Error("TTLExpirer() should be nil without TTL config")
	}
	if b.WASMRuntime() != nil {
		t.Error("WASMRuntime() should be nil without WASM config")
	}
	if b.Processor() != nil {
		t.Error("Processor() should be nil without processing config")
	}
	if b.AuthProvider() != nil {
		t.Error("AuthProvider() should be nil without auth config")
	}
	if b.Tracer() != nil {
		t.Error("Tracer() should be nil without tracing config")
	}
	if b.ACLEngine() != nil {
		t.Error("ACLEngine() should be nil without ACL config")
	}
	if b.DLQHandler() != nil {
		t.Error("DLQHandler() should be nil without DLQ config")
	}
	if b.FlowController() != nil {
		t.Error("FlowController() should be nil without flow control config")
	}
	if b.Deduper() != nil {
		t.Error("Deduper() should be nil without idempotent config")
	}
	if b.IsClustered() {
		t.Error("IsClustered() should be false in single-node mode")
	}
}

func TestBrokerAccessorsWithWarm(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Storage.Warm.Enabled = true

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.WarmEngine() == nil {
		t.Error("WarmEngine() should not be nil when warm enabled")
	}
}

func TestBrokerAccessorsWithEncryption(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Storage.Encryption.Enabled = true
	// Create a key file for the encryptor
	keyPath := dir + "/encrypt.key"
	os.WriteFile(keyPath, make([]byte, 32), 0600)
	cfg.Storage.Encryption.KeyPath = keyPath

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.Encryptor() == nil {
		t.Error("Encryptor() should not be nil when encryption enabled")
	}
}

func TestBrokerAccessorsWithSchema(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Schema.Enabled = true

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.SchemaRegistry() == nil {
		t.Error("SchemaRegistry() should not be nil when schema enabled")
	}
	if b.SchemaEnforcer() == nil {
		t.Error("SchemaEnforcer() should not be nil when schema enabled")
	}
}

func TestBrokerAccessorsWithTTL(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.TTL.Enabled = true

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.TTLExpirer() == nil {
		t.Error("TTLExpirer() should not be nil when TTL enabled")
	}
}

func TestBrokerAccessorsWithStaticAuth(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "static"
	cfg.Auth.Users = map[string]string{"admin": "password"}
	cfg.Auth.Tokens = map[string]string{"token1": "admin"}

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.AuthProvider() == nil {
		t.Error("AuthProvider() should not be nil when auth enabled")
	}
}

func TestBrokerAccessorsWithACL(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Auth.Enabled = true
	cfg.Auth.Type = "static"
	cfg.Auth.Users = map[string]string{"admin": "password"}
	cfg.ACL.Enabled = true
	cfg.ACL.DefaultPolicy = "deny"
	cfg.ACL.Entries = []ACLEntryConfig{
		{Principal: "admin", Resource: "topic", Name: "*", Operation: "all", Permission: "allow"},
	}

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.ACLEngine() == nil {
		t.Error("ACLEngine() should not be nil when ACL enabled")
	}
}

func TestBrokerAccessorsWithCold(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Storage.Warm.Enabled = true
	cfg.Storage.Cold.Enabled = true

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.ColdManager() == nil {
		t.Error("ColdManager() should not be nil when cold enabled")
	}
	if b.Migrator() == nil {
		t.Error("Migrator() should not be nil when warm+cold enabled")
	}
}

func TestBrokerAccessorsWithDLQ(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.DLQ.Enabled = true
	cfg.DLQ.TopicPrefix = "dlq-"
	cfg.DLQ.MaxRetries = 3

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.DLQHandler() == nil {
		t.Error("DLQHandler() should not be nil when DLQ enabled")
	}
}

func TestBrokerAccessorsWithFlowControl(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.FlowControl.Enabled = true
	cfg.FlowControl.MaxMemoryBytes = 1024 * 1024 * 1024

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.FlowController() == nil {
		t.Error("FlowController() should not be nil when flow control enabled")
	}
}

func TestBrokerAccessorsWithIdempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.Node.DataDir = dir
	cfg.Idempotent.Enabled = true

	b, err := NewBroker(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	defer b.Stop()

	if b.Deduper() == nil {
		t.Error("Deduper() should not be nil when idempotent enabled")
	}
}
