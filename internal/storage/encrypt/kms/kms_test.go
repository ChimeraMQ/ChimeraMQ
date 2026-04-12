package kms

import (
	"context"
	"testing"
)

func TestNewMockProvider(t *testing.T) {
	provider := NewMockProvider()
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}

	if provider.Name() != "mock" {
		t.Errorf("expected name 'mock', got %s", provider.Name())
	}

	if len(provider.key) != 32 {
		t.Errorf("expected 32-byte key, got %d", len(provider.key))
	}
}

func TestMockProviderGenerateDataKey(t *testing.T) {
	provider := NewMockProvider()
	ctx := context.Background()

	dataKey, err := provider.GenerateDataKey(ctx, "test-key", "AES_256")
	if err != nil {
		t.Fatalf("GenerateDataKey failed: %v", err)
	}

	if dataKey == nil {
		t.Fatal("expected non-nil data key")
	}

	if dataKey.KeyID != "test-key" {
		t.Errorf("expected key ID 'test-key', got %s", dataKey.KeyID)
	}

	if len(dataKey.Plaintext) != 32 {
		t.Errorf("expected 32-byte plaintext, got %d", len(dataKey.Plaintext))
	}

	if string(dataKey.Ciphertext) != "mock-encrypted-key" {
		t.Errorf("expected ciphertext 'mock-encrypted-key', got %s", string(dataKey.Ciphertext))
	}
}

func TestMockProviderDecryptDataKey(t *testing.T) {
	provider := NewMockProvider()
	ctx := context.Background()

	decrypted, err := provider.DecryptDataKey(ctx, []byte("any-encrypted-key"), "test-key")
	if err != nil {
		t.Fatalf("DecryptDataKey failed: %v", err)
	}

	if len(decrypted) != 32 {
		t.Errorf("expected 32-byte decrypted key, got %d", len(decrypted))
	}
}

func TestMockProviderEncryptDecrypt(t *testing.T) {
	provider := NewMockProvider()
	ctx := context.Background()

	plaintext := []byte("hello world, this is a test message for encryption")

	// Encrypt
	ciphertext, err := provider.Encrypt(ctx, plaintext, "test-key")
	if err != nil {
		t.Fatalf("Encrypt failed: %v", err)
	}

	if len(ciphertext) != len(plaintext) {
		t.Errorf("expected ciphertext length %d, got %d", len(plaintext), len(ciphertext))
	}

	// Decrypt
	decrypted, err := provider.Decrypt(ctx, ciphertext, "test-key")
	if err != nil {
		t.Fatalf("Decrypt failed: %v", err)
	}

	// XOR is symmetric, so encrypting ciphertext should give back plaintext
	if string(decrypted) != string(plaintext) {
		t.Errorf("decrypted text doesn't match plaintext: got %s", string(decrypted))
	}
}

func TestMockProviderClose(t *testing.T) {
	provider := NewMockProvider()

	err := provider.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestNewWithMock(t *testing.T) {
	// Test that New returns error for unsupported providers
	// but we can test the mock provider directly
	provider := NewMockProvider()

	if provider.Name() != "mock" {
		t.Errorf("expected provider name 'mock', got %s", provider.Name())
	}
}

func TestNewWithEmptyProvider(t *testing.T) {
	cfg := Config{
		Provider: "",
	}

	_, err := New(cfg)
	if err == nil {
		t.Error("expected error for empty provider")
	}

	if err.Error() != "KMS provider not specified" {
		t.Errorf("expected specific error message, got: %s", err.Error())
	}
}

func TestNewWithUnsupportedProvider(t *testing.T) {
	cfg := Config{
		Provider: "unsupported",
	}

	_, err := New(cfg)
	if err == nil {
		t.Error("expected error for unsupported provider")
	}

	expectedMsg := "unsupported KMS provider: unsupported"
	if err.Error() != expectedMsg {
		t.Errorf("expected error '%s', got: %s", expectedMsg, err.Error())
	}
}

func TestNewWithAWSProvider(t *testing.T) {
	cfg := Config{
		Provider: "aws",
		Region:   "us-east-1",
		AWS: AWSConfig{
			AccessKeyID:     "test-key",
			SecretAccessKey: "test-secret",
		},
	}

	provider, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if provider.Name() != "aws" {
		t.Errorf("expected provider name 'aws', got %s", provider.Name())
	}

	// AWS provider methods return errors since SDK is not included
	ctx := context.Background()
	_, err = provider.GenerateDataKey(ctx, "key", "spec")
	if err == nil {
		t.Error("expected error for AWS GenerateDataKey without SDK")
	}
}

func TestNewWithVaultProvider(t *testing.T) {
	cfg := Config{
		Provider: "vault",
		Vault: VaultConfig{
			Address: "https://vault.example.com:8200",
			Token:   "test-token",
		},
	}

	provider, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if provider.Name() != "vault" {
		t.Errorf("expected provider name 'vault', got %s", provider.Name())
	}

	// Vault provider methods return errors since SDK is not included
	ctx := context.Background()
	_, err = provider.GenerateDataKey(ctx, "key", "spec")
	if err == nil {
		t.Error("expected error for Vault GenerateDataKey without SDK")
	}
}

func TestNewWithAzureProvider(t *testing.T) {
	cfg := Config{
		Provider: "azure",
		Azure: AzureConfig{
			TenantID:     "test-tenant",
			ClientID:     "test-client",
			ClientSecret: "test-secret",
			VaultURL:     "https://test-vault.vault.azure.net",
		},
	}

	provider, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if provider.Name() != "azure" {
		t.Errorf("expected provider name 'azure', got %s", provider.Name())
	}
}

func TestNewWithGCPProvider(t *testing.T) {
	cfg := Config{
		Provider: "gcp",
		GCP: GCPConfig{
			ProjectID:       "test-project",
			Location:        "us-central1",
			CredentialsPath: "/path/to/credentials.json",
		},
	}

	provider, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if provider.Name() != "gcp" {
		t.Errorf("expected provider name 'gcp', got %s", provider.Name())
	}
}

func TestAWSProviderClose(t *testing.T) {
	cfg := Config{
		Provider: "aws",
	}

	provider, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	err = provider.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestVaultProviderClose(t *testing.T) {
	cfg := Config{
		Provider: "vault",
	}

	provider, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	err = provider.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestDataKeyStructure(t *testing.T) {
	dataKey := &DataKey{
		Plaintext:  []byte("plaintext-key-data"),
		Ciphertext: []byte("encrypted-key-data"),
		KeyID:      "my-key-id",
	}

	if string(dataKey.Plaintext) != "plaintext-key-data" {
		t.Error("Plaintext mismatch")
	}

	if string(dataKey.Ciphertext) != "encrypted-key-data" {
		t.Error("Ciphertext mismatch")
	}

	if dataKey.KeyID != "my-key-id" {
		t.Error("KeyID mismatch")
	}
}

func TestConfigStructure(t *testing.T) {
	cfg := Config{
		Provider: "vault",
		KeyID:    "my-key",
		Region:   "us-east-1",
		Endpoint: "https://custom.endpoint",
		AWS: AWSConfig{
			AccessKeyID:     "access-key",
			SecretAccessKey: "secret-key",
			SessionToken:    "session-token",
			RoleARN:         "arn:aws:iam::123456789012:role/my-role",
		},
		Vault: VaultConfig{
			Address:       "https://vault.example.com",
			Token:         "vault-token",
			Role:          "my-role",
			MountPath:     "transit",
			TLSSkipVerify: false,
			TLSCertPath:   "/path/to/cert",
			TLSKeyPath:    "/path/to/key",
			TLSCAPath:     "/path/to/ca",
		},
		Azure: AzureConfig{
			TenantID:     "tenant-id",
			ClientID:     "client-id",
			ClientSecret: "client-secret",
			VaultURL:     "https://my-vault.vault.azure.net",
		},
		GCP: GCPConfig{
			ProjectID:       "gcp-project",
			Location:        "us-central1",
			CredentialsPath: "/path/to/creds.json",
			CredentialsJSON: "{}",
		},
	}

	if cfg.Provider != "vault" {
		t.Error("Provider mismatch")
	}

	if cfg.Vault.Address != "https://vault.example.com" {
		t.Error("Vault address mismatch")
	}

	if cfg.AWS.AccessKeyID != "access-key" {
		t.Error("AWS access key mismatch")
	}
}
