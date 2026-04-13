package kms

import (
	"context"
	"strings"
	"testing"
)

// --- AWS Provider Stubs ---

func TestAWSProviderDecryptDataKey(t *testing.T) {
	cfg := Config{Provider: "aws"}
	p, _ := New(cfg)
	_, err := p.DecryptDataKey(context.Background(), []byte("key"), "kid")
	if err == nil || !strings.Contains(err.Error(), "AWS SDK") {
		t.Errorf("expected AWS SDK error, got: %v", err)
	}
}

func TestAWSProviderEncrypt(t *testing.T) {
	cfg := Config{Provider: "aws"}
	p, _ := New(cfg)
	_, err := p.Encrypt(context.Background(), []byte("plain"), "kid")
	if err == nil || !strings.Contains(err.Error(), "AWS SDK") {
		t.Errorf("expected AWS SDK error, got: %v", err)
	}
}

func TestAWSProviderDecrypt(t *testing.T) {
	cfg := Config{Provider: "aws"}
	p, _ := New(cfg)
	_, err := p.Decrypt(context.Background(), []byte("cipher"), "kid")
	if err == nil || !strings.Contains(err.Error(), "AWS SDK") {
		t.Errorf("expected AWS SDK error, got: %v", err)
	}
}

func TestAWSProviderName(t *testing.T) {
	cfg := Config{Provider: "aws"}
	p, _ := New(cfg)
	if p.Name() != "aws" {
		t.Errorf("expected name aws, got %s", p.Name())
	}
}

// --- Vault Provider Stubs ---

func TestVaultProviderDecryptDataKey(t *testing.T) {
	cfg := Config{Provider: "vault"}
	p, _ := New(cfg)
	_, err := p.DecryptDataKey(context.Background(), []byte("key"), "kid")
	if err == nil || !strings.Contains(err.Error(), "Vault SDK") {
		t.Errorf("expected Vault SDK error, got: %v", err)
	}
}

func TestVaultProviderEncrypt(t *testing.T) {
	cfg := Config{Provider: "vault"}
	p, _ := New(cfg)
	_, err := p.Encrypt(context.Background(), []byte("plain"), "kid")
	if err == nil || !strings.Contains(err.Error(), "Vault SDK") {
		t.Errorf("expected Vault SDK error, got: %v", err)
	}
}

func TestVaultProviderDecrypt(t *testing.T) {
	cfg := Config{Provider: "vault"}
	p, _ := New(cfg)
	_, err := p.Decrypt(context.Background(), []byte("cipher"), "kid")
	if err == nil || !strings.Contains(err.Error(), "Vault SDK") {
		t.Errorf("expected Vault SDK error, got: %v", err)
	}
}

func TestVaultProviderName(t *testing.T) {
	cfg := Config{Provider: "vault"}
	p, _ := New(cfg)
	if p.Name() != "vault" {
		t.Errorf("expected name vault, got %s", p.Name())
	}
}

// --- Azure Provider Stubs ---

func TestAzureProviderMethods(t *testing.T) {
	cfg := Config{Provider: "azure"}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	ctx := context.Background()

	_, err = p.GenerateDataKey(ctx, "kid", "AES_256")
	if err == nil || !strings.Contains(err.Error(), "Azure SDK") {
		t.Errorf("expected Azure SDK error for GenerateDataKey, got: %v", err)
	}

	_, err = p.DecryptDataKey(ctx, []byte("key"), "kid")
	if err == nil || !strings.Contains(err.Error(), "Azure SDK") {
		t.Errorf("expected Azure SDK error for DecryptDataKey, got: %v", err)
	}

	_, err = p.Encrypt(ctx, []byte("plain"), "kid")
	if err == nil || !strings.Contains(err.Error(), "Azure SDK") {
		t.Errorf("expected Azure SDK error for Encrypt, got: %v", err)
	}

	_, err = p.Decrypt(ctx, []byte("cipher"), "kid")
	if err == nil || !strings.Contains(err.Error(), "Azure SDK") {
		t.Errorf("expected Azure SDK error for Decrypt, got: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if p.Name() != "azure" {
		t.Errorf("expected name azure, got %s", p.Name())
	}
}

// --- GCP Provider Stubs ---

func TestGCPProviderMethods(t *testing.T) {
	cfg := Config{Provider: "gcp"}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	ctx := context.Background()

	_, err = p.GenerateDataKey(ctx, "kid", "AES_256")
	if err == nil || !strings.Contains(err.Error(), "GCP SDK") {
		t.Errorf("expected GCP SDK error for GenerateDataKey, got: %v", err)
	}

	_, err = p.DecryptDataKey(ctx, []byte("key"), "kid")
	if err == nil || !strings.Contains(err.Error(), "GCP SDK") {
		t.Errorf("expected GCP SDK error for DecryptDataKey, got: %v", err)
	}

	_, err = p.Encrypt(ctx, []byte("plain"), "kid")
	if err == nil || !strings.Contains(err.Error(), "GCP SDK") {
		t.Errorf("expected GCP SDK error for Encrypt, got: %v", err)
	}

	_, err = p.Decrypt(ctx, []byte("cipher"), "kid")
	if err == nil || !strings.Contains(err.Error(), "GCP SDK") {
		t.Errorf("expected GCP SDK error for Decrypt, got: %v", err)
	}

	if err := p.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if p.Name() != "gcp" {
		t.Errorf("expected name gcp, got %s", p.Name())
	}
}
