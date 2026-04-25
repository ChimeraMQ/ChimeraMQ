package kms

import (
	"context"
	"fmt"
)

// vaultProvider implements HashiCorp Vault provider.
type vaultProvider struct {
	cfg Config
}

// newVaultProvider creates a new HashiCorp Vault provider.
// Requires HashiCorp Vault SDK: go get github.com/hashicorp/vault/api
func newVaultProvider(cfg Config) (Provider, error) {
	return &vaultProvider{cfg: cfg}, nil
}

// GenerateDataKey generates a data key using Vault Transit engine.
func (v *vaultProvider) GenerateDataKey(ctx context.Context, keyID string, keySpec string) (*DataKey, error) {
	return nil, fmt.Errorf("vault support requires HashiCorp Vault SDK: go get github.com/hashicorp/vault/api")
}

// DecryptDataKey decrypts an encrypted data key using Vault Transit engine.
func (v *vaultProvider) DecryptDataKey(ctx context.Context, encryptedKey []byte, keyID string) ([]byte, error) {
	return nil, fmt.Errorf("vault support requires HashiCorp Vault SDK: go get github.com/hashicorp/vault/api")
}

// Encrypt encrypts plaintext using Vault Transit engine.
func (v *vaultProvider) Encrypt(ctx context.Context, plaintext []byte, keyID string) ([]byte, error) {
	return nil, fmt.Errorf("vault support requires HashiCorp Vault SDK: go get github.com/hashicorp/vault/api")
}

// Decrypt decrypts ciphertext using Vault Transit engine.
func (v *vaultProvider) Decrypt(ctx context.Context, ciphertext []byte, keyID string) ([]byte, error) {
	return nil, fmt.Errorf("vault support requires HashiCorp Vault SDK: go get github.com/hashicorp/vault/api")
}

// Close closes the Vault provider.
func (v *vaultProvider) Close() error {
	return nil
}

// Name returns the provider name.
func (v *vaultProvider) Name() string {
	return "vault"
}

// Ensure vaultProvider implements Provider interface.
var _ Provider = (*vaultProvider)(nil)
