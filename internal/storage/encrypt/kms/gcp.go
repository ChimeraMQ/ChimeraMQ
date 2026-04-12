package kms

import (
	"context"
	"fmt"
)

// gcpProvider implements Google Cloud KMS provider.
type gcpProvider struct {
	cfg    Config
	client interface{} // Would be *kms.KeyManagementClient from GCP SDK
}

// newGCPProvider creates a new Google Cloud KMS provider.
// Note: This requires Google Cloud SDK for Go:
// go get cloud.google.com/go/kms/apiv1
func newGCPProvider(cfg Config) (Provider, error) {
	return &gcpProvider{
		cfg:    cfg,
		client: nil,
	}, nil
}

func (g *gcpProvider) GenerateDataKey(ctx context.Context, keyID string, keySpec string) (*DataKey, error) {
	return nil, fmt.Errorf("Google Cloud KMS support requires GCP SDK: go get cloud.google.com/go/kms/apiv1")
}

func (g *gcpProvider) DecryptDataKey(ctx context.Context, encryptedKey []byte, keyID string) ([]byte, error) {
	return nil, fmt.Errorf("Google Cloud KMS support requires GCP SDK: go get cloud.google.com/go/kms/apiv1")
}

func (g *gcpProvider) Encrypt(ctx context.Context, plaintext []byte, keyID string) ([]byte, error) {
	return nil, fmt.Errorf("Google Cloud KMS support requires GCP SDK: go get cloud.google.com/go/kms/apiv1")
}

func (g *gcpProvider) Decrypt(ctx context.Context, ciphertext []byte, keyID string) ([]byte, error) {
	return nil, fmt.Errorf("Google Cloud KMS support requires GCP SDK: go get cloud.google.com/go/kms/apiv1")
}

func (g *gcpProvider) Close() error {
	return nil
}

func (g *gcpProvider) Name() string {
	return "gcp"
}

// Ensure gcpProvider implements Provider interface.
var _ Provider = (*gcpProvider)(nil)
