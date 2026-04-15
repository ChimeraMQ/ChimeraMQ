package kms

import (
	"context"
	"fmt"
)

// azureProvider implements Azure Key Vault provider.
type azureProvider struct {
	cfg    Config
	client interface{} // Would be *azkeys.Client from Azure SDK
}

// newAzureProvider creates a new Azure Key Vault provider.
// Note: This requires Azure SDK for Go:
// go get github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys
func newAzureProvider(cfg Config) (Provider, error) {
	return &azureProvider{
		cfg:    cfg,
		client: nil,
	}, nil
}

func (a *azureProvider) GenerateDataKey(ctx context.Context, keyID string, keySpec string) (*DataKey, error) {
	return nil, fmt.Errorf("azure Key Vault support requires Azure SDK: go get github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys")
}

func (a *azureProvider) DecryptDataKey(ctx context.Context, encryptedKey []byte, keyID string) ([]byte, error) {
	return nil, fmt.Errorf("azure Key Vault support requires Azure SDK: go get github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys")
}

func (a *azureProvider) Encrypt(ctx context.Context, plaintext []byte, keyID string) ([]byte, error) {
	return nil, fmt.Errorf("azure Key Vault support requires Azure SDK: go get github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys")
}

func (a *azureProvider) Decrypt(ctx context.Context, ciphertext []byte, keyID string) ([]byte, error) {
	return nil, fmt.Errorf("azure Key Vault support requires Azure SDK: go get github.com/Azure/azure-sdk-for-go/sdk/keyvault/azkeys")
}

func (a *azureProvider) Close() error {
	return nil
}

func (a *azureProvider) Name() string {
	return "azure"
}

// Ensure azureProvider implements Provider interface.
var _ Provider = (*azureProvider)(nil)
