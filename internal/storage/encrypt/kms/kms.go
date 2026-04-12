package kms

import (
	"context"
	"fmt"
)

// Provider is the interface for KMS providers.
type Provider interface {
	// GenerateDataKey generates a data key for encryption.
	GenerateDataKey(ctx context.Context, keyID string, keySpec string) (*DataKey, error)

	// DecryptDataKey decrypts an encrypted data key.
	DecryptDataKey(ctx context.Context, encryptedKey []byte, keyID string) ([]byte, error)

	// Encrypt encrypts plaintext using the KMS.
	Encrypt(ctx context.Context, plaintext []byte, keyID string) ([]byte, error)

	// Decrypt decrypts ciphertext using the KMS.
	Decrypt(ctx context.Context, ciphertext []byte, keyID string) ([]byte, error)

	// Close closes the KMS provider.
	Close() error

	// Name returns the provider name.
	Name() string
}

// DataKey represents a generated data key.
type DataKey struct {
	// Plaintext is the unencrypted data key.
	Plaintext []byte

	// Ciphertext is the encrypted data key.
	Ciphertext []byte

	// KeyID is the KMS key ID used to encrypt the data key.
	KeyID string
}

// Config holds KMS configuration.
type Config struct {
	// Provider is the KMS provider type: "aws", "vault", "azure", "gcp"
	Provider string `yaml:"provider"`

	// KeyID is the KMS key ID or ARN.
	KeyID string `yaml:"key_id"`

	// Region is the cloud region (for AWS/Azure/GCP).
	Region string `yaml:"region"`

	// Endpoint is the custom KMS endpoint (optional).
	Endpoint string `yaml:"endpoint"`

	// AWS specific configuration.
	AWS AWSConfig `yaml:"aws"`

	// Vault specific configuration.
	Vault VaultConfig `yaml:"vault"`

	// Azure specific configuration.
	Azure AzureConfig `yaml:"azure"`

	// GCP specific configuration.
	GCP GCPConfig `yaml:"gcp"`
}

// AWSConfig holds AWS KMS configuration.
type AWSConfig struct {
	// AccessKeyID is the AWS access key ID.
	AccessKeyID string `yaml:"access_key_id"`

	// SecretAccessKey is the AWS secret access key.
	SecretAccessKey string `yaml:"secret_access_key"`

	// SessionToken is the AWS session token (for temporary credentials).
	SessionToken string `yaml:"session_token"`

	// RoleARN is the IAM role to assume.
	RoleARN string `yaml:"role_arn"`
}

// VaultConfig holds HashiCorp Vault configuration.
type VaultConfig struct {
	// Address is the Vault server address.
	Address string `yaml:"address"`

	// Token is the Vault authentication token.
	Token string `yaml:"token"`

	// Role is the Vault role for authentication.
	Role string `yaml:"role"`

	// MountPath is the transit engine mount path.
	MountPath string `yaml:"mount_path"`

	// TLS configuration.
	TLSSkipVerify bool   `yaml:"tls_skip_verify"`
	TLSCertPath   string `yaml:"tls_cert_path"`
	TLSKeyPath    string `yaml:"tls_key_path"`
	TLSCAPath     string `yaml:"tls_ca_path"`
}

// AzureConfig holds Azure Key Vault configuration.
type AzureConfig struct {
	// TenantID is the Azure tenant ID.
	TenantID string `yaml:"tenant_id"`

	// ClientID is the Azure service principal client ID.
	ClientID string `yaml:"client_id"`

	// ClientSecret is the Azure service principal client secret.
	ClientSecret string `yaml:"client_secret"`

	// VaultURL is the Azure Key Vault URL.
	VaultURL string `yaml:"vault_url"`
}

// GCPConfig holds Google Cloud KMS configuration.
type GCPConfig struct {
	// ProjectID is the GCP project ID.
	ProjectID string `yaml:"project_id"`

	// Location is the GCP location.
	Location string `yaml:"location"`

	// CredentialsPath is the path to service account credentials file.
	CredentialsPath string `yaml:"credentials_path"`

	// CredentialsJSON is the raw service account credentials JSON.
	CredentialsJSON string `yaml:"credentials_json"`
}

// New creates a new KMS provider based on configuration.
func New(cfg Config) (Provider, error) {
	switch cfg.Provider {
	case "aws":
		return newAWSProvider(cfg)
	case "vault":
		return newVaultProvider(cfg)
	case "azure":
		return newAzureProvider(cfg)
	case "gcp":
		return newGCPProvider(cfg)
	case "":
		return nil, fmt.Errorf("KMS provider not specified")
	default:
		return nil, fmt.Errorf("unsupported KMS provider: %s", cfg.Provider)
	}
}

// MockProvider is a mock KMS provider for testing.
type MockProvider struct {
	key []byte
}

// NewMockProvider creates a new mock KMS provider.
func NewMockProvider() *MockProvider {
	return &MockProvider{
		key: make([]byte, 32), // 256-bit key
	}
}

// GenerateDataKey generates a data key.
func (m *MockProvider) GenerateDataKey(ctx context.Context, keyID string, keySpec string) (*DataKey, error) {
	// In mock mode, return a fixed key
	return &DataKey{
		Plaintext:  m.key,
		Ciphertext: []byte("mock-encrypted-key"),
		KeyID:      keyID,
	}, nil
}

// DecryptDataKey decrypts an encrypted data key.
func (m *MockProvider) DecryptDataKey(ctx context.Context, encryptedKey []byte, keyID string) ([]byte, error) {
	// In mock mode, return the fixed key
	return m.key, nil
}

// Encrypt encrypts plaintext.
func (m *MockProvider) Encrypt(ctx context.Context, plaintext []byte, keyID string) ([]byte, error) {
	// Simple XOR for mock (DO NOT USE IN PRODUCTION)
	ciphertext := make([]byte, len(plaintext))
	for i, p := range plaintext {
		ciphertext[i] = p ^ m.key[i%len(m.key)]
	}
	return ciphertext, nil
}

// Decrypt decrypts ciphertext.
func (m *MockProvider) Decrypt(ctx context.Context, ciphertext []byte, keyID string) ([]byte, error) {
	// XOR is symmetric
	return m.Encrypt(ctx, ciphertext, keyID)
}

// Close closes the mock provider.
func (m *MockProvider) Close() error {
	return nil
}

// Name returns the provider name.
func (m *MockProvider) Name() string {
	return "mock"
}

// Ensure MockProvider implements Provider interface.
var _ Provider = (*MockProvider)(nil)
