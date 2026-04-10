package auth

import "context"

// Identity represents an authenticated user or service.
type Identity struct {
	UserID string
	Roles  []string
	Groups []string
	Source string            // "static", "file", "oauth", "ldap", "mtls"
	Claims map[string]string // additional claims (e.g., from JWT)
}

// Credentials holds the authentication credentials presented by a client.
type Credentials struct {
	Username string
	Password string
	Token    string
}

// AuthProvider is the interface for authentication backends.
type AuthProvider interface {
	// Authenticate verifies credentials and returns an identity.
	Authenticate(ctx context.Context, creds Credentials) (*Identity, error)
	// Close releases resources held by the provider.
	Close() error
}
