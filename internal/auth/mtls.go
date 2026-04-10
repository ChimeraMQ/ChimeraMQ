package auth

import (
	"context"
	"crypto/x509"
	"fmt"
)

// MTLSProvider authenticates clients using mutual TLS client certificates.
type MTLSProvider struct {
	allowEmpty bool // if true, connections without client certs are allowed (passthrough)
}

// NewMTLSProvider creates a new mTLS authentication provider.
func NewMTLSProvider() *MTLSProvider {
	return &MTLSProvider{}
}

// SetAllowEmpty controls whether to allow connections without client certificates.
func (p *MTLSProvider) SetAllowEmpty(allow bool) {
	p.allowEmpty = allow
}

// Authenticate extracts identity from client certificates stored in context.
// The caller should inject peer certificates via context before calling this.
func (p *MTLSProvider) Authenticate(ctx context.Context, creds Credentials) (*Identity, error) {
	certs := PeerCertsFromContext(ctx)
	if len(certs) == 0 {
		if p.allowEmpty {
			return &Identity{UserID: "anonymous", Source: "mtls"}, nil
		}
		return nil, ErrInvalidCredentials
	}

	cert := certs[0]

	// Use Common Name as UserID
	userID := cert.Subject.CommonName
	if userID == "" {
		// Fall back to first SAN DNS name
		for _, name := range cert.DNSNames {
			userID = name
			break
		}
	}

	if userID == "" {
		return nil, fmt.Errorf("client certificate has no CN or SAN")
	}

	// Extract organization units as roles
	var roles []string
	for _, ou := range cert.Subject.OrganizationalUnit {
		roles = append(roles, ou)
	}

	// Extract organizations as groups
	var groups []string
	for _, org := range cert.Subject.Organization {
		groups = append(groups, org)
	}

	return &Identity{
		UserID: userID,
		Roles:  roles,
		Groups: groups,
		Source: "mtls",
	}, nil
}

// Close is a no-op for mTLS.
func (p *MTLSProvider) Close() error {
	return nil
}

// contextKey is used to store peer certificates in context.
type contextKey string

const peerCertsKey contextKey = "peer_certs"

// ContextWithPeerCerts creates a context with peer certificates.
func ContextWithPeerCerts(ctx context.Context, certs []*x509.Certificate) context.Context {
	return context.WithValue(ctx, peerCertsKey, certs)
}

// PeerCertsFromContext extracts peer certificates from context.
func PeerCertsFromContext(ctx context.Context) []*x509.Certificate {
	if ctx == nil {
		return nil
	}
	certs, _ := ctx.Value(peerCertsKey).([]*x509.Certificate)
	return certs
}
