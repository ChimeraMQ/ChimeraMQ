package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"
)

func makeTestCert(cn string, ous []string, orgs []string, sans []string) *x509.Certificate {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:         cn,
			OrganizationalUnit: ous,
			Organization:       orgs,
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(time.Hour),
		DNSNames:    sans,
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(certDER)
	return cert
}

func TestMTLSProviderBasic(t *testing.T) {
	p := NewMTLSProvider()
	defer p.Close()

	cert := makeTestCert("alice", []string{"admin"}, []string{"engineering"}, nil)
	ctx := ContextWithPeerCerts(context.Background(), []*x509.Certificate{cert})

	id, err := p.Authenticate(ctx, Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "alice" {
		t.Errorf("UserID = %q, want %q", id.UserID, "alice")
	}
	if id.Source != "mtls" {
		t.Errorf("Source = %q, want %q", id.Source, "mtls")
	}
	if len(id.Roles) != 1 || id.Roles[0] != "admin" {
		t.Errorf("Roles = %v, want [admin]", id.Roles)
	}
	if len(id.Groups) != 1 || id.Groups[0] != "engineering" {
		t.Errorf("Groups = %v, want [engineering]", id.Groups)
	}
}

func TestMTLSProviderSANFallback(t *testing.T) {
	p := NewMTLSProvider()
	defer p.Close()

	cert := makeTestCert("", nil, nil, []string{"svc1.example.com"})
	ctx := ContextWithPeerCerts(context.Background(), []*x509.Certificate{cert})

	id, err := p.Authenticate(ctx, Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "svc1.example.com" {
		t.Errorf("UserID = %q, want %q", id.UserID, "svc1.example.com")
	}
}

func TestMTLSProviderNoCerts(t *testing.T) {
	p := NewMTLSProvider()
	defer p.Close()

	ctx := context.Background()
	_, err := p.Authenticate(ctx, Credentials{})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestMTLSProviderNoCNNoSAN(t *testing.T) {
	p := NewMTLSProvider()
	defer p.Close()

	cert := makeTestCert("", nil, nil, nil)
	ctx := ContextWithPeerCerts(context.Background(), []*x509.Certificate{cert})

	_, err := p.Authenticate(ctx, Credentials{})
	if err == nil {
		t.Error("expected error for cert with no CN or SAN")
	}
}

func TestMTLSProviderAllowEmpty(t *testing.T) {
	p := NewMTLSProvider()
	p.SetAllowEmpty(true)
	defer p.Close()

	ctx := context.Background()
	id, err := p.Authenticate(ctx, Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "anonymous" {
		t.Errorf("UserID = %q, want %q", id.UserID, "anonymous")
	}
}

func TestMTLSProviderMultipleRoles(t *testing.T) {
	p := NewMTLSProvider()
	defer p.Close()

	cert := makeTestCert("admin", []string{"admin", "writer", "reader"}, nil, nil)
	ctx := ContextWithPeerCerts(context.Background(), []*x509.Certificate{cert})

	id, err := p.Authenticate(ctx, Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	if len(id.Roles) != 3 {
		t.Errorf("Roles = %v, want 3 roles", id.Roles)
	}
}
