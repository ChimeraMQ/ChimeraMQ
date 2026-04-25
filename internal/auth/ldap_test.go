package auth

import (
	"context"
	"testing"
)

func TestLDAPNoCredentials(t *testing.T) {
	p := NewLDAPProvider("ldap://localhost:389", "", "", "dc=example,dc=com", "(uid={username})", false, "")
	defer p.Close()

	_, err := p.Authenticate(context.Background(), Credentials{})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestLDAPNoUsername(t *testing.T) {
	p := NewLDAPProvider("ldap://localhost:389", "", "", "dc=example,dc=com", "", false, "")
	defer p.Close()

	_, err := p.Authenticate(context.Background(), Credentials{Password: "pass"})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestLDAPNoPassword(t *testing.T) {
	p := NewLDAPProvider("ldap://localhost:389", "", "", "dc=example,dc=com", "", false, "")
	defer p.Close()

	_, err := p.Authenticate(context.Background(), Credentials{Username: "user"})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestExtractCN(t *testing.T) {
	tests := map[string]string{
		"cn=admin,ou=groups,dc=example,dc=com": "admin",
		"CN=Admin,OU=Groups,DC=example":        "Admin",
		"ou=something,dc=example":              "ou=something,dc=example", // no CN
		"":                                     "",
	}
	for input, expected := range tests {
		got := extractCN(input)
		if got != expected {
			t.Errorf("extractCN(%q) = %q, want %q", input, got, expected)
		}
	}
}

func TestLDAPProviderClose(t *testing.T) {
	p := NewLDAPProvider("ldap://localhost:389", "", "", "", "", false, "")
	if err := p.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
}
