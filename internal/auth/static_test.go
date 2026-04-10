package auth

import (
	"context"
	"os"
	"testing"
)

func TestStaticProviderToken(t *testing.T) {
	p := NewStaticProvider(
		map[string]string{"admin": "password"},
		map[string]string{"my-token": "admin"},
	)
	defer p.Close()

	id, err := p.Authenticate(context.Background(), Credentials{Token: "my-token"})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "admin" {
		t.Errorf("UserID = %q, want %q", id.UserID, "admin")
	}
	if id.Source != "static" {
		t.Errorf("Source = %q, want %q", id.Source, "static")
	}
}

func TestStaticProviderTokenInvalid(t *testing.T) {
	p := NewStaticProvider(nil, map[string]string{"valid": "user"})
	defer p.Close()

	_, err := p.Authenticate(context.Background(), Credentials{Token: "invalid"})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestStaticProviderUserPlaintext(t *testing.T) {
	p := NewStaticProvider(
		map[string]string{"admin": "password123"},
		nil,
	)
	defer p.Close()

	id, err := p.Authenticate(context.Background(), Credentials{
		Username: "admin",
		Password: "password123",
	})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "admin" {
		t.Errorf("UserID = %q, want %q", id.UserID, "admin")
	}
}

func TestStaticProviderUserWrongPassword(t *testing.T) {
	p := NewStaticProvider(
		map[string]string{"admin": "password123"},
		nil,
	)
	defer p.Close()

	_, err := p.Authenticate(context.Background(), Credentials{
		Username: "admin",
		Password: "wrong",
	})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestStaticProviderUserNotFound(t *testing.T) {
	p := NewStaticProvider(map[string]string{"admin": "pass"}, nil)
	defer p.Close()

	_, err := p.Authenticate(context.Background(), Credentials{
		Username: "nonexistent",
		Password: "pass",
	})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestStaticProviderNoCredentials(t *testing.T) {
	p := NewStaticProvider(nil, nil)
	defer p.Close()

	_, err := p.Authenticate(context.Background(), Credentials{})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestStaticProviderRoles(t *testing.T) {
	p := NewStaticProvider(nil, map[string]string{"tok": "svc"})
	p.SetRoles("svc", []string{"admin", "writer"})
	defer p.Close()

	id, err := p.Authenticate(context.Background(), Credentials{Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	if len(id.Roles) != 2 || id.Roles[0] != "admin" || id.Roles[1] != "writer" {
		t.Errorf("Roles = %v, want [admin writer]", id.Roles)
	}
}

func TestFileProviderToken(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/auth.json"
	os.WriteFile(path, []byte(`{
		"users": {"admin": {"password": "secret", "roles": ["admin"]}},
		"tokens": {"api-key": "admin"}
	}`), 0600)

	p, err := NewFileProvider(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	id, err := p.Authenticate(context.Background(), Credentials{Token: "api-key"})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "admin" {
		t.Errorf("UserID = %q, want %q", id.UserID, "admin")
	}
	if id.Source != "file" {
		t.Errorf("Source = %q, want %q", id.Source, "file")
	}
}

func TestFileProviderUser(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/auth.json"
	os.WriteFile(path, []byte(`{
		"users": {"admin": {"password": "secret", "roles": ["admin"]}},
		"tokens": {}
	}`), 0600)

	p, err := NewFileProvider(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	id, err := p.Authenticate(context.Background(), Credentials{
		Username: "admin",
		Password: "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "admin" {
		t.Errorf("UserID = %q, want %q", id.UserID, "admin")
	}
	if len(id.Roles) != 1 || id.Roles[0] != "admin" {
		t.Errorf("Roles = %v, want [admin]", id.Roles)
	}
}

func TestFileProviderInvalidPath(t *testing.T) {
	_, err := NewFileProvider("/nonexistent/path/auth.json")
	if err == nil {
		t.Error("should fail for nonexistent file")
	}
}

func TestFileProviderReload(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/auth.json"
	os.WriteFile(path, []byte(`{
		"users": {"admin": {"password": "old"}},
		"tokens": {}
	}`), 0600)

	p, err := NewFileProvider(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Authenticate with old password
	_, err = p.Authenticate(context.Background(), Credentials{Username: "admin", Password: "old"})
	if err != nil {
		t.Fatal(err)
	}

	// Update file
	os.WriteFile(path, []byte(`{
		"users": {"admin": {"password": "new"}},
		"tokens": {}
	}`), 0600)

	if err := p.Reload(); err != nil {
		t.Fatal(err)
	}

	// Old password should fail
	_, err = p.Authenticate(context.Background(), Credentials{Username: "admin", Password: "old"})
	if err != ErrInvalidCredentials {
		t.Error("old password should fail after reload")
	}

	// New password should work
	_, err = p.Authenticate(context.Background(), Credentials{Username: "admin", Password: "new"})
	if err != nil {
		t.Fatal(err)
	}
}
