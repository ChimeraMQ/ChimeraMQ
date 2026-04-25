package auth

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"golang.org/x/crypto/bcrypt"
)

// StaticProvider authenticates against in-memory users and token maps.
type StaticProvider struct {
	mu     sync.RWMutex
	users  map[string]string // username -> bcrypt hash or plaintext
	tokens map[string]string // token -> label
	roles  map[string][]string
	tenants map[string]string // label -> tenant ID
}

// NewStaticProvider creates a static auth provider.
func NewStaticProvider(users, tokens map[string]string) *StaticProvider {
	return &StaticProvider{
		users:  users,
		tokens: tokens,
		roles:  make(map[string][]string),
		tenants: make(map[string]string),
	}
}

// Authenticate checks token or username/password credentials.
func (p *StaticProvider) Authenticate(ctx context.Context, creds Credentials) (*Identity, error) {
	// Token auth takes priority
	if creds.Token != "" {
		return p.authenticateToken(creds.Token)
	}

	// Username/password auth
	if creds.Username != "" {
		return p.authenticateUser(creds.Username, creds.Password)
	}

	return nil, ErrInvalidCredentials
}

func (p *StaticProvider) authenticateToken(token string) (*Identity, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Constant-time iteration: check ALL tokens regardless of match position
	// to prevent timing side-channel that would leak token position.
	var matchIdx int = -1
	var matchLabel string
	idx := 0
	for storedToken, label := range p.tokens {
		if subtle.ConstantTimeCompare([]byte(token), []byte(storedToken)) == 1 {
			matchIdx = idx
			matchLabel = label
		}
		idx++
	}

	if matchIdx < 0 {
		return nil, ErrInvalidCredentials
	}

	return &Identity{
		UserID:   matchLabel,
		TenantID: p.tenants[matchLabel],
		Source:   "static",
		Roles:    p.roles[matchLabel],
	}, nil
}

func (p *StaticProvider) authenticateUser(username, password string) (*Identity, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	hash, ok := p.users[username]
	if !ok {
		return nil, ErrInvalidCredentials
	}

	// Try bcrypt first, then fall back to plaintext comparison
	if strings.HasPrefix(hash, "$2a$") || strings.HasPrefix(hash, "$2b$") || strings.HasPrefix(hash, "$2y$") {
		if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)); err != nil {
			return nil, ErrInvalidCredentials
		}
	} else {
		// Reject non-bcrypt hashes in production
		return nil, ErrInvalidCredentials
	}

	return &Identity{
		UserID:   username,
		TenantID: p.tenants[username],
		Source:   "static",
		Roles:    p.roles[username],
	}, nil
}

// SetRoles assigns roles to a user.
func (p *StaticProvider) SetRoles(user string, roles []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.roles[user] = roles
}

// SetTenant assigns a tenant ID to a user.
func (p *StaticProvider) SetTenant(user string, tenant string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tenants[user] = tenant
}

// Close is a no-op for the static provider.
func (p *StaticProvider) Close() error { return nil }

// FileProvider authenticates against a JSON file with users and tokens.
type FileProvider struct {
	mu     sync.RWMutex
	path   string
	users  map[string]userInfo
	tokens map[string]string
}

type userInfo struct {
	Password string   `json:"password"` // bcrypt hash or plaintext
	Roles    []string `json:"roles,omitempty"`
	Tenant   string   `json:"tenant,omitempty"`
}

// NewFileProvider creates a file-based auth provider.
func NewFileProvider(path string) (*FileProvider, error) {
	fp := &FileProvider{
		path:   path,
		users:  make(map[string]userInfo),
		tokens: make(map[string]string),
	}
	if err := fp.load(); err != nil {
		return nil, fmt.Errorf("load auth file %q: %w", path, err)
	}
	return fp, nil
}

func (fp *FileProvider) load() error {
	// File format:
	// {
	//   "users": {"admin": {"password": "$2a$10$...", "roles": ["admin"]}},
	//   "tokens": {"my-key": "service-name"}
	// }
	data, err := os.ReadFile(fp.path)
	if err != nil {
		return err
	}

	var file struct {
		Users  map[string]userInfo `json:"users"`
		Tokens map[string]string   `json:"tokens"`
	}

	if err := json.Unmarshal(data, &file); err != nil {
		return err
	}

	fp.mu.Lock()
	defer fp.mu.Unlock()

	fp.users = file.Users
	if fp.users == nil {
		fp.users = make(map[string]userInfo)
	}
	fp.tokens = file.Tokens
	if fp.tokens == nil {
		fp.tokens = make(map[string]string)
	}
	return nil
}

// Authenticate checks credentials against the loaded file data.
func (fp *FileProvider) Authenticate(ctx context.Context, creds Credentials) (*Identity, error) {
	fp.mu.RLock()
	defer fp.mu.RUnlock()

	if creds.Token != "" {
		// Constant-time iteration: check ALL tokens regardless of match
		var matchLabel string
		var found bool
		for storedToken, label := range fp.tokens {
			if subtle.ConstantTimeCompare([]byte(creds.Token), []byte(storedToken)) == 1 {
				matchLabel = label
				found = true
			}
		}
		if !found {
			return nil, ErrInvalidCredentials
		}
		return &Identity{
			UserID:   matchLabel,
			TenantID: fp.users[matchLabel].Tenant,
			Source:   "file",
		}, nil
	}

	if creds.Username != "" {
		info, ok := fp.users[creds.Username]
		if !ok {
			return nil, ErrInvalidCredentials
		}

		hash := info.Password
		if strings.HasPrefix(hash, "$2a$") || strings.HasPrefix(hash, "$2b$") || strings.HasPrefix(hash, "$2y$") {
			if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(creds.Password)); err != nil {
				return nil, ErrInvalidCredentials
			}
		} else {
			// Reject non-bcrypt hashes in production
			return nil, ErrInvalidCredentials
		}

		return &Identity{
			UserID:   creds.Username,
			TenantID: info.Tenant,
			Source:   "file",
			Roles:    info.Roles,
		}, nil
	}

	return nil, ErrInvalidCredentials
}

// Reload re-reads the auth file from disk.
func (fp *FileProvider) Reload() error {
	return fp.load()
}

// Close is a no-op for the file provider.
func (fp *FileProvider) Close() error { return nil }
