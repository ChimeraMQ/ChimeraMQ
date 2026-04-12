package auth

import (
	"context"
	"crypto/subtle"
	"fmt"
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
}

// NewStaticProvider creates a static auth provider.
func NewStaticProvider(users, tokens map[string]string) *StaticProvider {
	return &StaticProvider{
		users:  users,
		tokens: tokens,
		roles:  make(map[string][]string),
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

	for storedToken, label := range p.tokens {
		if subtle.ConstantTimeCompare([]byte(token), []byte(storedToken)) == 1 {
			return &Identity{
				UserID: label,
				Source: "static",
				Roles:  p.roles[label],
			}, nil
		}
	}

	return nil, ErrInvalidCredentials
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
		UserID: username,
		Source: "static",
		Roles:  p.roles[username],
	}, nil
}

// SetRoles assigns roles to a user.
func (p *StaticProvider) SetRoles(user string, roles []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.roles[user] = roles
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
	data, err := readFile(fp.path)
	if err != nil {
		return err
	}

	var file struct {
		Users  map[string]userInfo `json:"users"`
		Tokens map[string]string   `json:"tokens"`
	}

	if err := parseJSON(data, &file); err != nil {
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
		for storedToken, label := range fp.tokens {
			if subtle.ConstantTimeCompare([]byte(creds.Token), []byte(storedToken)) == 1 {
				return &Identity{
					UserID: label,
					Source: "file",
				}, nil
			}
		}
		return nil, ErrInvalidCredentials
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
			UserID: creds.Username,
			Source: "file",
			Roles:  info.Roles,
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
