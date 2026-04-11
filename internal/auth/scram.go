package auth

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/crypto/pbkdf2"
)

const (
	scramSHA256KeyLen   = 32
	scramDefaultIters   = 4096
	scramMinIters       = 4096
	scramSaltLen        = 24
	scramNonceLen       = 24
	scramClientKey      = "Client Key"
	scramServerKey      = "Server Key"
)

var (
	ErrSCRAMInvalidMessage    = errors.New("scram: invalid message")
	ErrSCRAMInvalidProof      = errors.New("scram: invalid proof")
	ErrSCRAMProtocolViolation = errors.New("scram: protocol violation")
)

// SCRAMUser holds stored SCRAM-SHA-256 credentials for a user.
type SCRAMUser struct {
	Salt       []byte
	Iterations int
	StoredKey  []byte
	ServerKey  []byte
	Roles      []string
}

// SCRAMProvider implements SCRAM-SHA-256 authentication.
// It stores salted credentials and supports the full 4-step SASL exchange
// as well as direct password verification via AuthProvider interface.
type SCRAMProvider struct {
	mu    sync.RWMutex
	users map[string]*SCRAMUser
}

// NewSCRAMProvider creates a SCRAM-SHA-256 provider with no users.
func NewSCRAMProvider() *SCRAMProvider {
	return &SCRAMProvider{
		users: make(map[string]*SCRAMUser),
	}
}

// RegisterUser creates SCRAM credentials for a user from a plaintext password.
// It generates a random salt and derives StoredKey/ServerKey via PBKDF2.
func (p *SCRAMProvider) RegisterUser(username, password string, iterations int) error {
	if username == "" {
		return fmt.Errorf("scram: username is required")
	}
	if password == "" {
		return fmt.Errorf("scram: password is required")
	}
	if iterations < scramMinIters {
		iterations = scramDefaultIters
	}

	salt := make([]byte, scramSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return fmt.Errorf("scram: generate salt: %w", err)
	}

	storedKey, serverKey := deriveKeys(password, salt, iterations)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.users[username] = &SCRAMUser{
		Salt:       salt,
		Iterations: iterations,
		StoredKey:  storedKey,
		ServerKey:  serverKey,
	}
	return nil
}

// RemoveUser removes a user from the SCRAM provider.
func (p *SCRAMProvider) RemoveUser(username string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.users, username)
}

// GetUser returns the SCRAM user data (for inspection/testing).
func (p *SCRAMProvider) GetUser(username string) (*SCRAMUser, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	u, ok := p.users[username]
	return u, ok
}

// Authenticate implements AuthProvider for direct password verification.
// This performs PBKDF2 on the password and checks against the stored credentials.
func (p *SCRAMProvider) Authenticate(ctx context.Context, creds Credentials) (*Identity, error) {
	if creds.Username == "" || creds.Password == "" {
		return nil, ErrInvalidCredentials
	}

	p.mu.RLock()
	user, ok := p.users[creds.Username]
	p.mu.RUnlock()

	if !ok {
		return nil, ErrInvalidCredentials
	}

	// Derive keys from the presented password and compare
	storedKey, _ := deriveKeys(creds.Password, user.Salt, user.Iterations)
	if !hmac.Equal(storedKey, user.StoredKey) {
		return nil, ErrInvalidCredentials
	}

	return &Identity{
		UserID: creds.Username,
		Source: "scram",
		Roles:  user.Roles,
	}, nil
}

// Close is a no-op for the SCRAM provider.
func (p *SCRAMProvider) Close() error { return nil }

// --- SASL Exchange ---

// SCRAMSession tracks an ongoing SASL SCRAM-SHA-256 exchange.
type SCRAMSession struct {
	username        string
	clientNonce     string
	serverNonce     string
	salt            []byte
	iterations      int
	storedKey       []byte
	serverKey       []byte
	clientFirstBare string
	serverFirst     string
}

// StartExchange parses the client-first-message and returns the server-first-message.
// clientFirst format: "n,,n=<user>,r=<nonce>" or "n,,n=<user>,r=<nonce>,..."
func (p *SCRAMProvider) StartExchange(clientFirst string) (*SCRAMSession, string, error) {
	// Parse client-first-message
	// Format: gs2-header,username,nonce[,extensions]
	// gs2-header: "n,," (no channel binding) or "y,," or "p,<cbname>,"
	clientFirstBare := stripGS2Header(clientFirst)
	if clientFirstBare == "" {
		return nil, "", ErrSCRAMInvalidMessage
	}

 attrs, err := parseSCRAMAttributes(clientFirstBare)
	if err != nil {
		return nil, "", err
	}

	username := attrs['n']
	clientNonce := attrs['r']
	if username == "" || clientNonce == "" {
		return nil, "", ErrSCRAMInvalidMessage
	}

	// Look up user
	p.mu.RLock()
	user, ok := p.users[username]
	p.mu.RUnlock()

	if !ok {
		return nil, "", ErrInvalidCredentials
	}

	// Generate server nonce (client-nonce + server-part)
	serverPart := generateNonce()
	serverNonce := clientNonce + serverPart

	// Build server-first-message
	saltB64 := base64.StdEncoding.EncodeToString(user.Salt)
	serverFirst := fmt.Sprintf("r=%s,s=%s,i=%d", serverNonce, saltB64, user.Iterations)

	sess := &SCRAMSession{
		username:        username,
		clientNonce:     clientNonce,
		serverNonce:     serverNonce,
		salt:            user.Salt,
		iterations:      user.Iterations,
		storedKey:       user.StoredKey,
		serverKey:       user.ServerKey,
		clientFirstBare: clientFirstBare,
		serverFirst:     serverFirst,
	}

	return sess, serverFirst, nil
}

// VerifyClientFinal verifies the client-final-message and returns the server-final-message.
// clientFinal format: "c=<channel-binding>,r=<nonce>,p=<proof>"
func (s *SCRAMSession) VerifyClientFinal(clientFinal string) (string, error) {
	attrs, err := parseSCRAMAttributes(clientFinal)
	if err != nil {
		return "", err
	}

	// Verify nonce
	if attrs['r'] != s.serverNonce {
		return "", ErrSCRAMProtocolViolation
	}

	// Extract proof
	proofB64 := attrs['p']
	if proofB64 == "" {
		return "", ErrSCRAMInvalidMessage
	}

	proof, err := base64.StdEncoding.DecodeString(proofB64)
	if err != nil {
		return "", ErrSCRAMInvalidMessage
	}

	// Compute AuthMessage = client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
	withoutProof := stripProof(clientFinal)
	authMessage := s.clientFirstBare + "," + s.serverFirst + "," + withoutProof

	// Verify client proof
	// ClientSignature = HMAC(StoredKey, AuthMessage)
	clientSig := hmacSHA256(s.storedKey, []byte(authMessage))

	// RecoveredClientKey = ClientProof XOR ClientSignature
	clientKey := xorBytes(proof, clientSig)

	// H(RecoveredClientKey) should equal StoredKey
	computedStoredKey := sha256Sum(clientKey)
	if !hmac.Equal(computedStoredKey, s.storedKey) {
		return "", ErrSCRAMInvalidProof
	}

	// Compute server signature
	serverSig := hmacSHA256(s.serverKey, []byte(authMessage))
	serverFinal := fmt.Sprintf("v=%s", base64.StdEncoding.EncodeToString(serverSig))

	return serverFinal, nil
}

// Username returns the authenticated username for this session.
func (s *SCRAMSession) Username() string {
	return s.username
}

// --- Helper functions ---

// deriveKeys computes StoredKey and ServerKey from a password using PBKDF2.
func deriveKeys(password string, salt []byte, iterations int) (storedKey, serverKey []byte) {
	saltedPassword := pbkdf2.Key([]byte(password), salt, iterations, scramSHA256KeyLen, sha256.New)
	clientKey := hmacSHA256(saltedPassword, []byte(scramClientKey))
	storedKey = sha256Sum(clientKey)
	serverKey = hmacSHA256(saltedPassword, []byte(scramServerKey))
	return
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func xorBytes(a, b []byte) []byte {
	result := make([]byte, len(a))
	for i := range a {
		result[i] = a[i] ^ b[i]
	}
	return result
}

func generateNonce() string {
	b := make([]byte, scramNonceLen)
	_, _ = rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}

// stripGS2Header removes the GS2 header (e.g., "n,," or "y,," or "p,cbname,")
// and returns the bare client-first-message.
func stripGS2Header(msg string) string {
	// GS2 header ends at the second comma or end of first three fields
	parts := strings.SplitN(msg, ",", 3)
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

// parseSCRAMAttributes parses a SCRAM message into a map of key→value.
// Keys are single characters: n=username, r=nonce, s=salt, i=iterations,
// c=channel-binding, p=proof, e=error.
func parseSCRAMAttributes(msg string) (map[byte]string, error) {
	result := make(map[byte]string, 6)
	for _, field := range strings.Split(msg, ",") {
		if len(field) < 2 {
			continue
		}
		key := field[0]
		value := field[2:] // skip "key="
		result[key] = value
	}
	return result, nil
}

// stripProof removes the "p=<proof>" attribute from a client-final-message
// to construct the AuthMessage input.
func stripProof(msg string) string {
	var buf bytes.Buffer
	first := true
	for _, field := range strings.Split(msg, ",") {
		if len(field) >= 2 && field[0] == 'p' && field[1] == '=' {
			continue
		}
		if !first {
			buf.WriteByte(',')
		}
		buf.WriteString(field)
		first = false
	}
	return buf.String()
}
