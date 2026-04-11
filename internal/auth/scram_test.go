package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/crypto/pbkdf2"
)

func TestSCRAMRegisterAndAuthenticate(t *testing.T) {
	p := NewSCRAMProvider()

	if err := p.RegisterUser("alice", "password123", 4096); err != nil {
		t.Fatalf("RegisterUser: %v", err)
	}

	// Direct password auth via AuthProvider interface
	id, err := p.Authenticate(context.Background(), Credentials{
		Username: "alice",
		Password: "password123",
	})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.UserID != "alice" {
		t.Errorf("UserID = %q, want alice", id.UserID)
	}
	if id.Source != "scram" {
		t.Errorf("Source = %q, want scram", id.Source)
	}
}

func TestSCRAMAuthenticateWrongPassword(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("bob", "secret", 4096)

	_, err := p.Authenticate(context.Background(), Credentials{
		Username: "bob",
		Password: "wrong",
	})
	if err != ErrInvalidCredentials {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}
}

func TestSCRAMAuthenticateUnknownUser(t *testing.T) {
	p := NewSCRAMProvider()

	_, err := p.Authenticate(context.Background(), Credentials{
		Username: "nobody",
		Password: "pass",
	})
	if err != ErrInvalidCredentials {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}
}

func TestSCRAMRegisterEmptyUsername(t *testing.T) {
	p := NewSCRAMProvider()
	err := p.RegisterUser("", "pass", 4096)
	if err == nil {
		t.Error("expected error for empty username")
	}
}

func TestSCRAMRegisterEmptyPassword(t *testing.T) {
	p := NewSCRAMProvider()
	err := p.RegisterUser("user", "", 4096)
	if err == nil {
		t.Error("expected error for empty password")
	}
}

func TestSCRAMRegisterMinIterations(t *testing.T) {
	p := NewSCRAMProvider()
	// iterations below minimum should be bumped to default
	p.RegisterUser("user1", "pass", 100)

	u, ok := p.GetUser("user1")
	if !ok {
		t.Fatal("user not found")
	}
	if u.Iterations != scramDefaultIters {
		t.Errorf("iterations = %d, want %d", u.Iterations, scramDefaultIters)
	}
}

func TestSCRAMRemoveUser(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("charlie", "pass", 4096)

	if _, ok := p.GetUser("charlie"); !ok {
		t.Fatal("user should exist")
	}

	p.RemoveUser("charlie")
	if _, ok := p.GetUser("charlie"); ok {
		t.Error("user should be removed")
	}
}

func TestSCRAMFullSASLExchange(t *testing.T) {
	p := NewSCRAMProvider()
	password := "correct-horse-battery-staple"
	p.RegisterUser("alice", password, 4096)

	// Step 1: Client sends client-first-message
	clientNonce := generateTestNonce()
	clientFirst := fmt.Sprintf("n,,n=alice,r=%s", clientNonce)

	// Step 2: Server processes client-first, returns server-first
	sess, serverFirst, err := p.StartExchange(clientFirst)
	if err != nil {
		t.Fatalf("StartExchange: %v", err)
	}
	if sess.Username() != "alice" {
		t.Errorf("username = %q, want alice", sess.Username())
	}

	// Parse server-first to extract server nonce, salt, iterations
	sfAttrs := parseTestAttrs(serverFirst)
	serverNonce := sfAttrs['r']
	if !strings.HasPrefix(serverNonce, clientNonce) {
		t.Errorf("server nonce should start with client nonce")
	}
	salt, _ := base64.StdEncoding.DecodeString(sfAttrs['s'])

	// Step 3: Client computes proof and sends client-final
	clientFirstBare := "n=alice,r=" + clientNonce
	clientFinalBare := fmt.Sprintf("c=biws,r=%s", serverNonce) // "biws" = base64("n,,")

	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalBare

	// Derive client keys from password
	saltedPassword := pbkdf2.Key([]byte(password), salt, 4096, 32, sha256.New)
	clientKey := hmacSHA256Test(saltedPassword, []byte("Client Key"))
	storedKeyClient := sha256SumTest(clientKey) // H(ClientKey) = StoredKey
	clientSig := hmacSHA256Test(storedKeyClient, []byte(authMessage))

	clientProof := xorBytesTest(clientKey, clientSig)
	proofB64 := base64.StdEncoding.EncodeToString(clientProof)

	clientFinal := clientFinalBare + ",p=" + proofB64

	// Step 4: Server verifies client-final, returns server-final
	serverFinal, err := sess.VerifyClientFinal(clientFinal)
	if err != nil {
		t.Fatalf("VerifyClientFinal: %v", err)
	}

	// Verify server signature
	sfAttrs2 := parseTestAttrs(serverFinal)
	serverSigB64 := sfAttrs2['v']
	serverKey := hmacSHA256Test(saltedPassword, []byte("Server Key"))
	expectedSig := hmacSHA256Test(serverKey, []byte(authMessage))
	expectedSigB64 := base64.StdEncoding.EncodeToString(expectedSig)

	if serverSigB64 != expectedSigB64 {
		t.Errorf("server signature mismatch:\n  got  %s\n  want %s", serverSigB64, expectedSigB64)
	}
}

func TestSCRAMSASLWrongPassword(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("alice", "right-password", 4096)

	clientNonce := generateTestNonce()
	clientFirst := fmt.Sprintf("n,,n=alice,r=%s", clientNonce)

	sess, serverFirst, err := p.StartExchange(clientFirst)
	if err != nil {
		t.Fatalf("StartExchange: %v", err)
	}

	// Client computes proof with WRONG password
	sfAttrs := parseTestAttrs(serverFirst)
	serverNonce := sfAttrs['r']
	salt, _ := base64.StdEncoding.DecodeString(sfAttrs['s'])

	clientFirstBare := "n=alice,r=" + clientNonce
	clientFinalBare := fmt.Sprintf("c=biws,r=%s", serverNonce)
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalBare

	wrongSalted := pbkdf2.Key([]byte("wrong-password"), salt, 4096, 32, sha256.New)
	wrongClientKey := hmacSHA256Test(wrongSalted, []byte("Client Key"))
	wrongStoredKey := sha256SumTest(wrongClientKey)
	wrongSig := hmacSHA256Test(wrongStoredKey, []byte(authMessage))
	wrongProof := xorBytesTest(wrongClientKey, wrongSig)

	clientFinal := clientFinalBare + ",p=" + base64.StdEncoding.EncodeToString(wrongProof)

	_, err = sess.VerifyClientFinal(clientFinal)
	if err != ErrSCRAMInvalidProof {
		t.Errorf("expected ErrSCRAMInvalidProof, got %v", err)
	}
}

func TestSCRAMSASLUnknownUser(t *testing.T) {
	p := NewSCRAMProvider()
	// No users registered

	clientFirst := "n,,n=ghost,r=noncenonce"
	_, _, err := p.StartExchange(clientFirst)
	if err != ErrInvalidCredentials {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}
}

func TestSCRAMSASLInvalidClientFirst(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("alice", "pass", 4096)

	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"no-gs2", "n=alice,r=nonce"},
		{"missing-username", "n,,r=nonce"},
		{"missing-nonce", "n,,n=alice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := p.StartExchange(tt.input)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestSCRAMSASLWrongNonce(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("alice", "pass", 4096)

	clientFirst := "n,,n=alice,r=clientnonce123"
	sess, serverFirst, err := p.StartExchange(clientFirst)
	if err != nil {
		t.Fatalf("StartExchange: %v", err)
	}

	// Client sends back a wrong nonce
	sfAttrs := parseTestAttrs(serverFirst)
	_ = sfAttrs // Use serverFirst but send wrong nonce

	// Build client-final with WRONG nonce
	clientFinalBare := "c=biws,r=wrongnonce"
	// proof doesn't matter — nonce check should fail first
	clientFinal := clientFinalBare + ",p=dGhpc2RvZXNub3RtYXR0ZXI="

	_, err = sess.VerifyClientFinal(clientFinal)
	if err != ErrSCRAMProtocolViolation {
		t.Errorf("expected ErrSCRAMProtocolViolation, got %v", err)
	}
}

func TestSCRAMDeriveKeysConsistency(t *testing.T) {
	// Verify that deriveKeys produces the same result when called with the same inputs
	password := "test-password"
	salt := []byte("fixed-salt-value-here")
	iterations := 4096

	sk1, svk1 := deriveKeys(password, salt, iterations)
	sk2, svk2 := deriveKeys(password, salt, iterations)

	if !hmac.Equal(sk1, sk2) {
		t.Error("StoredKey not deterministic")
	}
	if !hmac.Equal(svk1, svk2) {
		t.Error("ServerKey not deterministic")
	}
}

func TestSCRAMProviderClose(t *testing.T) {
	p := NewSCRAMProvider()
	if err := p.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

func TestSCRAMEmptyCredentials(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("alice", "pass", 4096)

	_, err := p.Authenticate(context.Background(), Credentials{})
	if err != ErrInvalidCredentials {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}
}

func TestSCRAMSetRoles(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("admin", "pass", 4096)

	// Set roles directly on user
	u, ok := p.GetUser("admin")
	if !ok {
		t.Fatal("user not found")
	}
	u.Roles = []string{"admin", "superuser"}

	id, err := p.Authenticate(context.Background(), Credentials{
		Username: "admin",
		Password: "pass",
	})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if len(id.Roles) != 2 {
		t.Errorf("roles = %v, want 2", id.Roles)
	}
}

// --- Test helpers ---

func generateTestNonce() string {
	return "testnonce123456"
}

func parseTestAttrs(msg string) map[byte]string {
	result := make(map[byte]string)
	for _, field := range strings.Split(msg, ",") {
		if len(field) >= 2 && field[1] == '=' {
			result[field[0]] = field[2:]
		}
	}
	return result
}

func sha256SumTest(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

func hmacSHA256Test(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func xorBytesTest(a, b []byte) []byte {
	result := make([]byte, len(a))
	for i := range a {
		result[i] = a[i] ^ b[i]
	}
	return result
}
