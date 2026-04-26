package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"testing"

	"golang.org/x/crypto/pbkdf2"
)

func TestSCRAMValidateClientServerFinalValidSignature(t *testing.T) {
	p := NewSCRAMProvider()
	password := "test-password"
	p.RegisterUser("alice", password, 4096)

	// Run full SASL exchange to get a session with authMessage and serverKey
	clientNonce := generateTestNonce()
	clientFirst := fmt.Sprintf("n,,n=alice,r=%s", clientNonce)

	sess, serverFirst, err := p.StartExchange(clientFirst)
	if err != nil {
		t.Fatalf("StartExchange: %v", err)
	}

	sfAttrs := parseTestAttrs(serverFirst)
	serverNonce := sfAttrs['r']
	salt, _ := base64.StdEncoding.DecodeString(sfAttrs['s'])

	clientFirstBare := "n=alice,r=" + clientNonce
	clientFinalBare := fmt.Sprintf("c=biws,r=%s", serverNonce)
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalBare

	saltedPassword := pbkdf2.Key([]byte(password), salt, 4096, 32, sha256.New)
	clientKey := hmacSHA256Test(saltedPassword, []byte("Client Key"))
	storedKeyClient := sha256SumTest(clientKey)
	clientSig := hmacSHA256Test(storedKeyClient, []byte(authMessage))
	clientProof := xorBytesTest(clientKey, clientSig)
	clientFinal := clientFinalBare + ",p=" + base64.StdEncoding.EncodeToString(clientProof)

	// Verify client final to get server final
	serverFinal, err := sess.VerifyClientFinal(clientFinal)
	if err != nil {
		t.Fatalf("VerifyClientFinal: %v", err)
	}

	// Now ValidateClientServerFinal — client echoes back the server signature
	sfAttrs2 := parseTestAttrs(serverFinal)
	serverSigB64 := sfAttrs2['v']

	// Client sends back "v=<sig>" to confirm it verified the server
	err = sess.ValidateClientServerFinal("v=" + serverSigB64)
	if err != nil {
		t.Errorf("ValidateClientServerFinal: %v", err)
	}
}

func TestSCRAMValidateClientServerFinalMissingPrefix(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("alice", "pass", 4096)

	clientNonce := generateTestNonce()
	clientFirst := fmt.Sprintf("n,,n=alice,r=%s", clientNonce)

	sess, _, err := p.StartExchange(clientFirst)
	if err != nil {
		t.Fatalf("StartExchange: %v", err)
	}

	// Missing "v=" prefix
	err = sess.ValidateClientServerFinal("wrong-format")
	if err == nil {
		t.Error("expected error for missing v= prefix")
	}
}

func TestSCRAMValidateClientServerFinalWrongSignature(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("alice", "pass", 4096)

	clientNonce := generateTestNonce()
	clientFirst := fmt.Sprintf("n,,n=alice,r=%s", clientNonce)

	sess, _, err := p.StartExchange(clientFirst)
	if err != nil {
		t.Fatalf("StartExchange: %v", err)
	}

	// Wrong signature
	err = sess.ValidateClientServerFinal("v=d3Jvbmc=")
	if err == nil {
		t.Error("expected error for wrong signature")
	}
}

func TestSCRAMUsername(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("alice", "pass", 4096)

	clientNonce := generateTestNonce()
	clientFirst := fmt.Sprintf("n,,n=alice,r=%s", clientNonce)

	sess, _, err := p.StartExchange(clientFirst)
	if err != nil {
		t.Fatalf("StartExchange: %v", err)
	}

	if sess.Username() != "alice" {
		t.Errorf("Username = %q, want alice", sess.Username())
	}
}

func TestSCRAMParseSCRAMAttributes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  map[byte]string
	}{
		{
			name:  "simple",
			input: "n=alice,r=nonce123",
			want:  map[byte]string{'n': "alice", 'r': "nonce123"},
		},
		{
			name:  "with-channel-binding",
			input: "c=biws,r=nonce,p=proof",
			want:  map[byte]string{'c': "biws", 'r': "nonce", 'p': "proof"},
		},
		{
			name:  "empty-attribute-value",
			input: "n=,r=nonce",
			want:  map[byte]string{'n': "", 'r': "nonce"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSCRAMAttributes(tt.input)
			if err != nil {
				t.Fatalf("parseSCRAMAttributes: %v", err)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Errorf("attr[%c] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestSCRAMParseSCRAMAttributesInvalid(t *testing.T) {
	// Short fields (no '=') are silently skipped
	result, err := parseSCRAMAttributes("n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty map, got %d entries", len(result))
	}
}

func TestSCRAMStripGS2Header(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"no-channel-binding", "n,,n=alice,r=nonce", "n=alice,r=nonce"},
		{"channel-binding-y", "y,,n=alice,r=nonce", "n=alice,r=nonce"},
		{"channel-binding-p", "p=tls-server,,n=alice,r=nonce", "n=alice,r=nonce"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripGS2Header(tt.input)
			if got != tt.want {
				t.Errorf("stripGS2Header = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSCRAMHMACSHA256(t *testing.T) {
	key := []byte("key")
	data := []byte("data")
	result := hmacSHA256(key, data)
	if len(result) != 32 {
		t.Errorf("hmacSHA256 length = %d, want 32", len(result))
	}
	// Verify it matches standard library
	h := hmac.New(sha256.New, key)
	h.Write(data)
	expected := h.Sum(nil)
	if !hmac.Equal(result, expected) {
		t.Error("hmacSHA256 doesn't match standard library")
	}
}

func TestSCRAMSHA256Sum(t *testing.T) {
	data := []byte("test")
	result := sha256Sum(data)
	if len(result) != 32 {
		t.Errorf("sha256Sum length = %d, want 32", len(result))
	}
}

func TestSCRAMXORBytes(t *testing.T) {
	a := []byte{0xff, 0x00, 0xaa}
	b := []byte{0x0f, 0xf0, 0x55}
	result := xorBytes(a, b)
	expected := []byte{0xf0, 0xf0, 0xff}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("xorBytes[%d] = 0x%02x, want 0x%02x", i, result[i], expected[i])
		}
	}
}

func TestSCRAMGenerateNonce(t *testing.T) {
	n1 := generateNonce()
	n2 := generateNonce()
	if n1 == n2 {
		t.Error("nonces should be unique")
	}
	if len(n1) == 0 {
		t.Error("nonce should not be empty")
	}
}

func TestSCRAMProviderUsers(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("alice", "pass1", 4096)
	p.RegisterUser("bob", "pass2", 4096)

	// Verify both users can be retrieved
	_, ok := p.GetUser("alice")
	if !ok {
		t.Error("expected alice to exist")
	}
	_, ok = p.GetUser("bob")
	if !ok {
		t.Error("expected bob to exist")
	}
	_, ok = p.GetUser("nonexistent")
	if ok {
		t.Error("expected nonexistent user to not exist")
	}
}

func TestSCRAMAuthenticateWithToken(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("alice", "pass", 4096)

	// SCRAM doesn't support token auth, should fail
	_, err := p.Authenticate(context.Background(), Credentials{Token: "some-token"})
	if err == nil {
		t.Error("expected error for token auth with SCRAM")
	}
}

func TestSCRAMAuthenticateMissingUsername(t *testing.T) {
	p := NewSCRAMProvider()
	p.RegisterUser("alice", "pass", 4096)

	// Only password without username
	_, err := p.Authenticate(context.Background(), Credentials{Password: "pass"})
	if err == nil {
		t.Error("expected error for missing username")
	}
}

func TestSCRAMRegisterUserDuplicate(t *testing.T) {
	p := NewSCRAMProvider()
	err := p.RegisterUser("alice", "pass", 4096)
	if err != nil {
		t.Fatalf("first RegisterUser: %v", err)
	}

	// Second registration overwrites silently — should succeed
	err = p.RegisterUser("alice", "different-pass", 4096)
	if err != nil {
		t.Errorf("expected no error for duplicate registration, got %v", err)
	}

	// Verify the new password works
	_, err = p.Authenticate(context.Background(), Credentials{Username: "alice", Password: "different-pass"})
	if err != nil {
		t.Errorf("expected auth with new password to succeed: %v", err)
	}
}

func TestSCRAMRemoveNonexistentUser(t *testing.T) {
	p := NewSCRAMProvider()
	p.RemoveUser("nonexistent")
	// Should not panic
}
