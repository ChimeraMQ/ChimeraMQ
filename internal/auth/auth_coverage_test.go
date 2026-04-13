package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// --- mtls.go: genuinely nil context (extra_test uses context.TODO) ---

func TestPeerCertsFromContextTrulyNil(t *testing.T) {
	certs := PeerCertsFromContext(nil)
	if certs != nil {
		t.Error("expected nil for truly nil context")
	}
}

// --- static.go: plaintext hash rejection ---

func TestStaticProviderPlaintextHashRejected(t *testing.T) {
	p := NewStaticProvider(
		map[string]string{"admin": "plaintextpassword"},
		nil,
	)
	defer p.Close()

	_, err := p.Authenticate(context.Background(), Credentials{
		Username: "admin",
		Password: "plaintextpassword",
	})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

// --- oauth.go: refreshKeys error paths not covered by extra_test ---

func TestOAuthRefreshKeysDiscoveryNon200(t *testing.T) {
	disc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer disc.Close()

	p := &OAuthProvider{
		issuer:     disc.URL,
		keySet:     make(map[string]interface{}),
		httpClient: &http.Client{Timeout: 5 * time.Second},
		closeCh:    make(chan struct{}),
	}
	defer p.Close()

	err := p.refreshKeys()
	if err == nil {
		t.Error("expected error for non-200 discovery")
	}
}

func TestOAuthRefreshKeysInvalidDiscoveryJSON(t *testing.T) {
	disc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	}))
	defer disc.Close()

	p := &OAuthProvider{
		issuer:     disc.URL,
		keySet:     make(map[string]interface{}),
		httpClient: &http.Client{Timeout: 5 * time.Second},
		closeCh:    make(chan struct{}),
	}
	defer p.Close()

	err := p.refreshKeys()
	if err == nil {
		t.Error("expected error for invalid discovery JSON")
	}
}

func TestOAuthRefreshKeysJWKSNon200(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"jwks_uri": "http://127.0.0.1:1/jwks"})
	})
	disc := httptest.NewServer(mux)
	defer disc.Close()

	p := &OAuthProvider{
		issuer:     disc.URL,
		keySet:     make(map[string]interface{}),
		httpClient: &http.Client{Timeout: 5 * time.Second},
		closeCh:    make(chan struct{}),
	}
	defer p.Close()

	err := p.refreshKeys()
	if err == nil {
		t.Error("expected error for non-200 JWKS")
	}
}

func TestOAuthRefreshKeysInvalidJWKSJSON(t *testing.T) {
	jwksURL := ""
	disc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/jwks" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("not json"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(oidcDiscovery{JWKSURI: jwksURL})
	}))
	defer disc.Close()
	jwksURL = disc.URL + "/jwks"

	p := &OAuthProvider{
		issuer:     disc.URL,
		keySet:     make(map[string]interface{}),
		httpClient: &http.Client{Timeout: 5 * time.Second},
		closeCh:    make(chan struct{}),
	}
	defer p.Close()

	err := p.refreshKeys()
	if err == nil {
		t.Error("expected error for invalid JWKS JSON")
	}
}

// --- oauth.go: algMatchesKey unsupported key type ---

func TestAlgMatchesKeyUnsupported(t *testing.T) {
	err := algMatchesKey("RS256", "not-a-key")
	if err == nil {
		t.Error("expected error for unsupported key type")
	}
}

// --- oauth.go: ecdsaVerify invalid length ---

func TestECDSAVerifyInvalidLength(t *testing.T) {
	privKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	err := ecdsaVerify(&privKey.PublicKey, "data", []byte("short"))
	if err == nil {
		t.Error("expected error for invalid ECDSA signature length")
	}
}

// --- oauth.go: parseJWK unsupported curve ---

func TestParseJWKUnsupportedCurve(t *testing.T) {
	key := jwkKey{Kty: "EC", Crv: "P-128", X: "AQ", Y: "Ag"}
	_, err := parseJWK(key)
	if err == nil {
		t.Error("expected error for unsupported curve")
	}
}

// --- scram.go: RegisterUser edge cases ---

func TestSCRAMRegisterUserEmptyUsername(t *testing.T) {
	p := NewSCRAMProvider()
	err := p.RegisterUser("", "password", 4096)
	if err == nil {
		t.Error("expected error for empty username")
	}
}

func TestSCRAMRegisterUserEmptyPassword(t *testing.T) {
	p := NewSCRAMProvider()
	err := p.RegisterUser("user", "", 4096)
	if err == nil {
		t.Error("expected error for empty password")
	}
}

func TestSCRAMRegisterUserLowIterations(t *testing.T) {
	p := NewSCRAMProvider()
	err := p.RegisterUser("user", "password", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	u, ok := p.GetUser("user")
	if !ok {
		t.Fatal("user not found")
	}
	if u.Iterations != scramDefaultIters {
		t.Errorf("iterations = %d, want %d", u.Iterations, scramDefaultIters)
	}
}

func TestParseSCRAMAttributesShortField(t *testing.T) {
	attrs, err := parseSCRAMAttributes("n=user,x,p=proof")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := attrs['x']; ok {
		t.Error("short field 'x' should be skipped")
	}
	if attrs['p'] != "proof" {
		t.Errorf("p = %q, want proof", attrs['p'])
	}
}

// --- FileProvider missing paths ---

func TestFileProviderTokenInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	os.WriteFile(path, []byte(`{"users":{},"tokens":{"tok":"user"}}`), 0600)
	p, err := NewFileProvider(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	_, err = p.Authenticate(context.Background(), Credentials{Token: "wrong"})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestFileProviderUserNotFound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	os.WriteFile(path, []byte(`{"users":{"admin":{"password":"$2a$04$30HxUYsoBvQUHavktOfcG.sVFvdBFj3f4WmdeirSqsOcm5kb4mmVa"}},"tokens":{}}`), 0600)
	p, err := NewFileProvider(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	_, err = p.Authenticate(context.Background(), Credentials{Username: "nobody", Password: "secret"})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestFileProviderUserWrongPassword(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	os.WriteFile(path, []byte(`{"users":{"admin":{"password":"$2a$04$30HxUYsoBvQUHavktOfcG.sVFvdBFj3f4WmdeirSqsOcm5kb4mmVa"}},"tokens":{}}`), 0600)
	p, err := NewFileProvider(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	_, err = p.Authenticate(context.Background(), Credentials{Username: "admin", Password: "wrong"})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestFileProviderUserPlaintextPassword(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	os.WriteFile(path, []byte(`{"users":{"admin":{"password":"plaintext"}},"tokens":{}}`), 0600)
	p, err := NewFileProvider(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	_, err = p.Authenticate(context.Background(), Credentials{Username: "admin", Password: "plaintext"})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestFileProviderNoCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	os.WriteFile(path, []byte(`{"users":{},"tokens":{}}`), 0600)
	p, err := NewFileProvider(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	_, err = p.Authenticate(context.Background(), Credentials{})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

// --- OAuthProvider missing paths ---

func TestOAuthArrayAudienceSuccess(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-aud-arr"

	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader(kid)
	payloadMap := map[string]interface{}{
		"iss": "https://test.example.com",
		"sub": "user-arr",
		"exp": 9999999999,
		"aud": []interface{}{"other-aud", "test-audience"},
	}
	pb, _ := json.Marshal(payloadMap)
	payload := base64.RawURLEncoding.EncodeToString(pb)
	token := signJWT(header, payload, privKey)

	id, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if id.UserID != "user-arr" {
		t.Errorf("UserID = %q, want user-arr", id.UserID)
	}
}

func TestOAuthInvalidBase64Header(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-b64"

	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	token := "!!!invalid!!!." + makeJWTPayload("https://test.example.com", "user1", "test-audience", 9999999999, nil, nil) + ".sig"

	_, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err == nil {
		t.Error("expected error for invalid base64 header")
	}
}

func TestOAuthInvalidBase64Signature(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-sig"

	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader(kid)
	payload := makeJWTPayload("https://test.example.com", "user1", "test-audience", 9999999999, nil, nil)
	token := header + "." + payload + ".!!!notbase64!!!"

	_, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err == nil {
		t.Error("expected error for invalid base64 signature")
	}
}

func TestOAuthRefreshLoopClose(t *testing.T) {
	p := &OAuthProvider{
		issuer:     "https://test.example.com",
		keySet:     make(map[string]interface{}),
		httpClient: &http.Client{Timeout: 5 * time.Second},
		closeCh:    make(chan struct{}),
	}
	go p.refreshLoop()
	p.Close()
	// If we get here without deadlock, the close path works
}

func TestParseJWKInvalidBase64RSA(t *testing.T) {
	_, err := parseJWK(jwkKey{Kty: "RSA", N: "!!!", E: "AQAB"})
	if err == nil {
		t.Error("expected error for invalid base64 N")
	}
	_, err = parseJWK(jwkKey{Kty: "RSA", N: "AQAB", E: "!!!"})
	if err == nil {
		t.Error("expected error for invalid base64 E")
	}
}

func TestParseJWKInvalidBase64EC(t *testing.T) {
	_, err := parseJWK(jwkKey{Kty: "EC", Crv: "P-256", X: "!!!", Y: "Ag"})
	if err == nil {
		t.Error("expected error for invalid base64 X")
	}
	_, err = parseJWK(jwkKey{Kty: "EC", Crv: "P-256", X: "Ag", Y: "!!!"})
	if err == nil {
		t.Error("expected error for invalid base64 Y")
	}
}

func TestParseJWKEd25519(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	x := base64.RawURLEncoding.EncodeToString(pub)
	parsed, err := parseJWK(jwkKey{Kty: "OKP", X: x})
	if err != nil {
		t.Fatalf("parseJWK OKP: %v", err)
	}
	if _, ok := parsed.(ed25519.PublicKey); !ok {
		t.Errorf("expected ed25519.PublicKey, got %T", parsed)
	}
}
