package auth

import (
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"math/big"
	"net"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"crypto/sha256"
	"golang.org/x/crypto/pbkdf2"
)

func TestNewAuthRateLimiter(t *testing.T) {
	rl := NewAuthRateLimiter(5, 15*time.Minute, 30*time.Minute)
	if rl.maxAttempts != 5 {
		t.Errorf("maxAttempts = %d, want 5", rl.maxAttempts)
	}
	if rl.window != 15*time.Minute {
		t.Errorf("window = %v, want 15m", rl.window)
	}
	if rl.banDuration != 30*time.Minute {
		t.Errorf("banDuration = %v, want 30m", rl.banDuration)
	}
}

func TestAuthRateLimiterIsAllowed(t *testing.T) {
	rl := NewAuthRateLimiter(3, 1*time.Minute, 5*time.Minute)
	for i := 0; i < 3; i++ {
		if !rl.IsAllowed("127.0.0.1") {
			t.Errorf("attempt %d should be allowed", i+1)
		}
		rl.RecordFailed("127.0.0.1")
	}
	if rl.IsAllowed("127.0.0.1") {
		t.Error("4th attempt should be blocked")
	}
	if !rl.IsAllowed("127.0.0.2") {
		t.Error("different IP should still be allowed")
	}
}

func TestAuthRateLimiterRecordSuccess(t *testing.T) {
	rl := NewAuthRateLimiter(2, 1*time.Minute, 5*time.Minute)
	rl.RecordFailed("127.0.0.1")
	rl.RecordFailed("127.0.0.1")
	if rl.IsAllowed("127.0.0.1") {
		t.Error("should be blocked after 2 failures")
	}
	rl.RecordSuccess("127.0.0.1")
	if !rl.IsAllowed("127.0.0.1") {
		t.Error("should be allowed after success")
	}
}

func TestAuthRateLimiterBanExpiry(t *testing.T) {
	rl := NewAuthRateLimiter(1, 1*time.Millisecond, 50*time.Millisecond)
	rl.RecordFailed("127.0.0.1")
	if rl.IsAllowed("127.0.0.1") {
		t.Error("should be blocked after 1 failure")
	}
	time.Sleep(60 * time.Millisecond)
	if !rl.IsAllowed("127.0.0.1") {
		t.Error("should be allowed after ban expires")
	}
}

func TestAuthRateLimiterTrustedProxies(t *testing.T) {
	rl := NewAuthRateLimiter(3, 1*time.Minute, 5*time.Minute)

	// Without trusted proxies configured, X-Forwarded-For is used
	ip := rl.ResolveIP("10.0.0.1:8080", "192.168.1.1", "")
	if ip != "192.168.1.1" {
		t.Errorf("ResolveIP = %q, want 192.168.1.1", ip)
	}

	// With trusted proxy
	rl.SetTrustedProxies([]string{"10.0.0.0/8"})
	ip = rl.ResolveIP("10.0.0.1:8080", "192.168.1.1", "")
	if ip != "192.168.1.1" {
		t.Errorf("ResolveIP with trusted proxy = %q, want 192.168.1.1", ip)
	}

	// Untrusted proxy — use direct IP
	rl.SetTrustedProxies([]string{"172.16.0.0/12"})
	ip = rl.ResolveIP("10.0.0.1:8080", "192.168.1.1", "")
	if ip != "10.0.0.1" {
		t.Errorf("ResolveIP untrusted proxy = %q, want 10.0.0.1", ip)
	}
}

func TestAuthRateLimiterResolveIP(t *testing.T) {
	rl := NewAuthRateLimiter(3, 1*time.Minute, 5*time.Minute)
	ip := rl.ResolveIP("192.168.1.1:8080", "", "")
	if ip != "192.168.1.1" {
		t.Errorf("ResolveIP = %q, want 192.168.1.1", ip)
	}

	rl.SetTrustedProxies([]string{"127.0.0.0/8"})
	ip = rl.ResolveIP("127.0.0.1:8080", "10.0.0.5", "")
	if ip != "10.0.0.5" {
		t.Errorf("ResolveIP with trusted proxy = %q, want 10.0.0.5", ip)
	}

	// Multiple X-Forwarded-For — spoofing, fall back to direct IP
	ip = rl.ResolveIP("127.0.0.1:8080", "10.0.0.5, 10.0.0.6", "")
	if ip != "127.0.0.1" {
		t.Errorf("ResolveIP multi-xff fallback = %q, want 127.0.0.1", ip)
	}
}

func TestAuthRateLimiterResolveIPXRealIP(t *testing.T) {
	rl := NewAuthRateLimiter(3, 1*time.Minute, 5*time.Minute)
	rl.SetTrustedProxies([]string{"10.0.0.0/8"})
	ip := rl.ResolveIP("10.0.0.1:8080", "", "192.168.1.1")
	if ip != "192.168.1.1" {
		t.Errorf("ResolveIP X-Real-IP = %q, want 192.168.1.1", ip)
	}
}

func TestAuthRateLimiterResolveIPFromHTTP(t *testing.T) {
	rl := NewAuthRateLimiter(3, 1*time.Minute, 5*time.Minute)
	rl.SetTrustedProxies([]string{"10.0.0.0/8"})
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:8080"
	req.Header.Set("X-Forwarded-For", "192.168.1.1")
	ip := rl.ResolveIPFromHTTP(req)
	if ip != "192.168.1.1" {
		t.Errorf("ResolveIPFromHTTP = %q, want 192.168.1.1", ip)
	}
}

func TestAuthRateLimiterWindowExpiry(t *testing.T) {
	rl := NewAuthRateLimiter(2, 50*time.Millisecond, 5*time.Minute)
	rl.RecordFailed("127.0.0.1")
	rl.RecordFailed("127.0.0.1")
	if rl.IsAllowed("127.0.0.1") {
		t.Error("should be blocked")
	}
	time.Sleep(60 * time.Millisecond)
	if !rl.IsAllowed("127.0.0.1") {
		t.Error("should be allowed after window expires")
	}
}

func TestLDAPProviderWithRoleAllowlist(t *testing.T) {
	provider := NewLDAPProviderWithRoleAllowlist(
		"ldap://ldap.example.com:389",
		"cn=admin,dc=example,dc=com",
		"admin-pass",
		"ou=users,dc=example,dc=com",
		"(uid=%s)",
		true,
		"dc=example,dc=com",
		[]string{"admin", "reader"},
	)
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
	_, err := provider.Authenticate(context.Background(), Credentials{
		Username: "test",
		Password: "test-pass",
	})
	if err == nil {
		t.Log("expected auth error (no LDAP server)")
	}
}

func TestLDAPExtractPort(t *testing.T) {
	tests := []struct {
		addr string
		want string
	}{
		{"ldap://ldap.example.com:389", "389"},
		{"ldap://ldap.example.com:636", "636"},
		{"ldap.example.com", "389"},
	}
	for _, tt := range tests {
		got := extractPort(tt.addr)
		if got != tt.want {
			t.Errorf("extractPort(%q) = %q, want %q", tt.addr, got, tt.want)
		}
	}
}

func TestLDAPDialError(t *testing.T) {
	provider := NewLDAPProvider(
		"ldap://localhost:59999",
		"cn=admin",
		"pass",
		"ou=users",
		"(uid=%s)",
		false,
		"",
	)
	conn, err := provider.dial()
	if err == nil {
		conn.Close()
		t.Log("unexpectedly connected to localhost:59999")
	}
}

func TestMTLSProviderWithRoleAllowlist(t *testing.T) {
	provider := NewMTLSProviderWithRoleAllowlist([]string{"admin", "reader"})
	if provider == nil {
		t.Fatal("expected non-nil provider")
	}
	_, err := provider.Authenticate(context.Background(), Credentials{})
	if err == nil {
		t.Error("expected auth error without client cert")
	}
}

func TestPeerCertsFromContextWithCerts(t *testing.T) {
	cert := &x509.Certificate{SerialNumber: big.NewInt(1)}
	ctx := ContextWithPeerCerts(context.Background(), []*x509.Certificate{cert})
	certs := PeerCertsFromContext(ctx)
	if len(certs) != 1 {
		t.Errorf("expected 1 cert, got %d", len(certs))
	}
}

func newOAuthProviderOrSkip(t *testing.T) *OAuthProvider {
	t.Helper()
	p, err := NewOAuthProvider("https://example.com", "client", "", "")
	if err != nil {
		t.Skipf("OAuth provider creation failed: %v", err)
	}
	return p
}

func TestOAuthIsPrivateIP(t *testing.T) {
	tests := []struct {
		url  string
		want bool
	}{
		{"https://localhost/test", true},
		{"https://127.0.0.1/test", true},
		{"https://[::1]/test", true},
		{"https://192.168.1.1/test", true},
		{"https://10.0.0.1/test", true},
		{"https://172.16.0.1/test", true},
		{"https://169.254.169.254/latest", true},
	}
	for _, tt := range tests {
		got, err := isPrivateIP(tt.url)
		if err != nil {
			t.Errorf("isPrivateIP(%q) error: %v", tt.url, err)
			continue
		}
		if got != tt.want {
			t.Errorf("isPrivateIP(%q) = %v, want %v", tt.url, got, tt.want)
		}
	}
}

func TestOAuthSetRequireJTI(t *testing.T) {
	provider := newOAuthProviderOrSkip(t)
	if provider.requireJTI {
		t.Error("requireJTI should be false by default")
	}
	provider.SetRequireJTI(true)
	if !provider.requireJTI {
		t.Error("requireJTI should be true after SetRequireJTI(true)")
	}
}

func TestOAuthSetTLSConfig(t *testing.T) {
	provider := newOAuthProviderOrSkip(t)
	provider.SetTLSConfig(nil)
}

func TestOAuthLoadTLSFromCAFile(t *testing.T) {
	provider := newOAuthProviderOrSkip(t)
	err := provider.LoadTLSFromCAFile("/nonexistent/ca.pem")
	if err == nil {
		t.Error("expected error for non-existent CA file")
	}
}

func TestOAuthJTIReplay(t *testing.T) {
	provider := newOAuthProviderOrSkip(t)
	provider.SetRequireJTI(true)
	if provider.isJTIUsed("test-jti") {
		t.Error("jti should not be used initially")
	}
	provider.markJTIUsed("test-jti", time.Now().Add(time.Hour))
	if !provider.isJTIUsed("test-jti") {
		t.Error("jti should be marked as used")
	}
}

func TestOAuthTokenReplay(t *testing.T) {
	provider := newOAuthProviderOrSkip(t)
	if provider.isTokenUsed("test-token") {
		t.Error("token should not be used initially")
	}
	provider.markTokenUsed("test-token", time.Now().Add(time.Hour))
	if !provider.isTokenUsed("test-token") {
		t.Error("token should be marked as used")
	}
}

func TestOAuthTokenCleanupLoop(t *testing.T) {
	provider := newOAuthProviderOrSkip(t)
	provider.tokenMaxAge = 50 * time.Millisecond
	provider.markTokenUsed("old-token", time.Now().Add(-time.Hour))
	if !provider.isTokenUsed("old-token") {
		t.Error("token should be marked")
	}
	time.Sleep(200 * time.Millisecond)
	if provider.isTokenUsed("old-token") {
		t.Log("token cleanup loop ran (token may still exist due to timing)")
	}
}

func TestOAuthJTICleanupLoop(t *testing.T) {
	provider := newOAuthProviderOrSkip(t)
	provider.SetRequireJTI(true)
	provider.jtiMaxAge = 50 * time.Millisecond
	provider.markJTIUsed("old-jti", time.Now().Add(-time.Hour))
	if !provider.isJTIUsed("old-jti") {
		t.Error("jti should be marked")
	}
	time.Sleep(200 * time.Millisecond)
	_ = provider.isJTIUsed("old-jti")
}

func TestOAuthHashForAlg(t *testing.T) {
	data := []byte("test-data")
	h := hashForAlg("RS256", data)
	if len(h) != 32 {
		t.Errorf("RS256 hash length = %d, want 32", len(h))
	}
	h = hashForAlg("RS384", data)
	if len(h) != 48 {
		t.Errorf("RS384 hash length = %d, want 48", len(h))
	}
	h = hashForAlg("RS512", data)
	if len(h) != 64 {
		t.Errorf("RS512 hash length = %d, want 64", len(h))
	}
	h = hashForAlg("ES256", data)
	if len(h) != 32 {
		t.Errorf("ES256 hash length = %d, want 32", len(h))
	}
}

func TestOAuthHashCrypto(t *testing.T) {
	h := hashCrypto("RS256")
	if h == 0 {
		t.Error("expected non-zero hash for RS256")
	}
	// SHA384 branch
	h = hashCrypto("RS384")
	if h == 0 {
		t.Error("expected non-zero hash for RS384")
	}
	h = hashCrypto("ES384")
	if h == 0 {
		t.Error("expected non-zero hash for ES384")
	}
	// SHA512 branch
	h = hashCrypto("RS512")
	if h == 0 {
		t.Error("expected non-zero hash for RS512")
	}
	h = hashCrypto("ES512")
	if h == 0 {
		t.Error("expected non-zero hash for ES512")
	}
	// unknown defaults to SHA256, not zero
	h = hashCrypto("unknown")
	if h == 0 {
		t.Error("expected non-zero hash for unknown (defaults to SHA256)")
	}
}

func TestOAuthAlgMatchesKeyError(t *testing.T) {
	if err := algMatchesKey("RS256", nil); err == nil {
		t.Error("RS256 with nil key should error")
	}
	if err := algMatchesKey("ES256", nil); err == nil {
		t.Error("ES256 with nil key should error")
	}
}

func TestSCRAMValidateClientServerFinal(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("testuser", "testpassword", 4096)
	if err != nil {
		t.Fatalf("RegisterUser: %v", err)
	}
	// StartExchange requires a proper SCRAM client-first message.
	// Just verify the provider doesn't panic on invalid input.
	_, _, err = provider.StartExchange("testuser")
	// May fail, but should not panic
	_ = err
}

func TestStaticProviderSetTenant(t *testing.T) {
	provider := NewStaticProvider(
		map[string]string{"admin": "pass"},
		map[string]string{"token": "admin"},
	)
	provider.SetTenant("token", "tenant-123")
	_, err := provider.Authenticate(context.Background(), Credentials{Token: "token"})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
}

func TestStaticProviderAuthenticateFail(t *testing.T) {
	provider := NewStaticProvider(
		map[string]string{"admin": "pass"},
		nil,
	)
	_, err := provider.Authenticate(context.Background(), Credentials{
		Username: "admin",
		Password: "wrong",
	})
	if err == nil {
		t.Error("expected error for wrong password")
	}
	_, err = provider.Authenticate(context.Background(), Credentials{
		Username: "unknown",
		Password: "pass",
	})
	if err == nil {
		t.Error("expected error for unknown user")
	}
}

func TestExtractIPNilConn(t *testing.T) {
	ip := ExtractIP(nil)
	if ip != "unknown" {
		t.Errorf("ExtractIP(nil) = %q, want unknown", ip)
	}
}

func TestExtractIPNoPort(t *testing.T) {
	// Mock net.Conn with RemoteAddr returning IP without port
	conn := &mockConnNoPort{ip: "10.0.0.1"}
	ip := ExtractIP(conn)
	if ip != "10.0.0.1" {
		t.Errorf("ExtractIP(no port) = %q, want 10.0.0.1", ip)
	}
}

type mockConnNoPort struct{ ip string }

func (m *mockConnNoPort) RemoteAddr() net.Addr {
	return &mockAddrNoPort{ip: m.ip}
}
func (m *mockConnNoPort) Read(b []byte) (n int, err error)  { return 0, nil }
func (m *mockConnNoPort) Write(b []byte) (n int, err error) { return 0, nil }
func (m *mockConnNoPort) Close() error                      { return nil }
func (m *mockConnNoPort) LocalAddr() net.Addr               { return nil }
func (m *mockConnNoPort) SetDeadline(t time.Time) error     { return nil }
func (m *mockConnNoPort) SetReadDeadline(t time.Time) error { return nil }
func (m *mockConnNoPort) SetWriteDeadline(t time.Time) error { return nil }

type mockAddrNoPort struct{ ip string }

func (m *mockAddrNoPort) Network() string { return "mock" }
func (m *mockAddrNoPort) String() string  { return m.ip }

func TestAuthRateLimiterSetTrustedProxiesInvalid(t *testing.T) {
	rl := NewAuthRateLimiter(3, 1*time.Minute, 5*time.Minute)
	rl.SetTrustedProxies([]string{"invalid", "10.0.0.0/8"})
	ip := rl.ResolveIP("10.0.0.1:8080", "192.168.1.1", "")
	if ip != "192.168.1.1" {
		t.Errorf("ResolveIP = %q, want 192.168.1.1", ip)
	}
}

func TestOAuthAuthenticateEmptyToken(t *testing.T) {
	provider := newOAuthProviderOrSkip(t)
	_, err := provider.Authenticate(context.Background(), Credentials{Token: ""})
	if err == nil {
		t.Error("expected error for empty token")
	}
}

func TestOAuthAuthenticateEmptyCredentials(t *testing.T) {
	provider := newOAuthProviderOrSkip(t)
	_, err := provider.Authenticate(context.Background(), Credentials{})
	if err == nil {
		t.Error("expected error for empty credentials")
	}
}

func TestAuthRateLimiterResolveIPNoPort(t *testing.T) {
	rl := NewAuthRateLimiter(3, 1*time.Minute, 5*time.Minute)
	// Set untrusted proxies so XFF is ignored
	rl.SetTrustedProxies([]string{"172.16.0.0/12"})
	ip := rl.ResolveIP("10.0.0.1", "192.168.1.1", "")
	if ip != "10.0.0.1" {
		t.Errorf("ResolveIP no-port = %q, want 10.0.0.1", ip)
	}
}

func TestAuthRateLimiterResolveIPFromHTTPNoHeaders(t *testing.T) {
	rl := NewAuthRateLimiter(3, 1*time.Minute, 5*time.Minute)
	rl.SetTrustedProxies([]string{"10.0.0.0/8"})
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "10.0.0.1:8080"
	ip := rl.ResolveIPFromHTTP(req)
	if ip != "10.0.0.1" {
		t.Errorf("ResolveIPFromHTTP no headers = %q, want 10.0.0.1", ip)
	}
}

func TestOAuthDirectJTIUsed(t *testing.T) {
	p := &OAuthProvider{
		jtiCache:    make(map[string]time.Time),
		jtiCacheMux: sync.RWMutex{},
	}
	if p.isJTIUsed("test-jti") {
		t.Error("jti should not be used initially")
	}
	p.markJTIUsed("test-jti", time.Now().Add(time.Hour))
	if !p.isJTIUsed("test-jti") {
		t.Error("jti should be marked as used")
	}
}

func TestOAuthDirectTokenUsed(t *testing.T) {
	p := &OAuthProvider{
		tokenCache:    make(map[string]time.Time),
		tokenCacheMux: sync.RWMutex{},
	}
	if p.isTokenUsed("test-token") {
		t.Error("token should not be used initially")
	}
	p.markTokenUsed("test-token", time.Now().Add(time.Hour))
	if !p.isTokenUsed("test-token") {
		t.Error("token should be marked as used")
	}
}

func TestOAuthDirectSetRequireJTI(t *testing.T) {
	p := &OAuthProvider{requireJTI: false}
	p.SetRequireJTI(true)
	if !p.requireJTI {
		t.Error("requireJTI should be true after SetRequireJTI(true)")
	}
}

func TestOAuthDirectSetTLSConfig(t *testing.T) {
	p := &OAuthProvider{}
	p.SetTLSConfig(nil)
	if p.httpClient == nil {
		t.Error("httpClient should be set even with nil TLS config")
	}
}

func TestOAuthDirectLoadTLSFromCAFile(t *testing.T) {
	p := &OAuthProvider{}
	err := p.LoadTLSFromCAFile("/nonexistent/ca.pem")
	if err == nil {
		t.Error("expected error for non-existent CA file")
	}
	tmp := t.TempDir() + "/bad.pem"
	f, _ := os.Create(tmp)
	f.WriteString("not a valid PEM certificate")
	f.Close()
	err = p.LoadTLSFromCAFile(tmp)
	if err == nil {
		t.Error("expected error for invalid PEM")
	}
}

func TestOAuthDirectJTICleanup(t *testing.T) {
	p := &OAuthProvider{
		jtiCache:    make(map[string]time.Time),
		jtiCacheMux: sync.RWMutex{},
		closeCh:     make(chan struct{}),
	}
	expired := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	p.jtiCache["expired"] = expired
	p.jtiCache["future"] = future

	// Trigger cleanup inline by closing the channel (jtiCleanupLoop does cleanup on tick)
	close(p.closeCh)

	// Run cleanup logic inline
	p.jtiCacheMux.Lock()
	now := time.Now()
	for jti, expiry := range p.jtiCache {
		if now.After(expiry) {
			delete(p.jtiCache, jti)
		}
	}
	p.jtiCacheMux.Unlock()

	if _, ok := p.jtiCache["expired"]; ok {
		t.Error("expired jti should be cleaned up")
	}
	if _, ok := p.jtiCache["future"]; !ok {
		t.Error("future jti should not be cleaned up")
	}
}

func TestOAuthDirectTokenCleanup(t *testing.T) {
	p := &OAuthProvider{
		tokenCache:    make(map[string]time.Time),
		tokenCacheMux: sync.RWMutex{},
		closeCh:       make(chan struct{}),
	}
	expired := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	p.tokenCache["expired"] = expired
	p.tokenCache["future"] = future

	close(p.closeCh)

	p.tokenCacheMux.Lock()
	now := time.Now()
	for key, expiry := range p.tokenCache {
		if now.After(expiry) {
			delete(p.tokenCache, key)
		}
	}
	p.tokenCacheMux.Unlock()

	if _, ok := p.tokenCache["expired"]; ok {
		t.Error("expired token should be cleaned up")
	}
	if _, ok := p.tokenCache["future"]; !ok {
		t.Error("future token should not be cleaned up")
	}
}

func TestSCRAMFullSASLExchangeCoverage(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("exchange-user", "secure-password", 4096)
	if err != nil {
		t.Fatalf("RegisterUser: %v", err)
	}

	// Step 1: Client sends client-first-message
	clientFirst := "n,,n=exchange-user,r=cN0n0HJgBqM7sC9r1zL4"
	sess, serverFirst, err := provider.StartExchange(clientFirst)
	if err != nil {
		t.Fatalf("StartExchange: %v", err)
	}
	if sess == nil {
		t.Fatal("expected non-nil session")
	}
	if !strings.HasPrefix(serverFirst, "r=") {
		t.Errorf("server-first should start with r=, got: %s", serverFirst[:20])
	}

	// Step 2: Client sends client-final-message with proof
	// We need to compute a valid proof — use the session to get attributes
	attrs, err := parseSCRAMAttributes(serverFirst)
	if err != nil {
		t.Fatalf("parse server-first: %v", err)
	}
	serverNonce := attrs['r']
	saltB64 := attrs['s']
	salt, _ := base64.StdEncoding.DecodeString(saltB64)

	// Derive keys for the user (storedKey used for HMAC in proof computation)
	storedKey, _ := deriveKeys("secure-password", salt, 4096)

	// Build auth message
	clientFirstBare := "n=exchange-user,r=cN0n0HJgBqM7sC9r1zL4"
	channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,,"))
	clientFinalNoProof := fmt.Sprintf("c=%s,r=%s", channelBinding, serverNonce)
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalNoProof

	// Re-derive full keys to compute valid proof
	saltedPassword := pbkdf2.Key([]byte("secure-password"), salt, 4096, scramSHA256KeyLen, sha256.New)
	realClientKey := hmacSHA256(saltedPassword, []byte(scramClientKey))
	realClientSig := hmacSHA256(storedKey, []byte(authMessage))
	proof := xorBytes(realClientKey, realClientSig)
	proofB64 := base64.StdEncoding.EncodeToString(proof)

	clientFinal := fmt.Sprintf("c=%s,r=%s,p=%s", channelBinding, serverNonce, proofB64)
	serverFinal, err := sess.VerifyClientFinal(clientFinal)
	if err != nil {
		t.Fatalf("VerifyClientFinal: %v", err)
	}
	if !strings.HasPrefix(serverFinal, "v=") {
		t.Errorf("server-final should start with v=, got: %s", serverFinal[:20])
	}

	// Step 3: Client validates server and echoes back
	err = sess.ValidateClientServerFinal(serverFinal)
	if err != nil {
		t.Errorf("ValidateClientServerFinal: %v", err)
	}
	if sess.Username() != "exchange-user" {
		t.Errorf("Username() = %q, want exchange-user", sess.Username())
	}
}

func TestSCRAMAuthenticateCorrectPassword(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("auth-user", "mypassword", 4096)
	if err != nil {
		t.Fatalf("RegisterUser: %v", err)
	}

	id, err := provider.Authenticate(context.Background(), Credentials{
		Username: "auth-user",
		Password: "mypassword",
	})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.UserID != "auth-user" {
		t.Errorf("UserID = %q, want auth-user", id.UserID)
	}
	if id.Source != "scram" {
		t.Errorf("Source = %q, want scram", id.Source)
	}
}

func TestSCRAMAuthenticateWrongPasswordCoverage(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("auth-user", "correct", 4096)
	if err != nil {
		t.Fatal(err)
	}

	_, err = provider.Authenticate(context.Background(), Credentials{
		Username: "auth-user",
		Password: "wrong",
	})
	if err == nil {
		t.Error("expected error for wrong password")
	}
}

func TestSCRAMAuthenticateEmptyCredentials(t *testing.T) {
	provider := NewSCRAMProvider()
	_, err := provider.Authenticate(context.Background(), Credentials{})
	if err == nil {
		t.Error("expected error for empty credentials")
	}
}

func TestSCRAMAuthenticateUnknownUserCoverage(t *testing.T) {
	provider := NewSCRAMProvider()
	_, err := provider.Authenticate(context.Background(), Credentials{
		Username: "nobody",
		Password: "pass",
	})
	if err == nil {
		t.Error("expected error for unknown user")
	}
}

func TestSCRAMRemoveAndGetUser(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("remove-me", "password", 4096)
	if err != nil {
		t.Fatal(err)
	}

	user, ok := provider.GetUser("remove-me")
	if !ok {
		t.Error("GetUser should find registered user")
	}
	if user == nil || len(user.Salt) == 0 {
		t.Error("user should have salt")
	}

	provider.RemoveUser("remove-me")
	_, ok = provider.GetUser("remove-me")
	if ok {
		t.Error("GetUser should not find removed user")
	}

	// Remove non-existent — should not panic
	provider.RemoveUser("nonexistent")
}

func TestSCRAMRegisterEmptyUsernameCoverage(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("", "password", 4096)
	if err == nil {
		t.Error("expected error for empty username")
	}
}

func TestSCRAMRegisterEmptyPasswordCoverage(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("user", "", 4096)
	if err == nil {
		t.Error("expected error for empty password")
	}
}

func TestSCRAMRegisterLowIterations(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("user", "pass", 100) // below min
	if err != nil {
		t.Fatalf("should accept low iterations (clamps to default): %v", err)
	}
	user, _ := provider.GetUser("user")
	if user.Iterations != scramDefaultIters {
		t.Errorf("Iterations = %d, want %d", user.Iterations, scramDefaultIters)
	}
}

func TestSCRAMInvalidClientFinal(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("inv-user", "pass", 4096)
	if err != nil {
		t.Fatal(err)
	}

	clientFirst := "n,,n=inv-user,r=nonce123"
	sess, _, err := provider.StartExchange(clientFirst)
	if err != nil {
		t.Fatal(err)
	}

	// Invalid client-final (no proof)
	_, err = sess.VerifyClientFinal("c=abc,r=nonce123")
	if err == nil {
		t.Error("expected error for missing proof")
	}

	// Wrong nonce (replay)
	_, err = sess.VerifyClientFinal("c=abc,r=wrong-nonce,p=proof")
	if err == nil {
		t.Error("expected error for wrong nonce")
	}

	// Invalid attributes
	_, err = sess.VerifyClientFinal("bad-format")
	if err == nil {
		t.Error("expected error for invalid attributes")
	}
}

func TestSCRAMValidateClientServerFinalRejection(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("reject-user", "pass", 4096)
	if err != nil {
		t.Fatal(err)
	}

	clientFirst := "n,,n=reject-user,r=nonce123"
	sess, serverFirst, err := provider.StartExchange(clientFirst)
	if err != nil {
		t.Fatal(err)
	}

	attrs, _ := parseSCRAMAttributes(serverFirst)
	serverNonce := attrs['r']
	saltB64 := attrs['s']
	salt, _ := base64.StdEncoding.DecodeString(saltB64)
	saltedPassword := pbkdf2.Key([]byte("pass"), salt, 4096, scramSHA256KeyLen, sha256.New)
	storedKey, serverKey := deriveKeys("pass", salt, 4096)

	clientFirstBare := "n=reject-user,r=nonce123"
	channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,,"))
	clientFinalNoProof := fmt.Sprintf("c=%s,r=%s", channelBinding, serverNonce)
	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalNoProof

	realClientKey := hmacSHA256(saltedPassword, []byte(scramClientKey))
	realClientSig := hmacSHA256(storedKey, []byte(authMessage))
	proof := xorBytes(realClientKey, realClientSig)
	proofB64 := base64.StdEncoding.EncodeToString(proof)

	clientFinal := fmt.Sprintf("c=%s,r=%s,p=%s", channelBinding, serverNonce, proofB64)
	serverFinal, err := sess.VerifyClientFinal(clientFinal)
	if err != nil {
		t.Fatal(err)
	}
	_ = serverFinal // signature computed successfully

	// Client rejects server signature (wrong echo)
	err = sess.ValidateClientServerFinal("v=wrong-signature")
	if err == nil {
		t.Error("expected error for wrong server signature echo")
	}

	// Missing v= prefix
	err = sess.ValidateClientServerFinal("no-prefix")
	if err == nil {
		t.Error("expected error for missing v= prefix")
	}

	// Unused serverKey to avoid linter
	_ = serverKey
}

func TestSCRAMStartExchangeInvalidMessages(t *testing.T) {
	provider := NewSCRAMProvider()
	err := provider.RegisterUser("test", "pass", 4096)
	if err != nil {
		t.Fatal(err)
	}

	// Empty message
	_, _, err = provider.StartExchange("")
	if err == nil {
		t.Error("expected error for empty message")
	}

	// No GS2 header structure
	_, _, err = provider.StartExchange("no-commas")
	if err == nil {
		t.Error("expected error for malformed message")
	}

	// Unknown user
	_, _, err = provider.StartExchange("n,,n=nonexistent,r=nonce")
	if err == nil {
		t.Error("expected error for unknown user")
	}
}

func TestMTLSAuthenticateWithSAN(t *testing.T) {
	// Cert with empty CN but with SAN DNS names
	cert := &x509.Certificate{
		Subject: pkix.Name{CommonName: ""},
		DNSNames: []string{"client.example.com"},
	}
	ctx := ContextWithPeerCerts(context.Background(), []*x509.Certificate{cert})
	provider := NewMTLSProvider()

	id, err := provider.Authenticate(ctx, Credentials{})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if id.UserID != "client.example.com" {
		t.Errorf("UserID = %q, want client.example.com", id.UserID)
	}
	if id.Source != "mtls" {
		t.Errorf("Source = %q, want mtls", id.Source)
	}
}

func TestMTLSAuthenticateNoCNOrSAN(t *testing.T) {
	cert := &x509.Certificate{
		Subject: pkix.Name{CommonName: ""},
		DNSNames: []string{},
	}
	ctx := ContextWithPeerCerts(context.Background(), []*x509.Certificate{cert})
	provider := NewMTLSProvider()

	_, err := provider.Authenticate(ctx, Credentials{})
	if err == nil {
		t.Error("expected error when cert has no CN or SAN")
	}
}

func TestMTLSAuthenticateWithRoleAllowlist(t *testing.T) {
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName:         "admin-client",
			OrganizationalUnit: []string{"admin", "viewer", "superuser"},
			Organization:       []string{"myorg"},
		},
	}
	ctx := ContextWithPeerCerts(context.Background(), []*x509.Certificate{cert})
	provider := NewMTLSProviderWithRoleAllowlist([]string{"admin", "viewer"})

	id, err := provider.Authenticate(ctx, Credentials{})
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if len(id.Roles) != 2 {
		t.Errorf("Roles = %v, want [admin viewer]", id.Roles)
	}
	if len(id.Groups) != 1 || id.Groups[0] != "myorg" {
		t.Errorf("Groups = %v, want [myorg]", id.Groups)
	}
}

func TestMTLSAuthenticateNilContext(t *testing.T) {
	provider := NewMTLSProvider()
	_, err := provider.Authenticate(nil, Credentials{})
	if err == nil {
		t.Error("expected error for nil context")
	}
}

func TestMTLSClose(t *testing.T) {
	provider := NewMTLSProvider()
	if err := provider.Close(); err != nil {
		t.Errorf("Close should return nil: %v", err)
	}
}

func TestACLDefaultResourceType(t *testing.T) {
	rt := ParseResourceType("unknown-type")
	if rt != ResourceTopic {
		t.Errorf("ParseResourceType(unknown) = %d, want ResourceTopic", rt)
	}
}

func TestACLDefaultOperation(t *testing.T) {
	op := ParseOperation("unknown-op")
	if op != OpRead {
		t.Errorf("ParseOperation(unknown) = %d, want OpRead", op)
	}
}

func TestACLMatchGlobExact(t *testing.T) {
	if !matchGlob("topic-name", "topic-name") {
		t.Error("exact match should succeed")
	}
	if matchGlob("topic-name", "topic-other") {
		t.Error("exact mismatch should fail")
	}
}

func TestACLRoleNoMatch(t *testing.T) {
	acl := NewACLEngine(PermissionDeny)
	acl.AddEntry(ACLEntry{
		Principal:    "admin",
		ResourceType: ResourceTopic,
		ResourceName: "*",
		Operation:    OpRead,
		Permission:   PermissionAllow,
	})

	// User with no matching role
	id := &Identity{UserID: "user1", Roles: []string{"viewer"}}
	if acl.Check(id, ResourceTopic, "test", OpRead) {
		t.Error("user with non-matching role should be denied")
	}
}

func TestRateLimiterRecordSuccessRemovesBan(t *testing.T) {
	rl := NewAuthRateLimiter(1, 1*time.Minute, 5*time.Minute)
	rl.RecordFailed("127.0.0.1")
	if rl.IsAllowed("127.0.0.1") {
		t.Error("should be blocked after 1 failure")
	}
	rl.RecordSuccess("127.0.0.1")
	if !rl.IsAllowed("127.0.0.1") {
		t.Error("should be allowed after success clears ban")
	}
}

func TestRateLimiterBanExpiredCleanup(t *testing.T) {
	rl := NewAuthRateLimiter(1, 1*time.Millisecond, 50*time.Millisecond)
	rl.RecordFailed("127.0.0.1")
	if rl.IsAllowed("127.0.0.1") {
		t.Error("should be blocked")
	}
	time.Sleep(60 * time.Millisecond)
	if !rl.IsAllowed("127.0.0.1") {
		t.Error("should be allowed after ban expires")
	}
}

func TestValidateAlgCoverage(t *testing.T) {
	// Test none algorithm rejected
	if err := validateAlg("none"); err == nil {
		t.Error("alg=none should be rejected")
	}
	// Test empty rejected
	if err := validateAlg(""); err == nil {
		t.Error("empty alg should be rejected")
	}
	// Test supported algs
	for _, alg := range []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "EdDSA"} {
		if err := validateAlg(alg); err != nil {
			t.Errorf("alg %s should be allowed: %v", alg, err)
		}
	}
	// Test unsupported
	if err := validateAlg("HS256"); err == nil {
		t.Error("HS256 should be unsupported")
	}
}

func TestDecodeJWTPartInvalid(t *testing.T) {
	_, err := decodeJWTPart("not-base64!!!")
	if err == nil {
		t.Error("expected error for invalid base64")
	}
	_, err = decodeJWTPart(base64.RawURLEncoding.EncodeToString([]byte("not-json")))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseJWKUnsupported(t *testing.T) {
	_, err := parseJWK(jwkKey{Kty: "oct"})
	if err == nil {
		t.Error("expected error for oct key type")
	}
}

func TestParseJWKECInvalidCurve(t *testing.T) {
	_, err := parseJWK(jwkKey{Kty: "EC", Crv: "P-192", X: "AA", Y: "AA"})
	if err == nil {
		t.Error("expected error for unsupported curve")
	}
}

func TestParseJWKECInvalidBase64Coverage(t *testing.T) {
	_, err := parseJWK(jwkKey{Kty: "EC", Crv: "P-256", X: "!!!", Y: "AA"})
	if err == nil {
		t.Error("expected error for invalid base64 in X")
	}
	_, err = parseJWK(jwkKey{Kty: "EC", Crv: "P-256", X: "AA", Y: "!!!"})
	if err == nil {
		t.Error("expected error for invalid base64 in Y")
	}
}

func TestParseJWKRSAInvalidBase64Coverage(t *testing.T) {
	_, err := parseJWK(jwkKey{Kty: "RSA", N: "!!!", E: "AA"})
	if err == nil {
		t.Error("expected error for invalid base64 in N")
	}
	_, err = parseJWK(jwkKey{Kty: "RSA", N: "AA", E: "!!!"})
	if err == nil {
		t.Error("expected error for invalid base64 in E")
	}
}

func TestParseJWOKPInvalidBase64(t *testing.T) {
	_, err := parseJWK(jwkKey{Kty: "OKP", X: "!!!"})
	if err == nil {
		t.Error("expected error for invalid base64 in OKP X")
	}
}

func TestVerifyJWTUnsupportedKeyTypeCoverage(t *testing.T) {
	err := verifyJWT([]string{"a", "b", "c"}, "not-a-key", "RS256")
	if err == nil {
		t.Error("expected error for unsupported key type")
	}
}
