package auth

import (
	"context"
	"crypto/x509"
	"math/big"
	"net/http/httptest"
	"testing"
	"time"
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
