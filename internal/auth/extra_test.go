package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"
)

func bcryptHash(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	return string(bytes), err
}

// ---------------------------------------------------------------------------
// ACL: ParseResourceType and ParseOperation exhaustive coverage
// ---------------------------------------------------------------------------

func TestParseResourceTypeAllVariants(t *testing.T) {
	tests := map[string]ResourceType{
		"topic":         ResourceTopic,
		"consumer_group": ResourceConsumerGroup,
		"consumergroup":  ResourceConsumerGroup,
		"group":         ResourceConsumerGroup,
		"cluster":       ResourceCluster,
		"schema":        ResourceSchema,
		"wasm":          ResourceWASM,
	}
	for input, expected := range tests {
		got := ParseResourceType(input)
		if got != expected {
			t.Errorf("ParseResourceType(%q) = %d, want %d", input, got, expected)
		}
	}
	// Unknown value defaults to ResourceTopic
	if got := ParseResourceType("unknown_thing"); got != ResourceTopic {
		t.Errorf("ParseResourceType(unknown) = %d, want ResourceTopic", got)
	}
	// Case-insensitive
	if got := ParseResourceType("TOPIC"); got != ResourceTopic {
		t.Errorf("ParseResourceType(TOPIC) = %d, want ResourceTopic", got)
	}
	if got := ParseResourceType("CLUSTER"); got != ResourceCluster {
		t.Errorf("ParseResourceType(CLUSTER) = %d, want ResourceCluster", got)
	}
}

func TestParseOperationAllVariants(t *testing.T) {
	tests := map[string]Operation{
		"read":     OpRead,
		"write":    OpWrite,
		"create":   OpCreate,
		"delete":   OpDelete,
		"alter":    OpAlter,
		"describe": OpDescribe,
		"all":      OpAll,
	}
	for input, expected := range tests {
		got := ParseOperation(input)
		if got != expected {
			t.Errorf("ParseOperation(%q) = %d, want %d", input, got, expected)
		}
	}
	// Unknown value defaults to OpRead
	if got := ParseOperation("unknown_op"); got != OpRead {
		t.Errorf("ParseOperation(unknown) = %d, want OpRead", got)
	}
	// Case-insensitive
	if got := ParseOperation("WRITE"); got != OpWrite {
		t.Errorf("ParseOperation(WRITE) = %d, want OpWrite", got)
	}
	if got := ParseOperation("DELETE"); got != OpDelete {
		t.Errorf("ParseOperation(DELETE) = %d, want OpDelete", got)
	}
}

// ---------------------------------------------------------------------------
// ACL: matchGlob patterns
// ---------------------------------------------------------------------------

func TestMatchGlobExact(t *testing.T) {
	if !matchGlob("test", "test") {
		t.Error("exact match should succeed")
	}
	if matchGlob("test", "other") {
		t.Error("non-matching strings should not match")
	}
}

func TestMatchGlobWildcardStar(t *testing.T) {
	if !matchGlob("*", "anything") {
		t.Error("* should match everything")
	}
}

func TestMatchGlobPrefixWildcard(t *testing.T) {
	if !matchGlob("public.*", "public.events") {
		t.Error("public.* should match public.events")
	}
	if !matchGlob("public.*", "public.") {
		t.Error("public.* should match public.")
	}
	if matchGlob("public.*", "private.events") {
		t.Error("public.* should not match private.events")
	}
}

func TestMatchGlobFilepathMatch(t *testing.T) {
	// filepath.Match supports ? single-char wildcard
	if !matchGlob("topic?", "topic1") {
		t.Error("topic? should match topic1")
	}
	if matchGlob("topic?", "topic12") {
		t.Error("topic? should not match topic12")
	}
}

// ---------------------------------------------------------------------------
// Static provider: file loading edge cases
// ---------------------------------------------------------------------------

func TestFileProviderInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/auth.json"
	os.WriteFile(path, []byte(`{invalid json`), 0600)

	_, err := NewFileProvider(path)
	if err == nil {
		t.Error("should fail for invalid JSON")
	}
}

func TestFileProviderEmptyUsers(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/auth.json"
	// Write JSON with no users or tokens at all
	os.WriteFile(path, []byte(`{}`), 0600)

	p, err := NewFileProvider(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Any auth should fail
	_, err = p.Authenticate(context.Background(), Credentials{Username: "anyone", Password: "pass"})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestFileProviderBcryptUser(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/auth.json"
	// Generate a bcrypt hash at minimum cost for speed
	hash, err := bcryptHash("secret")
	if err != nil {
		t.Fatal(err)
	}
	data := map[string]interface{}{
		"users": map[string]interface{}{
			"admin": map[string]interface{}{
				"password": hash,
				"roles":    []string{"admin"},
			},
		},
		"tokens": map[string]string{},
	}
	jsonData, _ := json.Marshal(data)
	os.WriteFile(path, jsonData, 0600)

	p, err := NewFileProvider(path)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// Correct password
	id, err := p.Authenticate(context.Background(), Credentials{Username: "admin", Password: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "admin" {
		t.Errorf("UserID = %q, want admin", id.UserID)
	}

	// Wrong password
	_, err = p.Authenticate(context.Background(), Credentials{Username: "admin", Password: "wrong"})
	if err != ErrInvalidCredentials {
		t.Errorf("wrong bcrypt password should fail: %v", err)
	}
}

func TestStaticProviderBcryptAuth(t *testing.T) {
	hash, err := bcryptHash("mypass")
	if err != nil {
		t.Fatal(err)
	}
	p := NewStaticProvider(
		map[string]string{"admin": string(hash)},
		nil,
	)
	defer p.Close()

	id, err := p.Authenticate(context.Background(), Credentials{Username: "admin", Password: "mypass"})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "admin" {
		t.Errorf("UserID = %q, want admin", id.UserID)
	}

	// Wrong password against bcrypt hash
	_, err = p.Authenticate(context.Background(), Credentials{Username: "admin", Password: "bad"})
	if err != ErrInvalidCredentials {
		t.Errorf("wrong bcrypt password should fail: %v", err)
	}
}

// ---------------------------------------------------------------------------
// OAuth: NewOAuthProvider constructor (with real test server)
// ---------------------------------------------------------------------------

func TestNewOAuthProviderWithTestServer(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	pub := &privKey.PublicKey

	n := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	eBytes := big.NewInt(int64(pub.E)).Bytes()
	e := base64.RawURLEncoding.EncodeToString(eBytes)

	jwksJSON := `{"keys":[{"kid":"k1","kty":"RSA","n":"` + n + `","e":"` + e + `"}]}`

	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"issuer":   srv.URL,
			"jwks_uri": srv.URL + "/jwks",
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(jwksJSON))
	})

	srv = httptest.NewServer(mux)
	defer srv.Close()

	p, err := NewOAuthProvider(srv.URL, "client-id", "aud")
	if err != nil {
		t.Fatalf("NewOAuthProvider failed: %v", err)
	}
	defer p.Close()

	if p.issuer != srv.URL {
		t.Errorf("issuer = %q, want %q", p.issuer, srv.URL)
	}
	if p.clientID != "client-id" {
		t.Errorf("clientID = %q, want client-id", p.clientID)
	}
}

func TestNewOAuthProviderBadDiscovery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	_, err := NewOAuthProvider(srv.URL, "cid", "aud")
	if err == nil {
		t.Error("should fail when discovery returns 500")
	}
}

// ---------------------------------------------------------------------------
// OAuth: refreshKeys with mock server
// ---------------------------------------------------------------------------

func TestOAuthRefreshKeys(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	pub := &privKey.PublicKey

	n := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	eBytes := big.NewInt(int64(pub.E)).Bytes()
	e := base64.RawURLEncoding.EncodeToString(eBytes)

	jwksJSON := `{"keys":[{"kid":"rk1","kty":"RSA","n":"` + n + `","e":"` + e + `"}]}`

	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"issuer":   srv.URL,
			"jwks_uri": srv.URL + "/jwks",
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(jwksJSON))
	})

	srv = httptest.NewServer(mux)
	defer srv.Close()

	p := &OAuthProvider{
		issuer:     srv.URL,
		clientID:   "test",
		audience:   "test",
		keySet:     make(map[string]interface{}),
		httpClient: srv.Client(),
		closeCh:    make(chan struct{}),
	}
	defer p.Close()

	if err := p.refreshKeys(); err != nil {
		t.Fatalf("refreshKeys failed: %v", err)
	}

	p.keySetMux.RLock()
	_, ok := p.keySet["rk1"]
	p.keySetMux.RUnlock()

	if !ok {
		t.Error("key rk1 should be present after refresh")
	}
}

// ---------------------------------------------------------------------------
// OAuth: decodeJWTPart edge cases
// ---------------------------------------------------------------------------

func TestDecodeJWTPartInvalidBase64(t *testing.T) {
	_, err := decodeJWTPart("!!!invalid-base64!!!")
	if err == nil {
		t.Error("should fail for invalid base64")
	}
}

func TestDecodeJWTPartInvalidJSON(t *testing.T) {
	// Valid base64 but not JSON object
	encoded := base64.RawURLEncoding.EncodeToString([]byte("not-json"))
	_, err := decodeJWTPart(encoded)
	if err == nil {
		t.Error("should fail for invalid JSON")
	}
}

func TestDecodeJWTPartEmptyObject(t *testing.T) {
	encoded := base64.RawURLEncoding.EncodeToString([]byte("{}"))
	m, err := decodeJWTPart(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %v", m)
	}
}

// ---------------------------------------------------------------------------
// OAuth: parseJWK edge cases
// ---------------------------------------------------------------------------

func TestParseJWKUnsupportedKeyType(t *testing.T) {
	_, err := parseJWK(jwkKey{Kid: "k", Kty: "UNKNOWN"})
	if err == nil {
		t.Error("should fail for unsupported key type")
	}
}

func TestParseJWKOKP(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	x := base64.RawURLEncoding.EncodeToString(pub)

	key := jwkKey{
		Kid: "okp1",
		Kty: "OKP",
		X:   x,
	}

	parsed, err := parseJWK(key)
	if err != nil {
		t.Fatalf("parseJWK OKP failed: %v", err)
	}

	edKey, ok := parsed.(ed25519.PublicKey)
	if !ok {
		t.Fatal("expected ed25519.PublicKey")
	}
	if !edKey.Equal(pub) {
		t.Error("key mismatch")
	}
}

func TestParseJWKRSAInvalidBase64(t *testing.T) {
	_, err := parseJWK(jwkKey{Kid: "k", Kty: "RSA", N: "!!!", E: "!!!"})
	if err == nil {
		t.Error("should fail for invalid RSA base64 params")
	}
}

func TestParseJWKECInvalidBase64(t *testing.T) {
	_, err := parseJWK(jwkKey{Kid: "k", Kty: "EC", Crv: "P-256", X: "!!!", Y: "!!!"})
	if err == nil {
		t.Error("should fail for invalid EC base64 params")
	}
}

func TestParseJWKECUnsupportedCurve(t *testing.T) {
	privKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	pub := &privKey.PublicKey
	byteLen := (pub.Curve.Params().BitSize + 7) / 8
	x := base64.RawURLEncoding.EncodeToString(pub.X.FillBytes(make([]byte, byteLen)))
	y := base64.RawURLEncoding.EncodeToString(pub.Y.FillBytes(make([]byte, byteLen)))

	_, err := parseJWK(jwkKey{Kid: "k", Kty: "EC", Crv: "P-UNKNOWN", X: x, Y: y})
	if err == nil {
		t.Error("should fail for unsupported curve")
	}
}

func TestParseJWKEC_P384(t *testing.T) {
	privKey, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	pub := &privKey.PublicKey
	byteLen := (pub.Curve.Params().BitSize + 7) / 8
	x := base64.RawURLEncoding.EncodeToString(pub.X.FillBytes(make([]byte, byteLen)))
	y := base64.RawURLEncoding.EncodeToString(pub.Y.FillBytes(make([]byte, byteLen)))

	key := jwkKey{Kid: "ec384", Kty: "EC", Crv: "P-384", X: x, Y: y}
	parsed, err := parseJWK(key)
	if err != nil {
		t.Fatalf("parseJWK P-384 failed: %v", err)
	}
	ecPub, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		t.Fatal("expected *ecdsa.PublicKey")
	}
	if ecPub.X.Cmp(pub.X) != 0 {
		t.Error("X mismatch")
	}
}

func TestParseJWKEC_P521(t *testing.T) {
	privKey, _ := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	pub := &privKey.PublicKey
	byteLen := (pub.Curve.Params().BitSize + 7) / 8
	x := base64.RawURLEncoding.EncodeToString(pub.X.FillBytes(make([]byte, byteLen)))
	y := base64.RawURLEncoding.EncodeToString(pub.Y.FillBytes(make([]byte, byteLen)))

	key := jwkKey{Kid: "ec521", Kty: "EC", Crv: "P-521", X: x, Y: y}
	parsed, err := parseJWK(key)
	if err != nil {
		t.Fatalf("parseJWK P-521 failed: %v", err)
	}
	ecPub, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		t.Fatal("expected *ecdsa.PublicKey")
	}
	if ecPub.X.Cmp(pub.X) != 0 {
		t.Error("X mismatch")
	}
}

// ---------------------------------------------------------------------------
// OAuth: verifyJWT with ed25519 key type
// ---------------------------------------------------------------------------

func TestVerifyJWTEd25519(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	kid := "ed-key"

	p := &OAuthProvider{
		issuer:     "https://ed.example.com",
		clientID:   "ed-client",
		audience:   "ed-aud",
		keySet:     map[string]interface{}{kid: pub},
		httpClient: nil,
		closeCh:    make(chan struct{}),
	}
	defer p.Close()

	header := map[string]interface{}{"alg": "EdDSA", "typ": "JWT", "kid": kid}
	hb, _ := json.Marshal(header)
	headerB64 := base64.RawURLEncoding.EncodeToString(hb)

	payload := map[string]interface{}{
		"iss": "https://ed.example.com",
		"sub": "ed-user",
		"aud": "ed-aud",
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	pb, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(pb)

	signed := headerB64 + "." + payloadB64
	sig := ed25519.Sign(priv, []byte(signed))
	token := signed + "." + base64.RawURLEncoding.EncodeToString(sig)

	id, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err != nil {
		t.Fatalf("Ed25519 auth failed: %v", err)
	}
	if id.UserID != "ed-user" {
		t.Errorf("UserID = %q, want ed-user", id.UserID)
	}
}

func TestVerifyJWTUnsupportedKeyType(t *testing.T) {
	// verifyJWT with a non-RSA, non-EC, non-ed25519 key should error
	parts := []string{"a.b", "c.d", "Zg=="}
	err := verifyJWT(parts, "not-a-key")
	if err == nil {
		t.Error("should fail for unsupported key type")
	}
}

func TestVerifyJWTBadSignatureBase64(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	parts := []string{"a", "b", "!!!invalid-base64!!!"}
	err := verifyJWT(parts, &privKey.PublicKey)
	if err == nil {
		t.Error("should fail for bad base64 in signature")
	}
}

// ---------------------------------------------------------------------------
// OAuth: refreshLoop exit via Close
// ---------------------------------------------------------------------------

func TestOAuthRefreshLoopExits(t *testing.T) {
	p := &OAuthProvider{
		issuer:     "https://loop.example.com",
		clientID:   "c",
		audience:   "a",
		keySet:     make(map[string]interface{}),
		httpClient: &http.Client{Timeout: time.Second},
		closeCh:    make(chan struct{}),
	}
	// refreshLoop should exit quickly after Close
	go p.refreshLoop()
	time.Sleep(50 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}
	// Give the goroutine time to exit
	time.Sleep(100 * time.Millisecond)
}

// ---------------------------------------------------------------------------
// OAuth: JWT with missing sub claim
// ---------------------------------------------------------------------------

func TestOAuthMissingSubClaim(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "no-sub-key"
	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader(kid)
	// Payload with no "sub"
	payload := map[string]interface{}{
		"iss": "https://test.example.com",
		"exp": 9999999999,
		"aud": "test-audience",
	}
	pb, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(pb)
	token := signJWT(header, payloadB64, privKey)

	_, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err == nil {
		t.Error("expected error for missing sub claim")
	}
}

// ---------------------------------------------------------------------------
// OAuth: JWT with string audience (single value, not array)
// ---------------------------------------------------------------------------

func TestOAuthStringAudience(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "str-aud"
	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader(kid)
	payload := map[string]interface{}{
		"iss": "https://test.example.com",
		"sub": "user1",
		"exp": 9999999999,
		"aud": "test-audience", // single string, not array
	}
	pb, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(pb)
	token := signJWT(header, payloadB64, privKey)

	id, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err != nil {
		t.Fatalf("string audience auth failed: %v", err)
	}
	if id.UserID != "user1" {
		t.Errorf("UserID = %q, want user1", id.UserID)
	}
}

// ---------------------------------------------------------------------------
// OAuth: JWT with wrong string audience (single value mismatch)
// ---------------------------------------------------------------------------

func TestOAuthWrongStringAudience(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "wrong-str-aud"
	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader(kid)
	payload := map[string]interface{}{
		"iss": "https://test.example.com",
		"sub": "user1",
		"exp": 9999999999,
		"aud": "wrong-audience",
	}
	pb, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(pb)
	token := signJWT(header, payloadB64, privKey)

	_, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err == nil {
		t.Error("expected error for wrong audience")
	}
}

// ---------------------------------------------------------------------------
// MTLS: PeerCertsFromContext edge cases
// ---------------------------------------------------------------------------

func TestPeerCertsFromContextNil(t *testing.T) {
	certs := PeerCertsFromContext(context.TODO())
	if certs != nil {
		t.Errorf("nil context should return nil, got %v", certs)
	}
}

func TestPeerCertsFromContextEmpty(t *testing.T) {
	ctx := context.Background()
	certs := PeerCertsFromContext(ctx)
	if certs != nil {
		t.Errorf("empty context should return nil, got %v", certs)
	}
}

func TestPeerCertsFromContextWrongType(t *testing.T) {
	// Put a non-certificate value in the context
	ctx := context.WithValue(context.Background(), peerCertsKey, "not-certs")
	certs := PeerCertsFromContext(ctx)
	if certs != nil {
		t.Errorf("wrong type should return nil, got %v", certs)
	}
}

func TestMTLSProviderContextWithMultipleCerts(t *testing.T) {
	p := NewMTLSProvider()
	defer p.Close()

	cert1 := makeTestCert("first", nil, nil, nil)
	cert2 := makeTestCert("second", nil, nil, nil)
	ctx := ContextWithPeerCerts(context.Background(), []*x509.Certificate{cert1, cert2})

	id, err := p.Authenticate(ctx, Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	// Should use the first cert
	if id.UserID != "first" {
		t.Errorf("UserID = %q, want first", id.UserID)
	}
}

// ---------------------------------------------------------------------------
// LDAP: struct creation and configuration methods
// ---------------------------------------------------------------------------

func TestLDAPProviderCreation(t *testing.T) {
	p := NewLDAPProvider("ldap://localhost:389", "cn=admin,dc=example", "pass", "dc=example", "(uid={username})", true)
	if p.url != "ldap://localhost:389" {
		t.Errorf("url = %q", p.url)
	}
	if p.bindDN != "cn=admin,dc=example" {
		t.Errorf("bindDN = %q", p.bindDN)
	}
	if p.bindPass != "pass" {
		t.Errorf("bindPass = %q", p.bindPass)
	}
	if !p.useTLS {
		t.Error("useTLS should be true")
	}
	if p.roleAttr != "memberOf" {
		t.Errorf("default roleAttr = %q, want memberOf", p.roleAttr)
	}
}

func TestLDAPProviderDefaults(t *testing.T) {
	p := NewLDAPProvider("ldap://localhost", "", "", "", "", false)
	defer p.Close()

	// Verify defaults
	if p.roleAttr != "memberOf" {
		t.Errorf("default roleAttr = %q, want memberOf", p.roleAttr)
	}
	if p.groupAttr != "" {
		t.Errorf("default groupAttr = %q, want empty", p.groupAttr)
	}
}

func TestLDAPProviderCloseNil(t *testing.T) {
	p := NewLDAPProvider("ldap://localhost", "", "", "", "", false)
	// Double close should not panic
	if err := p.Close(); err != nil {
		t.Errorf("first close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
}
