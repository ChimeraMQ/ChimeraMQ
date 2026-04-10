package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
)

func makeJWTHeader(kid string) string {
	header := map[string]interface{}{
		"alg": "RS256",
		"typ": "JWT",
		"kid": kid,
	}
	b, _ := json.Marshal(header)
	return base64.RawURLEncoding.EncodeToString(b)
}

func makeJWTPayload(iss, sub, aud string, exp int64, roles, groups []string) string {
	payload := map[string]interface{}{
		"iss":   iss,
		"sub":   sub,
		"exp":   exp,
		"aud":   aud,
		"roles": roles,
		"groups": groups,
	}
	b, _ := json.Marshal(payload)
	return base64.RawURLEncoding.EncodeToString(b)
}

func signJWT(header, payload string, privKey *rsa.PrivateKey) string {
	signed := header + "." + payload
	h := sha256.Sum256([]byte(signed))
	sig, _ := rsa.SignPKCS1v15(rand.Reader, privKey, crypto.SHA256, h[:])
	return signed + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func signJWTEC(header, payload string, privKey *ecdsa.PrivateKey) string {
	signed := header + "." + payload
	h := sha256.Sum256([]byte(signed))
	r, s, _ := ecdsa.Sign(rand.Reader, privKey, h[:])
	byteLen := (privKey.Curve.Params().BitSize + 7) / 8
	sig := make([]byte, 2*byteLen)
	r.FillBytes(sig[:byteLen])
	s.FillBytes(sig[byteLen:])
	return signed + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// testOAuthProvider creates an OAuthProvider with pre-loaded keys (no network).
func testOAuthProvider(kid string, pubKey interface{}) *OAuthProvider {
	p := &OAuthProvider{
		issuer:    "https://test.example.com",
		clientID:  "test-client",
		audience:  "test-audience",
		keySet:    map[string]interface{}{kid: pubKey},
		httpClient: nil,
		closeCh:   make(chan struct{}),
	}
	return p
}

func TestOAuthRS256(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-rs256"

	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader(kid)
	payload := makeJWTPayload(
		"https://test.example.com",
		"user1",
		"test-audience",
		9999999999,
		[]string{"admin"},
		[]string{"eng"},
	)
	token := signJWT(header, payload, privKey)

	id, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "user1" {
		t.Errorf("UserID = %q, want %q", id.UserID, "user1")
	}
	if id.Source != "oauth" {
		t.Errorf("Source = %q, want %q", id.Source, "oauth")
	}
	if len(id.Roles) != 1 || id.Roles[0] != "admin" {
		t.Errorf("Roles = %v, want [admin]", id.Roles)
	}
	if len(id.Groups) != 1 || id.Groups[0] != "eng" {
		t.Errorf("Groups = %v, want [eng]", id.Groups)
	}
}

func TestOAuthECDSA(t *testing.T) {
	privKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	kid := "test-ec"

	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader(kid)
	// Update alg for EC
	hMap := map[string]interface{}{"alg": "ES256", "typ": "JWT", "kid": kid}
	hb, _ := json.Marshal(hMap)
	header = base64.RawURLEncoding.EncodeToString(hb)

	payload := makeJWTPayload(
		"https://test.example.com",
		"ec-user",
		"test-audience",
		9999999999,
		nil,
		nil,
	)
	token := signJWTEC(header, payload, privKey)

	id, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err != nil {
		t.Fatal(err)
	}
	if id.UserID != "ec-user" {
		t.Errorf("UserID = %q, want %q", id.UserID, "ec-user")
	}
}

func TestOAuthExpired(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-exp"

	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader(kid)
	payload := makeJWTPayload(
		"https://test.example.com",
		"user1",
		"test-audience",
		1, // expired
		nil, nil,
	)
	token := signJWT(header, payload, privKey)

	_, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err == nil {
		t.Error("expected error for expired token")
	}
}

func TestOAuthWrongIssuer(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-wrong-iss"

	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader(kid)
	payload := makeJWTPayload(
		"https://wrong.example.com",
		"user1",
		"test-audience",
		9999999999,
		nil, nil,
	)
	token := signJWT(header, payload, privKey)

	_, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err == nil {
		t.Error("expected error for wrong issuer")
	}
}

func TestOAuthWrongAudience(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	kid := "test-wrong-aud"

	p := testOAuthProvider(kid, &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader(kid)
	payload := makeJWTPayload(
		"https://test.example.com",
		"user1",
		"wrong-audience",
		9999999999,
		nil, nil,
	)
	token := signJWT(header, payload, privKey)

	_, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err == nil {
		t.Error("expected error for wrong audience")
	}
}

func TestOAuthNoToken(t *testing.T) {
	p := testOAuthProvider("x", nil)
	defer p.Close()

	_, err := p.Authenticate(context.Background(), Credentials{})
	if err != ErrInvalidCredentials {
		t.Errorf("error = %v, want ErrInvalidCredentials", err)
	}
}

func TestOAuthInvalidFormat(t *testing.T) {
	p := testOAuthProvider("x", nil)
	defer p.Close()

	_, err := p.Authenticate(context.Background(), Credentials{Token: "not.a.valid.jwt"})
	if err == nil {
		t.Error("expected error for invalid JWT format")
	}
}

func TestParseJWKRSA(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	pub := &privKey.PublicKey

	n := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	eBytes := big.NewInt(int64(pub.E)).Bytes()
	e := base64.RawURLEncoding.EncodeToString(eBytes)

	key := jwkKey{
		Kid: "test",
		Kty: "RSA",
		N:   n,
		E:   e,
	}

	parsed, err := parseJWK(key)
	if err != nil {
		t.Fatal(err)
	}

	rsaPub, ok := parsed.(*rsa.PublicKey)
	if !ok {
		t.Fatal("expected *rsa.PublicKey")
	}
	if rsaPub.E != pub.E {
		t.Errorf("E = %d, want %d", rsaPub.E, pub.E)
	}
}

func TestParseJWKEC(t *testing.T) {
	privKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	pub := &privKey.PublicKey

	byteLen := (pub.Curve.Params().BitSize + 7) / 8
	x := base64.RawURLEncoding.EncodeToString(pub.X.FillBytes(make([]byte, byteLen)))
	y := base64.RawURLEncoding.EncodeToString(pub.Y.FillBytes(make([]byte, byteLen)))

	key := jwkKey{
		Kid: "test",
		Kty: "EC",
		Crv: "P-256",
		X:   x,
		Y:   y,
	}

	parsed, err := parseJWK(key)
	if err != nil {
		t.Fatal(err)
	}

	ecPub, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		t.Fatal("expected *ecdsa.PublicKey")
	}
	if ecPub.X.Cmp(pub.X) != 0 {
		t.Error("X mismatch")
	}
}

func TestDecodeJWTPart(t *testing.T) {
	m, err := decodeJWTPart(base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","kid":"1"}`)))
	if err != nil {
		t.Fatal(err)
	}
	if m["alg"] != "RS256" {
		t.Errorf("alg = %v, want RS256", m["alg"])
	}
	if m["kid"] != "1" {
		t.Errorf("kid = %v, want 1", m["kid"])
	}
}

func TestOAuthUnknownKeyID(t *testing.T) {
	privKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	p := testOAuthProvider("known-kid", &privKey.PublicKey)
	defer p.Close()

	header := makeJWTHeader("unknown-kid")
	payload := makeJWTPayload("https://test.example.com", "user1", "test-audience", 9999999999, nil, nil)
	token := signJWT(header, payload, privKey)

	_, err := p.Authenticate(context.Background(), Credentials{Token: token})
	if err == nil {
		t.Error("expected error for unknown key ID")
	}
	fmt.Println("unknown key error:", err)
}
