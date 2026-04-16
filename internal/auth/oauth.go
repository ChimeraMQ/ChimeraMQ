package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// OAuthProvider authenticates using JWT tokens validated against an OIDC issuer.
type OAuthProvider struct {
	issuer        string
	audience      string
	clientID      string
	roleAllowlist map[string]bool      // Role allowlist for filtering JWT roles
	keySet        map[string]interface{} // kid → pubKey
	keySetMux     sync.RWMutex

	httpClient *http.Client
	closeCh    chan struct{}

	// jtiCache prevents JWT replay attacks — tracks recently used jti values
	jtiCache   map[string]time.Time // jti → expiry time
	jtiCacheMux sync.RWMutex
	jtiMaxAge  time.Duration        // max age for jti entries (default: 5 min)
}

// jwksResponse is the JWKS endpoint response.
type jwksResponse struct {
	Keys []jwkKey `json:"keys"`
}

type jwkKey struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Use string `json:"use"`
	N   string `json:"n,omitempty"`
	E   string `json:"e,omitempty"`
	X   string `json:"x,omitempty"`
	Y   string `json:"y,omitempty"`
	Crv string `json:"crv,omitempty"`
	Alg string `json:"alg,omitempty"`
}

// oidcDiscovery is the OpenID discovery document.
type oidcDiscovery struct {
	Issuer  string `json:"issuer"`
	JWKSURI string `json:"jwks_uri"`
}

// NewOAuthProvider creates an OAuth provider that validates JWTs from the given issuer.
func NewOAuthProvider(issuer, clientID, audience string) (*OAuthProvider, error) {
	return NewOAuthProviderWithRoleAllowlist(issuer, clientID, audience, nil)
}

// NewOAuthProviderWithRoleAllowlist creates an OAuth provider with a role allowlist.
// The allowlist filters the roles extracted from JWT claims — only roles in the allowlist are granted.
// If allowlist is nil, all roles from the JWT are accepted.
func NewOAuthProviderWithRoleAllowlist(issuer, clientID, audience string, roleAllowlist []string) (*OAuthProvider, error) {
	allowmap := make(map[string]bool)
	for _, r := range roleAllowlist {
		allowmap[r] = true
	}
	p := &OAuthProvider{
		issuer:        issuer,
		clientID:      clientID,
		audience:      audience,
		roleAllowlist: allowmap,
		keySet:        make(map[string]interface{}),
		httpClient:    &http.Client{Timeout: 10 * time.Second},
		closeCh:       make(chan struct{}),
		jtiCache:      make(map[string]time.Time),
		jtiMaxAge:     5 * time.Minute,
	}

	// Fetch initial JWKS
	if err := p.refreshKeys(); err != nil {
		return nil, fmt.Errorf("initial JWKS fetch: %w", err)
	}

	// Background refresh every hour
	go p.refreshLoop()

	// Background jti cache cleanup
	go p.jtiCleanupLoop()

	return p, nil
}

// Authenticate validates a JWT token from credentials.
func (p *OAuthProvider) Authenticate(ctx context.Context, creds Credentials) (*Identity, error) {
	token := creds.Token
	if token == "" {
		return nil, ErrInvalidCredentials
	}

	// Parse JWT without verification first to get the header kid
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidCredentials
	}

	header, err := decodeJWTPart(parts[0])
	if err != nil {
		return nil, fmt.Errorf("jwt header: %w", err)
	}

	// Validate the alg header — reject alg:none and unknown algorithms
	alg, _ := header["alg"].(string)
	if err := validateAlg(alg); err != nil {
		return nil, fmt.Errorf("jwt alg: %w", err)
	}

	kid, _ := header["kid"].(string)

	// Get the public key
	p.keySetMux.RLock()
	pubKey, ok := p.keySet[kid]
	p.keySetMux.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown key id: %s", kid)
	}

	// Verify that the declared alg matches the key type
	if err := algMatchesKey(alg, pubKey); err != nil {
		return nil, fmt.Errorf("jwt alg/key mismatch: %w", err)
	}

	// Verify signature
	if err := verifyJWT(parts, pubKey, alg); err != nil {
		return nil, fmt.Errorf("jwt verification: %w", err)
	}

	// Decode payload
	payload, err := decodeJWTPart(parts[1])
	if err != nil {
		return nil, fmt.Errorf("jwt payload: %w", err)
	}

	// Validate claims
	iss, _ := payload["iss"].(string)
	if iss != p.issuer {
		return nil, fmt.Errorf("invalid issuer: %s", iss)
	}

	if p.audience != "" {
		aud, _ := payload["aud"].([]interface{})
		found := false
		for _, a := range aud {
			if s, ok := a.(string); ok && s == p.audience {
				found = true
				break
			}
		}
		if !found {
			// Also check single string aud
			if audStr, ok := payload["aud"].(string); !ok || audStr != p.audience {
				return nil, fmt.Errorf("invalid audience")
			}
		}
	}

	// Check expiration
	if exp, ok := payload["exp"].(float64); ok {
		if time.Now().Unix() > int64(exp) {
			return nil, fmt.Errorf("token expired")
		}
	}

	// Check not-before
	if nbf, ok := payload["nbf"].(float64); ok {
		if time.Now().Unix() < int64(nbf) {
			return nil, fmt.Errorf("token not yet valid")
		}
	}

	// Reject tokens issued too far in the past (30-day max lifetime)
	if iat, ok := payload["iat"].(float64); ok {
		const maxTokenAge = 30 * 24 * int64(time.Hour/time.Second)
		if time.Now().Unix()-int64(iat) > maxTokenAge {
			return nil, fmt.Errorf("token too old")
		}
	}

	// Check jti for replay attack prevention
	if jti, ok := payload["jti"].(string); ok {
		if p.isJTIUsed(jti) {
			return nil, fmt.Errorf("token replay detected")
		}
		// Compute expiry from exp or default
		exp := time.Now().Add(p.jtiMaxAge)
		if expVal, ok := payload["exp"].(float64); ok {
			exp = time.Unix(int64(expVal), 0)
		}
		p.markJTIUsed(jti, exp)
	}

	sub, _ := payload["sub"].(string)
	if sub == "" {
		return nil, fmt.Errorf("missing sub claim")
	}

	// Extract roles and groups from claims
	var roles []string
	if r, ok := payload["roles"].([]interface{}); ok {
		for _, v := range r {
			if s, ok := v.(string); ok {
				// If role allowlist is configured, filter roles through it
				if len(p.roleAllowlist) > 0 {
					if p.roleAllowlist[s] {
						roles = append(roles, s)
					}
				} else {
					roles = append(roles, s)
				}
			}
		}
	}

	var groups []string
	if g, ok := payload["groups"].([]interface{}); ok {
		for _, v := range g {
			if s, ok := v.(string); ok {
				groups = append(groups, s)
			}
		}
	}

	// Build claims map
	claims := make(map[string]string)
	for k, v := range payload {
		if s, ok := v.(string); ok {
			claims[k] = s
		}
	}

	return &Identity{
		UserID: sub,
		Roles:  roles,
		Groups: groups,
		Source: "oauth",
		Claims: claims,
	}, nil
}

// Close stops the background JWKS refresh and jti cache cleanup.
func (p *OAuthProvider) Close() error {
	close(p.closeCh)
	if p.httpClient != nil {
		p.httpClient.CloseIdleConnections()
	}
	return nil
}

func (p *OAuthProvider) refreshLoop() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
			if err := p.refreshKeys(); err != nil {
				// Log but don't fail — keep existing keys
				fmt.Fprintf(os.Stderr, "WARNING: JWKS refresh failed: %v\n", err)
			}
		}
	}
}

// isJTIUsed checks if a jti has already been used (replay attack).
func (p *OAuthProvider) isJTIUsed(jti string) bool {
	p.jtiCacheMux.RLock()
	defer p.jtiCacheMux.RUnlock()
	_, exists := p.jtiCache[jti]
	return exists
}

// markJTIUsed records a jti to prevent replay attacks.
func (p *OAuthProvider) markJTIUsed(jti string, expiry time.Time) {
	p.jtiCacheMux.Lock()
	defer p.jtiCacheMux.Unlock()
	p.jtiCache[jti] = expiry
}

// jtiCleanupLoop periodically removes expired jti entries to prevent unbounded memory growth.
func (p *OAuthProvider) jtiCleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-p.closeCh:
			return
		case <-ticker.C:
			p.jtiCacheMux.Lock()
			now := time.Now()
			for jti, expiry := range p.jtiCache {
				if now.After(expiry) {
					delete(p.jtiCache, jti)
				}
			}
			p.jtiCacheMux.Unlock()
		}
	}
}

func (p *OAuthProvider) refreshKeys() error {
	// Discover JWKS URI from issuer
	discURL := strings.TrimRight(p.issuer, "/") + "/.well-known/openid-configuration"
	resp, err := p.httpClient.Get(discURL)
	if err != nil {
		return fmt.Errorf("discovery: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("discovery returned %d", resp.StatusCode)
	}

	var disc oidcDiscovery
	if err := json.NewDecoder(resp.Body).Decode(&disc); err != nil {
		return fmt.Errorf("discovery decode: %w", err)
	}

	// Fetch JWKS — enforce HTTPS (except localhost for testing)
	isLocal := strings.Contains(disc.JWKSURI, "localhost") || strings.Contains(disc.JWKSURI, "127.0.0.1")
	if !strings.HasPrefix(disc.JWKSURI, "https://") && !isLocal {
		return fmt.Errorf("jwks URI must use HTTPS, got: %s", disc.JWKSURI)
	}
	jwksResp, err := p.httpClient.Get(disc.JWKSURI)
	if err != nil {
		return fmt.Errorf("jwks fetch: %w", err)
	}
	defer func() { _ = jwksResp.Body.Close() }()

	var jwks jwksResponse
	if err := json.NewDecoder(jwksResp.Body).Decode(&jwks); err != nil {
		return fmt.Errorf("jwks decode: %w", err)
	}

	newKeys := make(map[string]interface{})
	for _, key := range jwks.Keys {
		pubKey, err := parseJWK(key)
		if err != nil {
			continue
		}
		newKeys[key.Kid] = pubKey
	}

	p.keySetMux.Lock()
	p.keySet = newKeys
	p.keySetMux.Unlock()

	return nil
}

func decodeJWTPart(part string) (map[string]interface{}, error) {
	// Add base64url padding
	b, err := base64.RawURLEncoding.DecodeString(part)
	if err != nil {
		return nil, err
	}

	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func parseJWK(key jwkKey) (interface{}, error) {
	switch key.Kty {
	case "RSA":
		nBytes, err := base64.RawURLEncoding.DecodeString(key.N)
		if err != nil {
			return nil, err
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
		if err != nil {
			return nil, err
		}

		n := new(big.Int).SetBytes(nBytes)
		e := int(new(big.Int).SetBytes(eBytes).Int64())

		return &rsa.PublicKey{N: n, E: e}, nil

	case "EC":
		xBytes, err := base64.RawURLEncoding.DecodeString(key.X)
		if err != nil {
			return nil, err
		}
		yBytes, err := base64.RawURLEncoding.DecodeString(key.Y)
		if err != nil {
			return nil, err
		}

		var curve elliptic.Curve
		switch key.Crv {
		case "P-256":
			curve = elliptic.P256()
		case "P-384":
			curve = elliptic.P384()
		case "P-521":
			curve = elliptic.P521()
		default:
			return nil, fmt.Errorf("unsupported curve: %s", key.Crv)
		}

		x := new(big.Int).SetBytes(xBytes)
		y := new(big.Int).SetBytes(yBytes)

		return &ecdsa.PublicKey{Curve: curve, X: x, Y: y}, nil

	case "OKP":
		xBytes, err := base64.RawURLEncoding.DecodeString(key.X)
		if err != nil {
			return nil, err
		}
		return ed25519.PublicKey(xBytes), nil

	default:
		return nil, fmt.Errorf("unsupported key type: %s", key.Kty)
	}
}

// allowedAlgorithms is the set of JWT algorithms we accept.
var allowedAlgorithms = map[string]bool{
	"RS256": true, "RS384": true, "RS512": true,
	"ES256": true, "ES384": true, "ES512": true,
	"EdDSA": true,
}

func validateAlg(alg string) error {
	if alg == "" || alg == "none" {
		return fmt.Errorf("algorithm %q is not allowed", alg)
	}
	if !allowedAlgorithms[alg] {
		return fmt.Errorf("algorithm %q is not supported", alg)
	}
	return nil
}

func algMatchesKey(alg string, pubKey interface{}) error {
	switch pubKey.(type) {
	case *rsa.PublicKey:
		if !strings.HasPrefix(alg, "RS") {
			return fmt.Errorf("RSA key requires RS* algorithm, got %q", alg)
		}
	case *ecdsa.PublicKey:
		if !strings.HasPrefix(alg, "ES") {
			return fmt.Errorf("EC key requires ES* algorithm, got %q", alg)
		}
	case ed25519.PublicKey:
		if alg != "EdDSA" {
			return fmt.Errorf("Ed25519 key requires EdDSA algorithm, got %q", alg)
		}
	default:
		return fmt.Errorf("unsupported key type")
	}
	return nil
}

func verifyJWT(parts []string, pubKey interface{}, alg string) error {
	// Reconstruct the signed content
	signed := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return err
	}

	switch k := pubKey.(type) {
	case *rsa.PublicKey:
		return rsaVerifyPKCS1v15(k, signed, sig, alg)
	case *ecdsa.PublicKey:
		return ecdsaVerify(k, signed, sig, alg)
	case ed25519.PublicKey:
		if !ed25519.Verify(k, []byte(signed), sig) {
			return fmt.Errorf("ed25519 verification failed")
		}
		return nil
	default:
		return fmt.Errorf("unsupported key type")
	}
}

func rsaVerifyPKCS1v15(pub *rsa.PublicKey, data string, sig []byte, alg string) error {
	h := hashForAlg(alg, []byte(data))
	return rsa.VerifyPKCS1v15(pub, hashCrypto(alg), h, sig)
}

func ecdsaVerify(pub *ecdsa.PublicKey, data string, sig []byte, alg string) error {
	h := hashForAlg(alg, []byte(data))
	// ECDSA sig is r || s, each byteLen bytes for the curve
	byteLen := (pub.Curve.Params().BitSize + 7) / 8
	if len(sig) != 2*byteLen {
		return fmt.Errorf("invalid ECDSA signature length")
	}
	r := new(big.Int).SetBytes(sig[:byteLen])
	s := new(big.Int).SetBytes(sig[byteLen:])
	if !ecdsa.Verify(pub, h, r, s) {
		return fmt.Errorf("ecdsa verification failed")
	}
	return nil
}

func hashForAlg(alg string, data []byte) []byte {
	switch alg {
	case "RS384", "ES384":
		h := sha512.New384()
		h.Write(data)
		return h.Sum(nil)
	case "RS512", "ES512":
		h := sha512.New()
		h.Write(data)
		return h.Sum(nil)
	default: // RS256, ES256, EdDSA
		h := sha256.Sum256(data)
		return h[:]
	}
}

func hashCrypto(alg string) crypto.Hash {
	switch alg {
	case "RS384", "ES384":
		return crypto.SHA384
	case "RS512", "ES512":
		return crypto.SHA512
	default:
		return crypto.SHA256
	}
}

func sha256Sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}
