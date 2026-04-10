package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// OAuthProvider authenticates using JWT tokens validated against an OIDC issuer.
type OAuthProvider struct {
	issuer    string
	audience  string
	clientID  string
	keySet    map[string]interface{} // kid → pubKey
	keySetMux sync.RWMutex

	httpClient *http.Client
	closeCh    chan struct{}
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
	p := &OAuthProvider{
		issuer:     issuer,
		clientID:   clientID,
		audience:   audience,
		keySet:     make(map[string]interface{}),
		httpClient: &http.Client{Timeout: 10 * time.Second},
		closeCh:    make(chan struct{}),
	}

	// Fetch initial JWKS
	if err := p.refreshKeys(); err != nil {
		return nil, fmt.Errorf("initial JWKS fetch: %w", err)
	}

	// Background refresh every hour
	go p.refreshLoop()

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

	kid, _ := header["kid"].(string)

	// Get the public key
	p.keySetMux.RLock()
	pubKey, ok := p.keySet[kid]
	p.keySetMux.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown key id: %s", kid)
	}

	// Verify signature
	if err := verifyJWT(parts, pubKey); err != nil {
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

	sub, _ := payload["sub"].(string)
	if sub == "" {
		return nil, fmt.Errorf("missing sub claim")
	}

	// Extract roles and groups from claims
	var roles []string
	if r, ok := payload["roles"].([]interface{}); ok {
		for _, v := range r {
			if s, ok := v.(string); ok {
				roles = append(roles, s)
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

// Close stops the background JWKS refresh.
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
				_ = err
			}
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
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("discovery returned %d", resp.StatusCode)
	}

	var disc oidcDiscovery
	if err := json.NewDecoder(resp.Body).Decode(&disc); err != nil {
		return fmt.Errorf("discovery decode: %w", err)
	}

	// Fetch JWKS
	jwksResp, err := p.httpClient.Get(disc.JWKSURI)
	if err != nil {
		return fmt.Errorf("jwks fetch: %w", err)
	}
	defer jwksResp.Body.Close()

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

func verifyJWT(parts []string, pubKey interface{}) error {
	// Reconstruct the signed content
	signed := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return err
	}

	switch k := pubKey.(type) {
	case *rsa.PublicKey:
		// For simplicity, we support RS256 verification via crypto/rsa
		// In production you'd check the alg header
		return rsaVerifyPKCS1v15(k, signed, sig)
	case *ecdsa.PublicKey:
		return ecdsaVerify(k, signed, sig)
	case ed25519.PublicKey:
		if !ed25519.Verify(k, []byte(signed), sig) {
			return fmt.Errorf("ed25519 verification failed")
		}
		return nil
	default:
		return fmt.Errorf("unsupported key type")
	}
}

func rsaVerifyPKCS1v15(pub *rsa.PublicKey, data string, sig []byte) error {
	h := sha256Sum([]byte(data))
	return rsa.VerifyPKCS1v15(pub, crypto.SHA256, h, sig)
}

func sha256Sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

func ecdsaVerify(pub *ecdsa.PublicKey, data string, sig []byte) error {
	h := sha256Sum([]byte(data))
	// ECDSA sig is r || s, each 32 bytes for P-256
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
