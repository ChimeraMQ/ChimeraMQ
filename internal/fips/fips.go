// Package fips provides FIPS 140-2 compliance support for ChimeraMQ.
// When FIPS mode is enabled, only FIPS-approved cryptographic algorithms
// are used, and non-compliant features are disabled.
package fips

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"hash"
	"os"
	"runtime"
	"sync"
)

// Mode represents the FIPS compliance mode.
type Mode int

const (
	// ModeDisabled means FIPS compliance is not enforced.
	ModeDisabled Mode = 0

	// ModeEnabled means FIPS 140-2 compliance is enforced.
	ModeEnabled Mode = 1
)

var (
	currentMode     Mode = ModeDisabled
	modeMutex       sync.RWMutex
	complianceError error
)

// Config holds FIPS configuration.
type Config struct {
	// Enabled enables FIPS 140-2 compliance mode.
	Enabled bool `yaml:"enabled"`

	// StrictMode fails startup if FIPS validation fails.
	StrictMode bool `yaml:"strict_mode"`

	// AllowedCipherSuites restricts TLS to FIPS-approved cipher suites.
	AllowedCipherSuites []string `yaml:"allowed_cipher_suites"`

	// MinTLSVersion sets the minimum TLS version (must be 1.2 or higher for FIPS).
	MinTLSVersion string `yaml:"min_tls_version"`
}

// DefaultConfig returns default FIPS configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:             false,
		StrictMode:          false,
		AllowedCipherSuites: FIPSCipherSuites(),
		MinTLSVersion:       "1.2",
	}
}

// IsEnabled returns true if FIPS mode is enabled.
func IsEnabled() bool {
	modeMutex.RLock()
	defer modeMutex.RUnlock()
	return currentMode == ModeEnabled
}

// SetMode sets the FIPS compliance mode.
func SetMode(mode Mode) {
	modeMutex.Lock()
	defer modeMutex.Unlock()
	currentMode = mode
}

// GetMode returns the current FIPS mode.
func GetMode() Mode {
	modeMutex.RLock()
	defer modeMutex.RUnlock()
	return currentMode
}

// Initialize initializes FIPS mode.
// This should be called early in the application startup.
func Initialize(cfg Config) error {
	if !cfg.Enabled {
		SetMode(ModeDisabled)
		return nil
	}

	// Check if the Go runtime supports FIPS mode
	if err := validateGoRuntime(); err != nil {
		if cfg.StrictMode {
			return fmt.Errorf("FIPS validation failed: %w", err)
		}
		complianceError = err
		// Continue with warning
	}

	SetMode(ModeEnabled)
	return nil
}

// validateGoRuntime checks if the Go runtime is suitable for FIPS operation.
func validateGoRuntime() error {
	// In a real implementation, this would check:
	// 1. If using a FIPS-validated Go toolchain (like boringcrypto)
	// 2. If the system is in FIPS mode (on Linux, check /proc/sys/crypto/fips_enabled)
	// 3. If all cryptographic libraries are FIPS-validated

	// Check if FIPS mode is requested via environment
	if os.Getenv("CHIMERA_FIPS_FORCE") == "1" {
		return nil // Skip validation for testing
	}

	// Check for FIPS-enabled OpenSSL on Linux
	if runtime.GOOS == "linux" {
		if _, err := os.Stat("/proc/sys/crypto/fips_enabled"); err == nil {
			data, err := os.ReadFile("/proc/sys/crypto/fips_enabled")
			if err == nil && len(data) > 0 && data[0] == '1' {
				// System is in FIPS mode
				return nil
			}
		}
	}

	// Note: In production, this should validate the Go toolchain
	// For now, we allow operation but log a warning
	return fmt.Errorf("FIPS mode requested but system is not in FIPS mode")
}

// NewSHA256 returns a new SHA-256 hash (FIPS-approved).
func NewSHA256() hash.Hash {
	return sha256.New()
}

// NewSHA384 returns a new SHA-384 hash (FIPS-approved).
func NewSHA384() hash.Hash {
	return sha512.New384()
}

// NewSHA512 returns a new SHA-512 hash (FIPS-approved).
func NewSHA512() hash.Hash {
	return sha512.New()
}

// SecureRandom returns cryptographically secure random bytes using crypto/rand (FIPS-approved).
func SecureRandom(size int) ([]byte, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// FIPSApprovedCurves returns the list of FIPS-approved elliptic curves.
func FIPSApprovedCurves() []elliptic.Curve {
	return []elliptic.Curve{
		elliptic.P256(),
		elliptic.P384(),
		elliptic.P521(),
	}
}

// IsCurveFIPSApproved returns true if the curve is FIPS-approved.
func IsCurveFIPSApproved(curve elliptic.Curve) bool {
	switch curve {
	case elliptic.P256(), elliptic.P384(), elliptic.P521():
		return true
	default:
		return false
	}
}

// GenerateRSAKey generates a FIPS-approved RSA key.
// FIPS 186-4 requires minimum 2048 bits for RSA.
func GenerateRSAKey(bits int) (*rsa.PrivateKey, error) {
	if IsEnabled() && bits < 2048 {
		return nil, fmt.Errorf("RSA key size %d below FIPS minimum of 2048 bits", bits)
	}
	return rsa.GenerateKey(rand.Reader, bits)
}

// GenerateECDSAKey generates a FIPS-approved ECDSA key.
func GenerateECDSAKey(curve elliptic.Curve) (*ecdsa.PrivateKey, error) {
	if IsEnabled() && !IsCurveFIPSApproved(curve) {
		return nil, fmt.Errorf("curve not FIPS-approved")
	}
	return ecdsa.GenerateKey(curve, rand.Reader)
}

// GenerateEd25519Key generates an Ed25519 key.
// Note: Ed25519 is not FIPS-approved, so this will fail in FIPS mode.
func GenerateEd25519Key() (ed25519.PrivateKey, error) {
	if IsEnabled() {
		return nil, fmt.Errorf("Ed25519 is not FIPS-approved")
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	return priv, err
}

// NewAESGCM creates a new AES-GCM cipher (FIPS-approved).
func NewAESGCM(key []byte) (cipher.AEAD, error) {
	if IsEnabled() {
		// FIPS requires AES-128, AES-192, or AES-256
		if len(key) != 16 && len(key) != 24 && len(key) != 32 {
			return nil, fmt.Errorf("AES key size %d not FIPS-approved (must be 128, 192, or 256 bits)", len(key)*8)
		}
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// NewAESCTR creates a new AES-CTR cipher (FIPS-approved).
func NewAESCTR(key []byte, iv []byte) (cipher.Stream, error) {
	if IsEnabled() {
		if len(key) != 16 && len(key) != 24 && len(key) != 32 {
			return nil, fmt.Errorf("AES key size %d not FIPS-approved", len(key)*8)
		}
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewCTR(block, iv), nil
}

// FIPSCipherSuites returns FIPS-approved TLS cipher suites.
func FIPSCipherSuites() []string {
	return []string{
		"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
		"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
		"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
		"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
		"TLS_RSA_WITH_AES_128_GCM_SHA256",
		"TLS_RSA_WITH_AES_256_GCM_SHA384",
	}
}

// ValidateTLSCipherSuite returns true if the cipher suite is FIPS-approved.
func ValidateTLSCipherSuite(suite uint16) bool {
	// FIPS-approved cipher suites (TLS 1.2)
	fipsSuites := []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	}
	for _, s := range fipsSuites {
		if s == suite {
			return true
		}
	}
	return false
}

// GetMinFIPSTLSVersion returns the minimum TLS version for FIPS compliance.
func GetMinFIPSTLSVersion() uint16 {
	return tls.VersionTLS12
}

// SecureTLSConfig returns a TLS config that satisfies FIPS requirements.
func SecureTLSConfig() *tls.Config {
	cfg := &tls.Config{
		MinVersion: GetMinFIPSTLSVersion(),
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		},
	}

	if IsEnabled() {
		// In FIPS mode, only allow FIPS-approved curves
		cfg.CurvePreferences = []tls.CurveID{
			tls.CurveP256,
			tls.CurveP384,
			tls.CurveP521,
		}
	}

	return cfg
}

// ValidateCertificate validates that a certificate meets FIPS requirements.
func ValidateCertificate(cert *x509.Certificate) error {
	if !IsEnabled() {
		return nil
	}

	// Check signature algorithm
	switch cert.SignatureAlgorithm {
	case x509.SHA256WithRSA, x509.SHA384WithRSA, x509.SHA512WithRSA,
		x509.ECDSAWithSHA256, x509.ECDSAWithSHA384, x509.ECDSAWithSHA512:
		// These are FIPS-approved
	default:
		return fmt.Errorf("certificate uses non-FIPS signature algorithm: %v", cert.SignatureAlgorithm)
	}

	// Check public key type and size
	switch pub := cert.PublicKey.(type) {
	case *rsa.PublicKey:
		if pub.N.BitLen() < 2048 {
			return fmt.Errorf("RSA key size %d below FIPS minimum of 2048 bits", pub.N.BitLen())
		}
	case *ecdsa.PublicKey:
		if !IsCurveFIPSApproved(pub.Curve) {
			return fmt.Errorf("ECDSA curve not FIPS-approved")
		}
	default:
		return fmt.Errorf("public key type %T not FIPS-approved", pub)
	}

	return nil
}

// ComplianceError returns any error from FIPS initialization.
func ComplianceError() error {
	return complianceError
}

// IsAlgorithmAllowed returns true if the algorithm is allowed in current mode.
func IsAlgorithmAllowed(alg string) bool {
	if !IsEnabled() {
		return true
	}

	// FIPS-approved algorithms
	allowed := []string{
		"AES-128-GCM",
		"AES-192-GCM",
		"AES-256-GCM",
		"AES-128-CTR",
		"AES-192-CTR",
		"AES-256-CTR",
		"AES-128-CBC",
		"AES-192-CBC",
		"AES-256-CBC",
		"SHA-256",
		"SHA-384",
		"SHA-512",
		"HMAC-SHA256",
		"HMAC-SHA384",
		"HMAC-SHA512",
		"RSA-2048",
		"RSA-3072",
		"RSA-4096",
		"ECDSA-P256",
		"ECDSA-P384",
		"ECDSA-P521",
	}

	for _, a := range allowed {
		if a == alg {
			return true
		}
	}
	return false
}

// NonFIPSFeatures returns a list of features that are disabled in FIPS mode.
func NonFIPSFeatures() []string {
	return []string{
		"Ed25519 signatures",
		"ChaCha20-Poly1305 encryption",
		"TLS 1.0/1.1",
		"MD5 hashing",
		"SHA-1 for signatures",
		"RSA keys < 2048 bits",
		"DSA signatures",
		"Non-approved elliptic curves (Curve25519, secp256k1, etc.)",
	}
}
