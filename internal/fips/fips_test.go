package fips

import (
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Enabled {
		t.Error("FIPS should be disabled by default")
	}
	if cfg.StrictMode {
		t.Error("Strict mode should be disabled by default")
	}
	if cfg.MinTLSVersion != "1.2" {
		t.Errorf("expected min TLS version 1.2, got %s", cfg.MinTLSVersion)
	}
}

func TestSetMode(t *testing.T) {
	// Save original mode
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeDisabled)
	if IsEnabled() {
		t.Error("FIPS should be disabled")
	}

	SetMode(ModeEnabled)
	if !IsEnabled() {
		t.Error("FIPS should be enabled")
	}
}

func TestInitialize(t *testing.T) {
	// Save original mode
	origMode := GetMode()
	defer SetMode(origMode)

	// Test disabled
	cfg := Config{Enabled: false}
	if err := Initialize(cfg); err != nil {
		t.Errorf("Initialize should not error when disabled: %v", err)
	}
	if IsEnabled() {
		t.Error("FIPS should be disabled")
	}

	// Test enabled (may fail validation, but should not panic)
	cfg = Config{Enabled: true, StrictMode: false}
	_ = Initialize(cfg) // May return error on non-FIPS system
}

func TestIsCurveFIPSApproved(t *testing.T) {
	tests := []struct {
		curve    elliptic.Curve
		expected bool
	}{
		{elliptic.P256(), true},
		{elliptic.P384(), true},
		{elliptic.P521(), true},
		{elliptic.P224(), false}, // Not FIPS-approved
	}

	for _, tt := range tests {
		result := IsCurveFIPSApproved(tt.curve)
		if result != tt.expected {
			t.Errorf("IsCurveFIPSApproved(%v) = %v, want %v", tt.curve, result, tt.expected)
		}
	}
}

func TestGenerateRSAKey(t *testing.T) {
	// Save original mode
	origMode := GetMode()
	defer SetMode(origMode)

	// Test with FIPS disabled
	SetMode(ModeDisabled)
	key, err := GenerateRSAKey(1024) // Small key should work when not in FIPS mode
	if err != nil {
		t.Errorf("GenerateRSAKey(1024) should not error when FIPS disabled: %v", err)
	}
	if key == nil {
		t.Error("key should not be nil")
	}

	// Test with FIPS enabled
	SetMode(ModeEnabled)
	_, err = GenerateRSAKey(1024) // Should fail in FIPS mode
	if err == nil {
		t.Error("GenerateRSAKey(1024) should error in FIPS mode")
	}

	// Valid FIPS key size
	key, err = GenerateRSAKey(2048)
	if err != nil {
		t.Errorf("GenerateRSAKey(2048) should not error: %v", err)
	}
	if key == nil {
		t.Error("key should not be nil")
	}
}

func TestNewAESGCM(t *testing.T) {
	// Save original mode
	origMode := GetMode()
	defer SetMode(origMode)

	// Test with FIPS disabled
	SetMode(ModeDisabled)

	// Valid key sizes
	for _, size := range []int{16, 24, 32} {
		key := make([]byte, size)
		aead, err := NewAESGCM(key)
		if err != nil {
			t.Errorf("NewAESGCM(%d bytes) should not error: %v", size, err)
		}
		if aead == nil {
			t.Error("aead should not be nil")
		}
	}

	// Invalid key size (should still work when FIPS disabled)
	key := make([]byte, 8)
	_, err := NewAESGCM(key)
	if err == nil {
		t.Error("NewAESGCM(8 bytes) should error")
	}

	// Test with FIPS enabled
	SetMode(ModeEnabled)
	key = make([]byte, 8)
	_, err = NewAESGCM(key)
	if err == nil {
		t.Error("NewAESGCM(8 bytes) should error in FIPS mode")
	}
}

func TestSecureTLSConfig(t *testing.T) {
	cfg := SecureTLSConfig()
	if cfg == nil {
		t.Fatal("SecureTLSConfig should not return nil")
	}

	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("expected min version TLS 1.2, got %d", cfg.MinVersion)
	}

	if len(cfg.CipherSuites) == 0 {
		t.Error("CipherSuites should not be empty")
	}
}

func TestValidateTLSCipherSuite(t *testing.T) {
	// Valid FIPS cipher suites
	validSuites := []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	}

	for _, suite := range validSuites {
		if !ValidateTLSCipherSuite(suite) {
			t.Errorf("cipher suite %d should be FIPS-approved", suite)
		}
	}

	// Invalid cipher suite
	if ValidateTLSCipherSuite(0x0000) {
		t.Error("cipher suite 0x0000 should not be FIPS-approved")
	}
}

func TestValidateCertificate(t *testing.T) {
	// Save original mode
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeDisabled)

	// Create a self-signed certificate
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"test"},
	}

	// Test with RSA 2048 (valid)
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	certDER, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(certDER)

	// Should pass when FIPS disabled
	if err := ValidateCertificate(cert); err != nil {
		t.Errorf("ValidateCertificate should not error when FIPS disabled: %v", err)
	}

	// Should pass when FIPS enabled (RSA 2048 is valid)
	SetMode(ModeEnabled)
	if err := ValidateCertificate(cert); err != nil {
		t.Errorf("ValidateCertificate should not error for RSA 2048: %v", err)
	}
}

func TestIsAlgorithmAllowed(t *testing.T) {
	// Save original mode
	origMode := GetMode()
	defer SetMode(origMode)

	// All algorithms allowed when FIPS disabled
	SetMode(ModeDisabled)
	if !IsAlgorithmAllowed("Ed25519") {
		t.Error("Ed25519 should be allowed when FIPS disabled")
	}

	// Only approved algorithms when FIPS enabled
	SetMode(ModeEnabled)
	if IsAlgorithmAllowed("Ed25519") {
		t.Error("Ed25519 should not be allowed in FIPS mode")
	}
	if !IsAlgorithmAllowed("AES-256-GCM") {
		t.Error("AES-256-GCM should be allowed in FIPS mode")
	}
}

func TestSecureRandom(t *testing.T) {
	buf, err := SecureRandom(32)
	if err != nil {
		t.Errorf("SecureRandom failed: %v", err)
	}
	if len(buf) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(buf))
	}

	// Check that it's actually random (very basic check)
	buf2, _ := SecureRandom(32)
	if string(buf) == string(buf2) {
		t.Error("SecureRandom should produce different values")
	}
}

func TestFIPSCipherSuites(t *testing.T) {
	suites := FIPSCipherSuites()
	if len(suites) == 0 {
		t.Error("FIPSCipherSuites should return non-empty list")
	}
}

func TestNonFIPSFeatures(t *testing.T) {
	features := NonFIPSFeatures()
	if len(features) == 0 {
		t.Error("NonFIPSFeatures should return non-empty list")
	}
}
