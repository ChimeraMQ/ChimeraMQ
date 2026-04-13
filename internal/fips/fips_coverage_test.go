package fips

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"math/big"
	"os"
	"testing"
	"time"
)

func TestValidateGoRuntimeForceEnv(t *testing.T) {
	os.Setenv("CHIMERA_FIPS_FORCE", "1")
	defer os.Unsetenv("CHIMERA_FIPS_FORCE")

	if err := validateGoRuntime(); err != nil {
		t.Errorf("expected nil with CHIMERA_FIPS_FORCE=1, got %v", err)
	}
}

func TestValidateGoRuntimeNoFIPS(t *testing.T) {
	os.Unsetenv("CHIMERA_FIPS_FORCE")

	err := validateGoRuntime()
	if err == nil {
		t.Error("expected error when system is not in FIPS mode")
	}
}

func TestInitializeStrictModeError(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)
	os.Unsetenv("CHIMERA_FIPS_FORCE")

	cfg := Config{Enabled: true, StrictMode: true}
	err := Initialize(cfg)
	if err == nil {
		t.Error("expected strict mode initialization to fail when not in FIPS mode")
	}
}

func TestInitializeComplianceWarning(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)
	os.Unsetenv("CHIMERA_FIPS_FORCE")

	cfg := Config{Enabled: true, StrictMode: false}
	err := Initialize(cfg)
	if err != nil {
		t.Errorf("expected non-strict initialization to succeed with warning, got %v", err)
	}
	if ComplianceError() == nil {
		t.Error("expected complianceError to be set")
	}
}

func TestSecureTLSConfigFIPSEnabled(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeEnabled)
	cfg := SecureTLSConfig()
	if cfg == nil {
		t.Fatal("SecureTLSConfig should not return nil")
	}
	if len(cfg.CurvePreferences) == 0 {
		t.Error("expected CurvePreferences to be set in FIPS mode")
	}
}

func TestNewAESCTRInvalidKeyNonFIPS(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeDisabled)
	_, err := NewAESCTR(make([]byte, 8), make([]byte, 16))
	if err == nil {
		t.Error("expected error for invalid key size")
	}
}

func TestValidateCertificateBadSignature(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeEnabled)

	// Create RSA 2048 cert with MD5 signature (non-FIPS)
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"test"},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(certDER)

	// Force a non-FIPS signature algorithm by modifying the parsed cert
	// Since x509.CreateCertificate won't allow MD5, we test with the actual
	// signature algorithm that was generated (SHA256WithRSA which is valid).
	// To test the rejection path, we need a cert with an invalid algorithm.
	// We can parse a pre-generated cert or modify the struct directly.
	cert.SignatureAlgorithm = x509.MD5WithRSA

	err := ValidateCertificate(cert)
	if err == nil {
		t.Error("expected error for MD5 signature algorithm")
	}
}

func TestValidateCertificateSmallRSA(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeEnabled)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"test"},
	}
	key, _ := rsa.GenerateKey(rand.Reader, 1024)
	certDER, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(certDER)

	err := ValidateCertificate(cert)
	if err == nil {
		t.Error("expected error for RSA key < 2048 bits")
	}
}

func TestValidateCertificateBadECDSACurve(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeEnabled)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"test"},
	}
	key, _ := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	certDER, _ := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(certDER)

	err := ValidateCertificate(cert)
	if err == nil {
		t.Error("expected error for non-FIPS ECDSA curve")
	}
}

func TestValidateCertificateBadKeyType(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeEnabled)

	// Create a self-signed Ed25519 certificate
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"test"},
		PublicKey:    priv.Public(),
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, template, template, priv.Public(), priv)
	cert, _ := x509.ParseCertificate(certDER)

	err := ValidateCertificate(cert)
	if err == nil {
		t.Error("expected error for Ed25519 public key")
	}
}
