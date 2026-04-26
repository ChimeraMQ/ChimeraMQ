package fips

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"math/big"
	"testing"
)

func TestNewSHA256(t *testing.T) {
	h := NewSHA256()
	if h == nil {
		t.Fatal("NewSHA256 should not return nil")
	}
	h.Write([]byte("hello"))
	if len(h.Sum(nil)) != 32 {
		t.Errorf("expected 32 bytes, got %d", len(h.Sum(nil)))
	}
}

func TestNewSHA384(t *testing.T) {
	h := NewSHA384()
	if h == nil {
		t.Fatal("NewSHA384 should not return nil")
	}
	h.Write([]byte("hello"))
	if len(h.Sum(nil)) != 48 {
		t.Errorf("expected 48 bytes, got %d", len(h.Sum(nil)))
	}
}

func TestNewSHA512(t *testing.T) {
	h := NewSHA512()
	if h == nil {
		t.Fatal("NewSHA512 should not return nil")
	}
	h.Write([]byte("hello"))
	if len(h.Sum(nil)) != 64 {
		t.Errorf("expected 64 bytes, got %d", len(h.Sum(nil)))
	}
}

func TestFIPSApprovedCurves(t *testing.T) {
	curves := FIPSApprovedCurves()
	if len(curves) != 3 {
		t.Fatalf("expected 3 curves, got %d", len(curves))
	}
	found := map[string]bool{}
	for _, c := range curves {
		switch c {
		case elliptic.P256(), elliptic.P384(), elliptic.P521():
			found[c.Params().Name] = true
		}
	}
	if len(found) != 3 {
		t.Errorf("expected P-256, P-384, P-521, got: %v", found)
	}
}

func TestGenerateECDSAKey(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeDisabled)
	key, err := GenerateECDSAKey(elliptic.P256())
	if err != nil {
		t.Fatalf("GenerateECDSAKey failed: %v", err)
	}
	if key == nil {
		t.Fatal("key should not be nil")
	}

	SetMode(ModeEnabled)
	_, err = GenerateECDSAKey(elliptic.P224())
	if err == nil {
		t.Error("expected error for non-FIPS curve in FIPS mode")
	}
}

func TestGenerateEd25519Key(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeDisabled)
	key, err := GenerateEd25519Key()
	if err != nil {
		t.Fatalf("GenerateEd25519Key failed: %v", err)
	}
	if key == nil {
		t.Fatal("key should not be nil")
	}

	SetMode(ModeEnabled)
	_, err = GenerateEd25519Key()
	if err == nil {
		t.Error("expected error for Ed25519 in FIPS mode")
	}
}

func TestNewAESCTR(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeDisabled)
	key := make([]byte, 16)
	iv := make([]byte, 16)
	stream, err := NewAESCTR(key, iv)
	if err != nil {
		t.Fatalf("NewAESCTR failed: %v", err)
	}
	if stream == nil {
		t.Fatal("stream should not be nil")
	}

	SetMode(ModeEnabled)
	_, err = NewAESCTR(make([]byte, 8), iv)
	if err == nil {
		t.Error("expected error for invalid key size in FIPS mode")
	}
}

func TestComplianceError(t *testing.T) {
	// Just ensure it doesn't panic and returns either nil or an error
	_ = ComplianceError()
}

func TestValidateGoRuntimeForceFlag(t *testing.T) {
	// Set the force flag to skip validation
	t.Setenv("CHIMERA_FIPS_FORCE", "1")

	err := validateGoRuntime()
	if err != nil {
		t.Errorf("CHIMERA_FIPS_FORCE=1 should skip validation, got: %v", err)
	}
}

func TestValidateGoRuntimeNoForce(t *testing.T) {
	// Without force flag, validation should fail on non-Linux or systems
	// without /proc/sys/crypto/fips_enabled
	t.Setenv("CHIMERA_FIPS_FORCE", "0")

	err := validateGoRuntime()
	if err == nil {
		t.Log("validation passed (may be expected on some systems)")
	} else {
		t.Logf("validation failed (expected on most dev systems): %v", err)
	}
}

func TestValidateCertificateECDSANonApprovedCurve(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeEnabled)

	// Create a self-signed cert with P224 (non-approved)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		PublicKeyAlgorithm:    x509.ECDSA,
		SignatureAlgorithm:    x509.ECDSAWithSHA256,
	}
	priv, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	if err != nil {
		t.Skipf("P224 not available: %v", err)
	}
	template.PublicKey = &priv.PublicKey

	cert, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	parsed, err := x509.ParseCertificate(cert)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}

	err = ValidateCertificate(parsed)
	if err == nil {
		t.Error("expected error for non-approved ECDSA curve")
	}
}

func TestValidateCertificateUnknownPublicKeyType(t *testing.T) {
	origMode := GetMode()
	defer SetMode(origMode)

	SetMode(ModeEnabled)

	// Generate an ECDSA cert, then manually replace the public key
	// with an unsupported type to exercise the default case.
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber:       big.NewInt(2),
		PublicKeyAlgorithm: x509.ECDSA,
		SignatureAlgorithm: x509.ECDSAWithSHA256,
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(certBytes)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}

	// Manually replace the public key with an unsupported type
	parsed.PublicKey = struct{}{}

	err = ValidateCertificate(parsed)
	if err == nil {
		t.Error("expected error for non-approved public key type")
	}
}

func TestSecureRandomReturnsCorrectSize(t *testing.T) {
	for _, size := range []int{1, 16, 32, 256} {
		buf, err := SecureRandom(size)
		if err != nil {
			t.Errorf("SecureRandom(%d): %v", size, err)
		}
		if len(buf) != size {
			t.Errorf("SecureRandom(%d) len = %d, want %d", size, len(buf), size)
		}
	}
}
