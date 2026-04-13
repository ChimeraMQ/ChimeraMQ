package fips

import (
	"crypto/elliptic"
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
