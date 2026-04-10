package encrypt

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewEncryptorInvalidKeyPath(t *testing.T) {
	_, err := NewEncryptor(filepath.Join(t.TempDir(), "nonexistent.key"))
	if err == nil {
		t.Error("should fail for nonexistent key file")
	}
}

func TestNewEncryptorKeyTooShort(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "short.key")
	if err := os.WriteFile(keyPath, []byte("tooshort"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := NewEncryptor(keyPath)
	if err == nil {
		t.Error("should fail for key shorter than 32 bytes")
	}
}

func TestNewEncryptorKeyTooLong(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "long.key")
	if err := os.WriteFile(keyPath, make([]byte, 64), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := NewEncryptor(keyPath)
	if err == nil {
		t.Error("should fail for key longer than 32 bytes")
	}
}

func TestGenerateKeyFileOverwrite(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")

	if err := GenerateKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}
	data1, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	// Overwrite with a new key
	if err := GenerateKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}
	data2, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	// Keys should be different (extremely unlikely to match)
	if string(data1) == string(data2) {
		t.Error("regenerated key should differ (statistically)")
	}
}

func TestGenerateKeyFileInvalidPath(t *testing.T) {
	err := GenerateKeyFile("/nonexistent/directory/deep/key")
	if err == nil {
		t.Error("should fail for nonexistent directory")
	}
}

func TestDecryptTruncatedCiphertext(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, err := NewEncryptor(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	// Too short: less than 4+12+16 = 32 bytes
	_, err = enc.Decrypt([]byte("short"), "seg-1")
	if err == nil {
		t.Error("should fail for truncated ciphertext")
	}
}

func TestDecryptExactlyMinimumLength(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, err := NewEncryptor(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	// 32 bytes is the minimum: 4 (keyID) + 12 (nonce) + 0 (ciphertext) + 16 (tag)
	// but these random bytes won't decrypt, so we expect an auth error
	minCiphertext := make([]byte, 32)
	_, err = enc.Decrypt(minCiphertext, "seg-1")
	if err == nil {
		t.Error("should fail for random bytes at minimum length")
	}
}

func TestEncryptDecryptVariousSizes(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, err := NewEncryptor(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	sizes := []int{1, 15, 16, 17, 255, 256, 4095, 4096, 8192}
	for _, size := range sizes {
		plaintext := make([]byte, size)
		for i := range plaintext {
			plaintext[i] = byte(i % 256)
		}
		segID := "seg-" + filepath.Base(t.Name())

		ciphertext, err := enc.Encrypt(plaintext, segID)
		if err != nil {
			t.Errorf("Encrypt size %d: %v", size, err)
			continue
		}

		// Ciphertext must be longer than plaintext (overhead: 4+12+16=32)
		if len(ciphertext) != len(plaintext)+32 {
			t.Errorf("ciphertext len = %d, want %d (plaintext %d + 32 overhead)", len(ciphertext), len(plaintext)+32, plaintext)
		}

		decrypted, err := enc.Decrypt(ciphertext, segID)
		if err != nil {
			t.Errorf("Decrypt size %d: %v", size, err)
			continue
		}

		if string(decrypted) != string(plaintext) {
			t.Errorf("size %d: decrypted mismatch", size)
		}
	}
}

func TestEncryptProducesDifferentCiphertexts(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, err := NewEncryptor(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("same input data")
	ct1, _ := enc.Encrypt(plaintext, "seg-1")
	ct2, _ := enc.Encrypt(plaintext, "seg-1")

	// Different nonces should produce different ciphertexts
	if string(ct1) == string(ct2) {
		t.Error("two encryptions of same plaintext should differ (random nonce)")
	}
}

func TestRotateKeyInvalidPath(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, err := NewEncryptor(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	err = enc.RotateKey(filepath.Join(t.TempDir(), "missing.key"))
	if err == nil {
		t.Error("should fail for nonexistent key file on rotation")
	}
}

func TestRotateKeyWrongSize(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, err := NewEncryptor(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	badKeyPath := filepath.Join(dir, "bad.key")
	os.WriteFile(badKeyPath, []byte("only16bytes_1234"), 0600)

	err = enc.RotateKey(badKeyPath)
	if err == nil {
		t.Error("should fail for wrong-sized key file on rotation")
	}
}

func TestRotateKeyMultipleRotations(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, err := NewEncryptor(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		GenerateKeyFile(keyPath)
		if err := enc.RotateKey(keyPath); err != nil {
			t.Fatalf("rotation %d: %v", i, err)
		}
		if enc.KeyID() != uint32(i) {
			t.Errorf("after rotation %d, KeyID = %d, want %d", i, enc.KeyID(), i)
		}
	}

	// Encrypt/decrypt should still work after multiple rotations
	ct, err := enc.Encrypt([]byte("post-rotation"), "seg-final")
	if err != nil {
		t.Fatal(err)
	}
	dec, err := enc.Decrypt(ct, "seg-final")
	if err != nil {
		t.Fatal(err)
	}
	if string(dec) != "post-rotation" {
		t.Errorf("got %q, want %q", dec, "post-rotation")
	}
}

func TestKeyIDStartsAtZero(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, _ := NewEncryptor(keyPath)
	if enc.KeyID() != 0 {
		t.Errorf("initial KeyID = %d, want 0", enc.KeyID())
	}
}

func TestEncryptDecryptCiphertextFormat(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, _ := NewEncryptor(keyPath)

	plaintext := []byte("check the format")
	ct, err := enc.Encrypt(plaintext, "seg-fmt")
	if err != nil {
		t.Fatal(err)
	}

	// Verify format: [KeyID:4][Nonce:12][Ciphertext:N][Tag:16]
	// Total length = 4 + 12 + len(plaintext) + 16 = 32 + len(plaintext)
	expectedLen := 4 + 12 + len(plaintext) + 16
	if len(ct) != expectedLen {
		t.Errorf("ciphertext length = %d, want %d", len(ct), expectedLen)
	}

	// First 4 bytes should be KeyID = 0
	if ct[0] != 0 || ct[1] != 0 || ct[2] != 0 || ct[3] != 0 {
		t.Errorf("first 4 bytes (KeyID) = %v, want [0 0 0 0]", ct[:4])
	}
}

func TestEncryptDecryptAfterRotationOldCiphertextFails(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, _ := NewEncryptor(keyPath)

	// Encrypt with the original key
	oldCT, _ := enc.Encrypt([]byte("old secret"), "seg-old")

	// Rotate key
	GenerateKeyFile(keyPath)
	if err := enc.RotateKey(keyPath); err != nil {
		t.Fatal(err)
	}

	// Old ciphertext should fail to decrypt with new key
	_, err := enc.Decrypt(oldCT, "seg-old")
	if err == nil {
		t.Error("old ciphertext should fail to decrypt after key rotation")
	}
}

func TestEncryptDecryptWithEmptySegmentID(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, _ := NewEncryptor(keyPath)

	plaintext := []byte("data with empty AAD")
	ct, err := enc.Encrypt(plaintext, "")
	if err != nil {
		t.Fatal(err)
	}

	dec, err := enc.Decrypt(ct, "")
	if err != nil {
		t.Fatal(err)
	}
	if string(dec) != string(plaintext) {
		t.Errorf("got %q, want %q", dec, plaintext)
	}

	// Should fail with a non-empty segment ID (different AAD)
	_, err = enc.Decrypt(ct, "seg-1")
	if err == nil {
		t.Error("should fail with different AAD")
	}
}
