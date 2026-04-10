package encrypt

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	if err := GenerateKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}

	enc, err := NewEncryptor(keyPath)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("hello chimera, this is a secret message")
	ciphertext, err := enc.Encrypt(plaintext, "seg-001")
	if err != nil {
		t.Fatal(err)
	}

	// Ciphertext should be different from plaintext
	if string(ciphertext) == string(plaintext) {
		t.Error("ciphertext should not match plaintext")
	}

	// Decrypt
	decrypted, err := enc.Decrypt(ciphertext, "seg-001")
	if err != nil {
		t.Fatal(err)
	}
	if string(decrypted) != string(plaintext) {
		t.Errorf("decrypted = %q, want %q", decrypted, plaintext)
	}
}

func TestEncryptTamperDetection(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, _ := NewEncryptor(keyPath)
	ciphertext, _ := enc.Encrypt([]byte("secret"), "seg-1")

	// Tamper with ciphertext
	ciphertext[len(ciphertext)-2] ^= 0xFF

	_, err := enc.Decrypt(ciphertext, "seg-1")
	if err == nil {
		t.Error("should fail on tampered ciphertext")
	}
}

func TestEncryptWrongAAD(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, _ := NewEncryptor(keyPath)
	ciphertext, _ := enc.Encrypt([]byte("secret"), "seg-1")

	_, err := enc.Decrypt(ciphertext, "seg-2")
	if err == nil {
		t.Error("should fail with wrong segment ID (AAD)")
	}
}

func TestEncryptEmptyPlaintext(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, _ := NewEncryptor(keyPath)
	ciphertext, err := enc.Encrypt([]byte{}, "seg-empty")
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := enc.Decrypt(ciphertext, "seg-empty")
	if err != nil {
		t.Fatal(err)
	}
	if len(decrypted) != 0 {
		t.Errorf("decrypted len = %d, want 0", len(decrypted))
	}
}

func TestEncryptLargeData(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "test.key")
	GenerateKeyFile(keyPath)

	enc, _ := NewEncryptor(keyPath)
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	ciphertext, err := enc.Encrypt(largeData, "seg-large")
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := enc.Decrypt(ciphertext, "seg-large")
	if err != nil {
		t.Fatal(err)
	}
	if len(decrypted) != len(largeData) {
		t.Errorf("decrypted len = %d, want %d", len(decrypted), len(largeData))
	}
	for i := range largeData {
		if decrypted[i] != largeData[i] {
			t.Errorf("mismatch at byte %d", i)
			break
		}
	}
}

func TestGenerateKeyFile(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")

	if err := GenerateKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 32 {
		t.Errorf("key length = %d, want 32", len(data))
	}
}

func TestEncryptKeyRotation(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key")
	GenerateKeyFile(keyPath)

	enc, _ := NewEncryptor(keyPath)
	if enc.KeyID() != 0 {
		t.Errorf("initial KeyID = %d, want 0", enc.KeyID())
	}

	enc.Encrypt([]byte("first"), "seg-1")

	// Generate a new key
	GenerateKeyFile(keyPath)
	enc.RotateKey(keyPath)

	if enc.KeyID() != 1 {
		t.Errorf("KeyID after rotate = %d, want 1", enc.KeyID())
	}

	// New encryption should work
	ct, err := enc.Encrypt([]byte("second"), "seg-2")
	if err != nil {
		t.Fatal(err)
	}
	dec, err := enc.Decrypt(ct, "seg-2")
	if err != nil || string(dec) != "second" {
		t.Errorf("decrypt after rotation failed: %v", err)
	}
}
