package encrypt

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewEncryptorWeakKeyPatterns(t *testing.T) {
	dir := t.TempDir()

	tests := []struct {
		name string
		key  []byte
	}{
		{"all zeros", make([]byte, 32)},
		{"all ones", bytesRepeat(0xFF, 32)},
		{"alternating AA", bytesRepeat(0xAA, 32)},
		{"alternating 55", bytesRepeat(0x55, 32)},
		{"sequential 01-08", append([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, make([]byte, 24)...)},
		{"sequential 00-07", append([]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, make([]byte, 24)...)},
		{"all same bytes", bytesRepeat(0x42, 32)},
		{"low entropy", bytesRepeat(0xAB, 32)}, // only 1 unique byte
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyPath := filepath.Join(dir, tt.name+".key")
			os.WriteFile(keyPath, tt.key, 0600)
			_, err := NewEncryptor(keyPath)
			if err == nil {
				t.Error("expected weak key rejection")
			}
		})
	}
}

func TestNewEncryptorLowEntropy(t *testing.T) {
	dir := t.TempDir()
	// 15 unique bytes repeated -> should fail entropy check
	key := make([]byte, 32)
	for i := 0; i < 32; i++ {
		key[i] = byte(i % 15)
	}

	keyPath := filepath.Join(dir, "low-entropy.key")
	os.WriteFile(keyPath, key, 0600)
	_, err := NewEncryptor(keyPath)
	if err == nil {
		t.Error("expected weak key rejection for low entropy")
	}
}

func TestContainsPatternLongerThanData(t *testing.T) {
	data := []byte{0x01, 0x02}
	pattern := []byte{0x01, 0x02, 0x03}
	if containsPattern(data, pattern) {
		t.Error("expected false when pattern is longer than data")
	}
}

func TestContainsPatternNoMatch(t *testing.T) {
	data := bytesRepeat(0xAB, 32)
	pattern := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	if containsPattern(data, pattern) {
		t.Error("expected false when pattern is not present")
	}
}

func TestNewEncryptorValidKey(t *testing.T) {
	dir := t.TempDir()
	// Strong key with high entropy
	key := []byte{
		0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
		0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10,
		0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
		0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00,
	}
	keyPath := filepath.Join(dir, "strong.key")
	os.WriteFile(keyPath, key, 0600)

	enc, err := NewEncryptor(keyPath)
	if err != nil {
		t.Fatalf("expected valid key to be accepted: %v", err)
	}
	if enc == nil {
		t.Fatal("encryptor is nil")
	}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
