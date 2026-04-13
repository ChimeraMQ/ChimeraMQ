package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/chimeramq/chimera/internal/fips"
)

var (
	errBadCiphertext = errors.New("invalid ciphertext")
	errAuthFailed    = errors.New("decryption failed: authentication error")
	errWeakKey       = errors.New("encryption key is too weak")
)

// weakKeyPatterns contains patterns that indicate weak keys
var weakKeyPatterns = [][]byte{
	// All zeros
	{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
	// All ones
	{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
	// Alternating pattern
	{0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA},
	{0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55},
	// Sequential pattern
	{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
	{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07},
}

// Encryptor provides AES-256-GCM encryption for storage segments.
type Encryptor struct {
	mu     sync.Mutex
	key    [32]byte
	keyID  uint32
	keyLen int
}

// NewEncryptor creates an encryptor from a key file (32 bytes of raw key data).
func NewEncryptor(keyPath string) (*Encryptor, error) {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	if len(data) != 32 {
		return nil, errors.New("encryption key must be exactly 32 bytes")
	}
	// Validate key strength
	if err := validateKey(data); err != nil {
		return nil, fmt.Errorf("weak encryption key: %w", err)
	}
	// Validate FIPS compliance
	if fips.IsEnabled() && len(data) != 32 {
		return nil, errors.New("FIPS mode requires AES-256 (32 byte key)")
	}
	enc := &Encryptor{keyLen: 32}
	copy(enc.key[:], data)
	return enc, nil
}

// GenerateKeyFile creates a new random 32-byte key file.
func GenerateKeyFile(path string) error {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return err
	}
	return os.WriteFile(path, key, 0600)
}

// validateKey checks if the key is strong enough for production use.
func validateKey(key []byte) error {
	if len(key) != 32 {
		return errors.New("key must be exactly 32 bytes")
	}

	// Check for weak patterns
	for _, pattern := range weakKeyPatterns {
		if containsPattern(key, pattern) {
			return errWeakKey
		}
	}

	// Check for all identical bytes
	allSame := true
	for i := 1; i < len(key); i++ {
		if key[i] != key[0] {
			allSame = false
			break
		}
	}
	if allSame {
		return errWeakKey
	}

	// Check entropy (at least 16 unique bytes)
	uniqueBytes := make(map[byte]struct{})
	for _, b := range key {
		uniqueBytes[b] = struct{}{}
	}
	if len(uniqueBytes) < 16 {
		return errWeakKey
	}

	return nil
}

func containsPattern(data, pattern []byte) bool {
	if len(pattern) > len(data) {
		return false
	}
	for i := 0; i <= len(data)-len(pattern); i++ {
		match := true
		for j := 0; j < len(pattern); j++ {
			if data[i+j] != pattern[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// Encrypt encrypts plaintext with AES-256-GCM.
// segmentID is used as Additional Authenticated Data (AAD).
// Output format: [KeyID:4][Nonce:12][Ciphertext:var][Tag:16]
func (enc *Encryptor) Encrypt(plaintext []byte, segmentID string) ([]byte, error) {
	enc.mu.Lock()
	defer enc.mu.Unlock()

	block, err := aes.NewCipher(enc.key[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Allocate output: 4 (keyID) + 12 (nonce) + len(ciphertext) + 16 (tag)
	out := make([]byte, 4+len(nonce))
	binary.BigEndian.PutUint32(out[0:4], enc.keyID)
	copy(out[4:], nonce)

	// Seal appends ciphertext+tag to the nonce prefix
	aad := []byte(segmentID)
	out = gcm.Seal(out, nonce, plaintext, aad)

	return out, nil
}

// Decrypt decrypts data encrypted with Encrypt.
func (enc *Encryptor) Decrypt(ciphertext []byte, segmentID string) ([]byte, error) {
	if len(ciphertext) < 4+12+16 { // keyID + nonce + tag minimum
		return nil, errBadCiphertext
	}

	block, err := aes.NewCipher(enc.key[:])
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := ciphertext[4 : 4+gcm.NonceSize()]
	data := ciphertext[4:]

	aad := []byte(segmentID)
	plaintext, err := gcm.Open(nil, nonce, data[len(nonce):], aad)
	if err != nil {
		return nil, errAuthFailed
	}

	return plaintext, nil
}

// KeyID returns the current key ID.
func (enc *Encryptor) KeyID() uint32 {
	enc.mu.Lock()
	defer enc.mu.Unlock()
	return enc.keyID
}

// RotateKey loads a new key from the key file and increments the key ID.
func (enc *Encryptor) RotateKey(keyPath string) error {
	enc.mu.Lock()
	defer enc.mu.Unlock()

	data, err := os.ReadFile(keyPath)
	if err != nil {
		return err
	}
	if len(data) != 32 {
		return errors.New("encryption key must be exactly 32 bytes")
	}
	// Validate key strength
	if err := validateKey(data); err != nil {
		return fmt.Errorf("weak encryption key: %w", err)
	}
	copy(enc.key[:], data)
	enc.keyID++
	return nil
}
