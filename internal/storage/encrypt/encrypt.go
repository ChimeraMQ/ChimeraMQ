package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

var (
	errBadCiphertext = errors.New("invalid ciphertext")
	errAuthFailed    = errors.New("decryption failed: authentication error")
)

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
	copy(enc.key[:], data)
	enc.keyID++
	return nil
}
