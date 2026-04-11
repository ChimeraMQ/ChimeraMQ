package gossip

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	hmacSize = 32 // SHA-256 HMAC size
)

// HMACTransport wraps UDPTransport with HMAC-SHA256 message authentication.
// Wire format: [4-byte total length][32-byte HMAC][JSON payload]
type HMACTransport struct {
	inner *UDPTransport
	mu    sync.RWMutex
	keys  [][]byte // shared secret keys (supports key rotation)
}

// NewHMACTransport wraps a UDPTransport with HMAC authentication.
func NewHMACTransport(inner *UDPTransport, secret []byte) *HMACTransport {
	return &HMACTransport{
		inner: inner,
		keys:  [][]byte{copyKey(secret)},
	}
}

// AddKey adds a new HMAC key (for key rotation). The most recent key is used for signing.
func (t *HMACTransport) AddKey(secret []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.keys = append(t.keys, copyKey(secret))
}

// RemoveKey removes a key by index.
func (t *HMACTransport) RemoveKey(index int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if index >= 0 && index < len(t.keys) {
		t.keys = append(t.keys[:index], t.keys[index+1:]...)
	}
}

// Send sends an HMAC-authenticated message.
func (t *HMACTransport) Send(addr string, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	t.mu.RLock()
	key := t.keys[len(t.keys)-1] // Use latest key for signing
	t.mu.RUnlock()

	mac := computeHMAC(key, data)

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}

	// Wire: [4-byte length of (hmac+payload)][hmac][payload]
	totalPayload := len(mac) + len(data)
	buf := make([]byte, 4+totalPayload)
	binary.BigEndian.PutUint32(buf[:4], uint32(totalPayload))
	copy(buf[4:4+hmacSize], mac)
	copy(buf[4+hmacSize:], data)

	_ = t.inner.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err = t.inner.conn.WriteToUDP(buf, udpAddr)
	return err
}

// Receive receives and verifies an HMAC-authenticated message.
func (t *HMACTransport) Receive() (*Message, *net.UDPAddr, error) {
	buf := make([]byte, 64*1024)
	_ = t.inner.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	n, addr, err := t.inner.conn.ReadFromUDP(buf)
	if err != nil {
		return nil, nil, err
	}
	if n < 4+hmacSize {
		return nil, nil, nil
	}

	totalLength := int(binary.BigEndian.Uint32(buf[:4]))
	if totalLength > n-4 || totalLength < hmacSize {
		return nil, nil, nil
	}

	receivedMAC := buf[4 : 4+hmacSize]
	payload := buf[4+hmacSize : 4+totalLength]

	// Verify HMAC against all known keys
	t.mu.RLock()
	valid := false
	for _, key := range t.keys {
		expectedMAC := computeHMAC(key, payload)
		if hmac.Equal(receivedMAC, expectedMAC) {
			valid = true
			break
		}
	}
	t.mu.RUnlock()

	if !valid {
		return nil, nil, fmt.Errorf("gossip: HMAC verification failed")
	}

	var msg Message
	if err := json.Unmarshal(payload, &msg); err != nil {
		return nil, nil, err
	}
	return &msg, addr, nil
}

// Close closes the transport.
func (t *HMACTransport) Close() error {
	return t.inner.Close()
}

// LocalAddr returns the local address.
func (t *HMACTransport) LocalAddr() string {
	return t.inner.LocalAddr()
}

// GenerateHMACKey generates a random 32-byte key for HMAC authentication.
func GenerateHMACKey() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate HMAC key: %w", err)
	}
	return key, nil
}

func computeHMAC(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func copyKey(key []byte) []byte {
	cp := make([]byte, len(key))
	copy(cp, key)
	return cp
}
