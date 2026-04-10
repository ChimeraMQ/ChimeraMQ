package message

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

// UUIDv7Generator generates time-sortable UUIDs per RFC 9562.
type UUIDv7Generator struct {
	mu      sync.Mutex
	lastMS  int64
	counter uint16
}

var defaultGenerator = &UUIDv7Generator{}

// NewUUIDv7 generates a new UUIDv7 using the package-level generator.
func NewUUIDv7() [16]byte {
	return defaultGenerator.Generate()
}

// Generate creates a new UUIDv7.
// Layout: 48-bit ms timestamp | ver=7 | 12-bit counter | var=10 | 62-bit random
func (g *UUIDv7Generator) Generate() [16]byte {
	g.mu.Lock()
	defer g.mu.Unlock()

	var uuid [16]byte

	now := time.Now().UnixMilli()

	if now == g.lastMS {
		g.counter++
	} else {
		g.lastMS = now
		g.counter = 0
	}

	// Bytes 0-5: 48-bit timestamp (milliseconds)
	binary.BigEndian.PutUint16(uuid[0:], uint16(now>>32))
	binary.BigEndian.PutUint32(uuid[2:], uint32(now))

	// Bytes 6-7: version (7) + 12-bit counter
	binary.BigEndian.PutUint16(uuid[6:], g.counter)
	uuid[6] = (uuid[6] & 0x0F) | 0x70 // Version 7

	// Bytes 8-15: variant (10) + 62-bit random
	_, _ = rand.Read(uuid[8:])
	uuid[8] = (uuid[8] & 0x3F) | 0x80 // Variant 10

	return uuid
}

// UUIDString formats a UUID as a standard hex string.
func UUIDString(uuid [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}
