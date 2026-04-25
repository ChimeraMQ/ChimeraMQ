package mqtt_test

import (
	"bytes"
	"testing"

	"github.com/chimeramq/chimera/internal/protocol/mqtt"
)

// FuzzReadPacket verifies that the MQTT packet reader does not crash on
// arbitrary input. It feeds random bytes through a bytes.Reader and checks
// that the decoder either succeeds or returns an error — never panics.
func FuzzReadPacket(f *testing.F) {
	// Seed with a minimal valid CONNECT packet (fixed header + minimal remaining)
	// 0x10 = CONNECT packet, type=1, flags=0
	// Remaining length: 14 (minimal CONNECT with protocol name "MQTT")
	validConnect := []byte{
		0x10,       // Type=1 (CONNECT), Flags=0
		0x14,       // Remaining length: 20
		0x00, 0x04, // Protocol name length
		'M', 'Q', 'T', 'T',
		0x04,       // Protocol level (MQTT 3.1.1)
		0x02,       // Connect flags (Clean Session)
		0x00, 0x3c, // Keepalive (60s)
		0x00, 0x08, // Client ID length
		'f', 'u', 'z', 'z', '-', 'c', 'l', 'i',
	}
	f.Add(validConnect)

	// Known-bad inputs
	f.Add([]byte{})
	f.Add([]byte{0x10})                               // just type byte, no remaining length
	f.Add([]byte{0x10, 0xFF, 0xFF})                   // truncated remaining length encoding
	f.Add([]byte{0x10, 0x80, 0x80, 0x80, 0x80, 0x01}) // multi-byte remaining
	f.Add(bytes.Repeat([]byte{0x00}, 64))
	f.Add(bytes.Repeat([]byte{0xFF}, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_, _ = mqtt.ReadPacket(r)
	})
}

// FuzzParseConnect verifies that MQTT CONNECT payload parsing handles arbitrary
// bytes safely — no panics on malformed CONNECT payloads.
func FuzzParseConnect(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x04, 'M', 'Q', 'T', 'T'}) // just protocol name
	f.Add(bytes.Repeat([]byte{0xAB}, 128))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = mqtt.ParseConnect(data)
	})
}
