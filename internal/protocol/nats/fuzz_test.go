package nats_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/chimeramq/chimera/internal/protocol/nats"
)

// FuzzReadMessage verifies that the NATS message reader does not crash on
// arbitrary input. It feeds random bytes through a buffered reader and
// checks that the decoder either succeeds or returns an error — never panics.
func FuzzReadMessage(f *testing.F) {
	// Seed with valid CONNECT command
	f.Add([]byte("CONNECT {\"name\":\"test\"}\r\n"))

	// Seed with valid PUB command (no payload)
	f.Add([]byte("PUB test.subject\r\n"))

	// Seed with valid PUB command with payload length 0
	f.Add([]byte("PUB test.subject 0\r\n\r\n"))

	// Seed with valid SUB command
	f.Add([]byte("SUB test.subject 1\r\n"))

	// Seed with valid PING command
	f.Add([]byte("PING\r\n"))

	// Seed with valid PONG command
	f.Add([]byte("PONG\r\n"))

	// Seed with valid UNSUB command
	f.Add([]byte("UNSUB 1\r\n"))

	// Known-bad inputs
	f.Add([]byte{})
	f.Add([]byte("NO_NEWLINE"))                  // no trailing CRLF, will EOF
	f.Add([]byte("PUB"))                         // incomplete command
	f.Add([]byte("PUB test.subject -1\r\n"))     // negative payload length
	f.Add([]byte("PUB test.subject 999999\r\n")) // payload length exceeds data
	f.Add(bytes.Repeat([]byte{0x00}, 64))
	f.Add(bytes.Repeat([]byte{0xFF}, 128))
	f.Add([]byte("\r\n\r\n\r\n"))                // empty lines, then EOF
	f.Add([]byte("PUB a b c d e f 10\r\nshort")) // truncated payload

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bufio.NewReader(bytes.NewReader(data))
		_, _ = nats.ReadMessage(r)
	})
}

// FuzzIsClientCommand verifies that IsClientCommand handles arbitrary
// byte inputs safely.
func FuzzIsClientCommand(f *testing.F) {
	f.Add([]byte("CONNECT"))
	f.Add([]byte("PUB"))
	f.Add([]byte("SUB"))
	f.Add([]byte("UNSUB"))
	f.Add([]byte("PING"))
	f.Add([]byte("PONG"))
	f.Add([]byte("UNKNOWN"))
	f.Add([]byte(""))
	f.Add([]byte("connect")) // case-sensitive
	f.Add([]byte("PUBLISH")) // similar but not exact

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = nats.IsClientCommand(nats.Command(data))
	})
}

// FuzzIsServerCommand verifies that IsServerCommand handles arbitrary
// byte inputs safely.
func FuzzIsServerCommand(f *testing.F) {
	f.Add([]byte("INFO"))
	f.Add([]byte("MSG"))
	f.Add([]byte("+OK"))
	f.Add([]byte("-ERR"))
	f.Add([]byte("PING"))
	f.Add([]byte("PONG"))
	f.Add([]byte("UNKNOWN"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = nats.IsServerCommand(nats.Command(data))
	})
}
