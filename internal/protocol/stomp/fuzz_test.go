package stomp_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/chimeramq/chimera/internal/protocol/stomp"
)

// FuzzReadFrame verifies that the STOMP frame reader does not crash on
// arbitrary input. It feeds random bytes through a buffered reader and
// checks that the decoder either succeeds or returns an error.
func FuzzReadFrame(f *testing.F) {
	// Seed with a valid SEND frame
	validFrame := "SEND\ndestination:test\n\nhello\x00"
	f.Add([]byte(validFrame))

	// Seed with other valid frames
	f.Add([]byte("CONNECT\naccept-version:1.2\n\n\x00"))
	f.Add([]byte("SUBSCRIBE\ndestination:test\nid:1\n\n\x00"))

	// Known-bad inputs
	f.Add([]byte{})
	f.Add([]byte("NO_NEWLINE"))             // no newline, will EOF
	f.Add([]byte("\n\n\n"))                 // heartbeats then EOF
	f.Add([]byte("CMD\nheader\n\x00"))      // missing header colon
	f.Add(bytes.Repeat([]byte{0x41}, 512))  // long command, no NULL
	f.Add(bytes.Repeat([]byte{0x01}, 1024)) // arbitrary binary

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bufio.NewReader(bytes.NewReader(data))
		_, _ = stomp.ReadFrame(r)
	})
}
