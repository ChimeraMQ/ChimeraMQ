package amqp_test

import (
	"bytes"
	"testing"

	"github.com/chimeramq/chimera/internal/protocol/amqp"
)

// FuzzReadFrame verifies that the AMQP frame reader does not crash on
// arbitrary input. It feeds random bytes through a bytes.Reader and
// checks that the decoder either succeeds or returns an error — never panics.
func FuzzReadFrame(f *testing.F) {
	// Seed with a valid minimal frame (size=8, doff=2, type=0x00, channel=0, no body)
	validFrame := []byte{
		0x00, 0x00, 0x00, 0x08, // frame size: 8
		0x02,       // data offset: 2 (8 bytes header)
		0x00,       // frame type: AMQP
		0x00, 0x00, // channel: 0
	}
	f.Add(validFrame)

	// Seed with a valid frame containing a small body
	frameWithBody := []byte{
		0x00, 0x00, 0x00, 0x0C, // frame size: 12 (8 header + 4 body)
		0x02,       // data offset: 2
		0x00,       // frame type: AMQP
		0x00, 0x01, // channel: 1
		0x40, 0x40, 0x40, 0x40, // body: 4 null bytes
	}
	f.Add(frameWithBody)

	// Seed with a SASL frame
	saslFrame := []byte{
		0x00, 0x00, 0x00, 0x08, // frame size: 8
		0x02,       // data offset: 2
		0x01,       // frame type: SASL
		0x00, 0x00, // channel: 0
	}
	f.Add(saslFrame)

	// Known-bad inputs
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00})                               // truncated size
	f.Add([]byte{0x00, 0x00, 0x00, 0x07})                   // frame size < minFrameSize
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})                   // huge frame size
	f.Add([]byte{0x00, 0x00, 0x00, 0x08, 0x00})             // truncated header (missing doff/type/channel)
	f.Add([]byte{0x00, 0x00, 0x00, 0x08, 0x01, 0x00, 0x00}) // missing final channel byte
	f.Add(bytes.Repeat([]byte{0x00}, 1024))
	f.Add(bytes.Repeat([]byte{0xFF}, 256))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_, _ = amqp.ReadFrame(r, 64*1024)
	})
}

// FuzzParseDescribedType verifies that the AMQP described type parser handles
// arbitrary bytes safely — no panics on malformed input.
func FuzzParseDescribedType(f *testing.F) {
	// Seed with a valid described type (0x00 marker + ulong descriptor + null value)
	validDescribed := []byte{
		0x00,                                           // described type marker
		0x80,                                           // ulong type
		0x00, 0x00, 0x00, 0x00, 0x60, 0x00, 0x00, 0x01, // descriptor: descOpen
	}
	f.Add(validDescribed)

	// Seed with known-bad inputs
	f.Add([]byte{})
	f.Add([]byte{0x01})       // wrong marker (not 0x00)
	f.Add([]byte{0x00})       // marker but no descriptor
	f.Add([]byte{0x00, 0x80}) // truncated ulong descriptor
	f.Add(bytes.Repeat([]byte{0xAB}, 128))
	f.Add(bytes.Repeat([]byte{0x00}, 64))
	f.Add([]byte{0x00, 0xFF}) // unknown type code after marker

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _, _ = amqp.ParseDescribedType(data)
	})
}

// FuzzReadProtocolHeader verifies that the AMQP protocol header reader
// handles arbitrary input safely.
func FuzzReadProtocolHeader(f *testing.F) {
	// Seed with valid AMQP protocol header
	f.Add([]byte("AMQP\x00\x01\x00\x00"))

	// Known-bad inputs
	f.Add([]byte{})
	f.Add([]byte("AMQP"))                 // truncated
	f.Add([]byte("NOTAMQP\x00\x00"))      // wrong protocol
	f.Add([]byte("amqp\x00\x01\x00\x00")) // wrong case
	f.Add(bytes.Repeat([]byte{0x00}, 8))
	f.Add(bytes.Repeat([]byte{0xFF}, 8))

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		_ = amqp.ReadProtocolHeader(r)
	})
}
