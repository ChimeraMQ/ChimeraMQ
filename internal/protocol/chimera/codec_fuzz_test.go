package chimera

import (
	"bytes"
	"testing"
)

// FuzzDecodeFrame verifies that DecodeFrame does not crash on arbitrary input.
// It feeds random bytes to the decoder through a bytes.Reader and checks that
// it either succeeds (valid CRC + magic) or returns an error — never panics.
func FuzzDecodeFrame(f *testing.F) {
	// Seed with a valid frame
	validFrame, err := EncodeFrame(&Frame{
		Version: FrameVersion,
		OpCode:  OpPing,
		Payload: []byte("hello"),
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(validFrame)

	// Seed with known-bad inputs
	f.Add([]byte{})
	f.Add([]byte("garbage"))
	f.Add([]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06})                     // wrong magic
	f.Add([]byte{'C', 'H', 'M', 'R', 0x01, 0x03, 0x00})                         // right magic, truncated
	f.Add([]byte{'C', 'H', 'M', 'R', 0x01, 0x03, 0x00, 0xFF, 0xFF, 0xFF, 0xFF}) // huge payload len
	f.Add(bytes.Repeat([]byte{0x00}, 1024))

	f.Fuzz(func(t *testing.T, data []byte) {
		// DecodeFrame will reject most random input with errors — we just verify
		// it never panics or causes unbounded allocations.
		r := bytes.NewReader(data)
		_, _ = DecodeFrame(r)
	})
}

// FuzzDecodeConnect verifies that the CONNECT payload decoder handles arbitrary
// bytes safely — no panics on malformed input.
func FuzzDecodeConnect(f *testing.F) {
	// Seed with a manually constructed valid connect payload
	valid := appendUint16(nil, 8) // clientID length
	valid = append(valid, "client-1"...)
	valid = appendUint16(valid, 4) // username length
	valid = append(valid, "user"...)
	valid = appendUint16(valid, 4) // password length
	valid = append(valid, "pass"...)
	valid = appendUint16(valid, 30) // keepalive
	f.Add(valid)
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x01})       // truncated length
	f.Add([]byte{0xFF, 0xFF, 0x00}) // length says 65535 but no data

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = decodeConnect(data)
	})
}

// FuzzDecodePublish verifies that the PUBLISH payload decoder handles arbitrary
// bytes safely.
func FuzzDecodePublish(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x05, 0x74, 0x65, 0x73, 0x74}) // topic="test"
	f.Add([]byte{0xFF, 0xFF, 0x00})
	f.Add(bytes.Repeat([]byte{0xAB}, 256))

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = decodePublish(data)
	})
}

// FuzzDecodeSubscribe verifies that the SUBSCRIBE payload decoder handles
// arbitrary bytes safely.
func FuzzDecodeSubscribe(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x03, 0x61, 0x62, 0x63}) // topic="abc"
	f.Add(bytes.Repeat([]byte{0x00}, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = decodeSubscribe(data)
	})
}

// FuzzDecodeAck verifies that the ACK payload decoder handles arbitrary bytes safely.
func FuzzDecodeAck(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x01, 0x78}) // topic="x"
	f.Add(bytes.Repeat([]byte{0x00}, 32))

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = decodeAck(data)
	})
}

// FuzzDecodeCreateTopic verifies that the CREATE_TOPIC payload decoder handles
// arbitrary bytes safely.
func FuzzDecodeCreateTopic(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x04, 0x74, 0x65, 0x73, 0x74}) // name="test"
	f.Add(bytes.Repeat([]byte{0xCD}, 128))

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = decodeCreateTopic(data)
	})
}
