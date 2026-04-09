package chimera

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func encodeDecode(t *testing.T, f *Frame) *Frame {
	t.Helper()
	data, err := EncodeFrame(f)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeFrame(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return decoded
}

func TestEncodeDecodeFrame(t *testing.T) {
	decoded := encodeDecode(t, &Frame{
		Version: FrameVersion,
		OpCode:  OpPublish,
		Payload: []byte("hello chimera"),
	})

	if decoded.OpCode != OpPublish {
		t.Errorf("expected OpPublish, got %d", decoded.OpCode)
	}
	if string(decoded.Payload) != "hello chimera" {
		t.Errorf("payload mismatch: %s", string(decoded.Payload))
	}
}

func TestFrameMagic(t *testing.T) {
	f := &Frame{Version: FrameVersion, OpCode: OpPing, Payload: nil}
	data, _ := EncodeFrame(f)

	if data[0] != 'C' || data[1] != 'H' || data[2] != 'M' || data[3] != 'R' {
		t.Error("invalid magic bytes")
	}
}

func TestFrameCRCValidation(t *testing.T) {
	f := &Frame{Version: FrameVersion, OpCode: OpConnect, Payload: []byte("test")}
	data, _ := EncodeFrame(f)

	// Corrupt payload
	data[10] ^= 0xFF

	_, err := DecodeFrame(bytes.NewReader(data))
	if err == nil {
		t.Error("expected CRC error for corrupted frame")
	}
}

func TestFrameEmptyPayload(t *testing.T) {
	decoded := encodeDecode(t, &Frame{Version: FrameVersion, OpCode: OpPing, Payload: nil})

	if decoded.OpCode != OpPing {
		t.Errorf("expected OpPing, got %d", decoded.OpCode)
	}
	if len(decoded.Payload) != 0 {
		t.Errorf("expected empty payload, got %d bytes", len(decoded.Payload))
	}
}

func TestReadFrameFromStream(t *testing.T) {
	f := &Frame{Version: FrameVersion, OpCode: OpPublish, Payload: []byte("stream-test")}
	data, _ := EncodeFrame(f)

	decoded, err := DecodeFrame(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.OpCode != OpPublish {
		t.Errorf("expected OpPublish, got %d", decoded.OpCode)
	}
}

func TestReadFrameTruncated(t *testing.T) {
	f := &Frame{Version: FrameVersion, OpCode: OpPing, Payload: nil}
	data, _ := EncodeFrame(f)

	// Truncate the CRC
	_, err := DecodeFrame(bytes.NewReader(data[:len(data)-2]))
	if err == nil {
		t.Error("expected error for truncated frame")
	}
}

func TestAllOpCodes(t *testing.T) {
	codes := []OpCode{
		OpConnect, OpConnAck, OpPublish, OpPubAck,
		OpSubscribe, OpSubAck, OpAck, OpNack,
		OpPing, OpPong, OpCreateTopic, OpDeleteTopic,
		OpDisconnect, OpError, OpCommitOffset,
	}
	for _, op := range codes {
		decoded := encodeDecode(t, &Frame{Version: FrameVersion, OpCode: op, Payload: []byte("x")})
		if decoded.OpCode != op {
			t.Errorf("opcode mismatch: expected %d, got %d", op, decoded.OpCode)
		}
	}
}

func TestDecodeFrameInvalidMagic(t *testing.T) {
	buf := make([]byte, FrameHeaderLen)
	buf[0] = 'X'
	buf[1] = 'Y'
	buf[2] = 'Z'
	buf[3] = 'W'

	_, err := DecodeFrame(bytes.NewReader(buf))
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

func TestDecodeFrameTooLarge(t *testing.T) {
	var header [FrameHeaderLen]byte
	header[0] = FrameMagic0
	header[1] = FrameMagic1
	header[2] = FrameMagic2
	header[3] = FrameMagic3
	header[4] = FrameVersion
	binary.BigEndian.PutUint32(header[7:], MaxFrameSize+1)

	_, err := DecodeFrame(bytes.NewReader(header[:]))
	if err == nil {
		t.Error("expected error for oversized frame")
	}
}

func TestDecodeFrameCRCMismatch(t *testing.T) {
	f := &Frame{Version: FrameVersion, OpCode: OpPing, Payload: []byte("test")}
	data, _ := EncodeFrame(f)

	// Corrupt CRC trailer
	data[len(data)-1] ^= 0xFF

	_, err := DecodeFrame(bytes.NewReader(data))
	if err == nil {
		t.Error("expected CRC mismatch error")
	}
}

func TestDecodeFrameReadError(t *testing.T) {
	_, err := DecodeFrame(errReader{})
	if err == nil {
		t.Error("expected error from failing reader")
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestEncodeFrameFlags(t *testing.T) {
	f := &Frame{Version: FrameVersion, OpCode: OpPublish, Flags: 0x03, Payload: []byte("x")}
	data, _ := EncodeFrame(f)
	decoded, err := DecodeFrame(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Flags != 0x03 {
		t.Errorf("flags = %d, want 3", decoded.Flags)
	}
}

func TestEncodeFrameLargePayload(t *testing.T) {
	payload := make([]byte, 64*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	f := &Frame{Version: FrameVersion, OpCode: OpPublish, Payload: payload}
	data, _ := EncodeFrame(f)

	decoded, err := DecodeFrame(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Payload) != len(payload) {
		t.Errorf("payload len = %d, want %d", len(decoded.Payload), len(payload))
	}
}
