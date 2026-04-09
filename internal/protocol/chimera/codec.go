package chimera

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	FrameMagic0    byte = 'C'
	FrameMagic1    byte = 'H'
	FrameMagic2    byte = 'M'
	FrameMagic3    byte = 'R'
	FrameVersion   uint8  = 1
	FrameHeaderLen        = 11 // magic(4) + version(1) + opcode(1) + flags(1) + length(4)
	FrameTrailerLen       = 4  // CRC32C
	MaxFrameSize          = 16 * 1024 * 1024 // 16MB
)

type OpCode uint8

const (
	OpConnect      OpCode = 0x01
	OpConnAck      OpCode = 0x02
	OpPublish      OpCode = 0x03
	OpPubAck       OpCode = 0x04
	OpSubscribe    OpCode = 0x05
	OpSubAck       OpCode = 0x06
	OpUnsubscribe  OpCode = 0x07
	OpUnsubAck     OpCode = 0x08
	OpFetch        OpCode = 0x09
	OpFetchResp    OpCode = 0x0A
	OpAck          OpCode = 0x0B
	OpNack         OpCode = 0x0C
	OpSeek         OpCode = 0x0D
	OpSeekAck      OpCode = 0x0E
	OpPing         OpCode = 0x0F
	OpPong         OpCode = 0x10
	OpCreateTopic  OpCode = 0x11
	OpDeleteTopic  OpCode = 0x12
	OpBatchPublish OpCode = 0x13
	OpBatchPubAck  OpCode = 0x14
	OpCommitOffset OpCode = 0x17
	OpCommitAck    OpCode = 0x18
	OpDisconnect   OpCode = 0x19
	OpError        OpCode = 0x1A
)

// Frame represents a single protocol frame.
type Frame struct {
	Version uint8
	OpCode  OpCode
	Flags   uint8
	Payload []byte
}

// EncodeFrame serializes a frame to binary.
func EncodeFrame(f *Frame) ([]byte, error) {
	totalLen := FrameHeaderLen + len(f.Payload) + FrameTrailerLen
	buf := make([]byte, totalLen)

	buf[0] = FrameMagic0
	buf[1] = FrameMagic1
	buf[2] = FrameMagic2
	buf[3] = FrameMagic3
	buf[4] = f.Version
	buf[5] = byte(f.OpCode)
	buf[6] = f.Flags
	binary.BigEndian.PutUint32(buf[7:], uint32(len(f.Payload)))

	copy(buf[FrameHeaderLen:], f.Payload)

	crc := crc32.Checksum(buf[:FrameHeaderLen+len(f.Payload)], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(buf[FrameHeaderLen+len(f.Payload):], crc)

	return buf, nil
}

// DecodeFrame reads and validates a frame from a reader.
func DecodeFrame(reader io.Reader) (*Frame, error) {
	var header [FrameHeaderLen]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}

	if header[0] != FrameMagic0 || header[1] != FrameMagic1 || header[2] != FrameMagic2 || header[3] != FrameMagic3 {
		return nil, fmt.Errorf("invalid frame magic")
	}

	f := &Frame{
		Version: header[4],
		OpCode:  OpCode(header[5]),
		Flags:   header[6],
	}

	payloadLen := binary.BigEndian.Uint32(header[7:])
	if payloadLen > MaxFrameSize {
		return nil, fmt.Errorf("frame too large: %d", payloadLen)
	}

	buf := make([]byte, payloadLen+4)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}

	f.Payload = buf[:payloadLen]

	expectedCRC := binary.BigEndian.Uint32(buf[payloadLen:])
	actualCRC := crc32.Checksum(append(header[:], f.Payload...), crc32.MakeTable(crc32.Castagnoli))
	if actualCRC != expectedCRC {
		return nil, fmt.Errorf("CRC mismatch")
	}

	return f, nil
}
