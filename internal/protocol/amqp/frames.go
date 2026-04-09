package amqp

import (
	"encoding/binary"
	"fmt"
	"io"
)

// AMQP 1.0 frame constants.
const (
	// Frame types
	frameTypeAMQP  byte = 0x00
	frameTypeSASL  byte = 0x01

	// Protocol header
	protocolHeader = "AMQP\x00\x01\x00\x00"

	// Minimum frame size: size(4) + doff(1) + type(1) + channel(2) = 8 bytes
	minFrameSize = 8

	// Default max frame size
	defaultMaxFrameSize = 16 * 1024 // 16KB
)

// Frame represents an AMQP 1.0 frame.
type Frame struct {
	Type     byte     // 0x00=AMQP, 0x01=SASL
	Channel  uint16   // Channel number (0 for SASL)
	Body     []byte   // Frame body (performative + payload)
	DataOff  byte     // Data offset in 4-byte words
}

// Performative type codes (descriptor codes).
const (
	// Connection
	descOpen       uint64 = 0x0000000060000001
	descBegin      uint64 = 0x0000000060000002
	descAttach     uint64 = 0x0000000060000003
	descFlow       uint64 = 0x0000000060000004
	descTransfer   uint64 = 0x0000000060000005
	descDisposition uint64 = 0x0000000060000006
	descDetach     uint64 = 0x0000000060000007
	descEnd        uint64 = 0x0000000060000008
	descClose      uint64 = 0x0000000060000009

	// SASL
	descSASLInit       uint64 = 0x0000000040000001
	descSASLMechanisms uint64 = 0x0000000040000002
	descSASLChallenge  uint64 = 0x0000000040000003
	descSASLResponse   uint64 = 0x0000000040000004
	descSASLOutcome    uint64 = 0x0000000040000005
)

// ReadFrame reads one AMQP 1.0 frame.
func ReadFrame(r io.Reader, maxFrameSize uint32) (*Frame, error) {
	// Frame header: SIZE (4 bytes)
	var sizeBuf [4]byte
	if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
		return nil, err
	}
	frameSize := binary.BigEndian.Uint32(sizeBuf[:])
	if frameSize < minFrameSize {
		return nil, fmt.Errorf("frame size %d too small", frameSize)
	}
	if frameSize > maxFrameSize {
		return nil, fmt.Errorf("frame size %d exceeds max %d", frameSize, maxFrameSize)
	}

	// Read remaining bytes: DOFF(1) + TYPE(1) + CHANNEL(2) + BODY
	remaining := make([]byte, frameSize-4)
	if _, err := io.ReadFull(r, remaining); err != nil {
		return nil, err
	}

	doff := remaining[0]
	frameType := remaining[1]
	channel := binary.BigEndian.Uint16(remaining[2:4])

	// Body starts after data offset (in bytes from frame start)
	// We already consumed 4 bytes (SIZE), so offset into remaining is doff*4 - 4
	bodyOffset := int(doff)*4 - 4
	if bodyOffset < 0 || bodyOffset > len(remaining) {
		return nil, fmt.Errorf("data offset %d exceeds frame", bodyOffset)
	}

	body := remaining[bodyOffset:]

	return &Frame{
		Type:    frameType,
		Channel: channel,
		Body:    body,
		DataOff: doff,
	}, nil
}

// WriteFrame writes an AMQP 1.0 frame.
func WriteFrame(w io.Writer, frameType byte, channel uint16, body []byte) error {
	doff := byte(2) // 2 * 4 = 8 bytes header
	frameSize := uint32(4 + 1 + 1 + 2 + len(body))

	// Size
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], frameSize)
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}

	// DOFF + TYPE + CHANNEL
	header := []byte{doff, frameType, byte(channel >> 8), byte(channel)}
	if _, err := w.Write(header); err != nil {
		return err
	}

	// Body
	if len(body) > 0 {
		_, err := w.Write(body)
		return err
	}
	return nil
}

// WriteProtocolHeader sends the AMQP 1.0 protocol header.
func WriteProtocolHeader(w io.Writer) error {
	_, err := w.Write([]byte(protocolHeader))
	return err
}

// ReadProtocolHeader reads and validates the AMQP 1.0 protocol header.
func ReadProtocolHeader(r io.Reader) error {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}
	if string(header) != protocolHeader {
		return fmt.Errorf("invalid AMQP header: %x", header)
	}
	return nil
}

// --- AMQP Type System (simplified) ---

// Type codes for AMQP encoded values.
const (
	typeNull     byte = 0x40
	typeBoolean  byte = 0x56 // boolean
	typeUbyte    byte = 0x50
	typeUshort   byte = 0x60
	typeUint     byte = 0x70
	typeUlong    byte = 0x80
	typeByte     byte = 0x51
	typeShort    byte = 0x61
	typeInt      byte = 0x71
	typeLong     byte = 0x81
	typeFloat    byte = 0x72
	typeDouble   byte = 0x82
	typeVbin32   byte = 0xA0
	typeStr32    byte = 0xA1
	typeSymbol   byte = 0xA3
	typeTimestamp byte = 0x83
	typeList0    byte = 0x45 // empty list
	typeList32   byte = 0xD0
	typeMap32    byte = 0xD1
)

// typeReader provides AMQP type reading from a byte slice.
type typeReader struct {
	data []byte
	pos  int
}

func newTypeReader(data []byte) *typeReader {
	return &typeReader{data: data}
}

func (r *typeReader) remaining() int {
	return len(r.data) - r.pos
}

func (r *typeReader) readByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

func (r *typeReader) readUint16() (uint16, error) {
	if r.pos+2 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint16(r.data[r.pos:])
	r.pos += 2
	return v, nil
}

func (r *typeReader) readUint32() (uint32, error) {
	if r.pos+4 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *typeReader) readUint64() (uint64, error) {
	if r.pos+8 > len(r.data) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.BigEndian.Uint64(r.data[r.pos:])
	r.pos += 8
	return v, nil
}

func (r *typeReader) readBytes(n int) ([]byte, error) {
	if r.pos+n > len(r.data) {
		return nil, io.ErrUnexpectedEOF
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

// readAny reads an AMQP-typed value. Returns a Go value.
func (r *typeReader) readAny() (interface{}, error) {
	typeCode, err := r.readByte()
	if err != nil {
		return nil, err
	}

	switch typeCode {
	case typeNull:
		return nil, nil
	case typeBoolean:
		b, err := r.readByte()
		return b != 0, err
	case typeUbyte:
		return r.readByte()
	case typeUshort:
		return r.readUint16()
	case typeUint:
		return r.readUint32()
	case typeUlong:
		return r.readUint64()
	case typeByte:
		b, err := r.readByte()
		return int8(b), err
	case typeShort:
		v, err := r.readUint16()
		return int16(v), err
	case typeInt:
		v, err := r.readUint32()
		return int32(v), err
	case typeLong:
		v, err := r.readUint64()
		return int64(v), err
	case typeFloat:
		v, err := r.readBytes(4)
		if err != nil {
			return nil, err
		}
		return binary.BigEndian.Uint32(v), err
	case typeDouble:
		v, err := r.readBytes(8)
		if err != nil {
			return nil, err
		}
		return binary.BigEndian.Uint64(v), err
	case typeStr32:
		length, err := r.readByte()
		if err != nil {
			return nil, err
		}
		return r.readBytes(int(length))
	case typeSymbol:
		length, err := r.readByte()
		if err != nil {
			return nil, err
		}
		return r.readBytes(int(length))
	case typeVbin32:
		length, err := r.readByte()
		if err != nil {
			return nil, err
		}
		return r.readBytes(int(length))
	case typeTimestamp:
		return r.readUint64()
	case typeList0:
		return []interface{}{}, nil
	case typeList32:
		size, err := r.readUint32()
		if err != nil {
			return nil, err
		}
		count, err := r.readUint32()
		if err != nil {
			return nil, err
		}
		return r.readListItems(int(count), int(size)-8)
	case 0x00:
		// Described type — skip descriptor, read value
		_, err := r.readAny() // read descriptor
		if err != nil {
			return nil, err
		}
		return r.readAny() // read value
	default:
		return nil, fmt.Errorf("unsupported AMQP type: 0x%02x", typeCode)
	}
}

func (r *typeReader) readListItems(count int, _ int) ([]interface{}, error) {
	items := make([]interface{}, count)
	for i := 0; i < count; i++ {
		v, err := r.readAny()
		if err != nil {
			return nil, err
		}
		items[i] = v
	}
	return items, nil
}

// --- Frame body parsing helpers ---

// ParseDescribedType extracts a described type (descriptor + value).
func ParseDescribedType(data []byte) (descriptor uint64, value []byte, err error) {
	r := newTypeReader(data)

	// Described type constructor: 0x00
	marker, err := r.readByte()
	if err != nil {
		return 0, nil, err
	}
	if marker != 0x00 {
		return 0, nil, fmt.Errorf("expected described type marker 0x00, got 0x%02x", marker)
	}

	// Descriptor (ulong)
	descBytes, err := r.readAny()
	if err != nil {
		return 0, nil, err
	}

	switch d := descBytes.(type) {
	case uint64:
		descriptor = d
	case int64:
		descriptor = uint64(d)
	default:
		return 0, nil, fmt.Errorf("descriptor has unexpected type: %T", descBytes)
	}

	// Remaining is the value
	value = r.data[r.pos:]
	return descriptor, value, nil
}

// --- Build helpers ---

// BuildOpen builds an AMQP OPEN frame body.
func BuildOpen(containerID, hostname string) []byte {
	return buildDescribedList(descOpen, []interface{}{
		containerID, // container-id
		hostname,    // hostname
		nil,         // max-frame-size (use default)
	})
}

// BuildBegin builds an AMQP BEGIN frame body.
func BuildBegin(remoteChannel uint16, nextOutgoingID uint32, incomingWindow, outgoingWindow uint32, handleMax uint32) []byte {
	return buildDescribedList(descBegin, []interface{}{
		nil,               // remote-channel
		nextOutgoingID,    // next-outgoing-id
		incomingWindow,    // incoming-window
		outgoingWindow,    // outgoing-window
		nil,               // handle-max
	})
}

// BuildAttach builds an AMQP ATTACH frame body.
func BuildAttach(name string, handle uint32, role byte, targetAddr string) []byte {
	return buildDescribedList(descAttach, []interface{}{
		name,       // name
		handle,     // handle
		role,       // role: 0=sender, 1=receiver
		nil,        // snd-settle-mode
		nil,        // rcv-settle-mode
		targetAddr, // target (simplified as string)
	})
}

// BuildClose builds an AMQP CLOSE frame body.
func BuildClose() []byte {
	return buildDescribedList(descClose, []interface{}{nil, nil})
}

// BuildEnd builds an AMQP END frame body.
func BuildEnd() []byte {
	return buildDescribedList(descEnd, []interface{}{nil})
}

// BuildDetach builds an AMQP DETACH frame body.
func BuildDetach(handle uint32, closed bool) []byte {
	closedVal := byte(0)
	if closed {
		closedVal = 1
	}
	return buildDescribedList(descDetach, []interface{}{handle, closedVal})
}

// BuildDisposition builds an AMQP DISPOSITION frame body.
func BuildDisposition(role byte, first uint64, last uint64, settled bool, state string) []byte {
	settledVal := byte(0)
	if settled {
		settledVal = 1
	}
	return buildDescribedList(descDisposition, []interface{}{
		role,
		first,
		last,
		settledVal,
		state,
	})
}

// BuildSASLMechanisms builds SASL MECHANISMS frame.
func BuildSASLMechanisms() []byte {
	return buildDescribedList(descSASLMechanisms, []interface{}{
		[]byte("PLAIN"),
	})
}

// BuildSASLOutcome builds SASL OUTCOME frame.
func BuildSASLOutcome(code byte) []byte {
	return buildDescribedList(descSASLOutcome, []interface{}{code})
}

func buildDescribedList(descriptor uint64, fields []interface{}) []byte {
	var buf []byte

	// Described type constructor
	buf = append(buf, 0x00)

	// Descriptor as ulong
	buf = append(buf, typeUlong)
	buf = binary.BigEndian.AppendUint64(buf, descriptor)

	// List32
	buf = append(buf, typeList32)

	// Calculate list body
	var listBody []byte
	for _, f := range fields {
		listBody = appendAMQPValue(listBody, f)
	}

	// Size = 4 (size) + 4 (count) + len(listBody)
	listSize := uint32(4 + 4 + len(listBody))
	buf = binary.BigEndian.AppendUint32(buf, listSize)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(fields)))
	buf = append(buf, listBody...)

	return buf
}

func appendAMQPValue(buf []byte, v interface{}) []byte {
	switch val := v.(type) {
	case nil:
		return append(buf, typeNull)
	case bool:
		buf = append(buf, typeBoolean)
		if val {
			return append(buf, 0x01)
		}
		return append(buf, 0x00)
	case byte:
		buf = append(buf, typeUbyte)
		return append(buf, val)
	case uint16:
		buf = append(buf, typeUshort)
		return binary.BigEndian.AppendUint16(buf, val)
	case uint32:
		buf = append(buf, typeUint)
		return binary.BigEndian.AppendUint32(buf, val)
	case uint64:
		buf = append(buf, typeUlong)
		return binary.BigEndian.AppendUint64(buf, val)
	case int:
		buf = append(buf, typeUint)
		return binary.BigEndian.AppendUint32(buf, uint32(val))
	case string:
		buf = append(buf, typeStr32)
		buf = append(buf, byte(len(val)))
		return append(buf, val...)
	case []byte:
		buf = append(buf, typeVbin32)
		buf = append(buf, byte(len(val)))
		return append(buf, val...)
	case int8:
		buf = append(buf, typeByte)
		return append(buf, byte(val))
	case int16:
		buf = append(buf, typeShort)
		return binary.BigEndian.AppendUint16(buf, uint16(val))
	case int32:
		buf = append(buf, typeInt)
		return binary.BigEndian.AppendUint32(buf, uint32(val))
	case int64:
		buf = append(buf, typeLong)
		return binary.BigEndian.AppendUint64(buf, uint64(val))
	default:
		return append(buf, typeNull)
	}
}
