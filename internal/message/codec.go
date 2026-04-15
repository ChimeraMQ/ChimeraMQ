package message

import (
	"encoding/binary"
	"fmt"
	"sync"
)

var bufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 16384) // 16KB — handles most real-world messages
		return &buf
	},
}

// MaxMessageSize is the maximum allowed serialized message size.
const MaxMessageSize = 16 * 1024 * 1024 // 16MB

// Marshal serializes an Envelope to binary wire format.
func Marshal(e *Envelope) ([]byte, error) {
	if len(e.Topic) > 65535 {
		return nil, fmt.Errorf("topic name too long: %d bytes (max 65535)", len(e.Topic))
	}
	if len(e.RoutingKey) > 65535 {
		return nil, fmt.Errorf("routing key too long: %d bytes (max 65535)", len(e.RoutingKey))
	}
	if len(e.Payload) > 0xFFFFFFFF {
		return nil, fmt.Errorf("payload too large: %d bytes (max 4GB)", len(e.Payload))
	}

	hdrBytes := marshalHeaders(e.Headers)

	size := FixedHeaderSize
	size += len(e.Topic)
	size += len(e.RoutingKey)
	size += len(e.Payload)
	size += len(hdrBytes)
	if e.TraceID != [16]byte{} {
		size += 24
	}

	if size > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes (max %d)", size, MaxMessageSize)
	}

	bufPtr := bufferPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	pos := 0

	// Bytes 0-15: MessageID
	copy(buf[pos:], e.MessageID[:])
	pos += 16

	// Bytes 16-23: Timestamp
	binary.BigEndian.PutUint64(buf[pos:], uint64(e.Timestamp))
	pos += 8

	// Bytes 24-31: Sequence
	binary.BigEndian.PutUint64(buf[pos:], e.Sequence)
	pos += 8

	// Bytes 32-35: PartitionID
	binary.BigEndian.PutUint32(buf[pos:], e.PartitionID)
	pos += 4

	// Bytes 36-39: SchemaID
	binary.BigEndian.PutUint32(buf[pos:], e.SchemaID)
	pos += 4

	// Byte 40: Priority
	buf[pos] = e.Priority
	pos++

	// Byte 41: Encoding
	buf[pos] = byte(e.Encoding)
	pos++

	// Byte 42: SourceProto
	buf[pos] = byte(e.SourceProto)
	pos++

	// Byte 43: Flags
	var flags uint8
	if len(e.Headers) > 0 {
		flags |= FlagHasHeaders
	}
	if e.RoutingKey != "" {
		flags |= FlagHasRoutingKey
	}
	if e.TraceID != [16]byte{} {
		flags |= FlagHasTrace
	}
	if e.TTL > 0 {
		flags |= FlagHasTTL
	}
	if e.DeliverAt > 0 {
		flags |= FlagHasDelay
	}
	buf[pos] = flags
	pos++

	// Bytes 44-47: PayloadLength
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(e.Payload)))
	pos += 4

	// Bytes 48-51: HeadersLength
	binary.BigEndian.PutUint32(buf[pos:], uint32(len(hdrBytes)))
	pos += 4

	// Bytes 52-53: TopicLength (uint16)
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(e.Topic)))
	pos += 2

	// Bytes 54-55: RoutingKeyLength (uint16)
	binary.BigEndian.PutUint16(buf[pos:], uint16(len(e.RoutingKey)))
	pos += 2

	// Bytes 56-63: conditional field
	if flags&FlagHasTTL != 0 {
		binary.BigEndian.PutUint64(buf[pos:], uint64(e.TTL))
	} else if flags&FlagHasDelay != 0 {
		binary.BigEndian.PutUint64(buf[pos:], uint64(e.DeliverAt))
	} else {
		binary.BigEndian.PutUint32(buf[pos:], e.DeliverCount)
		binary.BigEndian.PutUint32(buf[pos+4:], e.MaxRetries)
	}
	pos += 8

	// Variable fields
	copy(buf[pos:], e.Topic)
	pos += len(e.Topic)

	if e.RoutingKey != "" {
		copy(buf[pos:], e.RoutingKey)
		pos += len(e.RoutingKey)
	}

	if len(hdrBytes) > 0 {
		copy(buf[pos:], hdrBytes)
		pos += len(hdrBytes)
	}

	if e.TraceID != [16]byte{} {
		copy(buf[pos:], e.TraceID[:])
		pos += 16
		copy(buf[pos:], e.SpanID[:])
		pos += 8
	}

	copy(buf[pos:], e.Payload)

	return buf, nil
}

// ReleaseBuffer returns a buffer to the pool for reuse.
func ReleaseBuffer(buf []byte) {
	bufferPool.Put(&buf)
}

// Unmarshal deserializes binary data into an Envelope.
// Payload is zero-copy — references the input slice.
func Unmarshal(data []byte) (*Envelope, error) {
	if len(data) < FixedHeaderSize {
		return nil, fmt.Errorf("data too short: %d < %d", len(data), FixedHeaderSize)
	}

	e := &Envelope{}
	pos := 0

	// Bytes 0-15: MessageID
	copy(e.MessageID[:], data[pos:pos+16])
	pos += 16

	// Bytes 16-23: Timestamp
	e.Timestamp = int64(binary.BigEndian.Uint64(data[pos:]))
	pos += 8

	// Bytes 24-31: Sequence
	e.Sequence = binary.BigEndian.Uint64(data[pos:])
	pos += 8

	// Bytes 32-35: PartitionID
	e.PartitionID = binary.BigEndian.Uint32(data[pos:])
	pos += 4

	// Bytes 36-39: SchemaID
	e.SchemaID = binary.BigEndian.Uint32(data[pos:])
	pos += 4

	// Byte 40: Priority
	e.Priority = data[pos]
	pos++

	// Byte 41: Encoding
	e.Encoding = EncodingType(data[pos])
	pos++

	// Byte 42: SourceProto
	e.SourceProto = ProtocolType(data[pos])
	pos++

	// Byte 43: Flags
	flags := data[pos]
	pos++

	// Bytes 44-47: PayloadLength
	payloadLen := binary.BigEndian.Uint32(data[pos:])
	pos += 4

	// Bytes 48-51: HeadersLength
	headersLen := binary.BigEndian.Uint32(data[pos:])
	pos += 4

	// Bytes 52-53: TopicLength
	topicLen := binary.BigEndian.Uint16(data[pos:])
	pos += 2

	// Bytes 54-55: RoutingKeyLength
	rkLen := binary.BigEndian.Uint16(data[pos:])
	pos += 2

	// Bytes 56-63: conditional
	if flags&FlagHasTTL != 0 {
		e.TTL = int64(binary.BigEndian.Uint64(data[pos:]))
	} else if flags&FlagHasDelay != 0 {
		e.DeliverAt = int64(binary.BigEndian.Uint64(data[pos:]))
	} else {
		e.DeliverCount = binary.BigEndian.Uint32(data[pos:])
		e.MaxRetries = binary.BigEndian.Uint32(data[pos+4:])
	}
	pos += 8

	// Variable fields
	if int(topicLen) > len(data)-pos {
		return nil, fmt.Errorf("topic extends beyond data")
	}
	e.Topic = string(data[pos : pos+int(topicLen)])
	pos += int(topicLen)

	if rkLen > 0 {
		if int(rkLen) > len(data)-pos {
			return nil, fmt.Errorf("routing key extends beyond data")
		}
		e.RoutingKey = string(data[pos : pos+int(rkLen)])
		pos += int(rkLen)
	}

	if headersLen > 0 {
		if int(headersLen) > len(data)-pos {
			return nil, fmt.Errorf("headers extend beyond data")
		}
		e.Headers = unmarshalHeaders(data[pos : pos+int(headersLen)])
		pos += int(headersLen)
	}

	if flags&FlagHasTrace != 0 {
		if pos+24 > len(data) {
			return nil, fmt.Errorf("trace extends beyond data")
		}
		copy(e.TraceID[:], data[pos:pos+16])
		pos += 16
		copy(e.SpanID[:], data[pos:pos+8])
		pos += 8
	}

	// Payload — zero-copy reference to input slice
	if uint64(payloadLen) > uint64(len(data)-pos) {
		return nil, fmt.Errorf("payload extends beyond data: need %d, have %d", payloadLen, len(data)-pos)
	}
	e.Payload = data[pos : pos+int(payloadLen)]

	return e, nil
}
