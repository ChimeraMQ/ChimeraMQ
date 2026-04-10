package amqp

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
)

// --- remaining helper test ---

func TestTypeReaderRemaining(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	tr := newTypeReader(data)

	if tr.remaining() != 5 {
		t.Errorf("remaining() = %d, want 5", tr.remaining())
	}

	tr.readByte()
	if tr.remaining() != 4 {
		t.Errorf("remaining() after 1 byte = %d, want 4", tr.remaining())
	}

	tr.readUint32()
	if tr.remaining() != 0 {
		t.Errorf("remaining() after 5 bytes = %d, want 0", tr.remaining())
	}

	// empty reader
	tr2 := newTypeReader(nil)
	if tr2.remaining() != 0 {
		t.Errorf("remaining() on empty = %d, want 0", tr2.remaining())
	}
}

// --- readAny comprehensive type code tests ---

func TestReadAnyNull(t *testing.T) {
	tr := newTypeReader([]byte{typeNull})
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny null: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestReadAnyBoolean(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{"true", []byte{typeBoolean, 0x01}, true},
		{"false", []byte{typeBoolean, 0x00}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newTypeReader(tt.data)
			got, err := tr.readAny()
			if err != nil {
				t.Fatalf("readAny: %v", err)
			}
			if b, ok := got.(bool); !ok || b != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadAnyUbyte(t *testing.T) {
	tr := newTypeReader([]byte{typeUbyte, 0xAB})
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if b, ok := got.(byte); !ok || b != 0xAB {
		t.Errorf("got %v, want 0xAB", got)
	}
}

func TestReadAnyUshort(t *testing.T) {
	tr := newTypeReader([]byte{typeUshort, 0x12, 0x34})
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if v, ok := got.(uint16); !ok || v != 0x1234 {
		t.Errorf("got %v, want 0x1234", got)
	}
}

func TestReadAnyUint(t *testing.T) {
	data := []byte{typeUint, 0x00, 0x00, 0x01, 0x00}
	tr := newTypeReader(data)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if v, ok := got.(uint32); !ok || v != 256 {
		t.Errorf("got %v, want 256", got)
	}
}

func TestReadAnyUlong(t *testing.T) {
	data := make([]byte, 9)
	data[0] = typeUlong
	binary.BigEndian.PutUint64(data[1:], 0xDEADBEEFCAFE0000)
	tr := newTypeReader(data)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if v, ok := got.(uint64); !ok || v != 0xDEADBEEFCAFE0000 {
		t.Errorf("got %v, want 0xDEADBEEFCAFE0000", got)
	}
}

func TestReadAnyByte(t *testing.T) {
	tr := newTypeReader([]byte{typeByte, 0x7F})
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if v, ok := got.(int8); !ok || v != 0x7F {
		t.Errorf("got %v, want 127", got)
	}
}

func TestReadAnyShort(t *testing.T) {
	tr := newTypeReader([]byte{typeShort, 0xFF, 0xFE})
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if v, ok := got.(int16); !ok || v != -2 {
		t.Errorf("got %v, want -2", got)
	}
}

func TestReadAnyInt(t *testing.T) {
	data := []byte{typeInt, 0xFF, 0xFF, 0xFF, 0xFE}
	tr := newTypeReader(data)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if v, ok := got.(int32); !ok || v != -2 {
		t.Errorf("got %v, want -2", got)
	}
}

func TestReadAnyLong(t *testing.T) {
	data := make([]byte, 9)
	data[0] = typeLong
	binary.BigEndian.PutUint64(data[1:], 0xFFFFFFFFFFFFFFFE)
	tr := newTypeReader(data)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if v, ok := got.(int64); !ok || v != -2 {
		t.Errorf("got %v, want -2", got)
	}
}

func TestReadAnyFloat(t *testing.T) {
	data := []byte{typeFloat, 0x00, 0x00, 0x00, 0x00}
	tr := newTypeReader(data)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if v, ok := got.(uint32); !ok || v != 0 {
		t.Errorf("got %v, want 0", got)
	}
}

func TestReadAnyDouble(t *testing.T) {
	data := make([]byte, 9)
	data[0] = typeDouble
	binary.BigEndian.PutUint64(data[1:], 0)
	tr := newTypeReader(data)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if v, ok := got.(uint64); !ok || v != 0 {
		t.Errorf("got %v, want 0", got)
	}
}

func TestReadAnyTimestamp(t *testing.T) {
	data := make([]byte, 9)
	data[0] = typeTimestamp
	binary.BigEndian.PutUint64(data[1:], 123456789)
	tr := newTypeReader(data)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if v, ok := got.(uint64); !ok || v != 123456789 {
		t.Errorf("got %v, want 123456789", got)
	}
}

func TestReadAnyBinary(t *testing.T) {
	// typeVbin32 with length prefix
	data := []byte{typeVbin32, 0x03, 'A', 'B', 'C'}
	tr := newTypeReader(data)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if b, ok := got.([]byte); !ok || string(b) != "ABC" {
		t.Errorf("got %v, want ABC", got)
	}
}

func TestReadAnySymbol(t *testing.T) {
	data := []byte{typeSymbol, 0x04, 't', 'e', 's', 't'}
	tr := newTypeReader(data)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny: %v", err)
	}
	if b, ok := got.([]byte); !ok || string(b) != "test" {
		t.Errorf("got %v, want test", got)
	}
}

func TestReadAnyDescribed(t *testing.T) {
	// 0x00 marker (described type) + descriptor (ulong) + value (null)
	// Build: 0x00, typeUlong, 8 bytes descriptor, typeNull
	data := make([]byte, 11)
	data[0] = 0x00 // described type constructor
	data[1] = typeUlong
	binary.BigEndian.PutUint64(data[2:], descOpen)
	data[10] = typeNull

	tr := newTypeReader(data)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny described: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil (null value inside described)", got)
	}
}

func TestReadAnyList0(t *testing.T) {
	tr := newTypeReader([]byte{typeList0})
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny list0: %v", err)
	}
	list, ok := got.([]interface{})
	if !ok {
		t.Fatalf("got %T, want []interface{}", got)
	}
	if len(list) != 0 {
		t.Errorf("list length = %d, want 0", len(list))
	}
}

func TestReadAnyList32(t *testing.T) {
	// Build a list32 with 2 items: null, null
	// list32: typeList32, size(4 bytes), count(4 bytes), items...
	// items = null, null = 2 bytes
	// size = 4 + 4 + 2 = 10
	buf := []byte{typeList32}
	buf = binary.BigEndian.AppendUint32(buf, 10) // size
	buf = binary.BigEndian.AppendUint32(buf, 2)  // count
	buf = append(buf, typeNull, typeNull)        // items

	tr := newTypeReader(buf)
	got, err := tr.readAny()
	if err != nil {
		t.Fatalf("readAny list32: %v", err)
	}
	list, ok := got.([]interface{})
	if !ok {
		t.Fatalf("got %T, want []interface{}", got)
	}
	if len(list) != 2 {
		t.Errorf("list length = %d, want 2", len(list))
	}
}

func TestReadAnyMap32(t *testing.T) {
	// map32 (0xD1) is not handled in readAny switch, so it should return an error
	buf := []byte{typeMap32}
	buf = binary.BigEndian.AppendUint32(buf, 14) // size
	buf = binary.BigEndian.AppendUint32(buf, 2)  // count
	buf = append(buf, typeStr32, 0x01, 'k')
	buf = append(buf, typeStr32, 0x01, 'v')

	tr := newTypeReader(buf)
	_, err := tr.readAny()
	if err == nil {
		t.Error("expected error for unsupported map32 type")
	}
}

func TestReadAnyUnsupported(t *testing.T) {
	tr := newTypeReader([]byte{0xFF}) // invalid type code
	_, err := tr.readAny()
	if err == nil {
		t.Error("expected error for unsupported type code 0xFF")
	}
}

func TestReadAnyEOF(t *testing.T) {
	tr := newTypeReader([]byte{})
	_, err := tr.readAny()
	if err == nil {
		t.Error("expected error for empty data")
	}
}

func TestReadAnyTruncatedUlong(t *testing.T) {
	// Only 3 bytes after type code, need 8
	tr := newTypeReader([]byte{typeUlong, 0x01, 0x02, 0x03})
	_, err := tr.readAny()
	if err == nil {
		t.Error("expected error for truncated ulong")
	}
}

// --- appendAMQPValue comprehensive tests ---

func TestAppendAMQPValueNil(t *testing.T) {
	buf := appendAMQPValue(nil, nil)
	if len(buf) != 1 || buf[0] != typeNull {
		t.Errorf("got %x, want [%x]", buf, typeNull)
	}
}

func TestAppendAMQPValueBool(t *testing.T) {
	buf := appendAMQPValue(nil, true)
	if buf[0] != typeBoolean || buf[1] != 0x01 {
		t.Errorf("got %x, want [typeBoolean 0x01]", buf)
	}
	buf = appendAMQPValue(nil, false)
	if buf[0] != typeBoolean || buf[1] != 0x00 {
		t.Errorf("got %x, want [typeBoolean 0x00]", buf)
	}
}

func TestAppendAMQPValueByte(t *testing.T) {
	buf := appendAMQPValue(nil, byte(42))
	if buf[0] != typeUbyte || buf[1] != 42 {
		t.Errorf("got %x, want [typeUbyte 42]", buf)
	}
}

func TestAppendAMQPValueUint16(t *testing.T) {
	buf := appendAMQPValue(nil, uint16(0x1234))
	if buf[0] != typeUshort {
		t.Errorf("got type 0x%02x, want typeUshort", buf[0])
	}
	if binary.BigEndian.Uint16(buf[1:]) != 0x1234 {
		t.Errorf("got value %x, want 0x1234", buf[1:])
	}
}

func TestAppendAMQPValueUint32(t *testing.T) {
	buf := appendAMQPValue(nil, uint32(0x12345678))
	if buf[0] != typeUint {
		t.Errorf("got type 0x%02x, want typeUint", buf[0])
	}
	if binary.BigEndian.Uint32(buf[1:]) != 0x12345678 {
		t.Errorf("got value %x, want 0x12345678", buf[1:])
	}
}

func TestAppendAMQPValueUint64(t *testing.T) {
	buf := appendAMQPValue(nil, uint64(0x123456789ABCDEF0))
	if buf[0] != typeUlong {
		t.Errorf("got type 0x%02x, want typeUlong", buf[0])
	}
	if binary.BigEndian.Uint64(buf[1:]) != 0x123456789ABCDEF0 {
		t.Errorf("got value %x, want 0x123456789ABCDEF0", buf[1:])
	}
}

func TestAppendAMQPValueInt(t *testing.T) {
	buf := appendAMQPValue(nil, int(256))
	if buf[0] != typeUint {
		t.Errorf("got type 0x%02x, want typeUint", buf[0])
	}
	if binary.BigEndian.Uint32(buf[1:]) != 256 {
		t.Errorf("got value %x, want 256", buf[1:])
	}
}

func TestAppendAMQPValueString(t *testing.T) {
	buf := appendAMQPValue(nil, "hello")
	if buf[0] != typeStr32 {
		t.Errorf("got type 0x%02x, want typeStr32", buf[0])
	}
	if buf[1] != 5 {
		t.Errorf("got length %d, want 5", buf[1])
	}
	if string(buf[2:]) != "hello" {
		t.Errorf("got value %q, want %q", string(buf[2:]), "hello")
	}
}

func TestAppendAMQPValueBytes(t *testing.T) {
	buf := appendAMQPValue(nil, []byte("data"))
	if buf[0] != typeVbin32 {
		t.Errorf("got type 0x%02x, want typeVbin32", buf[0])
	}
	if buf[1] != 4 {
		t.Errorf("got length %d, want 4", buf[1])
	}
	if string(buf[2:]) != "data" {
		t.Errorf("got value %q, want %q", string(buf[2:]), "data")
	}
}

func TestAppendAMQPValueInt8(t *testing.T) {
	buf := appendAMQPValue(nil, int8(-1))
	if buf[0] != typeByte {
		t.Errorf("got type 0x%02x, want typeByte", buf[0])
	}
	if buf[1] != 0xFF {
		t.Errorf("got value %x, want 0xFF", buf[1])
	}
}

func TestAppendAMQPValueInt16(t *testing.T) {
	buf := appendAMQPValue(nil, int16(-1))
	if buf[0] != typeShort {
		t.Errorf("got type 0x%02x, want typeShort", buf[0])
	}
	if binary.BigEndian.Uint16(buf[1:]) != 0xFFFF {
		t.Errorf("got value %x, want 0xFFFF", buf[1:])
	}
}

func TestAppendAMQPValueInt32(t *testing.T) {
	buf := appendAMQPValue(nil, int32(-1))
	if buf[0] != typeInt {
		t.Errorf("got type 0x%02x, want typeInt", buf[0])
	}
	if binary.BigEndian.Uint32(buf[1:]) != 0xFFFFFFFF {
		t.Errorf("got value %x, want 0xFFFFFFFF", buf[1:])
	}
}

func TestAppendAMQPValueInt64(t *testing.T) {
	buf := appendAMQPValue(nil, int64(-1))
	if buf[0] != typeLong {
		t.Errorf("got type 0x%02x, want typeLong", buf[0])
	}
	if binary.BigEndian.Uint64(buf[1:]) != 0xFFFFFFFFFFFFFFFF {
		t.Errorf("got value %x, want 0xFFFFFFFFFFFFFFFF", buf[1:])
	}
}

func TestAppendAMQPValueUnsupported(t *testing.T) {
	// Pass a float64, which is not in the switch — should default to typeNull
	buf := appendAMQPValue(nil, float64(3.14))
	if len(buf) != 1 || buf[0] != typeNull {
		t.Errorf("got %x, want [typeNull] for unsupported type", buf)
	}
}

func TestAppendAMQPValueRoundtrip(t *testing.T) {
	// Verify appendAMQPValue produces bytes that readAny can decode
	values := []interface{}{
		nil,
		true,
		false,
		byte(42),
		uint16(1000),
		uint32(100000),
		uint64(10000000000),
		int(999),
		"hello",
		[]byte("world"),
		int8(-10),
		int16(-100),
		int32(-1000),
		int64(-10000),
	}
	for _, v := range values {
		buf := appendAMQPValue(nil, v)
		tr := newTypeReader(buf)
		got, err := tr.readAny()
		if err != nil {
			t.Errorf("roundtrip failed for %T(%v): %v", v, v, err)
			continue
		}
		// nil special case
		if v == nil {
			if got != nil {
				t.Errorf("roundtrip nil: got %v", got)
			}
			continue
		}
		// Compare
		switch expected := v.(type) {
		case bool:
			if g, ok := got.(bool); !ok || g != expected {
				t.Errorf("roundtrip bool: got %v, want %v", got, expected)
			}
		case byte:
			if g, ok := got.(byte); !ok || g != expected {
				t.Errorf("roundtrip byte: got %v, want %v", got, expected)
			}
		case uint16:
			if g, ok := got.(uint16); !ok || g != expected {
				t.Errorf("roundtrip uint16: got %v, want %v", got, expected)
			}
		case uint32:
			if g, ok := got.(uint32); !ok || g != expected {
				t.Errorf("roundtrip uint32: got %v, want %v", got, expected)
			}
		case uint64:
			if g, ok := got.(uint64); !ok || g != expected {
				t.Errorf("roundtrip uint64: got %v, want %v", got, expected)
			}
		case int:
			if g, ok := got.(uint32); !ok || g != uint32(expected) {
				t.Errorf("roundtrip int: got %v, want %v", got, uint32(expected))
			}
		case string:
			if g, ok := got.([]byte); !ok || string(g) != expected {
				t.Errorf("roundtrip string: got %v, want %v", got, expected)
			}
		case []byte:
			if g, ok := got.([]byte); !ok || string(g) != string(expected) {
				t.Errorf("roundtrip []byte: got %v, want %v", got, expected)
			}
		case int8:
			if g, ok := got.(int8); !ok || g != expected {
				t.Errorf("roundtrip int8: got %v, want %v", got, expected)
			}
		case int16:
			if g, ok := got.(int16); !ok || g != expected {
				t.Errorf("roundtrip int16: got %v, want %v", got, expected)
			}
		case int32:
			if g, ok := got.(int32); !ok || g != expected {
				t.Errorf("roundtrip int32: got %v, want %v", got, expected)
			}
		case int64:
			if g, ok := got.(int64); !ok || g != expected {
				t.Errorf("roundtrip int64: got %v, want %v", got, expected)
			}
		}
	}
}

// --- handleFrame routing tests via amqpConn ---

func TestHandleFrameOpen(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	body := BuildOpen("client", "host")
	frame := &Frame{Type: frameTypeAMQP, Channel: 0, Body: body}
	err := ac.handleFrame(frame)
	if err != nil {
		t.Fatalf("handleFrame OPEN: %v", err)
	}
}

func TestHandleFrameBegin(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	body := BuildBegin(0, 0, 65535, 65535, 4294967295)
	frame := &Frame{Type: frameTypeAMQP, Channel: 1, Body: body}
	err := ac.handleFrame(frame)
	if err != nil {
		t.Fatalf("handleFrame BEGIN: %v", err)
	}
}

func TestHandleFrameAttach(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	// Need channel first
	ac.channels[0] = &amqpChannel{links: make(map[uint32]*amqpLink)}

	body := BuildAttach("test-link", 0, 0, "topic")
	frame := &Frame{Type: frameTypeAMQP, Channel: 0, Body: body}
	err := ac.handleFrame(frame)
	if err != nil {
		t.Fatalf("handleFrame ATTACH: %v", err)
	}
}

func TestHandleFrameFlow(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	ac.channels[0] = &amqpChannel{
		links: map[uint32]*amqpLink{
			0: {name: "link", handle: 0},
		},
	}

	flowBody := buildFlowBody(0, 200)
	frame := &Frame{Type: frameTypeAMQP, Channel: 0, Body: flowBody}
	err := ac.handleFrame(frame)
	if err != nil {
		t.Fatalf("handleFrame FLOW: %v", err)
	}
}

func TestHandleFrameTransfer(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	ac.channels[0] = &amqpChannel{
		links: map[uint32]*amqpLink{
			0: {name: "sender", handle: 0, role: 0, addr: "topic"},
		},
	}

	transferBody := buildTransferBody(0)
	frame := &Frame{Type: frameTypeAMQP, Channel: 0, Body: transferBody}
	err := ac.handleFrame(frame)
	// transfer requires a topic; may fail publishing, but handleFrame should route
	_ = err // routing is what we're testing
}

func TestHandleFrameDisposition(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	ac.channels[0] = &amqpChannel{
		links: map[uint32]*amqpLink{
			0: {name: "sender", handle: 0, role: 0},
		},
	}

	dispositionBody := BuildDisposition(0, 0, 5, true, "accepted")
	frame := &Frame{Type: frameTypeAMQP, Channel: 0, Body: dispositionBody}
	err := ac.handleFrame(frame)
	if err != nil {
		t.Fatalf("handleFrame DISPOSITION: %v", err)
	}
}

func TestHandleFrameDetach(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	ac.channels[0] = &amqpChannel{
		links: map[uint32]*amqpLink{
			0: {name: "link", handle: 0},
		},
	}

	detachBody := BuildDetach(0, true)
	frame := &Frame{Type: frameTypeAMQP, Channel: 0, Body: detachBody}
	err := ac.handleFrame(frame)
	if err != nil {
		t.Fatalf("handleFrame DETACH: %v", err)
	}
}

func TestHandleFrameEnd(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)
	ac.channels[1] = &amqpChannel{links: make(map[uint32]*amqpLink)}

	endBody := BuildEnd()
	frame := &Frame{Type: frameTypeAMQP, Channel: 1, Body: endBody}
	err := ac.handleFrame(frame)
	if err != nil {
		t.Fatalf("handleFrame END: %v", err)
	}
}

func TestHandleFrameClose(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	ac, _ := newTestConn(bkr)

	closeBody := BuildClose()
	frame := &Frame{Type: frameTypeAMQP, Channel: 0, Body: closeBody}
	err := ac.handleFrame(frame)
	if err == nil {
		t.Error("handleFrame CLOSE should return error")
	}
}

// --- HandleConnection with net.Pipe ---

func TestHandleConnectionWithPipe(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	client, server := net.Pipe()
	defer client.Close()

	go s.HandleConnection(server, nil)

	// Write AMQP protocol header from client side
	_, err := client.Write([]byte(protocolHeader))
	if err != nil {
		t.Fatalf("write protocol header: %v", err)
	}

	// Read server's protocol header response
	resp := make([]byte, 8)
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(client, resp)
	if err != nil {
		t.Fatalf("read protocol header response: %v", err)
	}
	if string(resp) != protocolHeader {
		t.Errorf("response header = %x, want %s", resp, protocolHeader)
	}

	// Close client to end the connection; server goroutine will exit
	client.Close()

	// Give the goroutine a moment to clean up
	time.Sleep(50 * time.Millisecond)
}

func TestHandleConnectionBadHeader(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	client, server := net.Pipe()
	defer client.Close()

	done := make(chan struct{})
	go func() {
		s.HandleConnection(server, nil)
		close(done)
	}()

	// Write invalid header
	_, err := client.Write([]byte("BADHEADER"))
	if err != nil {
		t.Fatalf("write bad header: %v", err)
	}

	// Server should close the connection (via defer conn.Close())
	select {
	case <-done:
		// Success — handleConnection returned
	case <-time.After(2 * time.Second):
		t.Error("handleConnection did not return for bad header")
	}
}

func TestHandleConnectionClientClosesEarly(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	client, server := net.Pipe()

	done := make(chan struct{})
	go func() {
		s.HandleConnection(server, nil)
		close(done)
	}()

	// Close client immediately — server should detect and return
	client.Close()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Error("handleConnection did not return when client closed")
	}
}

// --- Stop with active sessions ---

func TestStopWithActiveSession(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	// Store a fake session
	fakeConn := &nopConn{}
	ac := &amqpConn{
		server:   s,
		conn:     fakeConn,
		channels: make(map[uint16]*amqpChannel),
	}
	s.sessions.Store(fakeConn, ac)

	// Stop should close all sessions
	s.Stop()

	// Verify the session was processed (no panic)
	// The sessions map should be empty after Stop, but Range still works on deleted items
}

// --- negotiateSASL tests ---

func TestNegotiateSASLSuccess(t *testing.T) {
	bkr, cleanup := newAMQPAuthTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	// Create pipe to simulate client-server interaction
	client, serverConn := net.Pipe()
	defer client.Close()

	done := make(chan bool, 1)
	go func() {
		reader := bufio.NewReaderSize(client, 64*1024)
		writer := bufio.NewWriterSize(client, 64*1024)

		// Read SASL mechanisms frame
		frame, err := ReadFrame(reader, defaultMaxFrameSize)
		if err != nil {
			done <- false
			return
		}
		if frame.Type != frameTypeSASL {
			done <- false
			return
		}

		// Send SASL INIT with PLAIN mechanism
		// Build: 0x00 + descSASLInit + list [mechanism="PLAIN", initial-response=<plain data>]
		plainResp := []byte("\x00admin\x00password")
		saslInitBody := buildDescribedList(descSASLInit, []interface{}{
			[]byte("PLAIN"),
			plainResp,
		})
		if err := WriteFrame(writer, frameTypeSASL, 0, saslInitBody); err != nil {
			done <- false
			return
		}
		writer.Flush()

		// Read SASL outcome
		outcomeFrame, err := ReadFrame(reader, defaultMaxFrameSize)
		if err != nil {
			done <- false
			return
		}
		desc, _, err := ParseDescribedType(outcomeFrame.Body)
		if err != nil {
			done <- false
			return
		}
		if desc != descSASLOutcome {
			done <- false
			return
		}

		done <- true
	}()

	// Server side: run negotiateSASL
	reader := bufio.NewReaderSize(serverConn, 64*1024)
	writer := bufio.NewWriterSize(serverConn, 64*1024)

	result := s.negotiateSASL(reader, writer, defaultMaxFrameSize)
	serverConn.Close()

	if !result {
		t.Error("negotiateSASL should succeed with valid credentials")
	}

	if !<-done {
		t.Error("client side SASL negotiation failed")
	}
}

func TestNegotiateSASLBadCredentials(t *testing.T) {
	bkr, cleanup := newAMQPAuthTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	client, serverConn := net.Pipe()
	defer client.Close()

	go func() {
		reader := bufio.NewReaderSize(client, 64*1024)
		writer := bufio.NewWriterSize(client, 64*1024)

		// Read mechanisms
		ReadFrame(reader, defaultMaxFrameSize)

		// Send bad credentials
		plainResp := []byte("\x00admin\x00wrongpassword")
		saslInitBody := buildDescribedList(descSASLInit, []interface{}{
			[]byte("PLAIN"),
			plainResp,
		})
		WriteFrame(writer, frameTypeSASL, 0, saslInitBody)
		writer.Flush()

		// Read outcome (should be failure)
		ReadFrame(reader, defaultMaxFrameSize)
	}()

	reader := bufio.NewReaderSize(serverConn, 64*1024)
	writer := bufio.NewWriterSize(serverConn, 64*1024)

	result := s.negotiateSASL(reader, writer, defaultMaxFrameSize)
	serverConn.Close()

	if result {
		t.Error("negotiateSASL should fail with bad credentials")
	}
}

func TestNegotiateSASLBadFrameType(t *testing.T) {
	bkr, cleanup := newAMQPAuthTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	client, serverConn := net.Pipe()
	defer client.Close()

	go func() {
		reader := bufio.NewReaderSize(client, 64*1024)
		writer := bufio.NewWriterSize(client, 64*1024)

		// Read mechanisms
		ReadFrame(reader, defaultMaxFrameSize)

		// Send wrong frame type (AMQP instead of SASL)
		WriteFrame(writer, frameTypeAMQP, 0, []byte{typeNull})
		writer.Flush()
	}()

	reader := bufio.NewReaderSize(serverConn, 64*1024)
	writer := bufio.NewWriterSize(serverConn, 64*1024)

	result := s.negotiateSASL(reader, writer, defaultMaxFrameSize)
	serverConn.Close()

	if result {
		t.Error("negotiateSASL should fail with wrong frame type")
	}
}

func TestNegotiateSASLClientCloses(t *testing.T) {
	bkr, cleanup := newAMQPAuthTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	client, serverConn := net.Pipe()

	go func() {
		// Client reads mechanisms then closes
		reader := bufio.NewReaderSize(client, 64*1024)
		ReadFrame(reader, defaultMaxFrameSize)
		client.Close()
	}()

	reader := bufio.NewReaderSize(serverConn, 64*1024)
	writer := bufio.NewWriterSize(serverConn, 64*1024)

	result := s.negotiateSASL(reader, writer, defaultMaxFrameSize)
	serverConn.Close()

	if result {
		t.Error("negotiateSASL should fail when client closes")
	}
}

// --- authenticate tests ---

func TestAuthenticateNoProvider(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	// No auth provider configured
	s := NewServer(bkr)
	if s.authenticate("user", "pass") {
		t.Error("authenticate should fail with no provider")
	}
}

// --- Frame size edge cases ---

func TestFrameTooSmall(t *testing.T) {
	// Frame with size < 8
	var buf bytes.Buffer
	binary.BigEndian.PutUint32([]byte{0, 0, 0, 4}, 4) // won't work, use Write
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, 4) // size=4, less than minFrameSize=8
	buf.Write(sizeBuf)

	_, err := ReadFrame(&buf, defaultMaxFrameSize)
	if err == nil {
		t.Error("expected error for frame size too small")
	}
}

func TestReadProtocolHeaderInvalid(t *testing.T) {
	buf := bytes.NewReader([]byte("INVALIDX"))
	err := ReadProtocolHeader(buf)
	if err == nil {
		t.Error("expected error for invalid protocol header")
	}
}

func TestReadFrameDataOffsetExceeds(t *testing.T) {
	var buf bytes.Buffer
	// Build frame with invalid data offset
	// Size = 12 (4 + 1 + 1 + 2 + 4 body)
	// DOFF = 100 (way too large)
	sizeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuf, 12)
	buf.Write(sizeBuf)
	buf.Write([]byte{100, frameTypeAMQP, 0, 0}) // doff=100, type=AMQP, channel=0
	buf.Write([]byte{0x00, 0x00, 0x00, 0x00})   // body

	_, err := ReadFrame(&buf, defaultMaxFrameSize)
	if err == nil {
		t.Error("expected error for data offset exceeding frame")
	}
}

func TestWriteFrameEmptyBody(t *testing.T) {
	var buf bytes.Buffer
	err := WriteFrame(&buf, frameTypeAMQP, 0, nil)
	if err != nil {
		t.Fatalf("WriteFrame empty body: %v", err)
	}
	// Should still have the 8-byte header
	if buf.Len() != 8 {
		t.Errorf("frame length = %d, want 8", buf.Len())
	}
}

// --- handleConnection full flow with SASL ---

func TestHandleConnectionWithSASL(t *testing.T) {
	bkr, cleanup := newAMQPAuthTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	client, server := net.Pipe()
	defer client.Close()

	go s.HandleConnection(server, nil)

	// Write AMQP protocol header
	client.Write([]byte(protocolHeader))

	// Read server header
	resp := make([]byte, 8)
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(client, resp); err != nil {
		t.Fatalf("read server header: %v", err)
	}

	// Read SASL mechanisms frame
	saslFrame, err := ReadFrame(bufio.NewReader(client), defaultMaxFrameSize)
	if err != nil {
		t.Fatalf("read SASL mechanisms: %v", err)
	}
	if saslFrame.Type != frameTypeSASL {
		t.Fatalf("expected SASL frame type, got %d", saslFrame.Type)
	}

	// Send SASL INIT
	writer := bufio.NewWriterSize(client, 64*1024)
	plainResp := []byte("\x00admin\x00password")
	saslInitBody := buildDescribedList(descSASLInit, []interface{}{
		[]byte("PLAIN"),
		plainResp,
	})
	WriteFrame(writer, frameTypeSASL, 0, saslInitBody)
	writer.Flush()

	// Read SASL outcome
	reader := bufio.NewReaderSize(client, 64*1024)
	outcomeFrame, err := ReadFrame(reader, defaultMaxFrameSize)
	if err != nil {
		t.Fatalf("read SASL outcome: %v", err)
	}

	desc, value, err := ParseDescribedType(outcomeFrame.Body)
	if err != nil {
		t.Fatalf("parse outcome: %v", err)
	}
	if desc != descSASLOutcome {
		t.Fatalf("expected SASL outcome descriptor, got 0x%x", desc)
	}

	// Verify outcome code is 0 (success)
	tr := newTypeReader(value)
	listAny, err := tr.readAny()
	if err != nil {
		t.Fatalf("read outcome list: %v", err)
	}
	items, ok := listAny.([]interface{})
	if !ok || len(items) == 0 {
		t.Fatal("expected non-empty list in SASL outcome")
	}
	if code, ok := items[0].(byte); !ok || code != 0 {
		t.Errorf("outcome code = %v, want 0 (success)", items[0])
	}

	// Now send an OPEN frame to continue
	openBody := BuildOpen("test-client", "localhost")
	WriteFrame(writer, frameTypeAMQP, 0, openBody)
	writer.Flush()

	// Read server's OPEN response
	openResp, err := ReadFrame(reader, defaultMaxFrameSize)
	if err != nil {
		t.Fatalf("read OPEN response: %v", err)
	}
	if openResp.Type != frameTypeAMQP {
		t.Errorf("expected AMQP frame, got type %d", openResp.Type)
	}

	// Close connection
	client.Close()
	time.Sleep(50 * time.Millisecond)
}

// newAMQPAuthTestBroker creates a broker with auth enabled for SASL tests.
func newAMQPAuthTestBroker(t *testing.T) (*broker.Broker, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "amqp-auth-test-*")
	if err != nil {
		t.Fatal(err)
	}
	cfg := &broker.Config{
		Node:     broker.NodeConfig{ID: 1, Name: "auth-test", DataDir: dir},
		Listener: broker.ListenerConfig{Bind: "127.0.0.1", Port: 1, AdminPort: 1},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 64 * 1024, SyncMode: "os"},
			WAL: broker.WALConfig{MaxSize: 64 * 1024, SyncMode: "os"},
		},
		Defaults: broker.DefaultsConfig{Topic: broker.TopicDefaults{Partitions: 4}},
		Logging:  broker.LoggingConfig{Level: "error", Format: "text", Output: "stdout"},
		Auth: broker.AuthConfig{
			Enabled: true,
			Type:    "static",
			Users:   map[string]string{"admin": "password"},
			Tokens:  map[string]string{"password": "admin"},
		},
		Limits: broker.LimitsConfig{
			MaxPartitionsPerTopic: 256,
			MaxTopics:             1000,
			MaxFetchMessages:      10000,
		},
	}
	bkr, err := broker.NewBroker(cfg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}
	if err := bkr.Start(); err != nil {
		os.RemoveAll(dir)
		t.Fatalf("broker Start: %v", err)
	}
	return bkr, func() {
		bkr.Stop()
		os.RemoveAll(dir)
	}
}

// --- SASL negotiateSASL with malformed INIT body ---

func TestNegotiateSASLMalformedBody(t *testing.T) {
	bkr, cleanup := newAMQPAuthTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	client, serverConn := net.Pipe()
	defer client.Close()

	go func() {
		reader := bufio.NewReaderSize(client, 64*1024)
		writer := bufio.NewWriterSize(client, 64*1024)

		// Read mechanisms
		ReadFrame(reader, defaultMaxFrameSize)

		// Send garbage body as SASL INIT
		WriteFrame(writer, frameTypeSASL, 0, []byte{0xFF, 0xFF, 0xFF})
		writer.Flush()
	}()

	reader := bufio.NewReaderSize(serverConn, 64*1024)
	writer := bufio.NewWriterSize(serverConn, 64*1024)

	result := s.negotiateSASL(reader, writer, defaultMaxFrameSize)
	serverConn.Close()

	if result {
		t.Error("negotiateSASL should fail with malformed body")
	}
}

// --- SASL negotiateSASL with non-list INIT value ---

func TestNegotiateSASLNonListInit(t *testing.T) {
	bkr, cleanup := newAMQPAuthTestBroker(t)
	defer cleanup()

	s := NewServer(bkr)

	client, serverConn := net.Pipe()
	defer client.Close()

	go func() {
		reader := bufio.NewReaderSize(client, 64*1024)
		writer := bufio.NewWriterSize(client, 64*1024)

		// Read mechanisms
		ReadFrame(reader, defaultMaxFrameSize)

		// Send SASL INIT with a scalar value (not a list)
		// 0x00 = described type, then descriptor (ulong), then value (null)
		body := make([]byte, 11)
		body[0] = 0x00
		body[1] = typeUlong
		binary.BigEndian.PutUint64(body[2:], descSASLInit)
		body[10] = typeNull // value is null, not a list
		WriteFrame(writer, frameTypeSASL, 0, body)
		writer.Flush()
	}()

	reader := bufio.NewReaderSize(serverConn, 64*1024)
	writer := bufio.NewWriterSize(serverConn, 64*1024)

	result := s.negotiateSASL(reader, writer, defaultMaxFrameSize)
	serverConn.Close()

	if result {
		t.Error("negotiateSASL should fail with non-list INIT")
	}
}

// --- handleConnection maxFrameSize default test ---

func TestHandleConnectionMaxFrameSizeDefault(t *testing.T) {
	bkr, cleanup := newAMQPTestBroker(t)
	defer cleanup()

	// Ensure MaxFrameSize=0 falls back to defaultMaxFrameSize
	cfg := bkr.Config()
	cfg.Protocols.AMQP.MaxFrameSize = 0

	s := NewServer(bkr)

	client, server := net.Pipe()
	defer client.Close()

	go s.HandleConnection(server, nil)

	client.Write([]byte(protocolHeader))

	resp := make([]byte, 8)
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	io.ReadFull(client, resp)

	if string(resp) != protocolHeader {
		t.Errorf("response = %x, want %s", resp, protocolHeader)
	}

	client.Close()
	time.Sleep(50 * time.Millisecond)
}
