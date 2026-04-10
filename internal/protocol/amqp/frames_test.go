package amqp

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func TestDetector(t *testing.T) {
	d := &Detector{}

	tests := []struct {
		peek  []byte
		match bool
	}{
		{[]byte("AMQP\x00\x01\x00\x00"), true},
		{[]byte("AMQP\x00\x01\x00"), false},
		{[]byte{0x10}, false},
		{[]byte{'C', 'H', 'M', 'R'}, false},
		{[]byte("GET /"), false},
	}
	for _, tt := range tests {
		got := d.Detect(tt.peek)
		if got != tt.match {
			t.Errorf("Detect(%x) = %v, want %v", tt.peek, got, tt.match)
		}
	}
	if d.BytesNeeded() != 8 {
		t.Errorf("BytesNeeded() = %d, want 8", d.BytesNeeded())
	}
}

func TestProtocolHeader(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteProtocolHeader(&buf); err != nil {
		t.Fatal(err)
	}
	if buf.Len() != 8 {
		t.Fatalf("header length = %d, want 8", buf.Len())
	}
	if err := ReadProtocolHeader(&buf); err != nil {
		t.Fatalf("ReadProtocolHeader: %v", err)
	}
}

func TestWriteReadFrame(t *testing.T) {
	var buf bytes.Buffer
	body := []byte{0x00, typeUlong, 0, 0, 0, 0, 0x60, 0, 0, 0, 0x01, typeNull}

	if err := WriteFrame(&buf, frameTypeAMQP, 0, body); err != nil {
		t.Fatal(err)
	}

	frame, err := ReadFrame(&buf, defaultMaxFrameSize)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Type != frameTypeAMQP {
		t.Errorf("type = %d, want %d", frame.Type, frameTypeAMQP)
	}
	if frame.Channel != 0 {
		t.Errorf("channel = %d, want 0", frame.Channel)
	}
	if len(frame.Body) != len(body) {
		t.Errorf("body length = %d, want %d", len(frame.Body), len(body))
	}
}

func TestFrameTooLarge(t *testing.T) {
	var buf bytes.Buffer
	body := make([]byte, 100)
	WriteFrame(&buf, frameTypeAMQP, 0, body)

	_, err := ReadFrame(&buf, 50)
	if err == nil {
		t.Error("expected error for oversized frame")
	}
}

func TestBuildOpen(t *testing.T) {
	body := BuildOpen("container-1", "localhost")
	if len(body) == 0 {
		t.Fatal("BuildOpen returned empty body")
	}
	if body[0] != 0x00 {
		t.Errorf("expected described type marker 0x00, got 0x%02x", body[0])
	}
}

func TestBuildBegin(t *testing.T) {
	body := BuildBegin(0, 1, 65535, 65535, 4294967295)
	if len(body) == 0 {
		t.Fatal("BuildBegin returned empty body")
	}
}

func TestBuildAttach(t *testing.T) {
	body := BuildAttach("link-1", 0, 0, "test-topic")
	if len(body) == 0 {
		t.Fatal("BuildAttach returned empty body")
	}
}

func TestBuildClose(t *testing.T) {
	body := BuildClose()
	if len(body) == 0 {
		t.Fatal("BuildClose returned empty body")
	}
}

func TestBuildEnd(t *testing.T) {
	body := BuildEnd()
	if len(body) == 0 {
		t.Fatal("BuildEnd returned empty body")
	}
	desc, _, err := ParseDescribedType(body)
	if err != nil {
		t.Fatalf("ParseDescribedType: %v", err)
	}
	if desc != descEnd {
		t.Errorf("descriptor = 0x%x, want 0x%x", desc, descEnd)
	}
}

func TestBuildDetach(t *testing.T) {
	body := BuildDetach(1, true)
	if len(body) == 0 {
		t.Fatal("BuildDetach returned empty body")
	}
	desc, _, err := ParseDescribedType(body)
	if err != nil {
		t.Fatalf("ParseDescribedType: %v", err)
	}
	if desc != descDetach {
		t.Errorf("descriptor = 0x%x, want 0x%x", desc, descDetach)
	}
}

func TestBuildDisposition(t *testing.T) {
	body := BuildDisposition(1, 0, 10, true, "accepted") // role 1=receiver
	if len(body) == 0 {
		t.Fatal("BuildDisposition returned empty body")
	}
	desc, _, err := ParseDescribedType(body)
	if err != nil {
		t.Fatalf("ParseDescribedType: %v", err)
	}
	if desc != descDisposition {
		t.Errorf("descriptor = 0x%x, want 0x%x", desc, descDisposition)
	}
}

func TestParseDescribedType(t *testing.T) {
	body := BuildOpen("test-container", "test-host")

	desc, value, err := ParseDescribedType(body)
	if err != nil {
		t.Fatalf("ParseDescribedType: %v", err)
	}
	if desc != descOpen {
		t.Errorf("descriptor = 0x%x, want 0x%x", desc, descOpen)
	}
	if len(value) == 0 {
		t.Error("expected non-empty value")
	}
}

func TestTypeReaderValues(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want interface{}
	}{
		{"null", []byte{typeNull}, nil},
		{"ubyte", []byte{typeUbyte, 42}, byte(42)},
		{"ushort", []byte{typeUshort, 0x01, 0x00}, uint16(256)},
		{"uint", []byte{typeUint, 0, 0, 0x01, 0x00}, uint32(256)},
		{"string", []byte{typeStr32, 5, 'h', 'e', 'l', 'l', 'o'}, []byte("hello")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := newTypeReader(tt.data)
			got, err := tr.readAny()
			if err != nil {
				t.Fatalf("readAny: %v", err)
			}
			switch v := tt.want.(type) {
			case nil:
				if got != nil {
					t.Errorf("got %v, want nil", got)
				}
			case byte:
				if g, ok := got.(byte); !ok || g != v {
					t.Errorf("got %v, want %v", got, v)
				}
			case uint16:
				if g, ok := got.(uint16); !ok || g != v {
					t.Errorf("got %v, want %v", got, v)
				}
			case uint32:
				if g, ok := got.(uint32); !ok || g != v {
					t.Errorf("got %v, want %v", got, v)
				}
			case []byte:
				if g, ok := got.([]byte); !ok || string(g) != string(v) {
					t.Errorf("got %v, want %v", got, v)
				}
			}
		})
	}
}

func TestSASLMechanisms(t *testing.T) {
	body := BuildSASLMechanisms()
	if len(body) == 0 {
		t.Fatal("BuildSASLMechanisms returned empty")
	}
	desc, _, err := ParseDescribedType(body)
	if err != nil {
		t.Fatalf("ParseDescribedType: %v", err)
	}
	if desc != descSASLMechanisms {
		t.Errorf("descriptor = 0x%x, want 0x%x", desc, descSASLMechanisms)
	}
}

func TestSASLOutcome(t *testing.T) {
	body := BuildSASLOutcome(0)
	desc, _, err := ParseDescribedType(body)
	if err != nil {
		t.Fatalf("ParseDescribedType: %v", err)
	}
	if desc != descSASLOutcome {
		t.Errorf("descriptor = 0x%x, want 0x%x", desc, descSASLOutcome)
	}
}

func TestEncodePlainSASL(t *testing.T) {
	encoded := EncodePlainSASL("user", "pass")
	if len(encoded) == 0 {
		t.Fatal("EncodePlainSASL returned empty")
	}
	// EncodePlainSASL returns base64("\x00user\x00pass")
	if len(encoded) == 0 {
		t.Error("encoded result should not be empty")
	}
	// Verify roundtrip: decode and check null-separated fields
	decoded, err := base64.StdEncoding.DecodeString(string(encoded))
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	if decoded[0] != 0 {
		t.Error("decoded PLAIN SASL should start with null byte")
	}
}

func TestFrameRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	openBody := BuildOpen("test", "localhost")
	WriteFrame(&buf, frameTypeAMQP, 0, openBody)

	frame, err := ReadFrame(&buf, defaultMaxFrameSize)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	desc, _, err := ParseDescribedType(frame.Body)
	if err != nil {
		t.Fatalf("ParseDescribedType: %v", err)
	}
	if desc != descOpen {
		t.Errorf("descriptor = 0x%x, want 0x%x", desc, descOpen)
	}
}

func TestNewServer(t *testing.T) {
	s := NewServer(nil)
	if s == nil {
		t.Fatal("server should not be nil")
	}
}

func TestServerStop(t *testing.T) {
	s := NewServer(nil)
	s.Stop()
}
