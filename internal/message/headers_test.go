package message

import (
	"bytes"
	"reflect"
	"testing"
)

func TestMarshalUnmarshalHeadersEmpty(t *testing.T) {
	encoded := marshalHeaders(nil)
	if encoded != nil {
		t.Errorf("expected nil for empty headers")
	}

	decoded := unmarshalHeaders(nil)
	if len(decoded) != 0 {
		t.Errorf("expected empty map")
	}
}

func TestMarshalUnmarshalHeadersSingle(t *testing.T) {
	original := map[string][]byte{
		"content-type": []byte("application/json"),
	}
	encoded := marshalHeaders(original)
	decoded := unmarshalHeaders(encoded)

	if !reflect.DeepEqual(original, decoded) {
		t.Errorf("roundtrip failed: %v != %v", original, decoded)
	}
}

func TestMarshalUnmarshalHeadersMultiple(t *testing.T) {
	original := map[string][]byte{
		"trace-id":   []byte("abc123"),
		"source":     []byte("test"),
		"priority":   []byte("high"),
	}
	encoded := marshalHeaders(original)
	decoded := unmarshalHeaders(encoded)

	if !reflect.DeepEqual(original, decoded) {
		t.Errorf("roundtrip failed")
	}
}

func TestMarshalUnmarshalHeadersEmptyValue(t *testing.T) {
	original := map[string][]byte{
		"empty": {},
	}
	encoded := marshalHeaders(original)
	decoded := unmarshalHeaders(encoded)

	if !reflect.DeepEqual(original, decoded) {
		t.Errorf("roundtrip failed for empty value")
	}
}

func TestMarshalUnmarshalHeadersBinaryValue(t *testing.T) {
	original := map[string][]byte{
		"binary": {0x00, 0x01, 0x02, 0xFF},
	}
	encoded := marshalHeaders(original)
	decoded := unmarshalHeaders(encoded)

	if !bytes.Equal(original["binary"], decoded["binary"]) {
		t.Errorf("binary value roundtrip failed")
	}
}

func TestHeadersSize(t *testing.T) {
	headers := map[string][]byte{
		"key1": []byte("val1"),
		"ab":   []byte("cd"),
	}
	expected := (2 + 4 + 4 + 4) + (2 + 2 + 4 + 2) // key1+val1 + ab+cd
	actual := headersSize(headers)
	if actual != expected {
		t.Errorf("headersSize = %d, want %d", actual, expected)
	}

	encoded := marshalHeaders(headers)
	if len(encoded) != actual {
		t.Errorf("encoded len %d != headersSize %d", len(encoded), actual)
	}
}

func TestUnmarshalHeadersTruncatedKeyLen(t *testing.T) {
	// Only 1 byte — can't read uint16 key length
	data := []byte{0x00}
	result := unmarshalHeaders(data)
	if len(result) != 0 {
		t.Errorf("expected empty map for truncated key len, got %d", len(result))
	}
}

func TestUnmarshalHeadersTruncatedKey(t *testing.T) {
	// keyLen=10 but only 2 bytes of key data
	var data []byte
	data = append(data, 0x00, 0x0A) // keyLen=10
	data = append(data, "ab"...)     // only 2 bytes
	result := unmarshalHeaders(data)
	if len(result) != 0 {
		t.Errorf("expected empty map for truncated key, got %d", len(result))
	}
}

func TestUnmarshalHeadersTruncatedValLen(t *testing.T) {
	// key complete but valLen truncated
	var data []byte
	data = append(data, 0x00, 0x03) // keyLen=3
	data = append(data, "key"...)   // key data
	data = append(data, 0x00)       // only 1 byte of valLen
	result := unmarshalHeaders(data)
	if len(result) != 0 {
		t.Errorf("expected empty map for truncated valLen, got %d", len(result))
	}
}

func TestUnmarshalHeadersTruncatedVal(t *testing.T) {
	// key and valLen complete but val truncated
	var data []byte
	data = append(data, 0x00, 0x03) // keyLen=3
	data = append(data, "key"...)   // key data
	data = append(data, 0x00, 0x00, 0x00, 0x0A) // valLen=10
	data = append(data, "val"...)   // only 3 bytes of val
	result := unmarshalHeaders(data)
	if len(result) != 0 {
		t.Errorf("expected empty map for truncated val, got %d", len(result))
	}
}

func TestUnmarshalHeadersPartialThenValid(t *testing.T) {
	// First header valid, second truncated
	var data []byte
	// Header 1: key="a", val="b"
	data = append(data, 0x00, 0x01) // keyLen=1
	data = append(data, 'a')
	data = append(data, 0x00, 0x00, 0x00, 0x01) // valLen=1
	data = append(data, 'b')
	// Header 2: truncated (only keyLen bytes)
	data = append(data, 0x00, 0x05)

	result := unmarshalHeaders(data)
	if len(result) != 1 {
		t.Errorf("expected 1 header (partial second), got %d", len(result))
	}
	if string(result["a"]) != "b" {
		t.Errorf("header a = %q, want b", result["a"])
	}
}

func TestHeadersSizeEmpty(t *testing.T) {
	if headersSize(nil) != 0 {
		t.Error("headersSize(nil) should be 0")
	}
	if headersSize(map[string][]byte{}) != 0 {
		t.Error("headersSize(empty) should be 0")
	}
}
