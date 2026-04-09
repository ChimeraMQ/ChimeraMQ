package message

import (
	"encoding/binary"
)

// marshalHeaders encodes headers to TLV binary: [key_len:uint16][key][val_len:uint32][val]
func marshalHeaders(headers map[string][]byte) []byte {
	if len(headers) == 0 {
		return nil
	}

	size := 0
	for k, v := range headers {
		size += 2 + len(k) + 4 + len(v)
	}

	buf := make([]byte, 0, size)
	for k, v := range headers {
		var kl [2]byte
		var vl [4]byte
		binary.BigEndian.PutUint16(kl[:], uint16(len(k)))
		binary.BigEndian.PutUint32(vl[:], uint32(len(v)))
		buf = append(buf, kl[:]...)
		buf = append(buf, k...)
		buf = append(buf, vl[:]...)
		buf = append(buf, v...)
	}
	return buf
}

// unmarshalHeaders decodes TLV binary back to map.
func unmarshalHeaders(data []byte) map[string][]byte {
	headers := make(map[string][]byte)
	pos := 0
	for pos < len(data) {
		if pos+2 > len(data) {
			break
		}
		keyLen := int(binary.BigEndian.Uint16(data[pos:]))
		pos += 2
		if pos+keyLen > len(data) {
			break
		}
		key := string(data[pos : pos+keyLen])
		pos += keyLen
		if pos+4 > len(data) {
			break
		}
		valLen := int(binary.BigEndian.Uint32(data[pos:]))
		pos += 4
		if pos+valLen > len(data) {
			break
		}
		val := make([]byte, valLen)
		copy(val, data[pos:pos+valLen])
		pos += valLen
		headers[key] = val
	}
	return headers
}

// headersSize returns the binary size of the headers map.
func headersSize(headers map[string][]byte) int {
	size := 0
	for k, v := range headers {
		size += 2 + len(k) + 4 + len(v)
	}
	return size
}
