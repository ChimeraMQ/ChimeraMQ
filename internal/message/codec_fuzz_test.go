package message

import (
	"bytes"
	"testing"
)

// FuzzMarshalUnmarshal verifies that Marshal and Unmarshal are inverse
// operations for valid envelopes and handle arbitrary input safely.
func FuzzMarshalUnmarshal(f *testing.F) {
	// Seed with valid envelope
	env := &Envelope{
		Topic:   "test-topic",
		Payload: []byte("hello"),
	}
	data, err := Marshal(env)
	if err == nil {
		f.Add(data)
	}

	// Seed with known-bad inputs
	f.Add([]byte{})
	f.Add([]byte("not a chimera message"))
	f.Add([]byte{0x00, 0x01, 0x02, 0x03})
	f.Add([]byte("CHIMERA\x01"))
	f.Add(bytes.Repeat([]byte{0x00}, 64))
	f.Add(bytes.Repeat([]byte{0xFF}, 64))
	f.Add([]byte("CHIMERA\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))

	f.Fuzz(func(t *testing.T, data []byte) {
		env, err := Unmarshal(data)
		if err != nil {
			return // expected for malformed data
		}
		// If unmarshal succeeded, marshal should not panic
		out, err := Marshal(env)
		if err != nil {
			return
		}
		// Round-trip: unmarshaled -> marshaled -> unmarshaled should match
		env2, err := Unmarshal(out)
		if err != nil {
			t.Errorf("round-trip unmarshal failed: %v", err)
			return
		}
		if env2.Topic != env.Topic {
			t.Errorf("topic mismatch: %q vs %q", env2.Topic, env.Topic)
		}
		if !bytes.Equal(env2.Payload, env.Payload) {
			t.Errorf("payload mismatch")
		}
	})
}

// FuzzParseHeaders verifies that header parsing handles arbitrary input safely.
func FuzzParseHeaders(f *testing.F) {
	f.Add([]byte(`{"key":"value"}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"a":"b","c":"d"}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"key":`))         // truncated
	f.Add(bytes.Repeat([]byte{0x00}, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		env := &Envelope{
			Topic:   "fuzz-topic",
			Headers: map[string][]byte{"fuzz-key": data},
		}
		out, err := Marshal(env)
		if err != nil {
			return
		}
		_, err = Unmarshal(out)
		if err != nil {
			t.Errorf("unmarshal failed with valid headers: %v", err)
		}
	})
}

// FuzzLargePayload verifies Marshal handles large payloads without panics.
func FuzzLargePayload(f *testing.F) {
	f.Add([]byte("small"))
	f.Add(make([]byte, 1024))
	f.Add(make([]byte, 4096))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Cap at 64KB to avoid OOM during fuzzing
		if len(data) > 65536 {
			data = data[:65536]
		}
		env := &Envelope{
			Topic:   "fuzz-large",
			Payload: data,
		}
		out, err := Marshal(env)
		if err != nil {
			t.Errorf("marshal failed: %v", err)
			return
		}
		env2, err := Unmarshal(out)
		if err != nil {
			t.Errorf("unmarshal failed: %v", err)
			return
		}
		if !bytes.Equal(env2.Payload, data) {
			t.Errorf("payload mismatch after round-trip")
		}
	})
}
