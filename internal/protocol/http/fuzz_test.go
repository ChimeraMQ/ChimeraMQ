package http

import (
	"bytes"
	"testing"
)

// FuzzDetect verifies that the HTTP protocol detector handles arbitrary
// input safely — no panics on malformed or truncated data.
func FuzzDetect(f *testing.F) {
	// Seed with valid HTTP method prefixes
	f.Add([]byte("GET /v1/topics"))
	f.Add([]byte("POST /v1/messages/test"))
	f.Add([]byte("PUT /v1/topics"))
	f.Add([]byte("DELETE /v1/topics"))
	f.Add([]byte("OPTIONS"))
	f.Add([]byte("PATCH /v1/topics"))
	f.Add([]byte("HEAD /v1/health"))
	f.Add([]byte("CONNECT"))

	// Known-bad inputs
	f.Add([]byte{})
	f.Add([]byte("G"))   // too short
	f.Add([]byte("GET")) // exactly 3 bytes
	f.Add(bytes.Repeat([]byte{0x00}, 64))
	f.Add(bytes.Repeat([]byte{0xFF}, 64))
	f.Add([]byte("GETX")) // not a valid method
	f.Add([]byte("NOTHTTP"))

	f.Fuzz(func(t *testing.T, data []byte) {
		d := &Detector{}
		_ = d.Detect(data)
	})
}

// FuzzValidateTopicName verifies that topic name validation handles
// arbitrary bytes safely — no panics or pathological behavior.
func FuzzValidateTopicName(f *testing.F) {
	// Seed with valid topic names
	f.Add([]byte("test-topic"))
	f.Add([]byte("a/b/c")) // valid chars but contains /
	f.Add([]byte("sensor.temperature"))
	f.Add([]byte("metrics/v2/errors"))
	f.Add([]byte("a"))
	f.Add([]byte("my-topic-123"))

	// Known-bad inputs
	f.Add([]byte{})
	f.Add([]byte(".."))
	f.Add([]byte("../etc/passwd"))
	f.Add([]byte("topic/with/slash"))
	f.Add([]byte("topic\\with\\backslash"))
	f.Add(bytes.Repeat([]byte("a"), 256)) // too long
	f.Add(bytes.Repeat([]byte("."), 100))
	f.Add([]byte("topic..double..dot"))

	f.Fuzz(func(t *testing.T, data []byte) {
		_ = validateTopicName(string(data))
	})
}

// FuzzSanitizeError verifies that error sanitization handles arbitrary
// errors and status codes safely.
func FuzzSanitizeError(f *testing.F) {
	f.Add(200, "OK")
	f.Add(400, "bad request")
	f.Add(404, "topic not found")
	f.Add(500, "internal server error")
	f.Add(500, "panic at /path/to/file.go:123")
	f.Add(400, "file.go: invalid input")
	f.Add(409, "conflict: already exists")
	f.Add(403, "/etc/passwd/forbidden")

	f.Fuzz(func(t *testing.T, status int, msg string) {
		err := testError(msg)
		_ = sanitizeError(status, err)
	})
}

// testError is a simple error wrapper for fuzz testing.
type testError string

func (e testError) Error() string { return string(e) }
