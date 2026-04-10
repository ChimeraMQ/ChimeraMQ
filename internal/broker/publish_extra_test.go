package broker

import (
	"testing"

	"github.com/chimeramq/chimera/internal/message"
)

func TestParseSchemaIDFromHeaders_NilHeaders(t *testing.T) {
	env := &message.Envelope{}
	id, ok := parseSchemaIDFromHeaders(env)
	if ok {
		t.Error("expected ok=false for nil headers")
	}
	if id != 0 {
		t.Errorf("expected id=0, got %d", id)
	}
}

func TestParseSchemaIDFromHeaders_MissingKey(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{"other": []byte("value")},
	}
	id, ok := parseSchemaIDFromHeaders(env)
	if ok {
		t.Error("expected ok=false for missing key")
	}
	if id != 0 {
		t.Errorf("expected id=0, got %d", id)
	}
}

func TestParseSchemaIDFromHeaders_InvalidValue(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{"x-chimera-schema-id": []byte("not-a-number")},
	}
	id, ok := parseSchemaIDFromHeaders(env)
	if ok {
		t.Error("expected ok=false for non-numeric value")
	}
	if id != 0 {
		t.Errorf("expected id=0, got %d", id)
	}
}

func TestParseSchemaIDFromHeaders_Valid(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{"x-chimera-schema-id": []byte("42")},
	}
	id, ok := parseSchemaIDFromHeaders(env)
	if !ok {
		t.Error("expected ok=true for valid value")
	}
	if id != 42 {
		t.Errorf("expected id=42, got %d", id)
	}
}

func TestParseSchemaIDFromHeaders_Zero(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{"x-chimera-schema-id": []byte("0")},
	}
	id, ok := parseSchemaIDFromHeaders(env)
	if !ok {
		t.Error("expected ok=true for zero value")
	}
	if id != 0 {
		t.Errorf("expected id=0, got %d", id)
	}
}

func TestParseSchemaIDFromHeaders_Overflow(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{"x-chimera-schema-id": []byte("4294967296")}, // max uint32 + 1
	}
	id, ok := parseSchemaIDFromHeaders(env)
	if ok {
		t.Error("expected ok=false for overflow value")
	}
	if id != 0 {
		t.Errorf("expected id=0, got %d", id)
	}
}

func TestExtractProducerInfo_NilHeaders(t *testing.T) {
	env := &message.Envelope{}
	pid, seq, ok := extractProducerInfo(env)
	if ok {
		t.Error("expected ok=false for nil headers")
	}
	if pid != "" {
		t.Errorf("expected empty pid, got %q", pid)
	}
	if seq != 0 {
		t.Errorf("expected seq=0, got %d", seq)
	}
}

func TestExtractProducerInfo_MissingProducerID(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{"x-chimera-producer-seq": []byte("5")},
	}
	pid, seq, ok := extractProducerInfo(env)
	if ok {
		t.Error("expected ok=false when producer-id missing")
	}
	if pid != "" {
		t.Errorf("expected empty pid, got %q", pid)
	}
	if seq != 0 {
		t.Errorf("expected seq=0, got %d", seq)
	}
}

func TestExtractProducerInfo_EmptyProducerID(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{
			"x-chimera-producer-id":  []byte(""),
			"x-chimera-producer-seq": []byte("5"),
		},
	}
	pid, _, ok := extractProducerInfo(env)
	if ok {
		t.Error("expected ok=false when producer-id is empty")
	}
	if pid != "" {
		t.Errorf("expected empty pid, got %q", pid)
	}
}

func TestExtractProducerInfo_MissingSeq(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{
			"x-chimera-producer-id": []byte("producer-1"),
		},
	}
	pid, seq, ok := extractProducerInfo(env)
	if ok {
		t.Error("expected ok=false when seq missing")
	}
	if pid != "producer-1" {
		t.Errorf("expected pid=producer-1, got %q", pid)
	}
	if seq != 0 {
		t.Errorf("expected seq=0, got %d", seq)
	}
}

func TestExtractProducerInfo_EmptySeq(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{
			"x-chimera-producer-id":  []byte("producer-1"),
			"x-chimera-producer-seq": []byte(""),
		},
	}
	pid, _, ok := extractProducerInfo(env)
	if ok {
		t.Error("expected ok=false when seq empty")
	}
	if pid != "producer-1" {
		t.Errorf("expected pid=producer-1, got %q", pid)
	}
}

func TestExtractProducerInfo_InvalidSeq(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{
			"x-chimera-producer-id":  []byte("producer-1"),
			"x-chimera-producer-seq": []byte("not-a-number"),
		},
	}
	pid, _, ok := extractProducerInfo(env)
	if ok {
		t.Error("expected ok=false for non-numeric seq")
	}
	if pid != "producer-1" {
		t.Errorf("expected pid=producer-1, got %q", pid)
	}
}

func TestExtractProducerInfo_Valid(t *testing.T) {
	env := &message.Envelope{
		Headers: map[string][]byte{
			"x-chimera-producer-id":  []byte("producer-42"),
			"x-chimera-producer-seq": []byte("12345"),
		},
	}
	pid, seq, ok := extractProducerInfo(env)
	if !ok {
		t.Error("expected ok=true for valid input")
	}
	if pid != "producer-42" {
		t.Errorf("expected pid=producer-42, got %q", pid)
	}
	if seq != 12345 {
		t.Errorf("expected seq=12345, got %d", seq)
	}
}
