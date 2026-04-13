package dlq

import (
	"regexp"
	"testing"

	"github.com/chimeramq/chimera/internal/message"
)

func newTestDLQ(t *testing.T, cfg Config) *DLQ {
	t.Helper()
	d, err := NewDLQ(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestNewDLQ(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	if !d.Enabled() {
		t.Error("should be enabled")
	}
	if d.MaxRetries() != 3 {
		t.Errorf("default max retries = %d, want 3", d.MaxRetries())
	}
}

func TestNewDLQDefaults(t *testing.T) {
	d := newTestDLQ(t, Config{})
	if d.Enabled() {
		t.Error("should be disabled by default")
	}
}

func TestNewDLQCustomRetries(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true, MaxRetries: 5})
	if d.MaxRetries() != 5 {
		t.Errorf("max retries = %d, want 5", d.MaxRetries())
	}
}

func TestDLQTopic(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	if d.DLQTopic("orders") != "__dlq_orders" {
		t.Errorf("DLQTopic = %q, want %q", d.DLQTopic("orders"), "__dlq_orders")
	}
}

func TestDLQTopicCustomPrefix(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true, TopicPrefix: "dead."})
	if d.DLQTopic("orders") != "dead.orders" {
		t.Errorf("DLQTopic = %q", d.DLQTopic("orders"))
	}
}

func TestDLQPush(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true, MaxRetries: 3})
	msg := &message.Envelope{Topic: "orders", Payload: []byte("test")}
	d.Push(msg, "orders", 0, "processing error", 3)

	if d.Size("orders") != 1 {
		t.Errorf("size = %d, want 1", d.Size("orders"))
	}
}

func TestDLQPushWhenDisabled(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: false})
	msg := &message.Envelope{Topic: "orders", Payload: []byte("test")}
	d.Push(msg, "orders", 0, "error", 1)

	if d.Size("orders") != 0 {
		t.Error("should not push when disabled")
	}
}

func TestDLQPushMultiple(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	for i := 0; i < 5; i++ {
		msg := &message.Envelope{Topic: "orders", Payload: []byte("test")}
		d.Push(msg, "orders", 0, "error", i)
	}
	if d.Size("orders") != 5 {
		t.Errorf("size = %d, want 5", d.Size("orders"))
	}
}

func TestDLQPushMultipleTopics(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Topic: "a"}, "topic-a", 0, "err", 1)
	d.Push(&message.Envelope{Topic: "b"}, "topic-b", 0, "err", 1)

	if d.Size("topic-a") != 1 {
		t.Errorf("topic-a size = %d, want 1", d.Size("topic-a"))
	}
	if d.Size("topic-b") != 1 {
		t.Errorf("topic-b size = %d, want 1", d.Size("topic-b"))
	}
	if d.TotalSize() != 2 {
		t.Errorf("total = %d, want 2", d.TotalSize())
	}
}

func TestDLQShouldDLQ(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true, MaxRetries: 3})

	if d.ShouldDLQ(0) {
		t.Error("retry 0 should not DLQ")
	}
	if d.ShouldDLQ(2) {
		t.Error("retry 2 should not DLQ")
	}
	if !d.ShouldDLQ(3) {
		t.Error("retry 3 should DLQ")
	}
	if !d.ShouldDLQ(5) {
		t.Error("retry 5 should DLQ")
	}
}

func TestDLQShouldDLQDisabled(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: false})
	if d.ShouldDLQ(100) {
		t.Error("should not DLQ when disabled")
	}
}

func TestDLQPeek(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Topic: "orders", Payload: []byte("a")}, "orders", 0, "err", 3)
	d.Push(&message.Envelope{Topic: "orders", Payload: []byte("b")}, "orders", 1, "err", 3)

	entries := d.Peek("orders", 0)
	if len(entries) != 2 {
		t.Fatalf("peek = %d entries, want 2", len(entries))
	}
	if entries[0].Reason != "err" {
		t.Errorf("reason = %q", entries[0].Reason)
	}
}

func TestDLQPeekWithLimit(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	for i := 0; i < 5; i++ {
		d.Push(&message.Envelope{Topic: "orders"}, "orders", 0, "err", 3)
	}
	entries := d.Peek("orders", 3)
	if len(entries) != 3 {
		t.Errorf("peek limit = %d, want 3", len(entries))
	}
}

func TestDLQPeekEmpty(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	if entries := d.Peek("nonexistent", 0); entries != nil {
		t.Error("peek on empty should be nil")
	}
}

func TestDLQPop(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Topic: "orders", Payload: []byte("first")}, "orders", 0, "err", 3)
	d.Push(&message.Envelope{Topic: "orders", Payload: []byte("second")}, "orders", 0, "err", 3)

	first := d.Pop("orders")
	if first == nil {
		t.Fatal("first pop should not be nil")
	}
	if string(first.OriginalMsg.Payload) != "first" {
		t.Errorf("first pop payload = %q", first.OriginalMsg.Payload)
	}
	if d.Size("orders") != 1 {
		t.Errorf("size after pop = %d, want 1", d.Size("orders"))
	}
}

func TestDLQPopEmpty(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	if entry := d.Pop("nonexistent"); entry != nil {
		t.Error("pop on empty should be nil")
	}
}

func TestDLQTopics(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{}, "a", 0, "err", 1)
	d.Push(&message.Envelope{}, "b", 0, "err", 1)

	topics := d.Topics()
	if len(topics) != 2 {
		t.Fatalf("topics count = %d, want 2", len(topics))
	}
}

func TestDLQClear(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{}, "orders", 0, "err", 3)
	d.Clear("orders")
	if d.Size("orders") != 0 {
		t.Error("should be empty after clear")
	}
}

func TestDLQEntryIDs(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{}, "orders", 0, "err", 3)
	d.Push(&message.Envelope{}, "orders", 0, "err", 3)
	d.Push(&message.Envelope{}, "orders", 0, "err", 3)

	entries := d.Peek("orders", 0)
	if entries[0].ID != 1 {
		t.Errorf("first ID = %d, want 1", entries[0].ID)
	}
	if entries[1].ID != 2 {
		t.Errorf("second ID = %d, want 2", entries[1].ID)
	}
	if entries[2].ID != 3 {
		t.Errorf("third ID = %d, want 3", entries[2].ID)
	}
}

func TestDLQEntryString(t *testing.T) {
	e := &DLQEntry{
		ID:        1,
		Topic:     "orders",
		Partition: 2,
		Reason:    "timeout",
		Retries:   3,
	}
	s := e.String()
	if s == "" {
		t.Error("string should not be empty")
	}
}

func TestDLQEntryMetadata(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	msg := &message.Envelope{Topic: "orders", Payload: []byte("data")}
	d.Push(msg, "orders", 3, "deserialization failed", 3)

	entries := d.Peek("orders", 0)
	if len(entries) != 1 {
		t.Fatal("expected 1 entry")
	}
	e := entries[0]
	if e.Topic != "orders" {
		t.Errorf("topic = %q", e.Topic)
	}
	if e.Partition != 3 {
		t.Errorf("partition = %d", e.Partition)
	}
	if e.Reason != "deserialization failed" {
		t.Errorf("reason = %q", e.Reason)
	}
	if e.Retries != 3 {
		t.Errorf("retries = %d", e.Retries)
	}
	if e.FailedAt.IsZero() {
		t.Error("failed at should not be zero")
	}
}

func TestDLQTotalSizeEmpty(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	if d.TotalSize() != 0 {
		t.Error("total size should be 0 for empty DLQ")
	}
}

func TestByReasonPatternMatches(t *testing.T) {
	entry := &DLQEntry{Reason: "connection timeout"}
	cond := ByReasonPattern("timeout")
	if !cond(entry) {
		t.Error("should match 'timeout' substring via regex")
	}
}

func TestByReasonPatternNoMatch(t *testing.T) {
	entry := &DLQEntry{Reason: "processing error"}
	cond := ByReasonPattern("timeout")
	if cond(entry) {
		t.Error("should not match unrelated reason")
	}
}

func TestByReasonPatternInvalidRegex(t *testing.T) {
	entry := &DLQEntry{Reason: "any"}
	cond := ByReasonPattern("(")
	if cond(entry) {
		t.Error("invalid regex should return false")
	}
}

func TestByReasonPatternEmptyPattern(t *testing.T) {
	entry := &DLQEntry{Reason: "any"}
	cond := ByReasonPattern("")
	if cond(entry) {
		t.Error("empty pattern should return false")
	}
}

func TestByReasonPatternTooLong(t *testing.T) {
	entry := &DLQEntry{Reason: "any"}
	cond := ByReasonPattern(string(make([]byte, 257)))
	if cond(entry) {
		t.Error("pattern exceeding 256 chars should return false")
	}
}

func TestCompileRegexWithTimeoutValid(t *testing.T) {
	re, err := compileRegexWithTimeout("^test$", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !re.MatchString("test") {
		t.Error("should match 'test'")
	}
}

func TestCompileRegexWithTimeoutInvalid(t *testing.T) {
	_, err := compileRegexWithTimeout("(", 1)
	if err == nil {
		t.Error("expected error for invalid pattern")
	}
}

func TestMatchWithTimeout(t *testing.T) {
	re := regexp.MustCompile("hello")
	if !matchWithTimeout(re, "hello world", 1) {
		t.Error("should match")
	}
	if matchWithTimeout(re, "goodbye", 1) {
		t.Error("should not match")
	}
}
