package dlq

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

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

// --- Replay Conditions ---

func TestAllMessages(t *testing.T) {
	cond := AllMessages()
	if !cond(&DLQEntry{}) {
		t.Error("AllMessages should match everything")
	}
}

func TestByReason(t *testing.T) {
	cond := ByReason("timeout")
	if !cond(&DLQEntry{Reason: "timeout"}) {
		t.Error("should match exact reason")
	}
	if cond(&DLQEntry{Reason: "error"}) {
		t.Error("should not match different reason")
	}
}

func TestByRetryCount(t *testing.T) {
	cond := ByRetryCount(3)
	if !cond(&DLQEntry{Retries: 3}) {
		t.Error("should match exact retry count")
	}
	if !cond(&DLQEntry{Retries: 5}) {
		t.Error("should match higher retry count")
	}
	if cond(&DLQEntry{Retries: 2}) {
		t.Error("should not match lower retry count")
	}
}

func TestByTimeRange(t *testing.T) {
	now := time.Now()
	cond := ByTimeRange(now.Add(-1*time.Hour), now.Add(1*time.Hour))
	if !cond(&DLQEntry{FailedAt: now}) {
		t.Error("should match time in range")
	}
	if cond(&DLQEntry{FailedAt: now.Add(-2 * time.Hour)}) {
		t.Error("should not match time before range")
	}
	if cond(&DLQEntry{FailedAt: now.Add(2 * time.Hour)}) {
		t.Error("should not match time after range")
	}
}

func TestByPayloadContains(t *testing.T) {
	cond := ByPayloadContains("error")
	if !cond(&DLQEntry{OriginalMsg: &message.Envelope{Payload: []byte("system error")}}) {
		t.Error("should match payload containing substring")
	}
	if cond(&DLQEntry{OriginalMsg: &message.Envelope{Payload: []byte("success")}}) {
		t.Error("should not match unrelated payload")
	}
	if cond(&DLQEntry{OriginalMsg: nil}) {
		t.Error("should not match nil message")
	}
}

func TestCompositeAND(t *testing.T) {
	cond := CompositeAND(ByReason("timeout"), ByRetryCount(3))
	if !cond(&DLQEntry{Reason: "timeout", Retries: 3}) {
		t.Error("should match when all conditions true")
	}
	if cond(&DLQEntry{Reason: "timeout", Retries: 2}) {
		t.Error("should not match when one condition false")
	}
}

func TestCompositeOR(t *testing.T) {
	cond := CompositeOR(ByReason("timeout"), ByReason("error"))
	if !cond(&DLQEntry{Reason: "timeout"}) {
		t.Error("should match first condition")
	}
	if !cond(&DLQEntry{Reason: "error"}) {
		t.Error("should match second condition")
	}
	if cond(&DLQEntry{Reason: "success"}) {
		t.Error("should not match when neither condition true")
	}
}

// --- Replay Transforms ---

func TestNoTransform(t *testing.T) {
	msg := &message.Envelope{Payload: []byte("data")}
	tf := NoTransform()
	result := tf(&DLQEntry{OriginalMsg: msg})
	if result != msg {
		t.Error("NoTransform should return original message")
	}
	if tf(&DLQEntry{OriginalMsg: nil}) != nil {
		t.Error("NoTransform should return nil for nil message")
	}
}

func TestAddHeader(t *testing.T) {
	tf := AddHeader("x-key", []byte("value"))
	result := tf(&DLQEntry{OriginalMsg: &message.Envelope{Payload: []byte("data")}})
	if string(result.Headers["x-key"]) != "value" {
		t.Error("AddHeader should add header")
	}
}

func TestRemoveHeader(t *testing.T) {
	tf := RemoveHeader("x-key")
	result := tf(&DLQEntry{OriginalMsg: &message.Envelope{
		Payload: []byte("data"),
		Headers: map[string][]byte{"x-key": []byte("value")},
	}})
	if _, ok := result.Headers["x-key"]; ok {
		t.Error("RemoveHeader should remove header")
	}
}

func TestUpdatePayload(t *testing.T) {
	tf := UpdatePayload(func(b []byte) []byte { return append(b, []byte("-fixed")...) })
	result := tf(&DLQEntry{OriginalMsg: &message.Envelope{Payload: []byte("data")}})
	if string(result.Payload) != "data-fixed" {
		t.Error("UpdatePayload should modify payload")
	}
}

func TestSetRoutingKey(t *testing.T) {
	tf := SetRoutingKey("orders")
	result := tf(&DLQEntry{OriginalMsg: &message.Envelope{Payload: []byte("data")}})
	if result.RoutingKey != "orders" {
		t.Error("SetRoutingKey should set routing key")
	}
}

func TestAddDLQMetadata(t *testing.T) {
	now := time.Now()
	tf := AddDLQMetadata()
	result := tf(&DLQEntry{
		OriginalMsg: &message.Envelope{Payload: []byte("data")},
		Reason:      "timeout",
		Retries:     3,
		FailedAt:    now,
	})
	if string(result.Headers["x-dlq-original-failure"]) != "timeout" {
		t.Error("should add failure reason header")
	}
	if string(result.Headers["x-dlq-retry-count"]) != "3" {
		t.Error("should add retry count header")
	}
}

func TestChainTransforms(t *testing.T) {
	tf := ChainTransforms(
		AddHeader("x-step", []byte("1")),
		AddHeader("x-step", []byte("2")),
	)
	result := tf(&DLQEntry{OriginalMsg: &message.Envelope{Payload: []byte("data")}})
	if string(result.Headers["x-step"]) != "2" {
		t.Error("ChainTransforms should apply transforms in order")
	}
}

func TestChainTransformsNilReturn(t *testing.T) {
	tf := ChainTransforms(
		NoTransform(),
		func(e *DLQEntry) *message.Envelope { return nil },
	)
	result := tf(&DLQEntry{OriginalMsg: &message.Envelope{Payload: []byte("data")}})
	if result != nil {
		t.Error("ChainTransforms should return nil if any transform returns nil")
	}
}

// --- Replay Operations ---

func TestReplayWithOptionsBasic(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Payload: []byte("a")}, "orders", 0, "err", 3)

	result, err := d.ReplayWithOptions("orders", DefaultReplayOptions(), func(m *message.Envelope, topic string) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ReplayedCount != 1 {
		t.Errorf("replayed = %d, want 1", result.ReplayedCount)
	}
	if d.Size("orders") != 1 {
		t.Error("entry should remain when DeleteAfterReplay is false")
	}
}

func TestReplayWithOptionsDryRun(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Payload: []byte("a")}, "orders", 0, "err", 3)

	opts := DefaultReplayOptions()
	opts.DryRun = true
	result, err := d.ReplayWithOptions("orders", opts, func(m *message.Envelope, topic string) error {
		t.Error("publisher should not be called in dry run")
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SkippedCount != 1 {
		t.Errorf("skipped = %d, want 1", result.SkippedCount)
	}
	if d.Size("orders") != 1 {
		t.Error("entries should not be removed in dry run")
	}
}

func TestReplayWithOptionsDeleteAfterReplay(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Payload: []byte("a")}, "orders", 0, "err", 3)

	opts := DefaultReplayOptions()
	opts.DeleteAfterReplay = true
	_, err := d.ReplayWithOptions("orders", opts, func(m *message.Envelope, topic string) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Size("orders") != 0 {
		t.Error("entry should be removed when DeleteAfterReplay is true")
	}
}

func TestReplayWithOptionsMaxMessages(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	for i := 0; i < 5; i++ {
		d.Push(&message.Envelope{Payload: []byte("a")}, "orders", 0, "err", 3)
	}

	opts := DefaultReplayOptions()
	opts.MaxMessages = 2
	result, _ := d.ReplayWithOptions("orders", opts, func(m *message.Envelope, topic string) error {
		return nil
	})
	if result.ReplayedCount != 2 {
		t.Errorf("replayed = %d, want 2", result.ReplayedCount)
	}
}

func TestReplayWithOptionsTargetTopic(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Payload: []byte("a")}, "orders", 0, "err", 3)

	opts := DefaultReplayOptions()
	opts.TargetTopic = "fixed-orders"
	var receivedTopic string
	_, _ = d.ReplayWithOptions("orders", opts, func(m *message.Envelope, topic string) error {
		receivedTopic = topic
		return nil
	})
	if receivedTopic != "fixed-orders" {
		t.Errorf("target topic = %q, want fixed-orders", receivedTopic)
	}
}

func TestReplayWithOptionsCondition(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Payload: []byte("a")}, "orders", 0, "timeout", 3)
	d.Push(&message.Envelope{Payload: []byte("b")}, "orders", 0, "error", 3)

	opts := DefaultReplayOptions()
	opts.Condition = ByReason("timeout")
	result, _ := d.ReplayWithOptions("orders", opts, func(m *message.Envelope, topic string) error {
		return nil
	})
	if result.ReplayedCount != 1 {
		t.Errorf("replayed = %d, want 1", result.ReplayedCount)
	}
	if result.MatchedEntries != 1 {
		t.Errorf("matched = %d, want 1", result.MatchedEntries)
	}
}

func TestReplayWithOptionsPublishFailure(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Payload: []byte("a")}, "orders", 0, "err", 3)

	result, _ := d.ReplayWithOptions("orders", DefaultReplayOptions(), func(m *message.Envelope, topic string) error {
		return fmt.Errorf("publish failed")
	})
	if result.FailedCount != 1 {
		t.Errorf("failed = %d, want 1", result.FailedCount)
	}
	if len(result.Errors) != 1 {
		t.Errorf("errors = %d, want 1", len(result.Errors))
	}
	if d.Size("orders") != 1 {
		t.Error("failed entry should remain in DLQ")
	}
}

func TestReplayWithOptionsTransformFailure(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Payload: []byte("a")}, "orders", 0, "err", 3)

	opts := DefaultReplayOptions()
	opts.Transform = func(e *DLQEntry) *message.Envelope { return nil }
	result, _ := d.ReplayWithOptions("orders", opts, func(m *message.Envelope, topic string) error {
		return nil
	})
	if result.FailedCount != 1 {
		t.Errorf("failed = %d, want 1", result.FailedCount)
	}
}

func TestReplayWithOptionsNotEnabled(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: false})
	_, err := d.ReplayWithOptions("orders", DefaultReplayOptions(), func(m *message.Envelope, topic string) error {
		return nil
	})
	if err == nil {
		t.Error("expected error when DLQ is disabled")
	}
}

func TestReplayWithOptionsEmptyQueue(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	result, err := d.ReplayWithOptions("orders", DefaultReplayOptions(), func(m *message.Envelope, topic string) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalEntries != 0 {
		t.Errorf("total = %d, want 0", result.TotalEntries)
	}
}

func TestReplayPreview(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Payload: []byte("a")}, "orders", 0, "timeout", 3)
	d.Push(&message.Envelope{Payload: []byte("b")}, "orders", 0, "error", 3)

	opts := DefaultReplayOptions()
	opts.Condition = ByReason("timeout")
	entries, err := d.ReplayPreview("orders", opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("preview entries = %d, want 1", len(entries))
	}
	if d.Size("orders") != 2 {
		t.Error("ReplayPreview should not modify DLQ")
	}
}

func TestReplayPreviewNotEnabled(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: false})
	_, err := d.ReplayPreview("orders", DefaultReplayOptions())
	if err == nil {
		t.Error("expected error when DLQ is disabled")
	}
}

func TestExportToJSON(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{Payload: []byte("a")}, "orders", 0, "err", 3)

	data, err := d.ExportToJSON("orders", DefaultReplayOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("export should return non-empty JSON")
	}
}

// --- Security / Path Traversal ---

func TestDLQPushInvalidTopicName(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{}, "../etc/passwd", 0, "err", 3)
	if d.TotalSize() != 0 {
		t.Error("should reject path traversal topic names")
	}
}

func TestDLQPushEmptyTopicName(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{}, "", 0, "err", 3)
	if d.TotalSize() != 0 {
		t.Error("should reject empty topic names")
	}
}

func TestDLQPushTopicWithSlash(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{}, "a/b", 0, "err", 3)
	if d.TotalSize() != 0 {
		t.Error("should reject topic names with slashes")
	}
}

func TestDLQPushTopicTooLong(t *testing.T) {
	d := newTestDLQ(t, Config{Enabled: true})
	d.Push(&message.Envelope{}, string(make([]byte, 257)), 0, "err", 3)
	if d.TotalSize() != 0 {
		t.Error("should reject topic names longer than 256 chars")
	}
}

// --- Persistence ---

func TestDLQPersistence(t *testing.T) {
	dir := t.TempDir()
	d := newTestDLQ(t, Config{Enabled: true, DataDir: dir})
	d.Push(&message.Envelope{Payload: []byte("persistent")}, "orders", 0, "err", 3)

	// Create new DLQ pointing to same dir
	d2 := newTestDLQ(t, Config{Enabled: true, DataDir: dir})
	if d2.Size("orders") != 1 {
		t.Errorf("loaded size = %d, want 1", d2.Size("orders"))
	}
	entries := d2.Peek("orders", 0)
	if string(entries[0].OriginalMsg.Payload) != "persistent" {
		t.Error("loaded entry payload mismatch")
	}
}

func TestDLQClearRemovesFile(t *testing.T) {
	dir := t.TempDir()
	d := newTestDLQ(t, Config{Enabled: true, DataDir: dir})
	d.Push(&message.Envelope{}, "orders", 0, "err", 3)

	d.Clear("orders")
	if d.Size("orders") != 0 {
		t.Error("should be empty after clear")
	}

	// Verify file is removed on reload
	d2 := newTestDLQ(t, Config{Enabled: true, DataDir: dir})
	if d2.Size("orders") != 0 {
		t.Error("file should be removed after clear")
	}
}

func TestDLQLoadAllSkipsInvalidNames(t *testing.T) {
	dir := t.TempDir()
	// Create a file with invalid topic name
	f, _ := os.Create(filepath.Join(dir, "../bad.jsonl"))
	if f != nil {
		f.Close()
	}

	d := newTestDLQ(t, Config{Enabled: true, DataDir: dir})
	if d.TotalSize() != 0 {
		t.Error("should skip files with invalid topic names")
	}
}
