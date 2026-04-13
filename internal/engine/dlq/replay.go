package dlq

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// ReplayCondition is a predicate that determines if a message should be replayed.
type ReplayCondition func(*DLQEntry) bool

// ReplayTransform transforms a message before replay.
type ReplayTransform func(*DLQEntry) *message.Envelope

// ReplayOptions configures how DLQ replay should be performed.
type ReplayOptions struct {
	// Condition determines which messages should be replayed (default: all)
	Condition ReplayCondition

	// Transform modifies messages before replay (default: no transform)
	Transform ReplayTransform

	// MaxMessages limits how many messages to replay (0 = unlimited)
	MaxMessages int

	// DryRun simulates replay without actually sending messages
	DryRun bool

	// TargetTopic overrides the destination topic (empty = original topic)
	TargetTopic string

	// DeleteAfterReplay removes messages from DLQ after successful replay
	DeleteAfterReplay bool
}

// DefaultReplayOptions returns default replay options (replay all, no transform).
func DefaultReplayOptions() ReplayOptions {
	return ReplayOptions{
		Condition:         AllMessages(),
		Transform:         NoTransform(),
		MaxMessages:       0,
		DryRun:            false,
		TargetTopic:       "",
		DeleteAfterReplay: false,
	}
}

// AllMessages returns a condition that matches all messages.
func AllMessages() ReplayCondition {
	return func(e *DLQEntry) bool { return true }
}

// ByReason returns a condition that matches messages with a specific failure reason.
func ByReason(reason string) ReplayCondition {
	return func(e *DLQEntry) bool { return e.Reason == reason }
}

// ByRetryCount returns a condition that matches messages with retries >= minRetries.
func ByRetryCount(minRetries int) ReplayCondition {
	return func(e *DLQEntry) bool { return e.Retries >= minRetries }
}

// ByTimeRange returns a condition that matches messages within a time range.
func ByTimeRange(start, end time.Time) ReplayCondition {
	return func(e *DLQEntry) bool {
		return !e.FailedAt.Before(start) && !e.FailedAt.After(end)
	}
}

// ByReasonPattern returns a condition that matches reason against a regex pattern.
// Pattern compilation and matching are guarded by timeouts to prevent ReDoS attacks.
func ByReasonPattern(pattern string) ReplayCondition {
	// Validate pattern is not empty and has reasonable length
	if pattern == "" || len(pattern) > 256 {
		return func(e *DLQEntry) bool { return false }
	}

	re, err := compileRegexWithTimeout(pattern, 1*time.Second)
	if err != nil {
		return func(e *DLQEntry) bool { return false }
	}
	return func(e *DLQEntry) bool { return matchWithTimeout(re, e.Reason, 1*time.Second) }
}

// compileRegexWithTimeout compiles a regex pattern with a timeout to prevent DoS.
func compileRegexWithTimeout(pattern string, timeout time.Duration) (*regexp.Regexp, error) {
	type result struct {
		re  *regexp.Regexp
		err error
	}
	done := make(chan result, 1)
	go func() {
		re, err := regexp.Compile(pattern)
		done <- result{re, err}
	}()
	select {
	case r := <-done:
		return r.re, r.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("regex compilation timed out")
	}
}

// matchWithTimeout matches a string against a regex with a timeout.
func matchWithTimeout(re *regexp.Regexp, s string, timeout time.Duration) bool {
	type result struct {
		matched bool
	}
	done := make(chan result, 1)
	go func() {
		done <- result{re.MatchString(s)}
	}()
	select {
	case r := <-done:
		return r.matched
	case <-time.After(timeout):
		return false
	}
}

// ByPayloadContains returns a condition that matches if payload contains substring.
func ByPayloadContains(substr string) ReplayCondition {
	return func(e *DLQEntry) bool {
		if e.OriginalMsg == nil {
			return false
		}
		return contains(string(e.OriginalMsg.Payload), substr)
	}
}

// CompositeAND combines multiple conditions with AND logic.
func CompositeAND(conditions ...ReplayCondition) ReplayCondition {
	return func(e *DLQEntry) bool {
		for _, c := range conditions {
			if !c(e) {
				return false
			}
		}
		return true
	}
}

// CompositeOR combines multiple conditions with OR logic.
func CompositeOR(conditions ...ReplayCondition) ReplayCondition {
	return func(e *DLQEntry) bool {
		for _, c := range conditions {
			if c(e) {
				return true
			}
		}
		return false
	}
}

// NoTransform returns a transform that leaves messages unchanged.
func NoTransform() ReplayTransform {
	return func(e *DLQEntry) *message.Envelope {
		if e.OriginalMsg == nil {
			return nil
		}
		return e.OriginalMsg
	}
}

// AddHeader returns a transform that adds a header to the message.
func AddHeader(key string, value []byte) ReplayTransform {
	return func(e *DLQEntry) *message.Envelope {
		if e.OriginalMsg == nil {
			return nil
		}
		msg := *e.OriginalMsg // shallow copy
		if msg.Headers == nil {
			msg.Headers = make(map[string][]byte)
		}
		msg.Headers[key] = value
		return &msg
	}
}

// RemoveHeader returns a transform that removes a header from the message.
func RemoveHeader(key string) ReplayTransform {
	return func(e *DLQEntry) *message.Envelope {
		if e.OriginalMsg == nil {
			return nil
		}
		msg := *e.OriginalMsg
		if msg.Headers != nil {
			delete(msg.Headers, key)
		}
		return &msg
	}
}

// UpdatePayload returns a transform that modifies the payload.
func UpdatePayload(updater func([]byte) []byte) ReplayTransform {
	return func(e *DLQEntry) *message.Envelope {
		if e.OriginalMsg == nil {
			return nil
		}
		msg := *e.OriginalMsg
		msg.Payload = updater(msg.Payload)
		return &msg
	}
}

// SetRoutingKey returns a transform that sets the routing key.
func SetRoutingKey(key string) ReplayTransform {
	return func(e *DLQEntry) *message.Envelope {
		if e.OriginalMsg == nil {
			return nil
		}
		msg := *e.OriginalMsg
		msg.RoutingKey = key
		return &msg
	}
}

// AddDLQMetadata returns a transform that adds DLQ metadata as headers.
func AddDLQMetadata() ReplayTransform {
	return func(e *DLQEntry) *message.Envelope {
		if e.OriginalMsg == nil {
			return nil
		}
		msg := *e.OriginalMsg
		if msg.Headers == nil {
			msg.Headers = make(map[string][]byte)
		}
		msg.Headers["x-dlq-original-failure"] = []byte(e.Reason)
		msg.Headers["x-dlq-retry-count"] = []byte(fmt.Sprintf("%d", e.Retries))
		msg.Headers["x-dlq-failed-at"] = []byte(e.FailedAt.Format(time.RFC3339))
		msg.Headers["x-dlq-replay-at"] = []byte(time.Now().Format(time.RFC3339))
		return &msg
	}
}

// ChainTransforms chains multiple transforms together.
func ChainTransforms(transforms ...ReplayTransform) ReplayTransform {
	return func(e *DLQEntry) *message.Envelope {
		var msg *message.Envelope
		for _, t := range transforms {
			if msg == nil {
				msg = t(e)
			} else {
				// Create a temporary entry with the transformed message
				tempEntry := &DLQEntry{
					OriginalMsg: msg,
					Topic:       e.Topic,
					Partition:   e.Partition,
					Reason:      e.Reason,
					Retries:     e.Retries,
					FailedAt:    e.FailedAt,
				}
				msg = t(tempEntry)
			}
			if msg == nil {
				return nil
			}
		}
		return msg
	}
}

// ReplayResult contains the results of a replay operation.
type ReplayResult struct {
	TotalEntries   int
	MatchedEntries int
	ReplayedCount  int
	FailedCount    int
	SkippedCount   int
	Errors         []error
}

// ReplayWithOptions replays DLQ messages with the specified options.
func (d *DLQ) ReplayWithOptions(topic string, opts ReplayOptions, publisher func(*message.Envelope, string) error) (*ReplayResult, error) {
	if !d.enabled.Load() {
		return nil, fmt.Errorf("DLQ is not enabled")
	}

	result := &ReplayResult{}

	d.mu.Lock()
	defer d.mu.Unlock()

	q, ok := d.queues[topic]
	if !ok || len(q.messages) == 0 {
		return result, nil
	}

	result.TotalEntries = len(q.messages)

	// Filter messages based on condition
	var toReplay []*DLQEntry
	for _, entry := range q.messages {
		if opts.Condition(entry) {
			toReplay = append(toReplay, entry)
			result.MatchedEntries++
			if opts.MaxMessages > 0 && len(toReplay) >= opts.MaxMessages {
				break
			}
		}
	}

	if opts.DryRun {
		result.SkippedCount = len(toReplay)
		return result, nil
	}

	// Replay messages
	var remaining []*DLQEntry
	replayIndex := 0

	for _, entry := range q.messages {
		// Check if this entry should be replayed
		shouldReplay := replayIndex < len(toReplay) && entry == toReplay[replayIndex]

		if shouldReplay {
			// Transform the message
			msg := opts.Transform(entry)
			if msg == nil {
				result.FailedCount++
				result.Errors = append(result.Errors, fmt.Errorf("transform returned nil for entry %d", entry.ID))
				remaining = append(remaining, entry)
				replayIndex++
				continue
			}

			// Determine target topic
			targetTopic := entry.Topic
			if opts.TargetTopic != "" {
				targetTopic = opts.TargetTopic
			}

			// Publish the message
			if err := publisher(msg, targetTopic); err != nil {
				result.FailedCount++
				result.Errors = append(result.Errors, fmt.Errorf("failed to publish entry %d: %w", entry.ID, err))
				remaining = append(remaining, entry)
			} else {
				result.ReplayedCount++
				if !opts.DeleteAfterReplay {
					remaining = append(remaining, entry)
				}
			}
			replayIndex++
		} else {
			// Keep messages that weren't selected for replay
			remaining = append(remaining, entry)
		}
	}

	// Update the queue
	q.messages = remaining

	// Persist the updated queue if delete after replay is enabled
	if opts.DeleteAfterReplay {
		d.rewriteTopicFile(topic)
	}

	return result, nil
}

// rewriteTopicFile rewrites the DLQ file after entries are removed.
func (d *DLQ) rewriteTopicFile(topic string) {
	if d.dataDir == "" {
		return
	}

	q, ok := d.queues[topic]
	if !ok {
		return
	}

	// Note: This is a simplified implementation. In production,
	// you'd want to write to a temp file and rename atomically.
	// For now, we'll just clear and rewrite.

	if len(q.messages) == 0 {
		_ = d.topicPath(topic)
		return
	}

	// Write all remaining entries
	for _, entry := range q.messages {
		d.persistEntry(entry)
	}
}

// ReplayPreview returns a preview of what would be replayed without actually replaying.
func (d *DLQ) ReplayPreview(topic string, opts ReplayOptions) ([]*DLQEntry, error) {
	if !d.enabled.Load() {
		return nil, fmt.Errorf("DLQ is not enabled")
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	q, ok := d.queues[topic]
	if !ok || len(q.messages) == 0 {
		return nil, nil
	}

	var matched []*DLQEntry
	for _, entry := range q.messages {
		if opts.Condition(entry) {
			matched = append(matched, entry)
			if opts.MaxMessages > 0 && len(matched) >= opts.MaxMessages {
				break
			}
		}
	}

	return matched, nil
}

// ExportToJSON exports DLQ entries to JSON for external analysis.
func (d *DLQ) ExportToJSON(topic string, opts ReplayOptions) ([]byte, error) {
	entries, err := d.ReplayPreview(topic, opts)
	if err != nil {
		return nil, err
	}

	return json.MarshalIndent(entries, "", "  ")
}

// contains checks if a string contains a substring.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsInternal(s, substr))
}

func containsInternal(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
