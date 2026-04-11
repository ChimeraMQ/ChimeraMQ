package processing

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// KeyExtractor extracts a join key from a message envelope.
type KeyExtractor func(env *message.Envelope) string

// JoinRecord is a buffered record waiting for its match.
type JoinRecord struct {
	Key       string
	Env       *message.Envelope
	Timestamp int64 // nanoseconds
}

// JoinResult is the output of a successful join.
type JoinResult struct {
	Left  *message.Envelope
	Right *message.Envelope
}

// JoinOp performs a time-windowed inner join between two streams.
// Both streams must provide a key extraction function.
// Records are buffered for the configured window duration.
// When a matching key arrives from the other side, the join fires.
type JoinOp struct {
	mu sync.Mutex

	leftKeyFn  KeyExtractor
	rightKeyFn KeyExtractor
	window     time.Duration
	onResult   func(result *JoinResult)

	// Buffers keyed by join key
	leftBuf  map[string][]*JoinRecord
	rightBuf map[string][]*JoinRecord
}

// NewJoinOp creates a new join operator.
// leftKeyFn and rightKeyFn extract join keys from left and right stream messages.
// window is the maximum time difference allowed between matched records.
// onResult is called when a join match is found.
func NewJoinOp(leftKeyFn, rightKeyFn KeyExtractor, window time.Duration, onResult func(*JoinResult)) *JoinOp {
	return &JoinOp{
		leftKeyFn:  leftKeyFn,
		rightKeyFn: rightKeyFn,
		window:     window,
		onResult:   onResult,
		leftBuf:    make(map[string][]*JoinRecord),
		rightBuf:   make(map[string][]*JoinRecord),
	}
}

// AddLeft adds a record from the left stream.
func (j *JoinOp) AddLeft(env *message.Envelope, timestamp int64) {
	key := j.leftKeyFn(env)

	j.mu.Lock()
	defer j.mu.Unlock()

	// Check for matches in the right buffer
	if rights, ok := j.rightBuf[key]; ok {
		for i, r := range rights {
			if absDiff(timestamp, r.Timestamp) <= j.window.Nanoseconds() {
				if j.onResult != nil {
					j.onResult(&JoinResult{Left: env, Right: r.Env})
				}
				// Remove matched right record
				j.rightBuf[key] = append(rights[:i], rights[i+1:]...)
				if len(j.rightBuf[key]) == 0 {
					delete(j.rightBuf, key)
				}
				return
			}
		}
	}

	// No match — buffer the left record
	j.leftBuf[key] = append(j.leftBuf[key], &JoinRecord{
		Key:       key,
		Env:       env,
		Timestamp: timestamp,
	})
}

// AddRight adds a record from the right stream.
func (j *JoinOp) AddRight(env *message.Envelope, timestamp int64) {
	key := j.rightKeyFn(env)

	j.mu.Lock()
	defer j.mu.Unlock()

	// Check for matches in the left buffer
	if lefts, ok := j.leftBuf[key]; ok {
		for i, r := range lefts {
			if absDiff(timestamp, r.Timestamp) <= j.window.Nanoseconds() {
				if j.onResult != nil {
					j.onResult(&JoinResult{Left: r.Env, Right: env})
				}
				// Remove matched left record
				j.leftBuf[key] = append(lefts[:i], lefts[i+1:]...)
				if len(j.leftBuf[key]) == 0 {
					delete(j.leftBuf, key)
				}
				return
			}
		}
	}

	// No match — buffer the right record
	j.rightBuf[key] = append(j.rightBuf[key], &JoinRecord{
		Key:       key,
		Env:       env,
		Timestamp: timestamp,
	})
}

// Tick removes expired records that have exceeded the join window.
func (j *JoinOp) Tick(now int64) {
	j.mu.Lock()
	defer j.mu.Unlock()

	windowNs := j.window.Nanoseconds()

	j.leftBuf = expireRecords(j.leftBuf, now, windowNs)
	j.rightBuf = expireRecords(j.rightBuf, now, windowNs)
}

// BufferSizes returns the number of buffered records in left and right.
func (j *JoinOp) BufferSizes() (left, right int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, recs := range j.leftBuf {
		left += len(recs)
	}
	for _, recs := range j.rightBuf {
		right += len(recs)
	}
	return
}

func expireRecords(buf map[string][]*JoinRecord, now, windowNs int64) map[string][]*JoinRecord {
	for key, recs := range buf {
		var kept []*JoinRecord
		for _, r := range recs {
			if now-r.Timestamp <= windowNs {
				kept = append(kept, r)
			}
		}
		if len(kept) == 0 {
			delete(buf, key)
		} else {
			buf[key] = kept
		}
	}
	return buf
}

func absDiff(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return b - a
}

// --- Key Extractors ---

// KeyFromHeader extracts a join key from a message header.
func KeyFromHeader(headerName string) KeyExtractor {
	return func(env *message.Envelope) string {
		if env.Headers == nil {
			return ""
		}
		if v, ok := env.Headers[headerName]; ok {
			return string(v)
		}
		return ""
	}
}

// KeyFromRoutingKey extracts a join key from the routing key.
func KeyFromRoutingKey() KeyExtractor {
	return func(env *message.Envelope) string {
		return env.RoutingKey
	}
}

// KeyFromPayloadJSON extracts a join key from a JSON payload field.
func KeyFromPayloadJSON(field string) KeyExtractor {
	return func(env *message.Envelope) string {
		var obj map[string]interface{}
		if err := json.Unmarshal(env.Payload, &obj); err != nil {
			return ""
		}
		if v, ok := obj[field]; ok {
			if s, ok := v.(string); ok {
				return s
			}
			return fmt.Sprintf("%v", v)
		}
		return ""
	}
}
