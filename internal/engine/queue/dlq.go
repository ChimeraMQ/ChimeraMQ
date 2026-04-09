package queue

import (
	"fmt"
	"time"

	"github.com/chimeramq/chimera/internal/message"
)

// DLQManager routes dead-letter messages to a DLQ topic.
type DLQManager struct {
	dlqTopic string
}

// NewDLQManager creates a DLQ manager for the given DLQ topic.
func NewDLQManager(dlqTopic string) *DLQManager {
	return &DLQManager{dlqTopic: dlqTopic}
}

// Route clones the message with DLQ headers (caller handles actual write).
func (dm *DLQManager) Route(original *message.Envelope, reason string, deliverCount uint32) (*message.Envelope, error) {
	if dm.dlqTopic == "" {
		return nil, nil
	}

	dlqEnv := *original
	if dlqEnv.Headers == nil {
		dlqEnv.Headers = make(map[string][]byte)
	}
	dlqEnv.Headers["x-chimera-original-topic"] = []byte(original.Topic)
	dlqEnv.Headers["x-chimera-death-reason"] = []byte(reason)
	dlqEnv.Headers["x-chimera-death-count"] = []byte(fmt.Sprintf("%d", deliverCount))
	dlqEnv.Headers["x-chimera-first-death-time"] = []byte(time.Now().Format(time.RFC3339Nano))
	if original.RoutingKey != "" {
		dlqEnv.Headers["x-chimera-original-routing-key"] = []byte(original.RoutingKey)
	}

	dlqEnv.Topic = dm.dlqTopic
	dlqEnv.MessageID = message.NewUUIDv7()
	dlqEnv.Timestamp = time.Now().UnixNano()

	return &dlqEnv, nil
}
