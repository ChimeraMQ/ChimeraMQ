package broker

import (
	"fmt"
	"time"

	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/wal"
)

// Publish handles message ingestion for all topic modes.
func (b *Broker) Publish(env *message.Envelope) (uint64, error) {
	topicCfg, ok := b.topics.GetTopic(env.Topic)
	if !ok {
		return 0, fmt.Errorf("topic %q not found", env.Topic)
	}

	// Resolve partition
	partID := b.topics.ResolvePartition(env.Topic, env.RoutingKey, topicCfg.Partitions)
	env.PartitionID = partID

	// Handle delayed messages
	if env.DeliverAt > 0 && time.Unix(0, env.DeliverAt).After(time.Now()) {
		if topicCfg.Mode == ModeQueue || topicCfg.Mode == ModeUnified {
			b.queueEngine.ScheduleDelayed(env.Topic, env)
			return 0, nil
		}
	}

	// Assign identity
	env.MessageID = message.NewUUIDv7()
	if env.Timestamp == 0 {
		env.Timestamp = time.Now().UnixNano()
	}

	// Serialize
	data, err := message.Marshal(env)
	if err != nil {
		return 0, err
	}
	defer message.ReleaseBuffer(data)

	// WAL first
	if _, err := b.wal.Append(wal.EntryMessage, data); err != nil {
		return 0, fmt.Errorf("WAL append: %w", err)
	}

	// Hot storage
	part, err := b.storage.GetOrCreatePartition(env.Topic, partID)
	if err != nil {
		return 0, err
	}

	offset, err := part.Append(data)
	if err != nil {
		return 0, err
	}

	env.Sequence = offset

	// Notify stream waiters
	b.streamEngine.NotifyWaiters(env.Topic, partID)

	// Dispatch to queue consumers (if queue or unified mode)
	if topicCfg.Mode == ModeQueue || topicCfg.Mode == ModeUnified {
		b.queueEngine.TryDispatch(env.Topic, partID, offset, env)
	}

	// Update metrics
	b.metrics.MessageIn(env.Topic, partID, env.SourceProto.String())

	return offset, nil
}
