package broker

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/chimeramq/chimera/internal/engine/ttl"
	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/storage/wal"
)

// Publish handles message ingestion for all topic modes.
func (b *Broker) Publish(env *message.Envelope) (uint64, error) {
	// MaxMessageSize enforcement
	if b.config.Limits.MaxMessageSize > 0 && int64(len(env.Payload)) > b.config.Limits.MaxMessageSize {
		return 0, fmt.Errorf("message size %d exceeds maximum %d", len(env.Payload), b.config.Limits.MaxMessageSize)
	}

	topicCfg, ok := b.topics.GetTopic(env.Topic)
	if !ok {
		return 0, fmt.Errorf("topic %q not found", env.Topic)
	}

	// Idempotent dedup check (producer ID and sequence from headers)
	if b.deduper != nil {
		if pid, seq, ok := extractProducerInfo(env); ok {
			if b.deduper.IsDuplicate(pid, seq) {
				return 0, nil // duplicate, silently skip
			}
		}
	}

	// Flow control: check rate limits
	if b.flowCtrl != nil && !b.flowCtrl.AllowPublish(env.Topic) {
		return 0, fmt.Errorf("rate limited for topic %q", env.Topic)
	}

	// Single tenant lookup — reused for quota and quota recording
	var tenantID string
	if b.tenantMgr != nil {
		if t := b.tenantMgr.GetTenant(env.Topic); t != nil {
			tenantID = t.ID
			if !b.tenantMgr.CheckQuota(tenantID, "publish") {
				return 0, fmt.Errorf("tenant %q publish rate exceeded", tenantID)
			}
		}
	}

	// Record publish for quota tracking
	if b.quotaEnforcer != nil && tenantID != "" {
		b.quotaEnforcer.RecordPublish(tenantID, int64(len(env.Payload)))
	}

	// Schema enforcement
	if b.schemaEnf != nil && topicCfg.SchemaEnforcement {
		schemaID := uint32(0)
		if id, ok := parseSchemaIDFromHeaders(env); ok {
			schemaID = id
		}
		if schemaID == 0 {
			return 0, fmt.Errorf("schema enforcement enabled for topic %q but no schema ID provided", env.Topic)
		}
		result, err := b.schemaEnf.Validate(schemaID, env.Payload)
		if err != nil {
			return 0, fmt.Errorf("schema validation error: %w", err)
		}
		if !result.Valid {
			return 0, fmt.Errorf("schema validation failed: %s", strings.Join(result.Errors, "; "))
		}
	}

	// Apply default TTL from topic config
	if topicCfg.DefaultTTL > 0 {
		ttl.ApplyDefaultTTL(env, topicCfg.DefaultTTL)
	}

	// WASM transform pipeline
	if b.wasmRT != nil && topicCfg.TransformPipeline != nil {
		transformed, err := topicCfg.TransformPipeline.Apply(b.ctx, b.wasmRT, env)
		if err != nil {
			b.metrics.WASMExecError(env.Topic)
			return 0, fmt.Errorf("transform pipeline: %w", err)
		}
		if transformed == nil {
			return 0, nil // message filtered by WASM
		}
		env = transformed
		b.metrics.WASMExecOK(env.Topic)
	}

	// Resolve partition
	partID := b.topics.ResolvePartition(env.Topic, env.RoutingKey, topicCfg.Partitions)
	env.PartitionID = partID

	// Capture timestamp once — reused for delay check and identity
	now := time.Now()
	nowNano := now.UnixNano()

	// Handle delayed messages
	if env.DeliverAt > 0 && time.Unix(0, env.DeliverAt).After(now) {
		if topicCfg.Mode == ModeQueue || topicCfg.Mode == ModeUnified {
			b.queueEngine.ScheduleDelayed(env.Topic, env)
			return 0, nil
		}
	}

	// Assign identity
	env.MessageID = message.NewUUIDv7()
	if env.Timestamp == 0 {
		env.Timestamp = nowNano
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
		_, _ = b.queueEngine.TryDispatch(env.Topic, partID, offset, env)
	}

	// Update metrics
	b.metrics.MessageIn(env.Topic, partID, env.SourceProto.String())

	// Geo-replication (if enabled)
	if b.geoManager != nil {
		if err := b.geoManager.Replicate(env); err != nil {
			// Log error but don't fail the publish - geo-replication is async
			b.logger.Warn("geo-replication failed", "topic", env.Topic, "error", err)
		}
	}

	return offset, nil
}

func parseSchemaIDFromHeaders(env *message.Envelope) (uint32, bool) {
	if env.Headers == nil {
		return 0, false
	}
	v, ok := env.Headers["x-chimera-schema-id"]
	if !ok {
		return 0, false
	}
	id, err := strconv.ParseUint(string(v), 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(id), true
}

func extractProducerInfo(env *message.Envelope) (string, uint64, bool) {
	if env.Headers == nil {
		return "", 0, false
	}
	pid, ok := env.Headers["x-chimera-producer-id"]
	if !ok || len(pid) == 0 {
		return "", 0, false
	}
	seq, ok := env.Headers["x-chimera-producer-seq"]
	if !ok || len(seq) == 0 {
		return string(pid), 0, false
	}
	n, err := strconv.ParseUint(string(seq), 10, 64)
	if err != nil {
		return string(pid), 0, false
	}
	return string(pid), n, true
}
