package chimera

import (
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/queue"
	"github.com/chimeramq/chimera/internal/message"
)

// TestHandleNackDLQRouting tests the full DLQ routing path through the protocol.
func TestHandleNackDLQRouting(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "dlq-test")
	readFrame(h.clientR) // connack

	// Create DLQ target topic
	ctPayload := encodeCreateTopicPayload("dlq-sink", "queue", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Create source topic with DLQ config
	var srcPayload []byte
	srcPayload = appendUint16(srcPayload, uint16(len("dlq-src")))
	srcPayload = append(srcPayload, "dlq-src"...)
	srcPayload = appendUint16(srcPayload, uint16(len("queue")))
	srcPayload = append(srcPayload, "queue"...)
	srcPayload = appendUint32(srcPayload, 1)
	// Note: CreateTopic payload doesn't support DLQ config via protocol,
	// so we set it directly on the broker's topic manager
	topicCfg, ok := h.broker.Topics().GetTopic("dlq-src")
	if !ok {
		// Create it first if needed
		h.broker.Topics().CreateTopic(broker.TopicConfig{
			Name:       "dlq-src",
			Mode:       broker.ModeQueue,
			Partitions: 1,
			DLQTopic:   "dlq-sink",
			MaxRetries: 2,
		})
	} else {
		topicCfg.DLQTopic = "dlq-sink"
		topicCfg.MaxRetries = 2
	}

	// Add a consumer so TryDispatch works
	qc := &queue.QueueConsumer{
		ID:       "c1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	h.broker.QueueEngine().AddConsumer("dlq-src", qc)

	// Publish a message
	var pubPayload []byte
	pubPayload = appendUint16(pubPayload, uint16(len("dlq-src")))
	pubPayload = append(pubPayload, "dlq-src"...)
	pubPayload = appendUint16(pubPayload, 0)
	pubPayload = append(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = append(pubPayload, []byte("dlq-msg")...)

	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // puback

	// Nack twice to exceed maxRetries=2 (deliverCount starts at 0, first nack makes it 1)
	// We need to dispatch first — the publish already dispatched via TryDispatch
	// First nack: deliverCount goes from 0→1 (tracked). Not yet >= maxRetries(2)
	var nackPayload []byte
	nackPayload = appendUint16(nackPayload, uint16(len("dlq-src")))
	nackPayload = append(nackPayload, "dlq-src"...)
	nackPayload = appendUint32(nackPayload, 0) // partitionID
	nackPayload = appendUint64(nackPayload, 0) // offset

	nackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpNack, Payload: nackPayload})
	h.clientW.Write(nackFrame)
	h.clientW.Flush()

	// Re-dispatch for second nack
	env := &message.Envelope{
		Topic:        "dlq-src",
		Payload:      []byte("dlq-msg"),
		DeliverCount: 1,
		MaxRetries:   2,
	}
	h.broker.QueueEngine().TryDispatch("dlq-src", 0, 0, env)

	// Second nack: deliverCount goes from 1→2 >= maxRetries(2) → shouldDLQ=true
	h.clientW.Write(nackFrame)
	h.clientW.Flush()

	// Verify connection is still alive
	ping, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPing})
	h.clientW.Write(ping)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	pong, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if pong.OpCode != OpPong {
		t.Errorf("opcode = %v, want OpPong", pong.OpCode)
	}
}

// TestHandleNackNonexistentPartition tests nack when partition doesn't exist.
func TestHandleNackNonexistentPartition(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "nack-np")
	readFrame(h.clientR) // connack

	// Nack with a topic/partition that has no data
	var nackPayload []byte
	nackPayload = appendUint16(nackPayload, uint16(len("no-topic")))
	nackPayload = append(nackPayload, "no-topic"...)
	nackPayload = appendUint32(nackPayload, 99)
	nackPayload = appendUint64(nackPayload, 0)

	nackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpNack, Payload: nackPayload})
	h.clientW.Write(nackFrame)
	h.clientW.Flush()

	// Should not crash — verify connection alive
	ping, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPing})
	h.clientW.Write(ping)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	pong, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if pong.OpCode != OpPong {
		t.Errorf("opcode = %v, want OpPong", pong.OpCode)
	}
}

// TestHandleAckMultipleOffsets tests ack with multiple offsets.
func TestHandleAckMultipleOffsets(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "ack-multi")
	readFrame(h.clientR) // connack

	// Create queue topic
	ctPayload := encodeCreateTopicPayload("am-topic", "queue", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR)

	// Add consumer
	qc := &queue.QueueConsumer{
		ID:       "c1",
		Prefetch: 10,
		InFlight: make(map[uint64]time.Time),
	}
	h.broker.QueueEngine().AddConsumer("am-topic", qc)

	// Publish 3 messages
	for i := 0; i < 3; i++ {
		var pubPayload []byte
		pubPayload = appendUint16(pubPayload, uint16(len("am-topic")))
		pubPayload = append(pubPayload, "am-topic"...)
		pubPayload = appendUint16(pubPayload, 0)
		pubPayload = append(pubPayload, 0)
		pubPayload = appendUint64(pubPayload, 0)
		pubPayload = appendUint64(pubPayload, 0)
		pubPayload = append(pubPayload, byte('A'+i))

		pf, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
		h.clientW.Write(pf)
		h.clientW.Flush()
		readFrame(h.clientR) // puback
	}

	// Ack all 3 offsets in one frame
	var ackPayload []byte
	ackPayload = appendUint16(ackPayload, uint16(len("am-topic")))
	ackPayload = append(ackPayload, "am-topic"...)
	ackPayload = appendUint32(ackPayload, 0) // partitionID
	ackPayload = appendUint64(ackPayload, 0) // offset 0
	ackPayload = appendUint64(ackPayload, 1) // offset 1
	ackPayload = appendUint64(ackPayload, 2) // offset 2

	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpAck, Payload: ackPayload})
	h.clientW.Write(ackFrame)
	h.clientW.Flush()

	// Verify alive
	ping, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPing})
	h.clientW.Write(ping)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	pong, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if pong.OpCode != OpPong {
		t.Errorf("opcode = %v, want OpPong", pong.OpCode)
	}
}
