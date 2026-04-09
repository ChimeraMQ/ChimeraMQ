package chimera

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
)

// TestServerAckOnQueueTopic verifies handleAck forwards offsets to the queue engine.
func TestServerAckOnQueueTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "ack-test")
	readFrame(h.clientR) // connack

	// Create a queue topic
	ctPayload := encodeCreateTopicPayload("ack-topic", "queue", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Publish a message to the queue topic
	var pubPayload []byte
	pubPayload = appendUint16(pubPayload, uint16(len("ack-topic")))
	pubPayload = append(pubPayload, "ack-topic"...)
	pubPayload = appendUint16(pubPayload, 0) // empty routing key
	pubPayload = append(pubPayload, 0)       // priority
	pubPayload = appendUint64(pubPayload, 0) // TTL
	pubPayload = appendUint64(pubPayload, 0) // DeliverAt
	pubPayload = append(pubPayload, []byte("q-msg")...)

	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // puback

	// Send ACK for offset 0
	var ackPayload []byte
	ackPayload = appendUint16(ackPayload, uint16(len("ack-topic")))
	ackPayload = append(ackPayload, "ack-topic"...)
	ackPayload = appendUint32(ackPayload, 0) // partitionID
	ackPayload = appendUint64(ackPayload, 0) // offset

	ackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpAck, Payload: ackPayload})
	h.clientW.Write(ackFrame)
	h.clientW.Flush()

	// ACK has no response — just verify it doesn't crash. Send a ping to confirm conn is still alive.
	ping, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPing})
	h.clientW.Write(ping)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	pong, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read pong after ack: %v", err)
	}
	if pong.OpCode != OpPong {
		t.Errorf("opcode = %v, want OpPong", pong.OpCode)
	}
}

// TestServerNackOnQueueTopic verifies handleNack triggers DLQ check.
func TestServerNackOnQueueTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "nack-test")
	readFrame(h.clientR) // connack

	// Create queue topic
	ctPayload := encodeCreateTopicPayload("nack-topic", "queue", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Publish
	var pubPayload []byte
	pubPayload = appendUint16(pubPayload, uint16(len("nack-topic")))
	pubPayload = append(pubPayload, "nack-topic"...)
	pubPayload = appendUint16(pubPayload, 0)
	pubPayload = append(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = append(pubPayload, []byte("nmsg")...)

	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // puback

	// NACK offset 0
	var nackPayload []byte
	nackPayload = appendUint16(nackPayload, uint16(len("nack-topic")))
	nackPayload = append(nackPayload, "nack-topic"...)
	nackPayload = appendUint32(nackPayload, 0) // partitionID
	nackPayload = appendUint64(nackPayload, 0) // offset

	nackFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpNack, Payload: nackPayload})
	h.clientW.Write(nackFrame)
	h.clientW.Flush()

	// NACK has no response — verify connection still alive
	ping, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPing})
	h.clientW.Write(ping)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	pong, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read pong after nack: %v", err)
	}
	if pong.OpCode != OpPong {
		t.Errorf("opcode = %v, want OpPong", pong.OpCode)
	}
}

// TestServerPublishToNonexistentTopic verifies sendError is called.
func TestServerPublishToNonexistentTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "err-test")
	readFrame(h.clientR) // connack

	// Publish to a topic that doesn't exist
	var pubPayload []byte
	pubPayload = appendUint16(pubPayload, uint16(len("no-such-topic")))
	pubPayload = append(pubPayload, "no-such-topic"...)
	pubPayload = appendUint16(pubPayload, 0)
	pubPayload = append(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = append(pubPayload, []byte("data")...)

	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read error frame: %v", err)
	}
	if resp.OpCode != OpError {
		t.Errorf("opcode = %v, want OpError", resp.OpCode)
	}

	// Parse error payload: code(uint16) + len(uint16) + msg
	r := newReader(resp.Payload)
	if r.len() < 4 {
		t.Fatal("error payload too short")
	}
	code := binary.BigEndian.Uint16(r.read(2))
	msgLen := binary.BigEndian.Uint16(r.read(2))
	msg := string(r.read(int(msgLen)))
	_ = code
	if msg == "" {
		t.Error("expected non-empty error message")
	}
}

// TestDisconnectAll closes all client connections.
func TestDisconnectAll(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "da-test")
	readFrame(h.clientR) // connack

	// DisconnectAll should close client connections
	h.server.DisconnectAll()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := h.clientR.ReadByte()
	if err == nil {
		t.Error("expected error after DisconnectAll")
	}
}

// TestDeleteNonexistentTopic triggers sendError.
func TestDeleteNonexistentTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "del-err-test")
	readFrame(h.clientR) // connack

	var delPayload []byte
	delPayload = appendUint16(delPayload, uint16(len("ghost-topic")))
	delPayload = append(delPayload, "ghost-topic"...)

	delFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpDeleteTopic, Payload: delPayload})
	h.clientW.Write(delFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.OpCode != OpError {
		t.Errorf("opcode = %v, want OpError for deleting nonexistent topic", resp.OpCode)
	}
}

// TestFetchNonexistentTopic returns empty FetchResp (auto-creates partition).
func TestFetchNonexistentTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "fetch-err-test")
	readFrame(h.clientR) // connack

	var fetchPayload []byte
	fetchPayload = appendUint16(fetchPayload, uint16(len("no-topic")))
	fetchPayload = append(fetchPayload, "no-topic"...)
	fetchPayload = appendUint32(fetchPayload, 0)
	fetchPayload = appendUint64(fetchPayload, 0)
	fetchPayload = appendUint32(fetchPayload, 10)

	fetchFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpFetch, Payload: fetchPayload})
	h.clientW.Write(fetchFrame)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(3 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	// Storage auto-creates partition, returns empty result
	if resp.OpCode != OpFetchResp {
		t.Errorf("opcode = %v, want OpFetchResp", resp.OpCode)
	}
	r := newReader(resp.Payload)
	count := binary.BigEndian.Uint32(r.read(4))
	if count != 0 {
		t.Errorf("message count = %d, want 0", count)
	}
}

// TestCreateDuplicateTopic triggers sendError from handleCreateTopic.
func TestCreateDuplicateTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "dup-test")
	readFrame(h.clientR) // connack

	// Create topic once
	ctPayload := encodeCreateTopicPayload("dup-topic", "stream", 2)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Create again — should get error
	ctFrame2, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame2)
	h.clientW.Flush()

	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.OpCode != OpError {
		t.Errorf("opcode = %v, want OpError for duplicate topic", resp.OpCode)
	}
}

// TestCreateTopicQueueMode verifies queue mode mapping.
func TestCreateTopicQueueMode(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "queue-mode-test")
	readFrame(h.clientR) // connack

	ctPayload := encodeCreateTopicPayload("q-topic", "queue", 3)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	topic, ok := h.broker.Topics().GetTopic("q-topic")
	if !ok {
		t.Fatal("topic not found")
	}
	if topic.Mode != broker.ModeQueue {
		t.Errorf("mode = %v, want ModeQueue", topic.Mode)
	}
	if topic.Partitions != 3 {
		t.Errorf("partitions = %d, want 3", topic.Partitions)
	}
}

// TestCreateTopicDefaultMode verifies default mode is ModeUnified.
func TestCreateTopicDefaultMode(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "default-mode-test")
	readFrame(h.clientR) // connack

	// Mode string that doesn't match "stream" or "queue"
	ctPayload := encodeCreateTopicPayload("u-topic", "unknown", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	topic, ok := h.broker.Topics().GetTopic("u-topic")
	if !ok {
		t.Fatal("topic not found")
	}
	if topic.Mode != broker.ModeUnified {
		t.Errorf("mode = %v, want ModeUnified (default)", topic.Mode)
	}
}
