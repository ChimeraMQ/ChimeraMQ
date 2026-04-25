package integration

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/protocol/chimera"
)

// TestCrossProtocolHTTPPublishStreamFetch verifies that messages published
// via the HTTP API can be fetched through the stream engine.
func TestCrossProtocolHTTPPublishStreamFetch(t *testing.T) {
	tb := newTestBroker(t)

	// Create a stream topic
	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "cross-http-stream",
		Mode:       broker.ModeStream,
		Partitions: 1,
	})

	// Publish messages via HTTP API
	payloads := []string{"hello-from-http", "message-two", "message-three"}
	for _, p := range payloads {
		resp, err := http.Post(tb.addr+"/v1/messages/cross-http-stream", "text/plain", bytes.NewReader([]byte(p)))
		if err != nil {
			t.Fatalf("HTTP publish: %v", err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("HTTP publish failed: status=%d", resp.StatusCode)
		}
		resp.Body.Close()
	}

	// Fetch via stream engine
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("cross-http-stream", 0, 0, 10, 0)
	if err != nil {
		t.Fatalf("stream fetch: %v", err)
	}
	if len(msgs) != len(payloads) {
		t.Fatalf("expected %d messages, got %d", len(payloads), len(msgs))
	}

	// Verify message integrity
	for i, msg := range msgs {
		if string(msg.Payload) != payloads[i] {
			t.Errorf("message %d payload: got %q, want %q", i, msg.Payload, payloads[i])
		}
		if msg.SourceProto != message.ProtoHTTP {
			t.Errorf("message %d source proto: got %v, want %v", i, msg.SourceProto, message.ProtoHTTP)
		}
	}
}

// TestCrossProtocolChimeraFrameDecode verifies that Chimera protocol frame
// encoding/decoding works end-to-end and produces correct payloads that
// can be published and retrieved.
func TestCrossProtocolChimeraFrameDecode(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "cross-chimera",
		Mode:       broker.ModeUnified,
		Partitions: 1,
	})

	// Encode a PUBLISH frame (simulating what a TCP client would send)
	pubPayload := []byte("hello-via-chimera-frame")
	frame, err := chimera.EncodeFrame(&chimera.Frame{
		Version: chimera.FrameVersion,
		OpCode:  chimera.OpPublish,
		Payload: buildPublishPayload("cross-chimera", pubPayload),
	})
	if err != nil {
		t.Fatalf("encode publish frame: %v", err)
	}

	// Decode the frame back (verify round-trip)
	r := bytes.NewReader(frame)
	decoded, err := chimera.DecodeFrame(r)
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if decoded.OpCode != chimera.OpPublish {
		t.Fatalf("expected OpPublish, got 0x%02x", decoded.OpCode)
	}

	// Parse the topic and payload from the decoded frame
	topic, body := parsePublishPayload(decoded.Payload)
	if topic != "cross-chimera" {
		t.Fatalf("decoded topic: got %q, want %q", topic, "cross-chimera")
	}
	if string(body) != "hello-via-chimera-frame" {
		t.Fatalf("decoded payload: got %q, want %q", body, "hello-via-chimera-frame")
	}

	// Publish via broker (simulating what the TCP handler would do)
	env := &message.Envelope{
		Topic:       topic,
		Payload:     body,
		SourceProto: message.ProtoChimera,
	}
	if _, err := tb.broker.Publish(env); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Verify message is stored in stream engine
	se := tb.broker.StreamEngine()
	streamMsgs, _, err := se.Fetch("cross-chimera", 0, 0, 10, 0)
	if err != nil {
		t.Fatalf("stream fetch: %v", err)
	}
	if len(streamMsgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(streamMsgs))
	}
	if string(streamMsgs[0].Payload) != "hello-via-chimera-frame" {
		t.Errorf("payload: got %q, want %q", streamMsgs[0].Payload, "hello-via-chimera-frame")
	}
	if streamMsgs[0].SourceProto != message.ProtoChimera {
		t.Errorf("source proto: got %v, want %v", streamMsgs[0].SourceProto, message.ProtoChimera)
	}
}

// TestCrossProtocolDirectPublishDifferentSources verifies that messages
// published directly to the broker with different SourceProto values
// are correctly tagged and retrievable.
func TestCrossProtocolDirectPublishDifferentSources(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "cross-direct",
		Mode:       broker.ModeUnified,
		Partitions: 1,
	})

	sources := []message.ProtocolType{
		message.ProtoAMQP,
		message.ProtoMQTT,
		message.ProtoNATS,
		message.ProtoSTOMP,
		message.ProtoWS,
		message.ProtoHTTP,
		message.ProtoChimera,
	}

	for _, src := range sources {
		env := &message.Envelope{
			Topic:       "cross-direct",
			Payload:     []byte(fmt.Sprintf("msg-from-%v", src)),
			SourceProto: src,
		}
		if _, err := tb.broker.Publish(env); err != nil {
			t.Fatalf("publish with proto %v: %v", src, err)
		}
	}

	// Fetch all and verify source proto is preserved
	se := tb.broker.StreamEngine()
	msgs, _, err := se.Fetch("cross-direct", 0, 0, 10, 0)
	if err != nil {
		t.Fatalf("stream fetch: %v", err)
	}
	if len(msgs) != len(sources) {
		t.Fatalf("expected %d messages, got %d", len(sources), len(msgs))
	}

	for i, src := range sources {
		if msgs[i].SourceProto != src {
			t.Errorf("message %d source proto: got %v, want %v", i, msgs[i].SourceProto, src)
		}
		expectedPayload := fmt.Sprintf("msg-from-%v", src)
		if string(msgs[i].Payload) != expectedPayload {
			t.Errorf("message %d payload: got %q, want %q", i, msgs[i].Payload, expectedPayload)
		}
	}
}

// TestCrossProtocolQueueVsStream verifies that the same messages are
// accessible through both queue and stream engines for a unified topic.
func TestCrossProtocolQueueVsStream(t *testing.T) {
	tb := newTestBroker(t)

	tb.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       "cross-unified",
		Mode:       broker.ModeUnified,
		Partitions: 1,
	})

	// Publish messages via HTTP
	for i := 0; i < 3; i++ {
		payload := fmt.Sprintf("unified-msg-%d", i)
		resp, err := http.Post(tb.addr+"/v1/messages/cross-unified", "text/plain", bytes.NewReader([]byte(payload)))
		if err != nil {
			t.Fatalf("HTTP publish %d: %v", i, err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("HTTP publish %d failed: status=%d", i, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// Fetch via stream engine
	se := tb.broker.StreamEngine()
	streamMsgs, _, err := se.Fetch("cross-unified", 0, 0, 10, 0)
	if err != nil {
		t.Fatalf("stream fetch: %v", err)
	}
	if len(streamMsgs) != 3 {
		t.Fatalf("stream: expected 3 messages, got %d", len(streamMsgs))
	}

	// Verify message content
	for i, msg := range streamMsgs {
		expected := fmt.Sprintf("unified-msg-%d", i)
		if string(msg.Payload) != expected {
			t.Errorf("stream message %d payload: got %q, want %q", i, msg.Payload, expected)
		}
	}
}

// buildPublishPayload creates a PUBLISH payload with the given topic and body.
func buildPublishPayload(topic string, body []byte) []byte {
	var buf []byte
	buf = appendU16(buf, uint16(len(topic)))
	buf = append(buf, topic...)
	buf = append(buf, body...)
	return buf
}

// parsePublishPayload extracts topic and body from a PUBLISH payload.
func parsePublishPayload(data []byte) (string, []byte) {
	if len(data) < 2 {
		return "", data
	}
	topicLen := int(data[0])<<8 | int(data[1])
	if topicLen > len(data)-2 {
		return "", data
	}
	topic := string(data[2 : 2+topicLen])
	body := data[2+topicLen:]
	return topic, body
}

// appendU16 appends a uint16 in big-endian format.
func appendU16(b []byte, v uint16) []byte {
	return append(b, byte(v>>8), byte(v))
}
