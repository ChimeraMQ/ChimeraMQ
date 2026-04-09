package chimera

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
)

// testHarness sets up a broker + chimera server + client pipe for testing.
type testHarness struct {
	broker   *broker.Broker
	server   *Server
	client   net.Conn
	clientR  *bufio.Reader
	clientW  *bufio.Writer
	cleanup  func()
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()

	dir := t.TempDir()
	cfg, err := broker.LoadConfig("", &broker.CLIFlags{DataDir: dir})
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.Listener.Bind = "127.0.0.1"
	cfg.Listener.Port = 0 // let OS pick
	cfg.Listener.AdminPort = 0

	b, err := broker.NewBroker(cfg)
	if err != nil {
		t.Fatalf("NewBroker: %v", err)
	}
	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	srv, err := NewServer(b)
	if err != nil {
		b.Stop()
		t.Fatalf("NewServer: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		srv.Serve()
	}()
	// Give listener a moment
	time.Sleep(10 * time.Millisecond)

	conn, err := net.DialTimeout("tcp", srv.listener.Addr().String(), 2*time.Second)
	if err != nil {
		srv.StopAccepting()
		b.Stop()
		t.Fatalf("dial: %v", err)
	}

	h := &testHarness{
		broker:  b,
		server:  srv,
		client:  conn,
		clientR: bufio.NewReaderSize(conn, 64*1024),
		clientW: bufio.NewWriterSize(conn, 64*1024),
	}
	h.cleanup = func() {
		conn.Close()
		srv.StopAccepting()
		wg.Wait()
		b.Stop()
	}

	return h
}

func sendConnect(h *testHarness, clientID string) error {
	payload := encodeConnectPayload(clientID, "user", "pass", 60)
	frame := &Frame{Version: FrameVersion, OpCode: OpConnect, Payload: payload}
	data, err := EncodeFrame(frame)
	if err != nil {
		return err
	}
	h.clientW.Write(data)
	return h.clientW.Flush()
}

func encodeConnectPayload(clientID, username, password string, keepalive uint16) []byte {
	var buf []byte
	buf = appendUint16(buf, uint16(len(clientID)))
	buf = append(buf, clientID...)
	buf = appendUint16(buf, uint16(len(username)))
	buf = append(buf, username...)
	buf = appendUint16(buf, uint16(len(password)))
	buf = append(buf, password...)
	buf = appendUint16(buf, keepalive)
	return buf
}

func readFrame(r *bufio.Reader) (*Frame, error) {
	return DecodeFrame(r)
}

func TestServerConnectAndConnAck(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	if err := sendConnect(h, "test-client"); err != nil {
		t.Fatalf("send connect: %v", err)
	}

	frame, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if frame.OpCode != OpConnAck {
		t.Errorf("opcode = %v, want OpConnAck", frame.OpCode)
	}

	r := newReader(frame.Payload)
	id, _ := r.readString()
	if id == "" {
		t.Error("expected non-empty client ID in connack")
	}
}

func TestServerConnectWithEmptyClientID(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	if err := sendConnect(h, ""); err != nil {
		t.Fatalf("send connect: %v", err)
	}

	frame, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if frame.OpCode != OpConnAck {
		t.Errorf("opcode = %v, want OpConnAck", frame.OpCode)
	}

	// Server should auto-assign a client ID
	r := newReader(frame.Payload)
	id, _ := r.readString()
	if id == "" {
		t.Error("server should assign an auto-generated client ID")
	}
}

func TestServerPingPong(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "ping-test")
	readFrame(h.clientR) // consume connack

	ping, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPing})
	h.clientW.Write(ping)
	h.clientW.Flush()

	pong, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read pong: %v", err)
	}
	if pong.OpCode != OpPong {
		t.Errorf("opcode = %v, want OpPong", pong.OpCode)
	}
}

func TestServerCreateTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "create-test")
	readFrame(h.clientR) // connack

	var payload []byte
	payload = appendUint16(payload, uint16(len("tcp-topic")))
	payload = append(payload, "tcp-topic"...)
	payload = appendUint16(payload, uint16(len("stream")))
	payload = append(payload, "stream"...)
	payload = appendUint32(payload, 4)

	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: payload})
	h.clientW.Write(frame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.OpCode != OpSubAck {
		t.Errorf("opcode = %v, want OpSubAck (create ack)", resp.OpCode)
	}

	// Verify topic exists
	topic, ok := h.broker.Topics().GetTopic("tcp-topic")
	if !ok {
		t.Fatal("topic not found")
	}
	if topic.Partitions != 4 {
		t.Errorf("partitions = %d, want 4", topic.Partitions)
	}
}

func TestServerPublish(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "pub-test")
	readFrame(h.clientR) // connack

	// Create topic first
	createTopicPayload := encodeCreateTopicPayload("pub-topic", "stream", 2)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: createTopicPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // consume ack

	// Publish message
	var pubPayload []byte
	pubPayload = appendUint16(pubPayload, uint16(len("pub-topic")))
	pubPayload = append(pubPayload, "pub-topic"...)
	pubPayload = appendUint16(pubPayload, 0) // empty routing key
	pubPayload = append(pubPayload, 0)       // priority
	pubPayload = appendUint64(pubPayload, 0) // TTL
	pubPayload = appendUint64(pubPayload, 0) // DeliverAt
	pubPayload = append(pubPayload, []byte("hello tcp")...)

	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read puback: %v", err)
	}
	if resp.OpCode != OpPubAck {
		t.Errorf("opcode = %v, want OpPubAck", resp.OpCode)
	}

	r := newReader(resp.Payload)
	topic, _ := r.readString()
	if topic != "pub-topic" {
		t.Errorf("topic = %q, want pub-topic", topic)
	}
	partID := binary.BigEndian.Uint32(r.read(4))
	if partID > 1 {
		t.Errorf("partition = %d, expected 0 or 1", partID)
	}
	offset := binary.BigEndian.Uint64(r.read(8))
	// First message in partition gets offset 0 — just verify we read it
	_ = offset
}

func TestServerSubscribe(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "sub-test")
	readFrame(h.clientR) // connack

	var payload []byte
	payload = appendUint16(payload, uint16(len("sub-topic")))
	payload = append(payload, "sub-topic"...)
	payload = append(payload, 1) // mode
	payload = appendUint16(payload, uint16(len("my-group")))
	payload = append(payload, "my-group"...)
	payload = appendUint32(payload, 10)
	payload = appendUint64(payload, 0)

	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpSubscribe, Payload: payload})
	h.clientW.Write(frame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read suback: %v", err)
	}
	if resp.OpCode != OpSubAck {
		t.Errorf("opcode = %v, want OpSubAck", resp.OpCode)
	}

	r := newReader(resp.Payload)
	topic, _ := r.readString()
	if topic != "sub-topic" {
		t.Errorf("topic = %q, want sub-topic", topic)
	}
	success := r.read(1)[0]
	if success != 1 {
		t.Error("expected success=1")
	}
}

func TestServerDeleteTopic(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "del-test")
	readFrame(h.clientR) // connack

	// Create topic first
	ctPayload := encodeCreateTopicPayload("del-topic", "queue", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Delete it
	var delPayload []byte
	delPayload = appendUint16(delPayload, uint16(len("del-topic")))
	delPayload = append(delPayload, "del-topic"...)

	delFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpDeleteTopic, Payload: delPayload})
	h.clientW.Write(delFrame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if resp.OpCode != OpSubAck {
		t.Errorf("opcode = %v, want OpSubAck", resp.OpCode)
	}

	// Verify deleted
	_, ok := h.broker.Topics().GetTopic("del-topic")
	if ok {
		t.Error("topic should be deleted")
	}
}

func TestServerDisconnect(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "disc-test")
	readFrame(h.clientR) // connack

	// Send DISCONNECT — server should close the connection
	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpDisconnect})
	h.clientW.Write(frame)
	h.clientW.Flush()

	// Next read should return EOF (connection closed by server)
	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := h.clientR.ReadByte()
	if err != io.EOF {
		// On some systems it may be a different error, but should be an error
		if err == nil {
			t.Error("expected EOF or error after disconnect")
		}
	}
}

func TestServerNonConnectFirstFrame(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	// Send PUBLISH without CONNECT first
	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: []byte("data")})
	h.clientW.Write(frame)
	h.clientW.Flush()

	// Server should close connection
	h.client.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := h.clientR.ReadByte()
	if err == nil {
		t.Error("expected error when non-CONNECT frame sent first")
	}
}

func TestServerFetchAfterPublish(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "fetch-test")
	readFrame(h.clientR) // connack

	// Create topic
	ctPayload := encodeCreateTopicPayload("fetch-t", "stream", 1)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Publish a message
	var pubPayload []byte
	pubPayload = appendUint16(pubPayload, uint16(len("fetch-t")))
	pubPayload = append(pubPayload, "fetch-t"...)
	pubPayload = appendUint16(pubPayload, 0)
	pubPayload = append(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = appendUint64(pubPayload, 0)
	pubPayload = append(pubPayload, []byte("fetched-data")...)

	pubFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpPublish, Payload: pubPayload})
	h.clientW.Write(pubFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // puback

	// Fetch
	var fetchPayload []byte
	fetchPayload = appendUint16(fetchPayload, uint16(len("fetch-t")))
	fetchPayload = append(fetchPayload, "fetch-t"...)
	fetchPayload = appendUint32(fetchPayload, 0) // partition
	fetchPayload = appendUint64(fetchPayload, 0) // offset
	fetchPayload = appendUint32(fetchPayload, 10) // max messages

	fetchFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpFetch, Payload: fetchPayload})
	h.clientW.Write(fetchFrame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read fetch resp: %v", err)
	}
	if resp.OpCode != OpFetchResp {
		t.Errorf("opcode = %v, want OpFetchResp", resp.OpCode)
	}

	r := newReader(resp.Payload)
	count := binary.BigEndian.Uint32(r.read(4))
	if count != 1 {
		t.Fatalf("message count = %d, want 1", count)
	}
	msgLen := binary.BigEndian.Uint32(r.read(4))
	msgData := r.read(int(msgLen))
	if len(msgData) == 0 {
		t.Error("expected non-empty message data")
	}
}

func TestServerCommitOffset(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	sendConnect(h, "commit-test")
	readFrame(h.clientR) // connack

	// Create topic
	ctPayload := encodeCreateTopicPayload("commit-t", "stream", 2)
	ctFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCreateTopic, Payload: ctPayload})
	h.clientW.Write(ctFrame)
	h.clientW.Flush()
	readFrame(h.clientR) // ack

	// Commit offset
	var commitPayload []byte
	commitPayload = appendUint16(commitPayload, uint16(len("test-group")))
	commitPayload = append(commitPayload, "test-group"...)
	commitPayload = appendUint16(commitPayload, uint16(len("commit-t")))
	commitPayload = append(commitPayload, "commit-t"...)
	commitPayload = appendUint32(commitPayload, 0) // partition
	commitPayload = appendUint64(commitPayload, 5) // offset

	commitFrame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpCommitOffset, Payload: commitPayload})
	h.clientW.Write(commitFrame)
	h.clientW.Flush()

	resp, err := readFrame(h.clientR)
	if err != nil {
		t.Fatalf("read commit ack: %v", err)
	}
	if resp.OpCode != OpCommitAck {
		t.Errorf("opcode = %v, want OpCommitAck", resp.OpCode)
	}
}

func TestServerMultipleClients(t *testing.T) {
	h := newTestHarness(t)
	defer h.cleanup()

	// Client 1 already connected
	sendConnect(h, "client1")
	readFrame(h.clientR) // connack

	// Connect client 2
	conn2, err := net.DialTimeout("tcp", h.server.listener.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial client2: %v", err)
	}
	defer conn2.Close()

	r2 := bufio.NewReaderSize(conn2, 64*1024)
	w2 := bufio.NewWriterSize(conn2, 64*1024)

	connectPayload := encodeConnectPayload("client2", "", "", 0)
	frame, _ := EncodeFrame(&Frame{Version: FrameVersion, OpCode: OpConnect, Payload: connectPayload})
	w2.Write(frame)
	w2.Flush()

	resp, err := readFrame(r2)
	if err != nil {
		t.Fatalf("read connack2: %v", err)
	}
	if resp.OpCode != OpConnAck {
		t.Errorf("client2 connack opcode = %v, want OpConnAck", resp.OpCode)
	}
}

func encodeCreateTopicPayload(name, mode string, partitions uint32) []byte {
	var buf []byte
	buf = appendUint16(buf, uint16(len(name)))
	buf = append(buf, name...)
	buf = appendUint16(buf, uint16(len(mode)))
	buf = append(buf, mode...)
	buf = appendUint32(buf, partitions)
	return buf
}
