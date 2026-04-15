package chimera

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"

	"github.com/chimeramq/chimera/internal/auth"
)

// ClientConn represents a connected client.
type ClientConn struct {
	mu       sync.Mutex
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	clientID string
	subs     map[string]*Subscription
	subsMu   sync.RWMutex
	identity *auth.Identity // authenticated identity for ACL checks
}

// writeFrame writes a frame to the client connection. Returns error on write failure.
func (c *ClientConn) writeFrame(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.writer.Write(data); err != nil {
		return err
	}
	return c.writer.Flush()
}

// Subscription tracks a client's subscription state.
type Subscription struct {
	topic string
	mode  uint8
}

// Payload types

type ConnectPayload struct {
	ClientID  string
	Username  string
	Password  string
	Keepalive uint16
}

type PublishPayload struct {
	Topic      string
	RoutingKey string
	Priority   uint8
	TTL        int64
	DeliverAt  int64
	Headers    map[string][]byte
	Body       []byte
}

type SubscribePayload struct {
	Topic         string
	Mode          uint8
	ConsumerGroup string
	Prefetch      int
	StartOffset   int64
}

type AckPayload struct {
	Topic       string
	PartitionID uint32
	Offsets     []uint64
}

type CreateTopicPayload struct {
	Name       string
	Mode       string
	Partitions uint32
}

// BatchPublishBatch holds the results of decoding a batch publish request.
type BatchPublishBatch struct {
	Messages []PublishPayload
}

// BatchPubAckResult holds one message's publish result.
type BatchPubAckResult struct {
	PartitionID uint32
	Offset      uint64
	OK          bool
}

// BatchPublishResult holds encoded results for batch publish ack.
type BatchPublishResult struct {
	Results []BatchPubAckResult
	OKCount int
}

// Payload encode/decode helpers

func decodeConnect(data []byte) ConnectPayload {
	p := ConnectPayload{}
	r := newReader(data)
	p.ClientID, _ = r.readString()
	p.Username, _ = r.readString()
	p.Password, _ = r.readString()
	if r.len() >= 2 {
		p.Keepalive = binary.BigEndian.Uint16(r.read(2))
	}
	return p
}

func encodeConnAck(clientID string, status uint8) []byte {
	var buf []byte
	buf = appendUint16(buf, uint16(len(clientID)))
	buf = append(buf, clientID...)
	buf = append(buf, status)
	return buf
}

func decodePublish(data []byte) PublishPayload {
	p := PublishPayload{}
	r := newReader(data)
	p.Topic, _ = r.readString()
	p.RoutingKey, _ = r.readString()
	if r.len() > 0 {
		p.Priority = r.read(1)[0]
	}
	if r.len() >= 8 {
		p.TTL = int64(binary.BigEndian.Uint64(r.read(8)))
	}
	if r.len() >= 8 {
		p.DeliverAt = int64(binary.BigEndian.Uint64(r.read(8)))
	}
	// Remaining is body
	if r.len() > 0 {
		p.Body = make([]byte, r.len())
		copy(p.Body, r.data[r.pos:])
	}
	return p
}

// decodeBatchPublish decodes a batch publish request.
// Wire format: [count:uint32] [for each: PublishPayload in serial format]
func decodeBatchPublish(data []byte) BatchPublishBatch {
	r := newReader(data)
	if r.len() < 4 {
		return BatchPublishBatch{}
	}
	count := binary.BigEndian.Uint32(r.read(4))
	batch := BatchPublishBatch{Messages: make([]PublishPayload, 0, count)}
	for i := uint32(0); i < count && r.len() > 0; i++ {
		if r.len() < 2 {
			break
		}
		topicLen := int(binary.BigEndian.Uint16(r.read(2)))
		if r.len() < topicLen {
			break
		}
		topic := string(r.read(topicLen))

		if r.len() < 2 {
			break
		}
		rkLen := int(binary.BigEndian.Uint16(r.read(2)))
		var routingKey string
		if r.len() >= rkLen {
			routingKey = string(r.read(rkLen))
		}

		var priority uint8
		if r.len() > 0 {
			priority = r.read(1)[0]
		}

		var ttl int64
		if r.len() >= 8 {
			ttl = int64(binary.BigEndian.Uint64(r.read(8)))
		}

		var deliverAt int64
		if r.len() >= 8 {
			deliverAt = int64(binary.BigEndian.Uint64(r.read(8)))
		}

		var body []byte
		if r.len() > 0 {
			body = make([]byte, r.len())
			copy(body, r.data[r.pos:])
		}

		batch.Messages = append(batch.Messages, PublishPayload{
			Topic:      topic,
			RoutingKey: routingKey,
			Priority:   priority,
			TTL:        ttl,
			DeliverAt:  deliverAt,
			Body:       body,
		})
	}
	return batch
}

func encodePubAck(topic string, partitionID uint32, offset uint64) []byte {
	var buf []byte
	buf = appendUint16(buf, uint16(len(topic)))
	buf = append(buf, topic...)
	buf = appendUint32(buf, partitionID)
	buf = appendUint64(buf, offset)
	return buf
}

func decodeSubscribe(data []byte) SubscribePayload {
	p := SubscribePayload{}
	r := newReader(data)
	p.Topic, _ = r.readString()
	if r.len() > 0 {
		p.Mode = r.read(1)[0]
	}
	p.ConsumerGroup, _ = r.readString()
	if r.len() >= 4 {
		p.Prefetch = int(binary.BigEndian.Uint32(r.read(4)))
	}
	if r.len() >= 8 {
		p.StartOffset = int64(binary.BigEndian.Uint64(r.read(8)))
	}
	return p
}

func encodeSubAck(topic string, success bool) []byte {
	var buf []byte
	buf = appendUint16(buf, uint16(len(topic)))
	buf = append(buf, topic...)
	if success {
		buf = append(buf, 1)
	} else {
		buf = append(buf, 0)
	}
	return buf
}

func decodeAck(data []byte) AckPayload {
	p := AckPayload{}
	r := newReader(data)
	p.Topic, _ = r.readString()
	if r.len() >= 4 {
		p.PartitionID = binary.BigEndian.Uint32(r.read(4))
	}
	for r.len() >= 8 {
		p.Offsets = append(p.Offsets, binary.BigEndian.Uint64(r.read(8)))
	}
	return p
}

func decodeCreateTopic(data []byte) CreateTopicPayload {
	p := CreateTopicPayload{}
	r := newReader(data)
	p.Name, _ = r.readString()
	p.Mode, _ = r.readString()
	if r.len() >= 4 {
		p.Partitions = binary.BigEndian.Uint32(r.read(4))
	}
	return p
}

func encodeError(code uint16, msg string) []byte {
	var buf []byte
	buf = appendUint16(buf, code)
	buf = appendUint16(buf, uint16(len(msg)))
	buf = append(buf, msg...)
	return buf
}

// Reader helper

type payloadReader struct {
	data []byte
	pos  int
}

func newReader(data []byte) *payloadReader {
	return &payloadReader{data: data}
}

func (r *payloadReader) len() int {
	return len(r.data) - r.pos
}

func (r *payloadReader) read(n int) []byte {
	if r.pos+n > len(r.data) {
		n = len(r.data) - r.pos
	}
	data := r.data[r.pos : r.pos+n]
	r.pos += n
	return data
}

func (r *payloadReader) readString() (string, error) {
	if r.len() < 2 {
		return "", io.EOF
	}
	length := int(binary.BigEndian.Uint16(r.read(2)))
	if r.len() < length {
		return "", io.EOF
	}
	return string(r.read(length)), nil
}

// Writer helpers

func appendUint16(buf []byte, v uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	return append(buf, b[:]...)
}

func appendUint32(buf []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(buf, b[:]...)
}

func appendUint64(buf []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return append(buf, b[:]...)
}
