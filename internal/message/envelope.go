package message

const (
	// FixedHeaderSize is the fixed header size in bytes for the binary wire format.
	FixedHeaderSize = 64

	// Flag bits for the Flags byte in the fixed header.
	FlagHasHeaders    uint8 = 1 << 0
	FlagHasRoutingKey uint8 = 1 << 1
	FlagHasTrace      uint8 = 1 << 2
	FlagHasTTL        uint8 = 1 << 3
	FlagHasDelay      uint8 = 1 << 4
)

// EncodingType represents payload compression encoding.
type EncodingType uint8

const (
	EncodingRaw    EncodingType = 0
	EncodingSnappy EncodingType = 1
	EncodingZstd   EncodingType = 2
	EncodingLZ4    EncodingType = 3
)

// ProtocolType identifies the ingestion protocol.
type ProtocolType uint8

const (
	ProtoChimera ProtocolType = 0
	ProtoAMQP    ProtocolType = 1
	ProtoMQTT    ProtocolType = 2
	ProtoWS      ProtocolType = 3
	ProtoHTTP    ProtocolType = 4
	ProtoSTOMP   ProtocolType = 5
	ProtoNATS    ProtocolType = 6
	ProtoGRPC    ProtocolType = 7
)

// String returns a human-readable protocol name.
func (p ProtocolType) String() string {
	switch p {
	case ProtoChimera:
		return "chimera"
	case ProtoAMQP:
		return "amqp"
	case ProtoMQTT:
		return "mqtt"
	case ProtoWS:
		return "websocket"
	case ProtoHTTP:
		return "http"
	case ProtoSTOMP:
		return "stomp"
	case ProtoNATS:
		return "nats"
	case ProtoGRPC:
		return "grpc"
	default:
		return "unknown"
	}
}

// Envelope is the protocol-agnostic internal message format.
// All messages are stored and processed in this format regardless
// of the ingestion protocol.
type Envelope struct {
	// Identity
	MessageID [16]byte // UUIDv7 (time-sortable)
	Timestamp int64    // Unix nanoseconds
	Sequence  uint64   // Per-partition monotonic sequence

	// Routing
	Topic       string
	PartitionID uint32
	RoutingKey  string
	Headers     map[string][]byte

	// Payload
	SchemaID    uint32
	ContentType string
	Encoding    EncodingType
	Payload     []byte

	// Delivery semantics
	Priority     uint8 // 0-9 (queue mode)
	TTL          int64 // Nanoseconds, 0 = no expiry
	DeliverAt    int64 // Delayed delivery timestamp (0 = immediate)
	DeliverCount uint32
	MaxRetries   uint32

	// Tracing
	TraceID     [16]byte
	SpanID      [8]byte
	SourceProto ProtocolType
}

// EstimateSize returns the estimated binary size of this envelope.
func (e *Envelope) EstimateSize() int {
	size := FixedHeaderSize
	size += len(e.Topic)
	size += len(e.RoutingKey)
	size += len(e.Payload)
	for k, v := range e.Headers {
		size += 2 + len(k) + 4 + len(v)
	}
	if e.TraceID != [16]byte{} {
		size += 24 // TraceID(16) + SpanID(8)
	}
	return size
}
