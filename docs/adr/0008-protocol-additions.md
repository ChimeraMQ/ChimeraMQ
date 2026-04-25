# ADR 0008: Protocol Adapter Additions (NATS, STOMP)

## Status: Accepted

## Context

ChimeraMQ launched with 7 protocol adapters: Chimera TCP, HTTP, MQTT, AMQP 1.0,
WebSocket, NATS, and STOMP. The original specification listed 7 protocols; NATS
and STOMP were added to cover additional ecosystem integration points.

## Protocol Additions

### NATS (internal/protocol/nats/)

**Purpose:** NATS-compatible publish/subscribe for existing NATS ecosystem clients.

**Scope:**
- CONNECT, SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PING, PONG
- Wildcard subscriptions (`*`, `>`)
- Queue groups for competing consumers
- No JetStream compatibility (outside scope)

**Design decisions:**
- Single TCP connection per client, multiplexed through protocol mux
- NATS detection via CONNECT command peek (first 7 bytes)
- Topic mapping: NATS subject → Chimera topic, one-to-one
- No NATS request-reply pattern (not applicable to Chimera's model)

**Why:**
- NATS has a large ecosystem of clients (Go, Python, Node.js, Java, Rust)
- Simple text-based protocol is easy to implement
- Wildcard subscriptions enable flexible routing patterns
- Low barrier to entry for NATS users to try Chimera

### STOMP (internal/protocol/stomp/)

**Purpose:** Simple Text Oriented Messaging Protocol for JMS-compatible clients.

**Scope:**
- CONNECT, SEND, SUBSCRIBE, UNSUBSCRIBE, ACK, NACK, BEGIN, COMMIT, ABORT
- Destination headers map to Chimera topics
- Ack modes: auto, client, client-individual
- Transaction support for batch operations

**Design decisions:**
- Frame-based text protocol with NUL terminator
- STOMP detection via CONNECT/SEND/SUBSCRIBE command peek
- No STOMP heartbeats (Chimera-level keepalive handles this)
- Receipt headers for synchronous acknowledgment

**Why:**
- STOMP is supported by virtually every messaging library (Spring JMS, Apache Camel, etc.)
- Text-based protocol is simple to debug and test
- Enterprise Java ecosystem expects STOMP support
- Spring Boot WebSocket/STOMP compatibility for web applications

## Protocol Multiplexing

All protocols share the same TCP port (default 5672). The mux in
`internal/protocol/mux.go` peeks at the first bytes and routes by detection
order: AMQP → MQTT → HTTP → Chimera native → NATS → STOMP → WebSocket upgrade.

Detection priority reflects protocol signature specificity:
1. AMQP: Fixed 8-byte header `AMQP\x00\x00\x01\x00`
2. MQTT: Fixed CONNECT type byte (0x10) + protocol name length
3. HTTP: `GET`, `POST`, `PUT`, `DELETE` methods
4. Chimera native: `CHMR` magic bytes
5. NATS: `CONNECT` or `PUB` or `SUB` commands
6. STOMP: `SEND` or `SUBSCRIBE` or `CONNECT` commands
7. WebSocket: HTTP upgrade request with `Upgrade: websocket`

## Consequences

- 7 protocol adapters cover the majority of messaging ecosystem integrations
- Protocol mux adds ~1µs per connection for peek detection
- Each protocol adapter is independently testable and fuzz-testable
- Fuzz tests added for all protocol decoders (15 fuzz tests total)
