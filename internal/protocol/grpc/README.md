# gRPC Adapter

This directory contains the gRPC protocol adapter for ChimeraMQ.

## Status: Planned

The gRPC adapter requires the following dependencies to be added to go.mod:

```bash
go get google.golang.org/grpc
go get google.golang.org/protobuf
```

## Implementation Plan

1. Define protobuf schema in `chimera.proto`
2. Generate Go code using protoc
3. Implement server with streaming support
4. Add gRPC-specific configuration
5. Register with protocol multiplexer

## Protocol Support

- Publish/Single message
- Subscribe/Streaming
- Bidirectional streaming
- Topic management
- Health checks

## Notes

The gRPC adapter runs on a separate port (default: 50051) since gRPC requires
HTTP/2 and has different connection handling than the other protocols.
