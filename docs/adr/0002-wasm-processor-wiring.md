# ADR 0002: WASM and Stream Processor Initially Dead Code

## Status: Accepted (Now Wired)

## Context

The WASM transform runtime and stream processor were implemented with full unit
tests but were never wired into `Broker.Start()`. This meant they compiled but
never executed in production.

## Decision

Features were initially shipped as dead code to decouple implementation from
integration risk.

## Why

- WASM runtime (wazero) is complex — wiring it before testing could destabilize
  the publish pipeline
- Stream processor depends on topic routing and consumer group infrastructure
- Both features needed the publish pipeline to be stable first
- Unit tests validated correctness independently

## Consequences

- Users who enabled WASM/Processor config saw no effect until Phase 0 wiring
- Risk of integration bugs when finally wired (mitigated by integration tests)
- Technical debt tracked in roadmap and resolved in Phase 0

## Resolution

Both features are now wired in `Broker.Start()`:
- WASM: initialized when `config.WASM.Enabled` is true
- Processor: initialized with `processing.BrokerAPI` adapter
