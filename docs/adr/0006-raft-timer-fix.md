# ADR 0006: Raft Election Timer Channel Preservation

## Status: Accepted

## Context

The Raft election timer used `time.NewTimer()` on every reset, creating a new
timer and therefore a new channel. The `run()` select loop reads from
`n.electionTimer.C`, which evaluates the field once per select iteration.

When `resetElectionTimerLocked()` was called concurrently (from RPC handlers like
`HandleAppendEntries` or `HandleRequestVote`), the select loop continued watching
the old channel. The new timer's fire was lost, causing nodes to never trigger
elections. This manifested as nodes stuck in Follower state after leader failure.

## Decision

Replace `time.NewTimer()` with `timer.Stop()` + channel drain + `timer.Reset()`
to preserve the same channel for the lifetime of the RaftNode.

```go
if !n.electionTimer.Stop() {
    select { case <-n.electionTimer.C: default: }
}
n.electionTimer.Reset(timeout)
```

## Why

- `timer.Reset()` reinitializes the existing timer on the same channel
- The `run()` select always watches the same channel reference — never stale
- Stop + drain is the documented-safe pattern for timer reuse
- No allocation per reset — reduces GC pressure during leader elections

## Consequences

- Election timers always fire correctly regardless of concurrent RPC handling
- Multi-node failover now works (nodes transition to Candidate after leader death)
- The timer object is allocated once at node creation and reused forever
