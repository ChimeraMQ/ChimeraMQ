package raft

import "time"

// NodeID identifies a Raft node.
type NodeID string

// Term is a Raft logical clock term.
type Term uint64

// Index is a log entry index.
type Index uint64

// NodeState represents the role of a Raft node.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
	Shutdown
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Shutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

// EntryType classifies a log entry.
type EntryType int

const (
	EntryCommand EntryType = iota
	EntryConfigChange
	EntryNoOp
)

// LogEntry is a single Raft log entry.
type LogEntry struct {
	Index Index
	Term  Term
	Type  EntryType
	Data  []byte
}

// AppendEntriesRequest is the Raft AppendEntries RPC request.
type AppendEntriesRequest struct {
	Term         Term
	LeaderID     NodeID
	PrevLogIndex Index
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit Index
}

// AppendEntriesResponse is the Raft AppendEntries RPC response.
type AppendEntriesResponse struct {
	Term    Term
	Success bool
	// ConflictIndex is set when Success=false to optimize fast rollback.
	ConflictIndex Index
	ConflictTerm  Term
}

// RequestVoteRequest is the Raft RequestVote RPC request.
type RequestVoteRequest struct {
	Term         Term
	CandidateID  NodeID
	LastLogIndex Index
	LastLogTerm  Term
}

// RequestVoteResponse is the Raft RequestVote RPC response.
type RequestVoteResponse struct {
	Term        Term
	VoteGranted bool
}

// InstallSnapshotRequest is the Raft InstallSnapshot RPC request.
type InstallSnapshotRequest struct {
	Term              Term
	LeaderID          NodeID
	LastIncludedIndex Index
	LastIncludedTerm  Term
	Data              []byte
	Done              bool
}

// InstallSnapshotResponse is the Raft InstallSnapshot RPC response.
type InstallSnapshotResponse struct {
	Term Term
}

// Config holds Raft configuration.
type Config struct {
	NodeID            NodeID
	Peers             []NodeID
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	SnapshotInterval  time.Duration
	MaxLogEntries     int
	// DataDir is the root directory for Raft persistent storage.
	DataDir string
	// TLS configuration for internode communication.
	TLSEnabled bool
	CertFile   string
	KeyFile    string
	CAFile     string
}

// DefaultConfig returns a sensible Raft config.
func DefaultConfig() Config {
	return Config{
		ElectionTimeout:   1 * time.Second,
		HeartbeatInterval: 150 * time.Millisecond,
		SnapshotInterval:  5 * time.Minute,
		MaxLogEntries:     100000,
	}
}
