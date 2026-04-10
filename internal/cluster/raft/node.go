package raft

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RaftNode implements a single Raft consensus node.
type RaftNode struct {
	mu sync.Mutex

	id    NodeID
	state NodeState
	cfg   Config

	// Persistent state (on all servers)
	currentTerm Term
	votedFor    NodeID
	log         *RaftLog

	// Volatile state (on all servers)
	commitIndex Index
	lastApplied Index

	// Leader-only state
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	// Election
	electionTimer *time.Timer
	heartbeatTick *time.Ticker

	// Components
	transport   Transport
	fsm         *MetadataFSM
	snapshotter *Snapshotter

	// Peer management
	peers []NodeID

	// Shutdown
	stopCh chan struct{}
	done   chan struct{}
}

// NewRaftNode creates a new Raft node.
func NewRaftNode(cfg Config) (*RaftNode, error) {
	if cfg.ElectionTimeout == 0 {
		cfg.ElectionTimeout = 1 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 150 * time.Millisecond
	}
	if cfg.SnapshotInterval == 0 {
		cfg.SnapshotInterval = 5 * time.Minute
	}
	if cfg.MaxLogEntries == 0 {
		cfg.MaxLogEntries = 100000
	}

	raftDir := filepath.Join(cfg.DataDir, "raft", "log")
	snapDir := filepath.Join(cfg.DataDir, "raft", "snapshots")

	raftLog := NewRaftLog(raftDir)
	fsm := NewMetadataFSM()
	snapshotter := NewSnapshotter(snapDir, fsm, raftLog)

	n := &RaftNode{
		id:          cfg.NodeID,
		state:       Follower,
		cfg:         cfg,
		log:         raftLog,
		fsm:         fsm,
		snapshotter: snapshotter,
		peers:       cfg.Peers,
		nextIndex:   make(map[NodeID]Index),
		matchIndex:  make(map[NodeID]Index),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
	}

	return n, nil
}

// Start starts the Raft node.
func (n *RaftNode) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Load persisted state
	if err := n.log.Load(); err != nil {
		return fmt.Errorf("load log: %w", err)
	}
	if meta, err := n.snapshotter.LoadSnapshot(); err != nil {
		return fmt.Errorf("load snapshot: %w", err)
	} else if meta != nil {
		n.commitIndex = meta.LastIncludedIndex
		n.lastApplied = meta.LastIncludedIndex
	}

	// Load term/votedFor from disk
	n.loadState()

	// Initialize leader state
	for _, p := range n.peers {
		n.nextIndex[p] = n.log.LastIndex() + 1
		n.matchIndex[p] = 0
	}

	// Start election timer
	n.resetElectionTimer()

	// Start background goroutines
	go n.run()

	return nil
}

// Stop stops the Raft node.
func (n *RaftNode) Stop() {
	n.mu.Lock()
	n.state = Shutdown
	n.mu.Unlock()

	close(n.stopCh)

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTick != nil {
		n.heartbeatTick.Stop()
	}

	<-n.done
}

// Propose submits a command to the Raft log.
func (n *RaftNode) Propose(data []byte) (Index, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return 0, fmt.Errorf("not leader")
	}

	idx := n.log.LastIndex() + 1
	entry := LogEntry{
		Index: idx,
		Term:  n.currentTerm,
		Type:  EntryCommand,
		Data:  data,
	}
	n.log.Append(entry)
	_ = n.log.Save()

	// Replicate to followers
	go n.replicateLog()

	return idx, nil
}

// State returns the current node state.
func (n *RaftNode) State() NodeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// Term returns the current term.
func (n *RaftNode) Term() Term {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

// IsLeader returns true if this node is the leader.
func (n *RaftNode) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// LeaderID returns the current leader ID (may be empty if unknown).
func (n *RaftNode) LeaderID() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state == Leader {
		return n.id
	}
	return n.votedFor
}

// FSM returns the metadata FSM for read access.
func (n *RaftNode) FSM() *MetadataFSM {
	return n.fsm
}

// CommitIndex returns the current commit index.
func (n *RaftNode) CommitIndex() Index {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// run is the main event loop.
func (n *RaftNode) run() {
	defer close(n.done)

	snapshotTicker := time.NewTicker(n.cfg.SnapshotInterval)
	defer snapshotTicker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-n.electionTimer.C:
			n.mu.Lock()
			if n.state != Leader {
				n.startElection()
			} else {
				n.resetElectionTimerLocked()
			}
			n.mu.Unlock()
		case <-snapshotTicker.C:
			n.maybeSnapshot()
		}
	}
}

// resetElectionTimer resets with a randomized timeout.
func (n *RaftNode) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := n.randomElectionTimeout()
	n.electionTimer = time.NewTimer(timeout)
}

func (n *RaftNode) resetElectionTimerLocked() {
	n.resetElectionTimer()
}

func (n *RaftNode) randomElectionTimeout() time.Duration {
	min := n.cfg.ElectionTimeout
	max := min * 2
	delta := max - min
	return min + time.Duration(rand.Int63n(int64(delta)))
}

// startElection starts a new election.
func (n *RaftNode) startElection() {
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.saveState()

	votesReceived := 1 // self

	lastIndex := n.log.LastIndex()
	lastTerm := n.log.LastTerm()

	req := &RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	n.resetElectionTimerLocked()

	for _, peer := range n.peers {
		peer := peer
		go func() {
			resp, err := n.transport.SendRequestVote(peer, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term)
				return
			}

			if n.state != Candidate || n.currentTerm != req.Term {
				return
			}

			if resp.VoteGranted {
				votesReceived++
				if votesReceived > len(n.peers)/2+1 {
					n.becomeLeader()
				}
			}
		}()
	}
}

// becomeLeader transitions to leader state.
func (n *RaftNode) becomeLeader() {
	n.state = Leader

	// Initialize nextIndex/matchIndex
	lastIdx := n.log.LastIndex()
	for _, p := range n.peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}

	// Append no-op entry for commit advancement
	noop := LogEntry{
		Index: lastIdx + 1,
		Term:  n.currentTerm,
		Type:  EntryNoOp,
	}
	n.log.Append(noop)
	_ = n.log.Save()

	n.resetElectionTimerLocked()

	// Start heartbeats
	go n.heartbeatLoop()
}

// becomeFollower transitions to follower state.
func (n *RaftNode) becomeFollower(term Term) {
	n.state = Follower
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.saveState()
	}
	n.resetElectionTimerLocked()
}

// heartbeatLoop sends periodic heartbeats to all peers.
func (n *RaftNode) heartbeatLoop() {
	ticker := time.NewTicker(n.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			n.replicateLog()
		}
	}
}

// replicateLog sends AppendEntries to all followers.
func (n *RaftNode) replicateLog() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	term := n.currentTerm
	commitIdx := n.commitIndex
	leaderID := n.id

	for _, peer := range n.peers {
		nextIdx := n.nextIndex[peer]
		prevIdx := nextIdx - 1
		prevTerm := n.log.TermAt(prevIdx)
		entries := n.log.Range(nextIdx, n.log.LastIndex()+1)

		req := &AppendEntriesRequest{
			Term:         term,
			LeaderID:     leaderID,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: commitIdx,
		}
		n.mu.Unlock()

		resp, err := n.transport.SendAppendEntries(peer, req)

		n.mu.Lock()
		if err != nil {
			n.mu.Unlock()
			continue
		}

		if resp.Term > n.currentTerm {
			n.becomeFollower(resp.Term)
			n.mu.Unlock()
			return
		}

		if resp.Success {
			if len(entries) > 0 {
				n.nextIndex[peer] = entries[len(entries)-1].Index + 1
				n.matchIndex[peer] = entries[len(entries)-1].Index
			}
			n.advanceCommitIndex()
		} else {
			// Fast rollback: use conflict info
			if resp.ConflictIndex > 0 {
				n.nextIndex[peer] = resp.ConflictIndex
			} else if n.nextIndex[peer] > 1 {
				n.nextIndex[peer]--
			}
		}
	}
	n.mu.Unlock()
}

// advanceCommitIndex updates commitIndex based on matchIndex.
func (n *RaftNode) advanceCommitIndex() {
	for idx := n.log.LastIndex(); idx > n.commitIndex; idx-- {
		if n.log.TermAt(idx) != n.currentTerm {
			continue
		}
		replicated := 1 // self
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				replicated++
			}
		}
		if replicated > (len(n.peers)+1)/2 {
			n.commitIndex = idx
			n.applyCommitted()
			break
		}
	}
}

// applyCommitted applies committed but unapplied entries to the FSM.
func (n *RaftNode) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log.Get(n.lastApplied)
		if entry != nil && entry.Type == EntryCommand {
			_ = n.fsm.Apply(*entry)
		}
	}
}

// HandleAppendEntries handles an incoming AppendEntries RPC.
func (n *RaftNode) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &AppendEntriesResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
		resp.Term = req.Term
	}

	n.resetElectionTimerLocked()

	// Check log consistency
	if req.PrevLogIndex > 0 {
		localTerm := n.log.TermAt(req.PrevLogIndex)
		if localTerm != req.PrevLogTerm {
			resp.Success = false
			resp.ConflictIndex = n.findConflictIndex(req.PrevLogIndex)
			resp.ConflictTerm = localTerm
			return resp
		}
	}

	// Append new entries
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + Index(i) + 1
		existing := n.log.Get(idx)
		if existing != nil {
			if existing.Term != entry.Term {
				// Conflict: truncate from here
				n.log.TruncateAfter(idx - 1)
				n.log.Append(entry)
			}
			// Same entry already exists, skip
		} else {
			n.log.Append(entry)
		}
	}
	_ = n.log.Save()

	resp.Success = true

	// Update commitIndex
	if req.LeaderCommit > n.commitIndex {
		lastNewIdx := req.PrevLogIndex + Index(len(req.Entries))
		if req.LeaderCommit < lastNewIdx {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIdx
		}
		n.applyCommitted()
	}

	return resp
}

// HandleRequestVote handles an incoming RequestVote RPC.
func (n *RaftNode) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &RequestVoteResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
		resp.Term = req.Term
	}

	// Check if we can vote
	if n.votedFor == "" || n.votedFor == req.CandidateID {
		// Check if candidate's log is at least as up-to-date
		lastTerm := n.log.LastTerm()
		lastIndex := n.log.LastIndex()

		upToDate := req.LastLogTerm > lastTerm ||
			(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)

		if upToDate {
			n.votedFor = req.CandidateID
			n.saveState()
			resp.VoteGranted = true
			n.resetElectionTimerLocked()
		}
	}

	return resp
}

// HandleInstallSnapshot handles an incoming InstallSnapshot RPC.
func (n *RaftNode) HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &InstallSnapshotResponse{Term: n.currentTerm}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
		resp.Term = req.Term
	}

	// Restore FSM from snapshot
	if err := n.fsm.Restore(req.Data); err != nil {
		return resp
	}

	n.log.Compact(req.LastIncludedIndex)
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex
	_ = n.log.Save()

	return resp
}

// findConflictIndex finds the first index with a conflicting term.
func (n *RaftNode) findConflictIndex(idx Index) Index {
	for i := idx; i >= 1; i-- {
		entry := n.log.Get(i)
		if entry == nil {
			continue
		}
		// Find the first entry in that term
		prev := n.log.Get(i - 1)
		if prev == nil || prev.Term != entry.Term {
			return i
		}
	}
	return 1
}

// maybeSnapshot triggers a snapshot if needed.
func (n *RaftNode) maybeSnapshot() {
	n.mu.Lock()
	shouldSnapshot := n.snapshotter.ShouldSnapshot(n.cfg.MaxLogEntries)
	n.mu.Unlock()

	if shouldSnapshot {
		_, _ = n.snapshotter.TakeSnapshot()
	}
}

// loadState loads persisted term and votedFor from disk.
func (n *RaftNode) loadState() {
	path := filepath.Join(n.cfg.DataDir, "raft", "state.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var state struct {
		CurrentTerm Term   `json:"current_term"`
		VotedFor    NodeID `json:"voted_for"`
	}
	if json.Unmarshal(data, &state) == nil {
		n.currentTerm = state.CurrentTerm
		n.votedFor = state.VotedFor
	}
}

// saveState persists term and votedFor to disk.
func (n *RaftNode) saveState() {
	path := filepath.Join(n.cfg.DataDir, "raft", "state.json")
	_ = os.MkdirAll(filepath.Dir(path), 0755)
	state := struct {
		CurrentTerm Term   `json:"current_term"`
		VotedFor    NodeID `json:"voted_for"`
	}{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
	}
	data, _ := json.Marshal(state)
	_ = os.WriteFile(path, data, 0644)
}

// SetTransport sets the transport layer.
func (n *RaftNode) SetTransport(t Transport) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.transport = t
}
