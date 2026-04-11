package integration

import (
	"encoding/json"
	"net"
	"os"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/cluster/raft"
)

// createRaftNode creates a RaftNode with a temp directory and TCP listener.
func createRaftNode(t *testing.T, id raft.NodeID, peers []raft.NodeID, electionTimeout time.Duration) (*raft.RaftNode, *raft.TCPTransport, net.Listener, string) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "chimera-raft-"+string(id)+"-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	// Find a free port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	cfg := raft.Config{
		NodeID:            id,
		Peers:             peers,
		ElectionTimeout:   electionTimeout,
		HeartbeatInterval: 50 * time.Millisecond,
		SnapshotInterval:  1 * time.Hour, // disable for tests
		MaxLogEntries:     10000,
		DataDir:           tmpDir,
	}

	node, err := raft.NewRaftNode(cfg)
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}

	transport := raft.NewTCPTransport()
	node.SetTransport(transport)

	return node, transport, ln, ln.Addr().String()
}

// startNode starts the RaftNode and serves RPC on the listener.
func startNode(t *testing.T, node *raft.RaftNode, ln net.Listener) {
	t.Helper()
	go raft.ServeRPC(ln, node)
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
}

// stopNode stops a node and closes its listener.
func stopNode(t *testing.T, node *raft.RaftNode, ln net.Listener) {
	t.Helper()
	node.Stop()
	ln.Close()
}

// waitForLeader polls until a node reports as leader or timeout.
func waitForLeader(t *testing.T, nodes []*raft.RaftNode, timeout time.Duration) *raft.RaftNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

// waitForCommitIndex waits until the node's commit index reaches target.
func waitForCommitIndex(t *testing.T, node *raft.RaftNode, target raft.Index, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if node.CommitIndex() >= target {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("commit index %d not reached within timeout (current: %d)", target, node.CommitIndex())
}

// waitForFSMTopic waits until the node's FSM has the topic replicated.
func waitForFSMTopic(t *testing.T, node *raft.RaftNode, topicName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if topic := node.FSM().GetTopic(topicName); topic != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("topic %q not found in FSM within timeout", topicName)
}

// TestClusterLeaderElection tests that a 3-node Raft cluster elects a leader.
func TestClusterLeaderElection(t *testing.T) {
	ids := []raft.NodeID{"node-1", "node-2", "node-3"}
	var nodes []*raft.RaftNode
	var transports []*raft.TCPTransport
	var listeners []net.Listener
	var addrs []string

	// Create nodes first to get addresses
	for _, id := range ids {
		peers := make([]raft.NodeID, 0, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		node, transport, ln, addr := createRaftNode(t, id, peers, 200*time.Millisecond)
		nodes = append(nodes, node)
		transports = append(transports, transport)
		listeners = append(listeners, ln)
		addrs = append(addrs, addr)
	}

	// Wire up peer addresses
	for i, transport := range transports {
		for j, addr := range addrs {
			if i != j {
				transport.SetAddr(ids[j], addr)
			}
		}
	}

	// Start all nodes
	for i, node := range nodes {
		startNode(t, node, listeners[i])
	}
	defer func() {
		for i, node := range nodes {
			stopNode(t, node, listeners[i])
		}
	}()

	leader := waitForLeader(t, nodes, 5*time.Second)
	t.Logf("Leader elected: %s", leader.ID())

	if leader.State() != raft.Leader {
		t.Errorf("expected Leader state, got %s", leader.State())
	}

	// Verify exactly one leader
	leaderCount := 0
	for _, n := range nodes {
		if n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Errorf("expected exactly 1 leader, got %d", leaderCount)
	}
}

// TestClusterLogReplication tests that a proposal on the leader replicates
// to follower FSMs.
func TestClusterLogReplication(t *testing.T) {
	ids := []raft.NodeID{"node-1", "node-2", "node-3"}
	var nodes []*raft.RaftNode
	var transports []*raft.TCPTransport
	var listeners []net.Listener
	var addrs []string

	for _, id := range ids {
		peers := make([]raft.NodeID, 0, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		node, transport, ln, addr := createRaftNode(t, id, peers, 200*time.Millisecond)
		nodes = append(nodes, node)
		transports = append(transports, transport)
		listeners = append(listeners, ln)
		addrs = append(addrs, addr)
	}

	for i, transport := range transports {
		for j, addr := range addrs {
			if i != j {
				transport.SetAddr(ids[j], addr)
			}
		}
	}

	for i, node := range nodes {
		startNode(t, node, listeners[i])
	}
	defer func() {
		for i, node := range nodes {
			stopNode(t, node, listeners[i])
		}
	}()

	leader := waitForLeader(t, nodes, 5*time.Second)
	t.Logf("Leader: %s", leader.ID())

	// Propose a topic creation via Raft
	cmd := raft.Command{
		Type: raft.CmdCreateTopic,
		Data: mustMarshal(t, raft.TopicEntry{
			Name:       "test-topic",
			Mode:       "unified",
			Partitions: 4,
		}),
	}
	cmdData := mustMarshal(t, cmd)

	idx, err := leader.Propose(cmdData)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	t.Logf("Proposed at index %d", idx)

	// Wait for commit
	waitForCommitIndex(t, leader, idx, 3*time.Second)

	// Wait for FSM to apply on all nodes
	for i, n := range nodes {
		waitForFSMTopic(t, n, "test-topic", 3*time.Second)
		topic := n.FSM().GetTopic("test-topic")
		t.Logf("Node %s: topic=%v partitions=%d", n.ID(), topic.Name, topic.Partitions)
		if topic.Name != "test-topic" {
			t.Errorf("node %d: expected topic name 'test-topic', got %q", i, topic.Name)
		}
		if topic.Partitions != 4 {
			t.Errorf("node %d: expected 4 partitions, got %d", i, topic.Partitions)
		}
	}
}

// TestClusterLeaderFailover tests that killing the leader triggers a new election.
func TestClusterLeaderFailover(t *testing.T) {
	ids := []raft.NodeID{"node-1", "node-2", "node-3"}
	var nodes []*raft.RaftNode
	var transports []*raft.TCPTransport
	var listeners []net.Listener
	var addrs []string

	for _, id := range ids {
		peers := make([]raft.NodeID, 0, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		node, transport, ln, addr := createRaftNode(t, id, peers, 200*time.Millisecond)
		nodes = append(nodes, node)
		transports = append(transports, transport)
		listeners = append(listeners, ln)
		addrs = append(addrs, addr)
	}

	for i, transport := range transports {
		for j, addr := range addrs {
			if i != j {
				transport.SetAddr(ids[j], addr)
			}
		}
	}

	for i, node := range nodes {
		startNode(t, node, listeners[i])
	}
	defer func() {
		for i, node := range nodes {
			if node.State() != raft.Shutdown {
				stopNode(t, node, listeners[i])
			}
		}
	}()

	// Wait for initial leader
	leader := waitForLeader(t, nodes, 5*time.Second)
	t.Logf("Initial leader: %s", leader.ID())

	// Stop the leader
	for i, n := range nodes {
		if n == leader {
			t.Logf("Stopping leader %s", n.ID())
			stopNode(t, n, listeners[i])
			break
		}
	}

	// Wait for a new leader among remaining nodes
	var remaining []*raft.RaftNode
	for _, n := range nodes {
		if n.State() != raft.Shutdown {
			remaining = append(remaining, n)
		}
	}

	newLeader := waitForLeader(t, remaining, 5*time.Second)
	t.Logf("New leader after failover: %s", newLeader.ID())

	if newLeader.ID() == leader.ID() {
		t.Error("new leader should not be the dead node")
	}
	if newLeader.State() != raft.Leader {
		t.Errorf("expected Leader state, got %s", newLeader.State())
	}
}

// TestClusterMultipleProposals tests sequential proposals on the leader.
func TestClusterMultipleProposals(t *testing.T) {
	ids := []raft.NodeID{"node-1", "node-2", "node-3"}
	var nodes []*raft.RaftNode
	var transports []*raft.TCPTransport
	var listeners []net.Listener
	var addrs []string

	for _, id := range ids {
		peers := make([]raft.NodeID, 0, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		node, transport, ln, addr := createRaftNode(t, id, peers, 200*time.Millisecond)
		nodes = append(nodes, node)
		transports = append(transports, transport)
		listeners = append(listeners, ln)
		addrs = append(addrs, addr)
	}

	for i, transport := range transports {
		for j, addr := range addrs {
			if i != j {
				transport.SetAddr(ids[j], addr)
			}
		}
	}

	for i, node := range nodes {
		startNode(t, node, listeners[i])
	}
	defer func() {
		for i, node := range nodes {
			stopNode(t, node, listeners[i])
		}
	}()

	leader := waitForLeader(t, nodes, 5*time.Second)

	// Propose 5 topics
	topics := []string{"orders", "payments", "notifications", "analytics", "events"}
	for _, topicName := range topics {
		cmd := raft.Command{
			Type: raft.CmdCreateTopic,
			Data: mustMarshal(t, raft.TopicEntry{
				Name:       topicName,
				Mode:       "unified",
				Partitions: 4,
			}),
		}
		if _, err := leader.Propose(mustMarshal(t, cmd)); err != nil {
			t.Fatalf("Propose %s failed: %v", topicName, err)
		}
	}

	// Wait for all topics to replicate
	for _, topicName := range topics {
		for _, n := range nodes {
			waitForFSMTopic(t, n, topicName, 5*time.Second)
		}
	}

	// Verify topic list on each node
	for _, n := range nodes {
		topicsList := n.FSM().ListTopics()
		if len(topicsList) != 5 {
			t.Errorf("node %s: expected 5 topics, got %d", n.ID(), len(topicsList))
		}
	}
}

// TestClusterSingleNode tests that a single node can elect itself leader.
func TestClusterSingleNode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "chimera-raft-single-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	cfg := raft.Config{
		NodeID:            "node-1",
		Peers:             []raft.NodeID{}, // no peers
		ElectionTimeout:   200 * time.Millisecond,
		HeartbeatInterval: 50 * time.Millisecond,
		SnapshotInterval:  1 * time.Hour,
		DataDir:           tmpDir,
	}

	node, err := raft.NewRaftNode(cfg)
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}

	transport := raft.NewTCPTransport()
	node.SetTransport(transport)

	go raft.ServeRPC(ln, node)
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	defer stopNode(t, node, ln)

	leader := waitForLeader(t, []*raft.RaftNode{node}, 3*time.Second)
	if leader.ID() != "node-1" {
		t.Errorf("expected node-1 as leader, got %s", leader.ID())
	}
}

// TestClusterPartitionAssignment tests proposing a partition assignment through Raft.
func TestClusterPartitionAssignment(t *testing.T) {
	ids := []raft.NodeID{"node-1", "node-2", "node-3"}
	var nodes []*raft.RaftNode
	var transports []*raft.TCPTransport
	var listeners []net.Listener
	var addrs []string

	for _, id := range ids {
		peers := make([]raft.NodeID, 0, len(ids)-1)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		node, transport, ln, addr := createRaftNode(t, id, peers, 200*time.Millisecond)
		nodes = append(nodes, node)
		transports = append(transports, transport)
		listeners = append(listeners, ln)
		addrs = append(addrs, addr)
	}

	for i, transport := range transports {
		for j, addr := range addrs {
			if i != j {
				transport.SetAddr(ids[j], addr)
			}
		}
	}

	for i, node := range nodes {
		startNode(t, node, listeners[i])
	}
	defer func() {
		for i, node := range nodes {
			stopNode(t, node, listeners[i])
		}
	}()

	leader := waitForLeader(t, nodes, 5*time.Second)

	// Propose partition assignment
	assignment := raft.PartitionAssignment{
		Topic:     "orders",
		Partition: 0,
		Leader:    "node-1",
		Replicas:  []raft.NodeID{"node-1", "node-2", "node-3"},
	}
	cmd := raft.Command{
		Type: raft.CmdAssignPartition,
		Data: mustMarshal(t, assignment),
	}
	idx, err := leader.Propose(mustMarshal(t, cmd))
	if err != nil {
		t.Fatalf("Propose partition assignment failed: %v", err)
	}

	waitForCommitIndex(t, leader, idx, 3*time.Second)

	// Verify assignment on all nodes
	for _, n := range nodes {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			a := n.FSM().GetAssignment("orders", 0)
			if a != nil && a.Leader == "node-1" {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		a := n.FSM().GetAssignment("orders", 0)
		if a == nil {
			t.Errorf("node %s: partition assignment not found", n.ID())
		} else if a.Leader != "node-1" {
			t.Errorf("node %s: expected leader node-1, got %s", n.ID(), a.Leader)
		}
	}
}

func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
