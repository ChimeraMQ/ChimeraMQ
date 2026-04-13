package cluster

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/chimeramq/chimera/internal/cluster/gossip"
	"github.com/chimeramq/chimera/internal/cluster/raft"
	"github.com/chimeramq/chimera/internal/cluster/replication"
)

// Manager coordinates all cluster components.
type Manager struct {
	mu       sync.Mutex
	cfg      ClusterConfig
	raftNode *raft.RaftNode
	swim     *gossip.SWIM
	nodeID   raft.NodeID
	started  bool
}

// ClusterConfig holds cluster configuration from the broker.
type ClusterConfig struct {
	NodeID            string
	DataDir           string
	Peers             []string
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	SnapshotInterval  time.Duration
	MaxLogEntries     int
	RaftTLSEnabled    bool
	RaftCertFile      string
	RaftKeyFile       string
	RaftCAFile        string
	GossipBindPort    int
	GossipSeeds       []string
	GossipHMACKey     []byte
	ProbeInterval     time.Duration
	ProbeTimeout      time.Duration
	IndirectNodes     int
	SuspicionTimeout  time.Duration
	ReplicationFactor int
	MinISR            int
	AckPolicy         string
	SyncMode          string
	MaxLag            int64
}

// NewManager creates a new cluster manager.
func NewManager(cfg ClusterConfig) (*Manager, error) {
	return &Manager{
		cfg:    cfg,
		nodeID: raft.NodeID(cfg.NodeID),
	}, nil
}

// Start initializes and starts all cluster components.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return nil
	}

	// Parse peers
	peerIDs := make([]raft.NodeID, len(m.cfg.Peers))
	peerAddrs := make(map[raft.NodeID]string, len(m.cfg.Peers))
	for i, p := range m.cfg.Peers {
		peerID := raft.NodeID(fmt.Sprintf("node-%d", i+1))
		peerIDs[i] = peerID
		peerAddrs[peerID] = p
	}

	// Start Raft
	raftCfg := raft.Config{
		NodeID:            m.nodeID,
		Peers:             peerIDs,
		ElectionTimeout:   m.cfg.ElectionTimeout,
		HeartbeatInterval: m.cfg.HeartbeatInterval,
		SnapshotInterval:  m.cfg.SnapshotInterval,
		MaxLogEntries:     m.cfg.MaxLogEntries,
		DataDir:           m.cfg.DataDir,
		TLSEnabled:        m.cfg.RaftTLSEnabled,
		CertFile:          m.cfg.RaftCertFile,
		KeyFile:           m.cfg.RaftKeyFile,
		CAFile:            m.cfg.RaftCAFile,
	}

	node, err := raft.NewRaftNode(raftCfg)
	if err != nil {
		return fmt.Errorf("create raft node: %w", err)
	}

	// Set up transport with optional TLS
	var transport *raft.TCPTransport
	if m.cfg.RaftTLSEnabled {
		tlsCfg, err := raft.LoadTLSConfig(m.cfg.RaftCertFile, m.cfg.RaftKeyFile, m.cfg.RaftCAFile)
		if err != nil {
			return fmt.Errorf("load raft TLS: %w", err)
		}
		transport = raft.NewTCPTransportWithTLS(tlsCfg)
	} else {
		transport = raft.NewTCPTransport()
	}
	for id, addr := range peerAddrs {
		transport.SetAddr(id, addr)
	}
	node.SetTransport(transport)

	if err := node.Start(); err != nil {
		return fmt.Errorf("start raft: %w", err)
	}
	m.raftNode = node

	// Start SWIM gossip
	swimCfg := gossip.Config{
		NodeID:           gossip.NodeID(m.cfg.NodeID),
		BindAddr:         "0.0.0.0",
		BindPort:         m.cfg.GossipBindPort,
		Seeds:            m.cfg.GossipSeeds,
		ProbeInterval:    m.cfg.ProbeInterval,
		ProbeTimeout:     m.cfg.ProbeTimeout,
		IndirectNodes:    m.cfg.IndirectNodes,
		SuspicionTimeout: m.cfg.SuspicionTimeout,
		HMACKey:          m.cfg.GossipHMACKey,
	}

	swim, err := gossip.NewSWIM(swimCfg)
	if err != nil {
		node.Stop()
		return fmt.Errorf("create swim: %w", err)
	}
	if err := swim.Start(); err != nil {
		node.Stop()
		return fmt.Errorf("start swim: %w", err)
	}
	m.swim = swim

	m.started = true
	return nil
}

// Stop gracefully shuts down all cluster components.
func (m *Manager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return
	}

	if m.swim != nil {
		m.swim.Stop()
	}
	if m.raftNode != nil {
		m.raftNode.Stop()
	}
	m.started = false
}

// IsLeader returns whether this node is the Raft leader.
func (m *Manager) IsLeader() bool {
	if m.raftNode == nil {
		return false
	}
	return m.raftNode.IsLeader()
}

// LeaderID returns the current leader's node ID.
func (m *Manager) LeaderID() string {
	if m.raftNode == nil {
		return ""
	}
	return string(m.raftNode.LeaderID())
}

// ProposeCreateTopic proposes topic creation through Raft.
func (m *Manager) ProposeCreateTopic(name, mode string, partitions uint32, replicaSet []string) error {
	if m.raftNode == nil {
		return fmt.Errorf("raft not started")
	}

	entry := raft.TopicEntry{
		Name:       name,
		Mode:       mode,
		Partitions: partitions,
		ReplicaSet: toNodeIDs(replicaSet),
	}
	data, _ := json.Marshal(entry)
	cmd := raft.Command{Type: raft.CmdCreateTopic, Data: data}
	cmdData, _ := json.Marshal(cmd)

	_, err := m.raftNode.Propose(cmdData)
	return err
}

// ProposeDeleteTopic proposes topic deletion through Raft.
func (m *Manager) ProposeDeleteTopic(name string) error {
	if m.raftNode == nil {
		return fmt.Errorf("raft not started")
	}

	data, _ := json.Marshal(map[string]string{"name": name})
	cmd := raft.Command{Type: raft.CmdDeleteTopic, Data: data}
	cmdData, _ := json.Marshal(cmd)

	_, err := m.raftNode.Propose(cmdData)
	return err
}

// GetTopic returns topic metadata from the FSM.
func (m *Manager) GetTopic(name string) *raft.TopicEntry {
	if m.raftNode == nil {
		return nil
	}
	return m.raftNode.FSM().GetTopic(name)
}

// ListTopics returns all topics from the FSM.
func (m *Manager) ListTopics() []*raft.TopicEntry {
	if m.raftNode == nil {
		return nil
	}
	return m.raftNode.FSM().ListTopics()
}

// GetAssignment returns partition assignment.
func (m *Manager) GetAssignment(topic string, partition uint32) *raft.PartitionAssignment {
	if m.raftNode == nil {
		return nil
	}
	return m.raftNode.FSM().GetAssignment(topic, partition)
}

// AssignPartition proposes a partition assignment through Raft.
func (m *Manager) AssignPartition(topic string, partition uint32, leader string, replicas []string) error {
	if m.raftNode == nil {
		return fmt.Errorf("raft not started")
	}

	pa := raft.PartitionAssignment{
		Topic:     topic,
		Partition: partition,
		Leader:    raft.NodeID(leader),
		Replicas:  toNodeIDs(replicas),
	}
	data, _ := json.Marshal(pa)
	cmd := raft.Command{Type: raft.CmdAssignPartition, Data: data}
	cmdData, _ := json.Marshal(cmd)

	_, err := m.raftNode.Propose(cmdData)
	return err
}

// Members returns the list of alive cluster members.
func (m *Manager) Members() []*gossip.Member {
	if m.swim == nil {
		return nil
	}
	return m.swim.Members()
}

// AliveCount returns the number of alive members.
func (m *Manager) AliveCount() int {
	if m.swim == nil {
		return 1
	}
	return m.swim.AliveCount()
}

// RaftNode returns the underlying Raft node.
func (m *Manager) RaftNode() *raft.RaftNode {
	return m.raftNode
}

// FSM returns the metadata FSM.
func (m *Manager) FSM() *raft.MetadataFSM {
	if m.raftNode == nil {
		return nil
	}
	return m.raftNode.FSM()
}

// NewReplicator creates a replication manager for a partition.
func (m *Manager) NewReplicator(topic string, partition uint32) *replication.Replicator {
	policy := replication.ParseAckPolicy(m.cfg.AckPolicy)
	rep := replication.NewReplicator(topic, partition, m.nodeID, policy, m.cfg.MaxLag)
	if m.raftNode != nil {
		rep.SetTransport(&replicationTransportAdapter{raftNode: m.raftNode})
	}
	return rep
}

func toNodeIDs(ss []string) []raft.NodeID {
	ids := make([]raft.NodeID, len(ss))
	for i, s := range ss {
		ids[i] = raft.NodeID(s)
	}
	return ids
}

// replicationTransportAdapter adapts the Raft node to serve as a ReplicationTransport.
// Data replication uses the Raft log replication mechanism, which handles
// consensus-based replication to followers via AppendEntries RPCs.
type replicationTransportAdapter struct {
	raftNode *raft.RaftNode
}

func (a *replicationTransportAdapter) Replicate(nodeID raft.NodeID, req *replication.ReplicateRequest) error {
	// Propose data through Raft — the entry will be replicated to all nodes
	// via the Raft log replication mechanism.
	_, err := a.raftNode.Propose(req.Data)
	return err
}

func (a *replicationTransportAdapter) FetchEntries(nodeID raft.NodeID, req *replication.FetchRequest) (*replication.FetchResponse, error) {
	// Followers fetch from their local storage after Raft commit.
	return &replication.FetchResponse{}, nil
}
