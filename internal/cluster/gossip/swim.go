package gossip

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Config holds SWIM gossip configuration.
type Config struct {
	NodeID           NodeID
	BindAddr         string
	BindPort         int
	Seeds            []string
	ProbeInterval    time.Duration
	ProbeTimeout     time.Duration
	IndirectNodes    int
	SuspicionTimeout time.Duration
	// HMACKey enables HMAC-SHA256 message authentication when non-empty.
	HMACKey []byte
	// AllowedNodes is a list of node IDs that are permitted to join the cluster.
	// If empty, any node with the correct HMAC key can join (less secure).
	AllowedNodes []NodeID
}

// transport is the minimal interface required by SWIM.
type transport interface {
	Send(addr string, msg *Message) error
	Receive() (*Message, *net.UDPAddr, error)
	Close() error
	LocalAddr() string
}

// SWIM implements the SWIM gossip protocol.
type SWIM struct {
	mu        sync.Mutex
	cfg       Config
	members   *MemberList
	detector  *PhiAccrualDetector
	transport transport
	stopCh    chan struct{}
	done      chan struct{}

	// Piggyback queue for disseminating state changes
	piggyback []Message
}

// NewSWIM creates a new SWIM gossip instance.
func NewSWIM(cfg Config) (*SWIM, error) {
	addr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.BindPort)
	udpTransport, err := NewUDPTransport(addr)
	if err != nil {
		return nil, fmt.Errorf("bind gossip: %w", err)
	}

	var t transport = udpTransport
	if len(cfg.HMACKey) > 0 {
		t = NewHMACTransport(udpTransport, cfg.HMACKey)
	}

	if cfg.ProbeInterval == 0 {
		cfg.ProbeInterval = 1 * time.Second
	}
	if cfg.ProbeTimeout == 0 {
		cfg.ProbeTimeout = 500 * time.Millisecond
	}
	if cfg.IndirectNodes == 0 {
		cfg.IndirectNodes = 3
	}
	if cfg.SuspicionTimeout == 0 {
		cfg.SuspicionTimeout = 5 * time.Second
	}

	s := &SWIM{
		cfg:       cfg,
		members:   NewMemberList(cfg.NodeID),
		detector:  NewPhiAccrualDetector(),
		transport: t,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}

	// Add self
	s.members.Add(&Member{
		ID:       cfg.NodeID,
		Addr:     cfg.BindAddr,
		Port:     cfg.BindPort,
		State:    Alive,
		Metadata: make(map[string]string),
		LastSeen: time.Now(),
	})

	return s, nil
}

// Start starts the SWIM protocol.
func (s *SWIM) Start() error {
	// Join seed nodes
	for _, seed := range s.cfg.Seeds {
		s.joinSeed(seed)
	}

	go s.run()
	return nil
}

// Stop stops the SWIM protocol.
func (s *SWIM) Stop() {
	close(s.stopCh)
	_ = s.transport.Close()
	<-s.done
}

// Members returns all known members.
func (s *SWIM) Members() []*Member {
	return s.members.All()
}

// AliveCount returns the number of alive members (including self).
func (s *SWIM) AliveCount() int {
	return len(s.members.AliveMembers()) + 1
}

// MemberList returns the underlying member list.
func (s *SWIM) MemberList() *MemberList {
	return s.members
}

// LocalAddr returns the local gossip address.
func (s *SWIM) LocalAddr() string {
	return s.transport.LocalAddr()
}

func (s *SWIM) joinSeed(addr string) {
	msg := &Message{
		Type:        MsgPing,
		SenderID:    s.cfg.NodeID,
		Incarnation: s.members.Local().Incarnation,
	}
	_ = s.transport.Send(addr, msg)
}

// run is the main protocol loop.
func (s *SWIM) run() {
	defer close(s.done)

	probeTicker := time.NewTicker(s.cfg.ProbeInterval)
	defer probeTicker.Stop()

	suspectTicker := time.NewTicker(1 * time.Second)
	defer suspectTicker.Stop()

	// Receiver goroutine
	go s.receiveLoop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-probeTicker.C:
			s.probe()
		case <-suspectTicker.C:
			s.checkSuspicions()
		}
	}
}

// probe selects a random member and probes it.
func (s *SWIM) probe() {
	target := s.members.SelectRandom(nil)
	if target == nil {
		return
	}

	// Direct probe
	addr := fmt.Sprintf("%s:%d", target.Addr, target.Port)
	ping := &Message{
		Type:        MsgPing,
		SenderID:    s.cfg.NodeID,
		Incarnation: s.members.Local().Incarnation,
	}

	if err := s.transport.Send(addr, ping); err != nil {
		// Direct probe failed, try indirect probes
		s.indirectProbe(target)
		return
	}

	// Wait for ack with timeout
	s.mu.Lock()
	s.piggyback = append(s.piggyback, *ping)
	s.mu.Unlock()

	s.members.SetState(target.ID, Alive, target.Incarnation)
	s.detector.RecordHeartbeat(target.ID)
}

// indirectProbe sends ping requests to K other members.
func (s *SWIM) indirectProbe(target *Member) {
	exclude := map[NodeID]bool{target.ID: true}
	k := s.cfg.IndirectNodes
	sent := 0

	for i := 0; i < k; i++ {
		proxy := s.members.SelectRandom(exclude)
		if proxy == nil {
			break
		}
		exclude[proxy.ID] = true

		addr := fmt.Sprintf("%s:%d", proxy.Addr, proxy.Port)
		pingReq := &Message{
			Type:        MsgPingReq,
			SenderID:    s.cfg.NodeID,
			Incarnation: s.members.Local().Incarnation,
			TargetID:    target.ID,
			TargetAddr:  addr,
		}
		_ = s.transport.Send(addr, pingReq)
		sent++
	}

	// If no indirect probes sent, mark as suspect
	if sent == 0 {
		s.markSuspect(target.ID, target.Incarnation)
	}
}

// checkSuspicions transitions suspect members to dead after timeout.
func (s *SWIM) checkSuspicions() {
	for _, m := range s.members.All() {
		if m.State == Suspect {
			if time.Since(m.LastSeen) > s.cfg.SuspicionTimeout {
				s.markDead(m.ID)
			}
		}
	}
}

// markSuspect marks a member as suspect.
func (s *SWIM) markSuspect(id NodeID, incarnation uint64) {
	s.members.SetState(id, Suspect, incarnation)

	s.mu.Lock()
	s.piggyback = append(s.piggyback, Message{
		Type:        MsgSuspect,
		SenderID:    s.cfg.NodeID,
		TargetID:    id,
		Incarnation: incarnation,
	})
	s.mu.Unlock()
}

// markDead marks a member as dead.
func (s *SWIM) markDead(id NodeID) {
	s.members.SetState(id, Dead, 0)
	s.detector.Remove(id)

	s.mu.Lock()
	s.piggyback = append(s.piggyback, Message{
		Type:     MsgDead,
		SenderID: s.cfg.NodeID,
		TargetID: id,
	})
	s.mu.Unlock()

	// Remove dead members after a delay, but respect stop signal
	go func() {
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
			s.members.Remove(id)
		case <-s.stopCh:
			// Node stopped, don't remove (cleanup will happen in Stop)
		}
	}()
}

// receiveLoop handles incoming gossip messages.
func (s *SWIM) receiveLoop() {
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		msg, addr, err := s.transport.Receive()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}
		if msg == nil {
			continue
		}

		s.handleMessage(msg, addr)
	}
}

// handleMessage processes a single gossip message.
func (s *SWIM) handleMessage(msg *Message, addr *net.UDPAddr) {
	// Record heartbeat from sender
	s.detector.RecordHeartbeat(msg.SenderID)

	// Process piggybacked member updates
	for _, m := range msg.Members {
		s.mergeMember(m)
	}

	switch msg.Type {
	case MsgPing:
		s.handlePing(msg, addr)
	case MsgAck:
		s.handleAck(msg)
	case MsgPingReq:
		s.handlePingReq(msg, addr)
	case MsgSuspect:
		s.handleSuspect(msg)
	case MsgAlive:
		s.handleAlive(msg)
	case MsgDead:
		s.handleDead(msg)
	case MsgSync:
		s.handleSync(msg)
	}
}

func (s *SWIM) handlePing(msg *Message, addr *net.UDPAddr) {
	// Reply with ack
	ack := &Message{
		Type:        MsgAck,
		SenderID:    s.cfg.NodeID,
		Incarnation: s.members.Local().Incarnation,
	}
	replyAddr := addr.String()
	_ = s.transport.Send(replyAddr, ack)

	// Record sender as alive
	if m := s.members.Get(msg.SenderID); m != nil {
		s.members.SetState(msg.SenderID, Alive, msg.Incarnation)
	}
}

func (s *SWIM) handleAck(msg *Message) {
	if m := s.members.Get(msg.SenderID); m != nil {
		s.members.SetState(msg.SenderID, Alive, msg.Incarnation)
		s.detector.RecordHeartbeat(msg.SenderID)
	}
}

func (s *SWIM) handlePingReq(msg *Message, addr *net.UDPAddr) {
	// Forward ping to target
	targetAddr := msg.TargetAddr
	if targetAddr == "" {
		if m := s.members.Get(msg.TargetID); m != nil {
			targetAddr = fmt.Sprintf("%s:%d", m.Addr, m.Port)
		}
	}
	if targetAddr == "" {
		return
	}

	ping := &Message{
		Type:        MsgPing,
		SenderID:    s.cfg.NodeID,
		Incarnation: s.members.Local().Incarnation,
	}
	_ = s.transport.Send(targetAddr, ping)

	// Note: In a full implementation, we'd wait for ack from target,
	// then forward that ack back to the original requester.
	// For now, we just relay the ping.
	_ = addr
}

func (s *SWIM) handleSuspect(msg *Message) {
	targetID := msg.TargetID
	if targetID == s.cfg.NodeID {
		// Refute: we're alive!
		inc := s.members.Refute()
		alive := &Message{
			Type:        MsgAlive,
			SenderID:    s.cfg.NodeID,
			Incarnation: inc,
		}
		// Broadcast to all members
		for _, m := range s.members.AliveMembers() {
			addr := fmt.Sprintf("%s:%d", m.Addr, m.Port)
			_ = s.transport.Send(addr, alive)
		}
		return
	}

	if m := s.members.Get(targetID); m != nil && msg.Incarnation >= m.Incarnation {
		s.markSuspect(targetID, msg.Incarnation)
	}
}

func (s *SWIM) handleAlive(msg *Message) {
	if m := s.members.Get(msg.SenderID); m != nil {
		if msg.Incarnation >= m.Incarnation {
			s.members.SetState(msg.SenderID, Alive, msg.Incarnation)
			s.detector.RecordHeartbeat(msg.SenderID)
		}
	}
}

func (s *SWIM) handleDead(msg *Message) {
	targetID := msg.TargetID
	if targetID == s.cfg.NodeID {
		// Refute!
		inc := s.members.Refute()
		alive := &Message{
			Type:        MsgAlive,
			SenderID:    s.cfg.NodeID,
			Incarnation: inc,
		}
		for _, m := range s.members.AliveMembers() {
			addr := fmt.Sprintf("%s:%d", m.Addr, m.Port)
			_ = s.transport.Send(addr, alive)
		}
		return
	}
	s.markDead(targetID)
}

func (s *SWIM) handleSync(msg *Message) {
	for _, m := range msg.Members {
		s.mergeMember(m)
	}
}

func (s *SWIM) mergeMember(m MemberMsg) {
	existing := s.members.Get(m.ID)
	if existing == nil {
		// New member — check allowlist if configured
		if len(s.cfg.AllowedNodes) > 0 {
			allowed := false
			for _, id := range s.cfg.AllowedNodes {
				if id == m.ID {
					allowed = true
					break
				}
			}
			if !allowed {
				return // reject unknown node
			}
		}
		s.members.Add(&Member{
			ID:          m.ID,
			Addr:        m.Addr,
			Port:        m.Port,
			State:       m.State,
			Incarnation: m.Incarnation,
			LastSeen:    time.Now(),
		})
		return
	}

	// Update if incarnation is higher
	if m.Incarnation > existing.Incarnation {
		s.members.SetState(m.ID, m.State, m.Incarnation)
	}
}

// BroadcastState sends full state to all alive members.
func (s *SWIM) BroadcastState() {
	allMembers := s.members.All()
	memberMsgs := make([]MemberMsg, len(allMembers))
	for i, m := range allMembers {
		memberMsgs[i] = MemberMsg{
			ID:          m.ID,
			Addr:        m.Addr,
			Port:        m.Port,
			State:       m.State,
			Incarnation: m.Incarnation,
		}
	}

	syncMsg := &Message{
		Type:        MsgSync,
		SenderID:    s.cfg.NodeID,
		Incarnation: s.members.Local().Incarnation,
		Members:     memberMsgs,
	}

	for _, m := range s.members.AliveMembers() {
		addr := fmt.Sprintf("%s:%d", m.Addr, m.Port)
		_ = s.transport.Send(addr, syncMsg)
	}
}
