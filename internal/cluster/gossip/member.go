package gossip

import (
	"fmt"
	"sync"
	"time"
)

// MemberState represents the health state of a member.
type MemberState int

const (
	Alive   MemberState = iota
	Suspect
	Dead
)

func (s MemberState) String() string {
	switch s {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	case Dead:
		return "dead"
	default:
		return "unknown"
	}
}

// Member describes a cluster node.
type Member struct {
	ID          NodeID
	Addr        string
	Port        int
	State       MemberState
	Incarnation uint64
	Metadata    map[string]string
	LastSeen    time.Time
}

// NodeID identifies a gossip node.
type NodeID string

// MemberList manages the set of known cluster members.
type MemberList struct {
	mu      sync.RWMutex
	localID NodeID
	members map[NodeID]*Member
}

// NewMemberList creates a new member list.
func NewMemberList(localID NodeID) *MemberList {
	return &MemberList{
		localID: localID,
		members: make(map[NodeID]*Member),
	}
}

// Add adds a member to the list.
func (ml *MemberList) Add(m *Member) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.members[m.ID] = m
}

// Get returns a member by ID.
func (ml *MemberList) Get(id NodeID) *Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.members[id]
}

// Remove removes a member.
func (ml *MemberList) Remove(id NodeID) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	delete(ml.members, id)
}

// All returns all members (except the local node).
func (ml *MemberList) All() []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	result := make([]*Member, 0, len(ml.members))
	for _, m := range ml.members {
		if m.ID != ml.localID {
			result = append(result, m)
		}
	}
	return result
}

// AliveMembers returns members in Alive state.
func (ml *MemberList) AliveMembers() []*Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	result := make([]*Member, 0)
	for _, m := range ml.members {
		if m.ID != ml.localID && m.State == Alive {
			result = append(result, m)
		}
	}
	return result
}

// SetState updates a member's state.
func (ml *MemberList) SetState(id NodeID, state MemberState, incarnation uint64) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if m, ok := ml.members[id]; ok {
		if incarnation >= m.Incarnation {
			m.State = state
			m.Incarnation = incarnation
			m.LastSeen = time.Now()
		}
	}
}

// Refute increments incarnation to refute a suspicion.
func (ml *MemberList) Refute() uint64 {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if m, ok := ml.members[ml.localID]; ok {
		m.Incarnation++
		m.State = Alive
		m.LastSeen = time.Now()
		return m.Incarnation
	}
	return 0
}

// Local returns the local member.
func (ml *MemberList) Local() *Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.members[ml.localID]
}

// Size returns the total number of members.
func (ml *MemberList) Size() int {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return len(ml.members)
}

// SelectRandom picks a random alive member (excluding local).
func (ml *MemberList) SelectRandom(exclude map[NodeID]bool) *Member {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	candidates := make([]*Member, 0)
	for _, m := range ml.members {
		if m.ID != ml.localID && m.State == Alive && !exclude[m.ID] {
			candidates = append(candidates, m)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return candidates[time.Now().UnixNano()%int64(len(candidates))]
}

func (m *Member) String() string {
	return fmt.Sprintf("%s@%s:%d (%s inc=%d)", m.ID, m.Addr, m.Port, m.State, m.Incarnation)
}
