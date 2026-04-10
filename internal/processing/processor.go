package processing

import "sync"

// OperatorType defines the type of stream operator.
type OperatorType uint8

const (
	OpFilter    OperatorType = 0
	OpMap       OperatorType = 1
	OpFlatMap   OperatorType = 2
	OpAggregate OperatorType = 3
)

// SourceSpec defines where a topology reads from.
type SourceSpec struct {
	Topic      string
	Partitions []uint32
	Group      string // consumer group name
}

// SinkSpec defines where a topology writes to.
type SinkSpec struct {
	Topic string // output topic (empty = internal only)
}

// OperatorSpec defines one operator in a topology.
type OperatorSpec struct {
	Type   OperatorType
	Module string            // WASM module name
	Config map[string]string // operator-specific config
}

// TopologySpec defines a complete stream processing topology.
type TopologySpec struct {
	Name        string
	Source      SourceSpec
	Operators   []OperatorSpec
	Sink        SinkSpec
	Parallelism int
	AutoStart   bool
}

// TopologyState indicates the state of a topology.
type TopologyState uint8

const (
	TopologyStopped TopologyState = 0
	TopologyRunning TopologyState = 1
)

func (s TopologyState) String() string {
	switch s {
	case TopologyRunning:
		return "running"
	default:
		return "stopped"
	}
}

// Topology is a running or stopped stream processing pipeline.
type Topology struct {
	Spec  TopologySpec
	State TopologyState
}

// Processor manages stream processing topologies.
type Processor struct {
	mu         sync.RWMutex
	topologies map[string]*Topology
	stateDir   string
}

// NewProcessor creates a new stream processor.
func NewProcessor(stateDir string) *Processor {
	return &Processor{
		topologies: make(map[string]*Topology),
		stateDir:   stateDir,
	}
}

// CreateTopology registers a new topology.
func (p *Processor) CreateTopology(spec TopologySpec) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.topologies[spec.Name]; exists {
		return ErrTopologyExists
	}
	if spec.Parallelism < 1 {
		spec.Parallelism = 1
	}

	p.topologies[spec.Name] = &Topology{
		Spec:  spec,
		State: TopologyStopped,
	}
	return nil
}

// StartTopology starts processing for a topology.
func (p *Processor) StartTopology(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	t, ok := p.topologies[name]
	if !ok {
		return ErrTopologyNotFound
	}
	if t.State == TopologyRunning {
		return nil
	}

	t.State = TopologyRunning
	return nil
}

// StopTopology stops a running topology.
func (p *Processor) StopTopology(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	t, ok := p.topologies[name]
	if !ok {
		return ErrTopologyNotFound
	}
	t.State = TopologyStopped
	return nil
}

// DeleteTopology removes a topology.
func (p *Processor) DeleteTopology(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	t, ok := p.topologies[name]
	if !ok {
		return ErrTopologyNotFound
	}
	if t.State == TopologyRunning {
		return ErrTopologyRunning
	}
	delete(p.topologies, name)
	return nil
}

// GetTopology returns a topology by name.
func (p *Processor) GetTopology(name string) (*Topology, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	t, ok := p.topologies[name]
	return t, ok
}

// ListTopologies returns all topology names.
func (p *Processor) ListTopologies() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	names := make([]string, 0, len(p.topologies))
	for name := range p.topologies {
		names = append(names, name)
	}
	return names
}

// Start starts all auto-start topologies.
func (p *Processor) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.topologies {
		if t.Spec.AutoStart {
			t.State = TopologyRunning
		}
	}
}

// Stop stops all running topologies.
func (p *Processor) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.topologies {
		t.State = TopologyStopped
	}
}

// Close shuts down the processor.
func (p *Processor) Close() {
	p.Stop()
}
