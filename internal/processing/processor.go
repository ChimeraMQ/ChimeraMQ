package processing

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/chimeramq/chimera/internal/message"
)

// TransformFunc is a function that transforms a message envelope.
// Returns nil to drop, the (possibly modified) envelope to pass through.
type TransformFunc func(env *message.Envelope) (*message.Envelope, error)

// BrokerAPI is the interface the processor uses to interact with the broker.
type BrokerAPI interface {
	FetchMessages(topic string, partition uint32, offset uint64, limit int) ([]*message.Envelope, error)
	PublishMessage(topic string, env *message.Envelope) (uint64, uint32, error)
	TopicPartitions(topic string) int
}

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
	Spec   TopologySpec
	State  TopologyState
	offsets map[uint32]uint64 // per-partition consume offset

	mu     sync.Mutex
	cancel context.CancelFunc
}

// Processor manages stream processing topologies.
type Processor struct {
	mu         sync.RWMutex
	topologies map[string]*Topology
	stateDir   string
	broker     BrokerAPI
	transforms map[string]TransformFunc // operator module name -> transform function
}

// NewProcessor creates a new stream processor.
func NewProcessor(stateDir string) *Processor {
	return &Processor{
		topologies: make(map[string]*Topology),
		stateDir:   stateDir,
		transforms: make(map[string]TransformFunc),
	}
}

// SetBroker sets the broker API for the processor.
func (p *Processor) SetBroker(broker BrokerAPI) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.broker = broker
}

// RegisterTransform registers a transform function for a WASM module name.
func (p *Processor) RegisterTransform(module string, fn TransformFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.transforms[module] = fn
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
		Spec:    spec,
		State:   TopologyStopped,
		offsets: make(map[uint32]uint64),
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

	if p.broker == nil {
		t.State = TopologyRunning
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	t.State = TopologyRunning

	go p.runTopology(ctx, t)

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

	t.mu.Lock()
	if t.cancel != nil {
		t.cancel()
		t.cancel = nil
	}
	t.mu.Unlock()

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

	t.mu.Lock()
	if t.cancel != nil {
		t.cancel()
		t.cancel = nil
	}
	t.mu.Unlock()

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
		if t.Spec.AutoStart && p.broker != nil {
			ctx, cancel := context.WithCancel(context.Background())
			t.cancel = cancel
			t.State = TopologyRunning
			go p.runTopology(ctx, t)
		} else if t.Spec.AutoStart {
			t.State = TopologyRunning
		}
	}
}

// Stop stops all running topologies.
func (p *Processor) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.topologies {
		t.mu.Lock()
		if t.cancel != nil {
			t.cancel()
			t.cancel = nil
		}
		t.mu.Unlock()
		t.State = TopologyStopped
	}
}

// Close shuts down the processor.
func (p *Processor) Close() {
	p.Stop()
}

// runTopology is the main processing loop for a topology.
func (p *Processor) runTopology(ctx context.Context, t *Topology) {
	spec := t.Spec
	batchSize := 10

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Determine partitions to consume
		partitions := spec.Source.Partitions
		if len(partitions) == 0 {
			n := p.broker.TopicPartitions(spec.Source.Topic)
			if n == 0 {
				n = 1
			}
			partitions = make([]uint32, n)
			for i := range partitions {
				partitions[i] = uint32(i)
			}
		}

		processed := 0
		for _, partID := range partitions {
			t.mu.Lock()
			offset := t.offsets[partID]
			t.mu.Unlock()

			envs, err := p.broker.FetchMessages(spec.Source.Topic, partID, offset, batchSize)
			if err != nil || len(envs) == 0 {
				continue
			}

			for _, env := range envs {
				result, err := p.applyOperators(env, spec.Operators)
				if err != nil {
					log.Printf("processor: %s operator error: %v", spec.Name, err)
					continue
				}

				if result == nil {
					// Dropped by filter
					processed++
					continue
				}

				// Write to sink if configured
				if spec.Sink.Topic != "" {
					_, _, err := p.broker.PublishMessage(spec.Sink.Topic, result)
					if err != nil {
						log.Printf("processor: %s sink error: %v", spec.Name, err)
					}
				}
				processed++
			}

			t.mu.Lock()
			t.offsets[partID] = envs[len(envs)-1].Sequence + 1
			t.mu.Unlock()
		}

		if processed == 0 {
			// No messages found — brief pause before retrying
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}
}

// applyOperators chains operators and returns the final result.
func (p *Processor) applyOperators(env *message.Envelope, operators []OperatorSpec) (*message.Envelope, error) {
	current := env

	for _, op := range operators {
		if current == nil {
			return nil, nil
		}

		transform, ok := p.transforms[op.Module]
		if !ok {
			// No transform registered — passthrough
			continue
		}

		result, err := transform(current)
		if err != nil {
			return nil, fmt.Errorf("operator %s: %w", op.Module, err)
		}

		switch op.Type {
		case OpFilter:
			if result == nil {
				return nil, nil
			}
		case OpMap, OpFlatMap:
			current = result
		case OpAggregate:
			current = result
		}
	}

	return current, nil
}
