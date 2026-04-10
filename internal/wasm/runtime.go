package wasm

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// RuntimeConfig controls the WASM runtime behavior.
type RuntimeConfig struct {
	MaxMemoryPages   uint32
	ExecutionTimeout time.Duration
	ModulePoolSize   int
	ModulesDir       string
}

// DefaultRuntimeConfig returns sensible defaults.
func DefaultRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		MaxMemoryPages:   256, // 16MB
		ExecutionTimeout: 100 * time.Millisecond,
		ModulePoolSize:   4,
	}
}

// Runtime manages WASM module compilation and execution.
type Runtime struct {
	rt      wazero.Runtime
	modules map[string]*CompiledModule
	mu      sync.RWMutex
	config  RuntimeConfig
	ctx     context.Context
	cancel  context.CancelFunc
}

// CompiledModule is a compiled WASM module with an instance pool.
type CompiledModule struct {
	Name   string
	seq    atomic.Uint64
	module wazero.CompiledModule
	pool   chan *ModuleInstance
	config RuntimeConfig
	rt     wazero.Runtime
	ctx    context.Context
}

// ModuleInstance is a pre-instantiated WASM module ready for execution.
type ModuleInstance struct {
	Mod    api.Module
	alloc  api.Function
	proc   api.Function
	memory api.Memory
}

// TransformResult contains the output of a WASM transform call.
type TransformResult struct {
	Data     []byte
	Drop     bool // if true, the message should be filtered
	Passthru bool // if true, return input unchanged
}

// NewRuntime creates a new WASM runtime.
func NewRuntime(cfg RuntimeConfig) *Runtime {
	ctx, cancel := context.WithCancel(context.Background())
	rt := wazero.NewRuntime(ctx)

	// Instantiate host module once
	builder := rt.NewHostModuleBuilder("chimera")
	builder.NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m api.Module, ptr uint32, len uint32) {
			// chimera_log - no-op
		}).
		Export("chimera_log")
	builder.Instantiate(ctx)

	return &Runtime{
		rt:      rt,
		modules: make(map[string]*CompiledModule),
		config:  cfg,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Compile compiles a WASM module from bytes and adds it to the runtime.
func (r *Runtime) Compile(name string, wasmBytes []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.modules[name]; exists {
		return fmt.Errorf("module %q already exists", name)
	}

	compiled, err := r.rt.CompileModule(r.ctx, wasmBytes)
	if err != nil {
		return fmt.Errorf("compile module %q: %w", name, err)
	}

	poolSize := r.config.ModulePoolSize
	if poolSize < 1 {
		poolSize = 1
	}

	cm := &CompiledModule{
		Name:   name,
		module: compiled,
		pool:   make(chan *ModuleInstance, poolSize),
		config: r.config,
		rt:     r.rt,
		ctx:    r.ctx,
	}

	// Pre-instantiate pool
	for i := 0; i < poolSize; i++ {
		inst, err := cm.newInstance()
		if err != nil {
			return fmt.Errorf("instantiate module %q: %w", name, err)
		}
		cm.pool <- inst
	}

	r.modules[name] = cm
	return nil
}

// Remove removes a compiled module.
func (r *Runtime) Remove(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	cm, ok := r.modules[name]
	if !ok {
		return fmt.Errorf("module %q not found", name)
	}

	// Drain pool
	for {
		select {
		case inst := <-cm.pool:
			inst.Mod.Close(r.ctx)
		default:
			goto done
		}
	}
done:
	cm.module.Close(r.ctx)
	delete(r.modules, name)
	return nil
}

// Transform executes a WASM transform on input data.
func (r *Runtime) Transform(ctx context.Context, moduleName string, input []byte) (*TransformResult, error) {
	r.mu.RLock()
	cm, ok := r.modules[moduleName]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("module %q not found", moduleName)
	}

	// Get instance from pool
	var inst *ModuleInstance
	select {
	case inst = <-cm.pool:
	default:
		// Pool exhausted, create temporary instance
		var err error
		inst, err = cm.newInstance()
		if err != nil {
			return nil, err
		}
		defer func() {
			inst.Mod.Close(ctx)
		}()
	}

	// Return instance to pool when done
	defer func() {
		cm.pool <- inst
	}()

	// Set execution timeout
	if cm.config.ExecutionTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cm.config.ExecutionTimeout)
		defer cancel()
	}

	return cm.execute(ctx, inst, input)
}

// ListModules returns all compiled module names.
func (r *Runtime) ListModules() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.modules))
	for name := range r.modules {
		names = append(names, name)
	}
	return names
}

// HasModule checks if a module exists.
func (r *Runtime) HasModule(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.modules[name]
	return ok
}

// Close shuts down the runtime.
func (r *Runtime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for name, cm := range r.modules {
		for {
			select {
			case inst := <-cm.pool:
				inst.Mod.Close(r.ctx)
			default:
				goto nextModule
			}
		}
	nextModule:
		cm.module.Close(r.ctx)
		delete(r.modules, name)
	}
	r.cancel()
	return r.rt.Close(r.ctx)
}

// newInstance creates a new ModuleInstance from the compiled module.
func (cm *CompiledModule) newInstance() (*ModuleInstance, error) {
	mod, err := cm.rt.InstantiateModule(cm.ctx, cm.module,
		wazero.NewModuleConfig().WithName(fmt.Sprintf("%s-%d", cm.Name, cm.seq.Add(1))),
	)
	if err != nil {
		return nil, err
	}

	alloc := mod.ExportedFunction("allocate")
	proc := mod.ExportedFunction("transform")
	mem := mod.Memory()

	if proc == nil {
		mod.Close(cm.ctx)
		return nil, fmt.Errorf("module %q missing export 'transform'", cm.Name)
	}

	return &ModuleInstance{
		Mod:    mod,
		alloc:  alloc,
		proc:   proc,
		memory: mem,
	}, nil
}

// execute runs the transform function on the WASM instance.
func (cm *CompiledModule) execute(ctx context.Context, inst *ModuleInstance, input []byte) (*TransformResult, error) {
	if len(input) == 0 {
		return &TransformResult{Passthru: true}, nil
	}

	// Allocate memory in guest for input
	results, err := inst.alloc.Call(ctx, uint64(len(input)))
	if err != nil {
		return nil, fmt.Errorf("allocate: %w", err)
	}
	inputPtr := uint32(results[0])

	// Write input to guest memory
	if !inst.memory.Write(inputPtr, input) {
		return nil, fmt.Errorf("failed to write input to guest memory")
	}

	// Call transform
	results, err = inst.proc.Call(ctx, uint64(inputPtr), uint64(len(input)))
	if err != nil {
		return nil, fmt.Errorf("transform: %w", err)
	}

	result := int64(results[0])

	// Result 0 = drop (filter)
	if result == 0 {
		return &TransformResult{Drop: true}, nil
	}

	// Result -1 = pass through unchanged
	if result == -1 {
		return &TransformResult{Passthru: true}, nil
	}

	// Result = (ptr << 32) | len
	ptr := uint32(result >> 32)
	length := uint32(result & 0xFFFFFFFF)

	if length == 0 || ptr == 0 {
		return &TransformResult{Passthru: true}, nil
	}

	data, ok := inst.memory.Read(ptr, length)
	if !ok {
		return nil, fmt.Errorf("failed to read result from guest memory")
	}

	// Copy since memory is owned by guest
	output := make([]byte, length)
	copy(output, data)

	return &TransformResult{Data: output}, nil
}
