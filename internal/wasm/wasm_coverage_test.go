package wasm

import (
	"context"
	"testing"
	"time"
)

func TestRuntimeTransformTempInstanceError(t *testing.T) {
	cfg := RuntimeConfig{MaxMemoryPages: 256, ExecutionTimeout: 100 * time.Millisecond, ModulePoolSize: 1}
	rt := NewRuntime(cfg)
	defer rt.Close()

	// Compile trap module
	rt.Compile("trap", testTrapWasm(t))

	// Run 2 concurrent transforms with pool size 1.
	// One uses pooled instance, the other creates a temp instance.
	// Both should fail because transform traps.
	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := rt.Transform(context.Background(), "trap", []byte("hello"))
			errCh <- err
		}()
	}

	for i := 0; i < 2; i++ {
		err := <-errCh
		if err == nil {
			t.Error("expected error for trap transform")
		}
	}
}
