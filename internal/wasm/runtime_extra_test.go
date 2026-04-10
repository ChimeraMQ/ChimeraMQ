package wasm

import (
	"context"
	"testing"
	"time"
)

// testDropWasm returns a WASM binary where transform always returns 0 (drop).
// Based on passthru wasm but with transform returning 0 instead of -1.
func testDropWasm(t *testing.T) []byte {
	t.Helper()
	return []byte{
		0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
		0x01, 0x0c, 0x02, 0x60, 0x01, 0x7f, 0x01, 0x7f,
		0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7e, 0x03, 0x03,
		0x02, 0x00, 0x01, 0x05, 0x03, 0x01, 0x00, 0x01,
		0x06, 0x07, 0x01, 0x7f, 0x01, 0x41, 0x80, 0x08,
		0x0b, 0x07, 0x21, 0x03, 0x06, 0x6d, 0x65, 0x6d,
		0x6f, 0x72, 0x79, 0x02, 0x00, 0x08, 0x61, 0x6c,
		0x6c, 0x6f, 0x63, 0x61, 0x74, 0x65, 0x00, 0x00,
		0x09, 0x74, 0x72, 0x61, 0x6e, 0x73, 0x66, 0x6f,
		0x72, 0x6d, 0x00, 0x01, 0x0a, 0x12, 0x02, 0x0b,
		0x00, 0x23, 0x00, 0x20, 0x00, 0x23, 0x00, 0x6a,
		0x24, 0x00, 0x0b, 0x04, 0x00, 0x42, 0x00, 0x0b,
	}
}

func TestRuntimeTransformDrop(t *testing.T) {
	cfg := RuntimeConfig{MaxMemoryPages: 256, ExecutionTimeout: 100 * time.Millisecond, ModulePoolSize: 1}
	rt := NewRuntime(cfg)
	defer rt.Close()

	if err := rt.Compile("drop", testDropWasm(t)); err != nil {
		t.Fatal(err)
	}

	result, err := rt.Transform(context.Background(), "drop", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Drop {
		t.Error("expected drop result")
	}
	if result.Passthru {
		t.Error("should not be passthru")
	}
}

func TestRuntimeTransformNilInput(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	rt.Compile("p", testPassthruWasm(t))

	result, err := rt.Transform(context.Background(), "p", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Passthru {
		t.Error("nil input should passthru")
	}
}

func TestRuntimeInvalidWasm(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	err := rt.Compile("bad", []byte{0x00, 0x01, 0x02})
	if err == nil {
		t.Error("should fail for invalid wasm")
	}
}

func TestRuntimeRemoveNonexistent(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	err := rt.Remove("nonexistent")
	if err == nil {
		t.Error("should fail for nonexistent module")
	}
}

func TestRuntimeCloseWithModules(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)

	rt.Compile("a", testPassthruWasm(t))
	rt.Compile("b", testPassthruWasm(t))

	if err := rt.Close(); err != nil {
		t.Errorf("Close with modules: %v", err)
	}
}

func TestRuntimeNoExecutionTimeout(t *testing.T) {
	cfg := RuntimeConfig{MaxMemoryPages: 256, ExecutionTimeout: 0, ModulePoolSize: 2}
	rt := NewRuntime(cfg)
	defer rt.Close()

	rt.Compile("p", testPassthruWasm(t))

	result, err := rt.Transform(context.Background(), "p", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Passthru {
		t.Error("should passthru without timeout config")
	}
}

func TestRuntimeCompileMissingTransform(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	// Minimal valid wasm with memory and allocate but no transform
	wasm := []byte{
		0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
		0x01, 0x04, 0x01, 0x60, 0x01, 0x7f, 0x01, 0x7f,
		0x03, 0x02, 0x01, 0x00, 0x05, 0x03, 0x01, 0x00,
		0x01, 0x07, 0x11, 0x01, 0x09, 0x61, 0x6c, 0x6c,
		0x6f, 0x63, 0x61, 0x74, 0x65, 0x00, 0x00, 0x0a,
		0x06, 0x01, 0x04, 0x00, 0x41, 0x00, 0x0b,
	}
	err := rt.Compile("notransform", wasm)
	if err == nil {
		t.Error("should fail for module missing 'transform' export")
	}
}

func TestRuntimeConcurrentTransforms(t *testing.T) {
	cfg := RuntimeConfig{MaxMemoryPages: 256, ExecutionTimeout: 200 * time.Millisecond, ModulePoolSize: 4}
	rt := NewRuntime(cfg)
	defer rt.Close()

	rt.Compile("p", testPassthruWasm(t))

	errCh := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func() {
			_, err := rt.Transform(context.Background(), "p", []byte("concurrent"))
			errCh <- err
		}()
	}
	for i := 0; i < 5; i++ {
		if err := <-errCh; err != nil {
			t.Errorf("concurrent transform: %v", err)
		}
	}
}
