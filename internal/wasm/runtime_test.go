package wasm

import (
	"context"
	"testing"
	"time"
)

func TestRuntimeCompileAndClose(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	wasm := testPassthruWasm(t)
	if err := rt.Compile("test", wasm); err != nil {
		t.Fatal(err)
	}

	modules := rt.ListModules()
	if len(modules) != 1 || modules[0] != "test" {
		t.Errorf("ListModules = %v, want [test]", modules)
	}

	if !rt.HasModule("test") {
		t.Error("HasModule should return true")
	}
	if rt.HasModule("nonexistent") {
		t.Error("HasModule should return false")
	}
}

func TestRuntimeDuplicateCompile(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	wasm := testPassthruWasm(t)
	rt.Compile("test", wasm)

	err := rt.Compile("test", wasm)
	if err == nil {
		t.Error("should fail for duplicate module name")
	}
}

func TestRuntimeRemove(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	rt.Compile("test", testPassthruWasm(t))
	if err := rt.Remove("test"); err != nil {
		t.Fatal(err)
	}
	if rt.HasModule("test") {
		t.Error("module should be removed")
	}
}

func TestRuntimeTransformPassthru(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	rt.Compile("passthru", testPassthruWasm(t))

	result, err := rt.Transform(context.Background(), "passthru", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Passthru {
		t.Error("should return passthru")
	}
	if result.Drop {
		t.Error("should not be drop")
	}
}

func TestRuntimeTransformModuleNotFound(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	_, err := rt.Transform(context.Background(), "nonexistent", []byte("hello"))
	if err == nil {
		t.Error("should fail for nonexistent module")
	}
}

func TestRuntimeTransformEmpty(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	rt := NewRuntime(cfg)
	defer rt.Close()

	rt.Compile("passthru", testPassthruWasm(t))

	result, err := rt.Transform(context.Background(), "passthru", []byte{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Passthru {
		t.Error("empty input should passthru")
	}
}

func TestRuntimeTransformTimeout(t *testing.T) {
	cfg := RuntimeConfig{
		MaxMemoryPages:   256,
		ExecutionTimeout: 5 * time.Millisecond,
		ModulePoolSize:   1,
	}
	rt := NewRuntime(cfg)
	defer rt.Close()

	rt.Compile("passthru", testPassthruWasm(t))

	result, err := rt.Transform(context.Background(), "passthru", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Passthru {
		t.Error("should complete within timeout")
	}
}

// testPassthruWasm returns a minimal WASM binary.
// The module:
//   - exports memory (1 page)
//   - exports allocate(size) -> ptr (bump allocator from 1024)
//   - exports transform(ptr, len) -> i64 (returns -1 = passthru)
func testPassthruWasm(t *testing.T) []byte {
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
		0x24, 0x00, 0x0b, 0x04, 0x00, 0x42, 0x7f, 0x0b,
	}
}
