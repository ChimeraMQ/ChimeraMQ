package schema

import (
	"testing"
)

func TestRegisterAndGet(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	sv, err := r.Register("test-value", SchemaJSON, `{"type":"object","properties":{"name":{"type":"string"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	if sv.ID == 0 {
		t.Error("expected non-zero ID")
	}
	if sv.Version != 1 {
		t.Errorf("Version = %d, want 1", sv.Version)
	}
	if sv.Subject != "test-value" {
		t.Errorf("Subject = %q, want %q", sv.Subject, "test-value")
	}

	got, err := r.Get("test-value", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != sv.ID {
		t.Errorf("Get ID = %d, want %d", got.ID, sv.ID)
	}
}

func TestGetLatest(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	schema1 := `{"type":"object","properties":{"name":{"type":"string"}}}`
	schema2 := `{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}}}`

	r.Register("test-value", SchemaJSON, schema1)
	sv2, _ := r.Register("test-value", SchemaJSON, schema2)

	latest, err := r.GetLatest("test-value")
	if err != nil {
		t.Fatal(err)
	}
	if latest.Version != 2 {
		t.Errorf("latest Version = %d, want 2", latest.Version)
	}
	if latest.ID != sv2.ID {
		t.Errorf("latest ID = %d, want %d", latest.ID, sv2.ID)
	}
}

func TestGetByID(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	sv, _ := r.Register("test-value", SchemaJSON, `{"type":"object"}`)

	got, err := r.GetByID(sv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Subject != "test-value" {
		t.Errorf("Subject = %q, want %q", got.Subject, "test-value")
	}

	_, err = r.GetByID(9999)
	if err == nil {
		t.Error("should fail for unknown ID")
	}
}

func TestListSubjects(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	r.Register("orders-value", SchemaJSON, `{"type":"object"}`)
	r.Register("users-value", SchemaJSON, `{"type":"object"}`)

	subjects := r.ListSubjects()
	if len(subjects) != 2 {
		t.Errorf("len(ListSubjects) = %d, want 2", len(subjects))
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	r.Register("test-value", SchemaJSON, `{"type":"object"}`)
	r.Register("test-value", SchemaJSON, `{"type":"object","properties":{"x":{"type":"integer"}}}`)

	err = r.Delete("test-value", 1)
	if err != nil {
		t.Fatal(err)
	}

	versions, _ := r.List("test-value")
	if len(versions) != 1 {
		t.Fatalf("len(versions) = %d, want 1", len(versions))
	}
	if versions[0].Version != 1 {
		t.Errorf("remaining version = %d, want 1", versions[0].Version)
	}
}

func TestPersistence(t *testing.T) {
	dir := t.TempDir()
	r1, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}

	sv, _ := r1.Register("test-value", SchemaJSON, `{"type":"object"}`)
	r1.Close()

	r2, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	got, err := r2.GetByID(sv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Subject != "test-value" {
		t.Errorf("Subject = %q after reload", got.Subject)
	}
	if got.Version != 1 {
		t.Errorf("Version = %d after reload", got.Version)
	}
}

func TestDuplicateFingerprint(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	schema := `{"type":"object","properties":{"name":{"type":"string"}}}`
	sv1, _ := r.Register("test-value", SchemaJSON, schema)
	sv2, _ := r.Register("test-value", SchemaJSON, schema)

	if sv1.ID != sv2.ID {
		t.Errorf("duplicate register should return same version: %d vs %d", sv1.ID, sv2.ID)
	}
}

func TestCompatibilityMode(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	r.SetCompatibility("test-value", CompatNone)
	if mode := r.GetCompatibility("test-value"); mode != CompatNone {
		t.Errorf("mode = %d, want %d", mode, CompatNone)
	}
}
