package schema

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseSchemaType(t *testing.T) {
	tests := []struct {
		input string
		want  SchemaType
	}{
		{"avro", SchemaAvro},
		{"protobuf", SchemaProtobuf},
		{"json", SchemaJSON},
		{"json_schema", SchemaJSON},
		{"jsonschema", SchemaJSON},
		{"unknown", SchemaJSON},
		{"", SchemaJSON},
	}
	for _, tt := range tests {
		got := ParseSchemaType(tt.input)
		if got != tt.want {
			t.Errorf("ParseSchemaType(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestSchemaTypeString(t *testing.T) {
	tests := []struct {
		st   SchemaType
		want string
	}{
		{SchemaAvro, "avro"},
		{SchemaProtobuf, "protobuf"},
		{SchemaJSON, "json"},
		{SchemaType(99), "unknown"},
	}
	for _, tt := range tests {
		got := tt.st.String()
		if got != tt.want {
			t.Errorf("String() = %q, want %q", got, tt.want)
		}
	}
}

func TestValidateSubjectName(t *testing.T) {
	tests := []struct {
		name    string
		subject string
		wantErr bool
	}{
		{"empty", "", true},
		{"valid simple", "test-value", false},
		{"valid with dot", "test.value", false},
		{"valid with underscore", "test_value", false},
		{"too long", strings.Repeat("a", 256), true},
		{"path traversal", "foo/../bar", true},
		{"forward slash", "foo/bar", true},
		{"backslash", "foo\\bar", true},
		{"starts with dot", ".hidden", true},
		{"starts with hyphen", "-test", true},
		{"invalid char space", "test value", true},
		{"invalid char at", "test@value", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSubjectName(tt.subject)
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestDeleteSubject(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}

	r.Register("delete-me", SchemaJSON, `{"type":"object"}`)
	r.Register("delete-me", SchemaJSON, `{"type":"object","properties":{"x":{"type":"integer"}}}`)

	if err := r.DeleteSubject("delete-me"); err != nil {
		t.Fatalf("DeleteSubject: %v", err)
	}

	if len(r.ListSubjects()) != 0 {
		t.Error("expected no subjects after deletion")
	}

	_, err = r.GetByID(1)
	if err == nil {
		t.Error("expected schema IDs to be removed")
	}

	// Directory should be removed
	if _, err := os.Stat(filepath.Join(dir, "delete-me")); !os.IsNotExist(err) {
		t.Error("subject directory should be removed")
	}
}

func TestDeleteSubjectInvalidName(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	if err := r.DeleteSubject(""); err == nil {
		t.Error("expected error for empty subject")
	}
}

func TestRegistryErrorPaths(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Register invalid subject
	_, err = r.Register("", SchemaJSON, `{}`)
	if err == nil {
		t.Error("expected error for empty subject on Register")
	}

	// Get invalid subject
	_, err = r.Get("", 1)
	if err == nil {
		t.Error("expected error for empty subject on Get")
	}

	// Get version out of bounds
	_, err = r.Get("nonexistent", 1)
	if err == nil {
		t.Error("expected error for nonexistent subject on Get")
	}

	// GetLatest invalid subject
	_, err = r.GetLatest("")
	if err == nil {
		t.Error("expected error for empty subject on GetLatest")
	}

	// GetLatest no schemas
	_, err = r.GetLatest("nonexistent")
	if err == nil {
		t.Error("expected error for subject with no schemas on GetLatest")
	}

	// List invalid subject
	_, err = r.List("")
	if err == nil {
		t.Error("expected error for empty subject on List")
	}

	// Delete invalid subject
	err = r.Delete("", 1)
	if err == nil {
		t.Error("expected error for empty subject on Delete")
	}

	// Delete version out of bounds
	err = r.Delete("nonexistent", 1)
	if err == nil {
		t.Error("expected error for nonexistent subject on Delete")
	}

	// SetCompatibility invalid subject
	err = r.SetCompatibility("", CompatNone)
	if err == nil {
		t.Error("expected error for empty subject on SetCompatibility")
	}

	// GetCompatibility invalid subject returns CompatNone
	if mode := r.GetCompatibility(""); mode != CompatNone {
		t.Errorf("GetCompatibility invalid = %d, want CompatNone", mode)
	}
}

func TestLoadCorruptCompat(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}

	r.Register("corrupt-compat", SchemaJSON, `{"type":"object"}`)
	r.SetCompatibility("corrupt-compat", CompatBackward)
	r.Close()

	// Corrupt compat.json
	compatPath := filepath.Join(dir, "corrupt-compat", "compat.json")
	os.WriteFile(compatPath, []byte("not json"), 0640)

	r2, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	// loadCompat should gracefully ignore corrupt file
	if mode := r2.GetCompatibility("corrupt-compat"); mode != CompatNone {
		t.Logf("compat mode after corrupt load = %d (ok if ignored)", mode)
	}
}

func TestLoadCorruptMeta(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}

	r.Register("corrupt-meta", SchemaJSON, `{"type":"object"}`)
	r.Close()

	// Corrupt meta.json
	metaPath := filepath.Join(dir, "corrupt-meta", "meta.json")
	os.WriteFile(metaPath, []byte("not json"), 0640)

	r2, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()

	_, err = r2.GetLatest("corrupt-meta")
	if err == nil {
		t.Error("expected error for corrupt meta")
	}
}

func TestRegistrySaveRollback(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Register first version successfully
	sv1, err := r.Register("rollback-test", SchemaJSON, `{"type":"object"}`)
	if err != nil {
		t.Fatal(err)
	}

	// Make directory read-only to force save failure on next register
	subjectDir := filepath.Join(dir, "rollback-test")
	os.Chmod(subjectDir, 0555)

	_, err = r.Register("rollback-test", SchemaJSON, `{"type":"object","properties":{"x":{"type":"integer"}}}`)
	if err == nil {
		// On some platforms chmod doesn't prevent writes; skip validation
		os.Chmod(subjectDir, 0755)
		t.Skip("chmod did not prevent write on this platform")
	}

	// Rollback should restore state
	versions, _ := r.List("rollback-test")
	if len(versions) != 1 {
		t.Errorf("versions = %d, want 1 after rollback", len(versions))
	}

	// globalID should be restored
	sv2, _ := r.Register("rollback-test-2", SchemaJSON, `{"type":"object"}`)
	if sv2.ID != sv1.ID+1 {
		t.Errorf("global ID after rollback = %d, want %d", sv2.ID, sv1.ID+1)
	}

	os.Chmod(subjectDir, 0755)
}
