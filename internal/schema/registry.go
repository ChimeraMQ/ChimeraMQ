package schema

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// SchemaType identifies the schema format.
type SchemaType uint8

const (
	SchemaAvro     SchemaType = 0
	SchemaProtobuf SchemaType = 1
	SchemaJSON     SchemaType = 2
)

func (t SchemaType) String() string {
	switch t {
	case SchemaAvro:
		return "avro"
	case SchemaProtobuf:
		return "protobuf"
	case SchemaJSON:
		return "json"
	default:
		return "unknown"
	}
}

// ParseSchemaType converts a string to SchemaType.
func ParseSchemaType(s string) SchemaType {
	switch s {
	case "avro":
		return SchemaAvro
	case "protobuf":
		return SchemaProtobuf
	case "json", "json_schema", "jsonschema":
		return SchemaJSON
	default:
		return SchemaJSON
	}
}

// SchemaVersion is one versioned schema definition under a subject.
type SchemaVersion struct {
	ID          uint32     `json:"id"`
	Subject     string     `json:"subject"`
	Version     int        `json:"version"`
	Type        SchemaType `json:"type"`
	Schema      string     `json:"schema"`
	Fingerprint string     `json:"fingerprint"`
	CreatedAt   int64      `json:"created_at"`
}

// Registry stores and versions schemas, persisted to disk.
type Registry struct {
	mu         sync.RWMutex
	schemasDir string
	globalID   uint32
	versions   map[string][]*SchemaVersion // subject -> ordered versions
	byID       map[uint32]*SchemaVersion   // global ID -> schema
	compat     map[string]CompatibilityMode // subject -> compatibility mode
}

// NewRegistry creates or loads a schema registry.
func NewRegistry(schemasDir string) (*Registry, error) {
	if err := os.MkdirAll(schemasDir, 0750); err != nil {
		return nil, fmt.Errorf("create schemas dir: %w", err)
	}
	r := &Registry{
		schemasDir: schemasDir,
		versions:   make(map[string][]*SchemaVersion),
		byID:       make(map[uint32]*SchemaVersion),
		compat:     make(map[string]CompatibilityMode),
	}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

// Register adds a new schema version. If an identical schema (same fingerprint)
// already exists under this subject, it returns the existing version.
func (r *Registry) Register(subject string, schemaType SchemaType, schemaText string) (*SchemaVersion, error) {
	fp := fingerprint(schemaText)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Check for duplicate fingerprint
	for _, sv := range r.versions[subject] {
		if sv.Fingerprint == fp {
			return sv, nil
		}
	}

	// Check compatibility with latest version
	if versions := r.versions[subject]; len(versions) > 0 {
		latest := versions[len(versions)-1]
		mode := r.compat[subject]
		if mode == CompatNone {
			mode = CompatBackward // default
		}
		if mode != CompatNone {
			newSV := &SchemaVersion{Type: schemaType, Schema: schemaText}
			if err := CheckCompatibility(mode, latest, newSV); err != nil {
				return nil, fmt.Errorf("compatibility check failed: %w", err)
			}
		}
	}

	r.globalID++
	version := len(r.versions[subject]) + 1

	sv := &SchemaVersion{
		ID:          r.globalID,
		Subject:     subject,
		Version:     version,
		Type:        schemaType,
		Schema:      schemaText,
		Fingerprint: fp,
		CreatedAt:   time.Now().UnixNano(),
	}

	r.versions[subject] = append(r.versions[subject], sv)
	r.byID[sv.ID] = sv

	if err := r.saveSubject(subject); err != nil {
		// Rollback
		r.versions[subject] = r.versions[subject][:len(r.versions[subject])-1]
		delete(r.byID, sv.ID)
		r.globalID--
		return nil, err
	}

	r.saveGlobalID()
	return sv, nil
}

// Get returns a specific schema version.
func (r *Registry) Get(subject string, version int) (*SchemaVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions := r.versions[subject]
	if version < 1 || version > len(versions) {
		return nil, fmt.Errorf("version %d not found for subject %q", version, subject)
	}
	return versions[version-1], nil
}

// GetLatest returns the latest schema version for a subject.
func (r *Registry) GetLatest(subject string) (*SchemaVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions := r.versions[subject]
	if len(versions) == 0 {
		return nil, fmt.Errorf("no schemas found for subject %q", subject)
	}
	return versions[len(versions)-1], nil
}

// GetByID returns a schema by its global ID.
func (r *Registry) GetByID(id uint32) (*SchemaVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sv, ok := r.byID[id]
	if !ok {
		return nil, fmt.Errorf("schema ID %d not found", id)
	}
	return sv, nil
}

// List returns all versions for a subject.
func (r *Registry) List(subject string) ([]*SchemaVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions := r.versions[subject]
	result := make([]*SchemaVersion, len(versions))
	copy(result, versions)
	return result, nil
}

// ListSubjects returns all registered subjects.
func (r *Registry) ListSubjects() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	subjects := make([]string, 0, len(r.versions))
	for s := range r.versions {
		if len(r.versions[s]) > 0 {
			subjects = append(subjects, s)
		}
	}
	return subjects
}

// Delete removes a specific version from a subject.
func (r *Registry) Delete(subject string, version int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	versions := r.versions[subject]
	if version < 1 || version > len(versions) {
		return fmt.Errorf("version %d not found for subject %q", version, subject)
	}

	sv := versions[version-1]
	delete(r.byID, sv.ID)

	r.versions[subject] = append(versions[:version-1], versions[version:]...)
	// Renumber
	for i, v := range r.versions[subject] {
		v.Version = i + 1
	}

	return r.saveSubject(subject)
}

// DeleteSubject removes all versions for a subject.
func (r *Registry) DeleteSubject(subject string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, sv := range r.versions[subject] {
		delete(r.byID, sv.ID)
	}
	delete(r.versions, subject)
	delete(r.compat, subject)

	// Remove directory
	dir := filepath.Join(r.schemasDir, subject)
	os.RemoveAll(dir)

	return nil
}

// SetCompatibility sets the compatibility mode for a subject.
func (r *Registry) SetCompatibility(subject string, mode CompatibilityMode) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.compat[subject] = mode
	return r.saveCompat(subject)
}

// GetCompatibility returns the compatibility mode for a subject.
func (r *Registry) GetCompatibility(subject string) CompatibilityMode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.compat[subject]
}

// Close persists any pending state.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saveGlobalID()
	return nil
}

func fingerprint(text string) string {
	h := sha256.Sum256([]byte(text))
	return fmt.Sprintf("%x", h[:16])
}

func (r *Registry) load() error {
	// Load global ID
	idData, err := os.ReadFile(filepath.Join(r.schemasDir, "_global_id.json"))
	if err == nil {
		var id struct {
			GlobalID uint32 `json:"global_id"`
		}
		if json.Unmarshal(idData, &id) == nil {
			r.globalID = id.GlobalID
		}
	}

	// Load subjects
	entries, err := os.ReadDir(r.schemasDir)
	if err != nil {
		return nil // empty dir is ok
	}
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "_" {
			continue
		}
		subject := e.Name()
		r.loadSubject(subject)
		r.loadCompat(subject)
	}
	return nil
}

func (r *Registry) loadSubject(subject string) {
	metaPath := filepath.Join(r.schemasDir, subject, "meta.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return
	}
	var versions []*SchemaVersion
	if json.Unmarshal(data, &versions) != nil {
		return
	}
	r.versions[subject] = versions
	for _, sv := range versions {
		r.byID[sv.ID] = sv
		if sv.ID > r.globalID {
			r.globalID = sv.ID
		}
	}
}

func (r *Registry) loadCompat(subject string) {
	compatPath := filepath.Join(r.schemasDir, subject, "compat.json")
	data, err := os.ReadFile(compatPath)
	if err != nil {
		return
	}
	var cfg struct {
		Mode CompatibilityMode `json:"mode"`
	}
	if json.Unmarshal(data, &cfg) == nil {
		r.compat[subject] = cfg.Mode
	}
}

func (r *Registry) saveSubject(subject string) error {
	dir := filepath.Join(r.schemasDir, subject)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	data, err := json.MarshalIndent(r.versions[subject], "", "  ")
	if err != nil {
		return err
	}

	metaPath := filepath.Join(dir, "meta.json")
	tmpPath := metaPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0640); err != nil {
		return err
	}
	os.Remove(metaPath) // Windows: remove before rename
	return os.Rename(tmpPath, metaPath)
}

func (r *Registry) saveCompat(subject string) error {
	dir := filepath.Join(r.schemasDir, subject)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}

	cfg := struct {
		Mode CompatibilityMode `json:"mode"`
	}{Mode: r.compat[subject]}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}

	compatPath := filepath.Join(dir, "compat.json")
	tmpPath := compatPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0640); err != nil {
		return err
	}
	os.Remove(compatPath)
	return os.Rename(tmpPath, compatPath)
}

func (r *Registry) saveGlobalID() {
	cfg := struct {
		GlobalID uint32 `json:"global_id"`
	}{GlobalID: r.globalID}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return
	}

	path := filepath.Join(r.schemasDir, "_global_id.json")
	tmpPath := path + ".tmp"
	if os.WriteFile(tmpPath, data, 0640) == nil {
		os.Remove(path)
		os.Rename(tmpPath, path)
	}
}
