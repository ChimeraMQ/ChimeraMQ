package schema

import "testing"

func TestValidateJSONSchemaValid(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-value", SchemaJSON, `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "integer"}
		},
		"required": ["name"]
	}`)

	e := NewEnforcer(r)
	result, err := e.Validate(sv.ID, []byte(`{"name":"Alice","age":30}`))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid {
		t.Errorf("should be valid: %v", result.Errors)
	}
}

func TestValidateJSONSchemaMissingRequired(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-value", SchemaJSON, `{
		"type": "object",
		"properties": {
			"name": {"type": "string"}
		},
		"required": ["name"]
	}`)

	e := NewEnforcer(r)
	result, _ := e.Validate(sv.ID, []byte(`{"age":30}`))
	if result.Valid {
		t.Error("should be invalid: missing required field")
	}
}

func TestValidateJSONSchemaWrongType(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-value", SchemaJSON, `{
		"type": "object",
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "integer"}
		},
		"required": ["name"]
	}`)

	e := NewEnforcer(r)
	result, _ := e.Validate(sv.ID, []byte(`{"name":"Alice","age":"thirty"}`))
	if result.Valid {
		t.Error("should be invalid: wrong type for age")
	}
}

func TestValidateJSONSchemaInvalidPayload(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-value", SchemaJSON, `{"type":"object"}`)

	e := NewEnforcer(r)
	result, _ := e.Validate(sv.ID, []byte(`not json`))
	if result.Valid {
		t.Error("should be invalid: not JSON")
	}
}

func TestValidateUnknownSchemaID(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	e := NewEnforcer(r)
	_, err := e.Validate(9999, []byte(`{}`))
	if err == nil {
		t.Error("should fail for unknown schema ID")
	}
}

func TestValidateAvroStructural(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-value", SchemaAvro, `{
		"type": "record",
		"name": "Test",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string", "default": ""}
		]
	}`)

	e := NewEnforcer(r)

	// Valid
	result, _ := e.Validate(sv.ID, []byte(`{"id":1,"name":"Alice"}`))
	if !result.Valid {
		t.Errorf("should be valid: %v", result.Errors)
	}

	// Missing required field
	result, _ = e.Validate(sv.ID, []byte(`{"name":"Alice"}`))
	if result.Valid {
		t.Error("should be invalid: missing required field 'id'")
	}
}

func TestValidateStringConstraints(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-value", SchemaJSON, `{
		"type": "object",
		"properties": {
			"name": {"type": "string", "minLength": 2, "maxLength": 10}
		},
		"required": ["name"]
	}`)

	e := NewEnforcer(r)

	// Too short
	result, _ := e.Validate(sv.ID, []byte(`{"name":"A"}`))
	if result.Valid {
		t.Error("should be invalid: string too short")
	}

	// Valid
	result, _ = e.Validate(sv.ID, []byte(`{"name":"Alice"}`))
	if !result.Valid {
		t.Errorf("should be valid: %v", result.Errors)
	}
}

func TestValidateNumberConstraints(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-value", SchemaJSON, `{
		"type": "object",
		"properties": {
			"age": {"type": "integer", "minimum": 0, "maximum": 150}
		}
	}`)

	e := NewEnforcer(r)

	result, _ := e.Validate(sv.ID, []byte(`{"age":-1}`))
	if result.Valid {
		t.Error("should be invalid: below minimum")
	}

	result, _ = e.Validate(sv.ID, []byte(`{"age":25}`))
	if !result.Valid {
		t.Errorf("should be valid: %v", result.Errors)
	}
}

func TestValidateEnum(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-value", SchemaJSON, `{
		"type": "object",
		"properties": {
			"status": {"type": "string", "enum": ["active", "inactive"]}
		}
	}`)

	e := NewEnforcer(r)

	result, _ := e.Validate(sv.ID, []byte(`{"status":"unknown"}`))
	if result.Valid {
		t.Error("should be invalid: not in enum")
	}

	result, _ = e.Validate(sv.ID, []byte(`{"status":"active"}`))
	if !result.Valid {
		t.Errorf("should be valid: %v", result.Errors)
	}
}

func TestParseSchemaID(t *testing.T) {
	headers := map[string]string{"x-chimera-schema-id": "42"}
	id, ok := ParseSchemaID(headers)
	if !ok || id != 42 {
		t.Errorf("ParseSchemaID = (%d, %v), want (42, true)", id, ok)
	}

	_, ok = ParseSchemaID(map[string]string{})
	if ok {
		t.Error("should not find schema ID in empty headers")
	}
}

func TestInferSchemaType(t *testing.T) {
	if InferSchemaType(`{"type":"object"}`) != SchemaJSON {
		t.Error("should infer JSON Schema")
	}
	if InferSchemaType(`{"type":"record","name":"T","fields":[]}`) != SchemaAvro {
		t.Error("should infer Avro")
	}
	if InferSchemaType("message Test {}") != SchemaProtobuf {
		t.Error("should infer Protobuf")
	}
}
