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
	if InferSchemaType("syntax = \"proto3\";") != SchemaProtobuf {
		t.Error("should infer Protobuf from syntax")
	}
	if InferSchemaType(`{"type": "record","name":"T"}`) != SchemaAvro {
		t.Error("should infer Avro with space")
	}
	if InferSchemaType("plain text") != SchemaJSON {
		t.Error("should default to JSON Schema")
	}
}

func TestFormatSchemaID(t *testing.T) {
	if FormatSchemaID(42) != "42" {
		t.Error("FormatSchemaID(42) should be '42'")
	}
}

func TestParseSchemaIDInvalid(t *testing.T) {
	_, ok := ParseSchemaID(map[string]string{"x-chimera-schema-id": "abc"})
	if ok {
		t.Error("should return false for non-numeric")
	}
}

func TestCheckTypeAllTypes(t *testing.T) {
	tests := []struct {
		expected string
		data     interface{}
		want     bool
	}{
		{"object", map[string]interface{}{}, true},
		{"array", []interface{}{}, true},
		{"string", "hello", true},
		{"number", float64(3.14), true},
		{"number", 42, true},
		{"integer", float64(7), true},
		{"integer", 42, true},
		{"integer", float64(3.14), false},
		{"boolean", true, true},
		{"null", nil, true},
		{"null", "x", false},
		{"object", "not-object", false},
		{"array", 42, false},
		{"string", 42, false},
		{"boolean", 42, false},
		{"custom", "anything", true},
	}
	for _, tt := range tests {
		got := checkType(tt.expected, tt.data)
		if got != tt.want {
			t.Errorf("checkType(%q, %v) = %v, want %v", tt.expected, tt.data, got, tt.want)
		}
	}
}

func TestTypeNameAll(t *testing.T) {
	tests := []struct {
		data interface{}
		want string
	}{
		{map[string]interface{}{}, "object"},
		{[]interface{}{}, "array"},
		{"hello", "string"},
		{float64(3.14), "number"},
		{42, "integer"},
		{true, "boolean"},
		{nil, "null"},
		{struct{}{}, "unknown"},
	}
	for _, tt := range tests {
		got := typeName(tt.data)
		if got != tt.want {
			t.Errorf("typeName(%v) = %q, want %q", tt.data, got, tt.want)
		}
	}
}

func TestValidateNodeAdditionalProperties(t *testing.T) {
	schema := map[string]interface{}{
		"type":                 "object",
		"properties":           map[string]interface{}{"name": map[string]interface{}{"type": "string"}},
		"additionalProperties": false,
	}
	errors := validateNode(schema, map[string]interface{}{"name": "ok", "extra": 123}, "")
	if len(errors) == 0 {
		t.Error("should reject additional properties")
	}
}

func TestValidateNodeArrayItems(t *testing.T) {
	schema := map[string]interface{}{
		"type":  "array",
		"items": map[string]interface{}{"type": "string"},
	}
	errors := validateNode(schema, []interface{}{"ok", 123}, "")
	if len(errors) == 0 {
		t.Error("should reject non-string array item")
	}
}

func TestValidateNodeEnum(t *testing.T) {
	schema := map[string]interface{}{
		"enum": []interface{}{"a", "b", "c"},
	}
	if len(validateNode(schema, "d", "")) == 0 {
		t.Error("should reject value not in enum")
	}
	if len(validateNode(schema, "a", "")) != 0 {
		t.Error("should accept value in enum")
	}
}

func TestValidateNodeNumberRange(t *testing.T) {
	schema := map[string]interface{}{
		"type":    "number",
		"minimum": float64(10),
		"maximum": float64(20),
	}
	if len(validateNode(schema, float64(5), "")) == 0 {
		t.Error("should reject below minimum")
	}
	if len(validateNode(schema, float64(25), "")) == 0 {
		t.Error("should reject above maximum")
	}
	if len(validateNode(schema, float64(15), "")) != 0 {
		t.Error("should accept in range")
	}
}

func TestValidateNodeStringLength(t *testing.T) {
	schema := map[string]interface{}{
		"type":      "string",
		"minLength": float64(2),
		"maxLength": float64(5),
	}
	if len(validateNode(schema, "a", "")) == 0 {
		t.Error("should reject too short")
	}
	if len(validateNode(schema, "abcdef", "")) == 0 {
		t.Error("should reject too long")
	}
	if len(validateNode(schema, "abc", "")) != 0 {
		t.Error("should accept in range")
	}
}

func TestEnforcerProtobufValid(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-subject", SchemaProtobuf, "message Test {\n  string name = 1;\n  int32 age = 2;\n}\n")
	e := NewEnforcer(r)

	// field 1 (tag=0x0a wire=2 len=5) "Alice", field 2 (tag=0x10 wire=0) varint 30
	payload := []byte{0x0a, 0x05, 'A', 'l', 'i', 'c', 'e', 0x10, 0x1e}
	res, err := e.Validate(sv.ID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid {
		t.Errorf("expected valid protobuf, got errors: %v", res.Errors)
	}
}

func TestEnforcerProtobufEmpty(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-subject", SchemaProtobuf, "message Test {\n  string name = 1;\n}\n")
	e := NewEnforcer(r)

	res, _ := e.Validate(sv.ID, []byte{})
	if res.Valid {
		t.Error("empty protobuf should be invalid")
	}
}

func TestEnforcerProtobufMissingField(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-subject", SchemaProtobuf, "message Test {\n  string name = 1;\n  int32 age = 2;\n}\n")
	e := NewEnforcer(r)

	// Only field 1, missing field 2
	res, _ := e.Validate(sv.ID, []byte{0x0a, 0x05, 'H', 'e', 'l', 'l', 'o'})
	if res.Valid {
		t.Error("missing required field should be invalid")
	}
}

func TestEnforcerProtobufInvalidVarint(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-subject", SchemaProtobuf, "message Test {\n  string name = 1;\n}\n")
	e := NewEnforcer(r)

	// All continuation bytes — unterminated varint
	res, _ := e.Validate(sv.ID, []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80})
	if res.Valid {
		t.Error("invalid varint should be invalid")
	}
}

func TestEnforcerProtobufNoFields(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-subject", SchemaProtobuf, "message Test {\n}\n")
	e := NewEnforcer(r)

	res, _ := e.Validate(sv.ID, []byte{0x0a, 0x01, 'x'})
	if !res.Valid {
		t.Error("no fields defined should accept any non-empty binary")
	}
}

func TestDecodeVarint(t *testing.T) {
	val, n := decodeVarint([]byte{0x01})
	if val != 1 || n != 1 {
		t.Errorf("single byte: %d, %d", val, n)
	}
	val, n = decodeVarint([]byte{0xAC, 0x02})
	if val != 300 || n != 2 {
		t.Errorf("multi-byte: %d, %d", val, n)
	}
	val, n = decodeVarint([]byte{})
	if val != 0 || n != 0 {
		t.Errorf("empty: %d, %d", val, n)
	}
}

func TestSkipFieldWireTypes(t *testing.T) {
	n, err := skipField([]byte{0x01}, 0)
	if err != nil || n != 1 {
		t.Errorf("varint: n=%d err=%v", n, err)
	}
	n, err = skipField(make([]byte, 8), 1)
	if err != nil || n != 8 {
		t.Errorf("64-bit: n=%d err=%v", n, err)
	}
	n, err = skipField([]byte{0x03, 'a', 'b', 'c'}, 2)
	if err != nil || n != 4 {
		t.Errorf("len-delimited: n=%d err=%v", n, err)
	}
	n, err = skipField(make([]byte, 4), 5)
	if err != nil || n != 4 {
		t.Errorf("32-bit: n=%d err=%v", n, err)
	}
	_, err = skipField(nil, 3)
	if err == nil {
		t.Error("should error on unknown wire type")
	}
}

func TestSkipFieldErrors(t *testing.T) {
	_, err := skipField([]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}, 0)
	if err == nil {
		t.Error("should error on unterminated varint")
	}
	_, err = skipField([]byte{0x01, 0x02}, 1)
	if err == nil {
		t.Error("should error on truncated 64-bit")
	}
	_, err = skipField([]byte{0x80, 0x80}, 2)
	if err == nil {
		t.Error("should error on invalid length-delimited size")
	}
	_, err = skipField([]byte{0x10, 'a'}, 2)
	if err == nil {
		t.Error("should error when field exceeds payload")
	}
	_, err = skipField([]byte{0x01}, 5)
	if err == nil {
		t.Error("should error on truncated 32-bit")
	}
}

func TestAvroInvalidSchema(t *testing.T) {
	res, err := validateAvroStructural("not json", []byte(`{"id":1}`))
	if err == nil {
		t.Error("invalid Avro schema should return error")
	}
	if res != nil {
		t.Error("result should be nil on schema parse error")
	}
}

func TestAvroNoFields(t *testing.T) {
	res, _ := validateAvroStructural(`{"type":"record"}`, []byte(`{}`))
	if !res.Valid {
		t.Error("Avro schema with no fields should be valid")
	}
}

func TestAvroNullableUnion(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-subject", SchemaAvro, `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"opt","type":["null","string"]}]}`)
	e := NewEnforcer(r)

	res, _ := e.Validate(sv.ID, []byte(`{"id":1}`))
	if !res.Valid {
		t.Errorf("nullable union field should be optional, got: %v", res.Errors)
	}
}

func TestAvroInvalidPayload(t *testing.T) {
	dir := t.TempDir()
	r, _ := NewRegistry(dir)
	defer r.Close()

	sv, _ := r.Register("test-subject", SchemaAvro, `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`)
	e := NewEnforcer(r)

	res, _ := e.Validate(sv.ID, []byte(`not json`))
	if res.Valid {
		t.Error("expected invalid for non-JSON payload")
	}
}

func TestExtractProtoFieldNums(t *testing.T) {
	schema := "message Test {\n  string name = 1;\n  int32 age = 2;\n}\n"
	nums := extractProtoFieldNums(schema)
	if len(nums) != 2 || nums[0] != 1 || nums[1] != 2 {
		t.Errorf("extractProtoFieldNums = %v, want [1 2]", nums)
	}
}

func TestParseCompatibilityModeAll(t *testing.T) {
	tests := []struct {
		input string
		want  CompatibilityMode
	}{
		{"none", CompatNone},
		{"backward", CompatBackward},
		{"forward", CompatForward},
		{"full", CompatFull},
		{"BACKWARD", CompatBackward},
		{"Full", CompatFull},
		{"unknown", CompatBackward},
	}
	for _, tt := range tests {
		got := ParseCompatibilityMode(tt.input)
		if got != tt.want {
			t.Errorf("ParseCompatibilityMode(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
