package schema

import "testing"

func TestJSONSchemaBackwardCompatible(t *testing.T) {
	old := `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`
	new_ := `{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name"]}`

	err := checkJSONSchemaCompat(old, new_, CompatBackward)
	if err != nil {
		t.Errorf("should be backward compatible: %v", err)
	}
}

func TestJSONSchemaBackwardBreaking(t *testing.T) {
	old := `{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}`
	new_ := `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`

	err := checkJSONSchemaCompat(old, new_, CompatBackward)
	if err == nil {
		t.Error("should fail: new schema missing required field from old")
	}
}

func TestJSONSchemaForwardCompatible(t *testing.T) {
	old := `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`
	new_ := `{"type":"object","properties":{"name":{"type":"string"}}}`

	err := checkJSONSchemaCompat(old, new_, CompatForward)
	if err != nil {
		t.Errorf("should be forward compatible: %v", err)
	}
}

func TestJSONSchemaForwardBreaking(t *testing.T) {
	old := `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`
	new_ := `{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name","age"]}`

	err := checkJSONSchemaCompat(old, new_, CompatForward)
	if err == nil {
		t.Error("should fail: new schema adds required field not in old")
	}
}

func TestJSONSchemaFullCompatible(t *testing.T) {
	old := `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`
	new_ := `{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name"]}`

	err := checkJSONSchemaCompat(old, new_, CompatFull)
	if err != nil {
		t.Errorf("should be fully compatible (optional field added): %v", err)
	}
}

func TestJSONSchemaTypeChange(t *testing.T) {
	old := `{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`
	new_ := `{"type":"object","properties":{"name":{"type":"integer"}},"required":["name"]}`

	err := checkJSONSchemaCompat(old, new_, CompatBackward)
	if err == nil {
		t.Error("should fail: field type changed")
	}
}

func TestAvroBackwardAddField(t *testing.T) {
	old := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`
	new_ := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":""}]}`

	err := checkAvroCompat(old, new_, CompatBackward)
	if err != nil {
		t.Errorf("should be backward compatible: %v", err)
	}
}

func TestAvroBackwardRemoveField(t *testing.T) {
	old := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	new_ := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`

	err := checkAvroCompat(old, new_, CompatBackward)
	if err == nil {
		t.Error("should fail: field removed")
	}
}

func TestProtobufFieldNumberReuse(t *testing.T) {
	old := "message Test {\n  string name = 1;\n  int32 age = 2;\n}\n"
	new_ := "message Test {\n  string name = 1;\n  string age = 2;\n}\n"

	err := checkProtobufCompat(old, new_, CompatBackward)
	if err == nil {
		t.Error("should fail: field type changed for same number")
	}
}

func TestProtobufAddField(t *testing.T) {
	old := "message Test {\n  string name = 1;\n}\n"
	new_ := "message Test {\n  string name = 1;\n  int32 age = 2;\n}\n"

	err := checkProtobufCompat(old, new_, CompatBackward)
	if err != nil {
		t.Errorf("should be backward compatible: %v", err)
	}
}

func TestCompatNone(t *testing.T) {
	oldSV := &SchemaVersion{Type: SchemaJSON, Schema: `{"type":"object"}`}
	newSV := &SchemaVersion{Type: SchemaJSON, Schema: `{"type":"string"}`}

	err := CheckCompatibility(CompatNone, oldSV, newSV)
	if err != nil {
		t.Errorf("NONE mode should allow any change: %v", err)
	}
}

func TestCompatTypeChange(t *testing.T) {
	oldSV := &SchemaVersion{Type: SchemaJSON, Schema: `{}`}
	newSV := &SchemaVersion{Type: SchemaAvro, Schema: `{}`}

	err := CheckCompatibility(CompatBackward, oldSV, newSV)
	if err == nil {
		t.Error("should fail: schema type changed")
	}
}

func TestCheckCompatibilityUnknownType(t *testing.T) {
	oldSV := &SchemaVersion{Type: SchemaType(99), Schema: `ignored`}
	newSV := &SchemaVersion{Type: SchemaType(99), Schema: `ignored`}

	err := CheckCompatibility(CompatBackward, oldSV, newSV)
	if err != nil {
		t.Errorf("unknown schema type should return nil: %v", err)
	}
}

func TestCheckCompatibilityAvroBackwardCompatible(t *testing.T) {
	oldSV := &SchemaVersion{Type: SchemaAvro, Schema: `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`}
	newSV := &SchemaVersion{Type: SchemaAvro, Schema: `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":""}]}`}

	err := CheckCompatibility(CompatBackward, oldSV, newSV)
	if err != nil {
		t.Errorf("avro backward compat should pass: %v", err)
	}
}

func TestCheckCompatibilityProtobufBackwardCompatible(t *testing.T) {
	oldSV := &SchemaVersion{Type: SchemaProtobuf, Schema: "message Test {\n  string name = 1;\n}\n"}
	newSV := &SchemaVersion{Type: SchemaProtobuf, Schema: "message Test {\n  string name = 1;\n  int32 age = 2;\n}\n"}

	err := CheckCompatibility(CompatBackward, oldSV, newSV)
	if err != nil {
		t.Errorf("protobuf backward compat should pass: %v", err)
	}
}

func TestCheckCompatibilityJSONBackwardCompatible(t *testing.T) {
	oldSV := &SchemaVersion{Type: SchemaJSON, Schema: `{"properties":{"name":{"type":"string"}},"required":["name"]}`}
	newSV := &SchemaVersion{Type: SchemaJSON, Schema: `{"properties":{"name":{"type":"string"},"age":{"type":"int"}},"required":["name"]}`}

	err := CheckCompatibility(CompatBackward, oldSV, newSV)
	if err != nil {
		t.Errorf("json backward compat should pass: %v", err)
	}
}

func TestCheckJSONSchemaCompatOldInvalidJSON(t *testing.T) {
	err := checkJSONSchemaCompat(`{bad json}`, `{"properties":{}}`, CompatBackward)
	if err == nil {
		t.Error("expected error for invalid old JSON schema")
	}
}

func TestCheckJSONSchemaCompatNewInvalidJSON(t *testing.T) {
	err := checkJSONSchemaCompat(`{"properties":{}}`, `{bad json}`, CompatBackward)
	if err == nil {
		t.Error("expected error for invalid new JSON schema")
	}
}

func TestJSONSchemaBackwardFieldTypeChange(t *testing.T) {
	old := `{"properties":{"name":{"type":"string"}},"required":["name"]}`
	new_ := `{"properties":{"name":{"type":"integer"}},"required":["name"]}`

	err := checkJSONSchemaCompat(old, new_, CompatBackward)
	if err == nil {
		t.Error("should fail: field type changed from string to integer")
	}
}

func TestJSONSchemaBackwardTypeChangeOptionalField(t *testing.T) {
	// Both schemas have same required fields, but an optional field type changes
	old := `{"properties":{"name":{"type":"string"},"age":{"type":"string"}},"required":["name"]}`
	new_ := `{"properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name"]}`

	err := checkJSONSchemaCompat(old, new_, CompatBackward)
	if err == nil {
		t.Error("should fail: optional field type changed")
	}
}

func TestJSONSchemaBackwardNoPropertiesInNew(t *testing.T) {
	// Old has required fields but new has no properties key
	old := `{"properties":{"name":{"type":"string"}},"required":["name"]}`
	new_ := `{}`

	err := checkJSONSchemaCompat(old, new_, CompatBackward)
	if err == nil {
		t.Error("should fail: new schema has no properties")
	}
}

func TestAvroForwardAddFieldWithoutDefault(t *testing.T) {
	old := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`
	new_ := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`

	err := checkAvroCompat(old, new_, CompatForward)
	if err == nil {
		t.Error("should fail: new field without default")
	}
}

func TestAvroForwardAddFieldWithDefault(t *testing.T) {
	old := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`
	new_ := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":""}]}`

	err := checkAvroCompat(old, new_, CompatForward)
	if err != nil {
		t.Errorf("should be forward compatible with default: %v", err)
	}
}

func TestAvroFullCompatibility(t *testing.T) {
	old := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`
	new_ := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":""}]}`

	err := checkAvroCompat(old, new_, CompatFull)
	if err != nil {
		t.Errorf("should be fully compatible: %v", err)
	}
}

func TestAvroInvalidJSON(t *testing.T) {
	err := checkAvroCompat(`{bad}`, `{"fields":[]}`, CompatBackward)
	if err == nil {
		t.Error("expected error for invalid old avro schema")
	}

	err = checkAvroCompat(`{"fields":[]}`, `{bad}`, CompatBackward)
	if err == nil {
		t.Error("expected error for invalid new avro schema")
	}
}

func TestAvroFieldsNoFieldsKey(t *testing.T) {
	result := avroFields(map[string]interface{}{"type": "record"})
	if len(result) != 0 {
		t.Errorf("expected 0 fields, got %d", len(result))
	}
}

func TestAvroFieldsNonMapEntry(t *testing.T) {
	result := avroFields(map[string]interface{}{"fields": []interface{}{"not-a-map", map[string]interface{}{"name": "ok"}}})
	if len(result) != 1 {
		t.Errorf("expected 1 valid field, got %d", len(result))
	}
	if _, ok := result["ok"]; !ok {
		t.Error("expected 'ok' field")
	}
}

func TestProtobufForwardFieldRemoval(t *testing.T) {
	old := "message Test {\n  string name = 1;\n  int32 age = 2;\n}\n"
	new_ := "message Test {\n  string name = 1;\n}\n"

	err := checkProtobufCompat(old, new_, CompatForward)
	if err == nil {
		t.Error("should fail: field number removed")
	}
}

func TestProtobufFullCompatibility(t *testing.T) {
	old := "message Test {\n  string name = 1;\n}\n"
	new_ := "message Test {\n  string name = 1;\n  int32 age = 2;\n}\n"

	err := checkProtobufCompat(old, new_, CompatFull)
	if err != nil {
		t.Errorf("should be fully compatible: %v", err)
	}
}

func TestProtoFieldsMalformedLines(t *testing.T) {
	result := protoFields(`message Test {
  no-equals-or-semicolon
  short
  string name 1;
  string ok_field = 3;
}`)
	if len(result) != 1 {
		t.Errorf("expected 1 field, got %d", len(result))
	}
	if _, ok := result[3]; !ok {
		t.Error("expected field number 3")
	}
}

func TestProtoFieldsZeroFieldNumber(t *testing.T) {
	result := protoFields("message Test {\n  string name = 0;\n}\n")
	if len(result) != 0 {
		t.Errorf("expected 0 fields (number 0 skipped), got %d", len(result))
	}
}

func TestProtoFieldsShortLine(t *testing.T) {
	result := protoFields("message Test {\n  short\n}")
	if len(result) != 0 {
		t.Errorf("expected 0 fields from short line, got %d", len(result))
	}
}

func TestProtoFieldsSemiBeforeEquals(t *testing.T) {
	// Line where ; appears before = — hits the semiIdx <= eqIdx check
	result := protoFields("message Test {\n  string foo ; = 1;\n}")
	if len(result) != 0 {
		t.Errorf("expected 0 fields, got %d", len(result))
	}
}

func TestProtoFieldsEqualsButNoSemi(t *testing.T) {
	// Line has "=" but no ";" — hits the semiIdx < 0 check
	result := protoFields("message Test {\n  string foo = 1\n}")
	if len(result) != 0 {
		t.Errorf("expected 0 fields (no semicolon), got %d", len(result))
	}
}

func TestToStringSliceNonArray(t *testing.T) {
	result := toStringSlice("not-an-array")
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestToStringSliceMixedArray(t *testing.T) {
	result := toStringSlice([]interface{}{"a", 123, "b"})
	if len(result) != 2 {
		t.Errorf("expected 2 strings, got %d", len(result))
	}
}

func TestGetTypeNonMap(t *testing.T) {
	result := getType("not-a-map")
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}

func TestGetTypeMissingTypeKey(t *testing.T) {
	result := getType(map[string]interface{}{"foo": "bar"})
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}
