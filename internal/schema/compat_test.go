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
