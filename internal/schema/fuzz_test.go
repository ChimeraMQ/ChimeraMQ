package schema

import (
	"testing"
)

// FuzzValidateJSONSchema verifies that JSON schema validation handles
// arbitrary schemas and payloads without panics or pathological behavior.
func FuzzValidateJSONSchema(f *testing.F) {
	f.Add(`{"type":"object"}`, `{"name":"test"}`)
	f.Add(`{"type":"string"}`, `"hello"`)
	f.Add(`{"type":"number"}`, `42`)
	f.Add(`{"type":"array","items":{"type":"string"}}`, `["a","b","c"]`)

	// Known-bad inputs
	f.Add(``, `{}`)                // empty schema
	f.Add(`not json`, `{}`)        // invalid schema JSON
	f.Add(`{"type":"object"}`, ``) // empty payload
	f.Add(`{"type":"object"}`, `not json`)
	f.Add(`{"type":"integer","minimum":0,"maximum":100}`, `999`)
	f.Add(`{"type":"string","minLength":1,"maxLength":10}`, `"x"`)
	f.Add(`{}`, `{}`) // empty schema

	f.Fuzz(func(t *testing.T, schema, payload string) {
		_, _ = validateJSONSchema(schema, []byte(payload))
	})
}

// FuzzAvroStructural verifies that Avro structural validation handles
// arbitrary input safely.
func FuzzAvroStructural(f *testing.F) {
	f.Add(`{"type":"record","name":"Test","fields":[{"name":"f","type":"string"}]}`, `{"f":"value"}`)
	f.Add(`{"type":"string"}`, `"hello"`)
	f.Add(``, `{}`)
	f.Add(`not json`, `{}`)
	f.Add(`{"type":"record","name":"Test","fields":[]}`, `{}`)

	f.Fuzz(func(t *testing.T, schema, payload string) {
		_, _ = validateAvroStructural(schema, []byte(payload))
	})
}

// FuzzRegistryRegister verifies that schema registry handles arbitrary
// registrations safely.
func FuzzRegistryRegister(f *testing.F) {
	dir := f.TempDir()
	reg, err := NewRegistry(dir)
	if err != nil {
		f.Skipf("cannot create registry: %v", err)
	}

	f.Add("json", "test-schema", `{"type":"object"}`)
	f.Add("avro", "avro-schema", `{"type":"string"}`)
	f.Add("", "empty-type", `{}`)
	f.Add("unknown", "unknown", `{}`)

	f.Fuzz(func(t *testing.T, schemaTypeStr, name, schema string) {
		var schemaType SchemaType
		switch schemaTypeStr {
		case "json":
			schemaType = SchemaJSON
		case "avro":
			schemaType = SchemaAvro
		case "protobuf":
			schemaType = SchemaProtobuf
		default:
			schemaType = SchemaJSON
		}
		_, _ = reg.Register(name, schemaType, schema)
	})
}

// FuzzEnforcePayload verifies that the schema enforcer handles arbitrary
// payloads safely.
func FuzzEnforcePayload(f *testing.F) {
	dir := f.TempDir()
	reg, err := NewRegistry(dir)
	if err != nil {
		f.Skipf("cannot create registry: %v", err)
	}

	// Register a simple schema
	schemaVer, _ := reg.Register("fuzz-schema", SchemaJSON, `{"type":"object"}`)

	enforcer := NewEnforcer(reg)

	f.Add(`{"key":"value"}`)
	f.Add(`"string"`)
	f.Add(`42`)
	f.Add(`true`)
	f.Add(`null`)
	f.Add(``)
	f.Add(`{"nested":{"a":1}}`)

	f.Fuzz(func(t *testing.T, payload string) {
		_, _ = enforcer.Validate(schemaVer.ID, []byte(payload))
	})
}
