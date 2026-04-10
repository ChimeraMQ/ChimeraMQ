package schema

import (
	"encoding/json"
	"fmt"
	"strings"
)

// CompatibilityMode controls how new schema versions are validated.
type CompatibilityMode uint8

const (
	CompatNone     CompatibilityMode = 0
	CompatBackward CompatibilityMode = 1 // New can read old data
	CompatForward  CompatibilityMode = 2 // Old can read new data
	CompatFull     CompatibilityMode = 3 // Both directions
)

// ParseCompatibilityMode converts a string to CompatibilityMode.
func ParseCompatibilityMode(s string) CompatibilityMode {
	switch strings.ToLower(s) {
	case "none":
		return CompatNone
	case "backward":
		return CompatBackward
	case "forward":
		return CompatForward
	case "full":
		return CompatFull
	default:
		return CompatBackward
	}
}

// CheckCompatibility validates that newSchema is compatible with oldSchema
// according to the given mode.
func CheckCompatibility(mode CompatibilityMode, oldSchema, newSchema *SchemaVersion) error {
	if mode == CompatNone {
		return nil
	}

	if oldSchema.Type != newSchema.Type {
		return fmt.Errorf("schema type changed from %s to %s", oldSchema.Type, newSchema.Type)
	}

	switch oldSchema.Type {
	case SchemaJSON:
		return checkJSONSchemaCompat(oldSchema.Schema, newSchema.Schema, mode)
	case SchemaAvro:
		return checkAvroCompat(oldSchema.Schema, newSchema.Schema, mode)
	case SchemaProtobuf:
		return checkProtobufCompat(oldSchema.Schema, newSchema.Schema, mode)
	default:
		return nil
	}
}

// checkJSONSchemaCompat validates JSON Schema compatibility.
func checkJSONSchemaCompat(oldStr, newStr string, mode CompatibilityMode) error {
	var oldSchema, newSchema map[string]interface{}
	if err := json.Unmarshal([]byte(oldStr), &oldSchema); err != nil {
		return fmt.Errorf("parse old schema: %w", err)
	}
	if err := json.Unmarshal([]byte(newStr), &newSchema); err != nil {
		return fmt.Errorf("parse new schema: %w", err)
	}

	if mode == CompatBackward || mode == CompatFull {
		if err := jsonSchemaBackward(oldSchema, newSchema); err != nil {
			return fmt.Errorf("backward compatibility: %w", err)
		}
	}
	if mode == CompatForward || mode == CompatFull {
		if err := jsonSchemaForward(oldSchema, newSchema); err != nil {
			return fmt.Errorf("forward compatibility: %w", err)
		}
	}
	return nil
}

// jsonSchemaBackward: new schema can read old data.
// New schema must be a superset: it must include all required fields from old.
func jsonSchemaBackward(old, new map[string]interface{}) error {
	oldRequired := toStringSlice(old["required"])
	newProps, _ := new["properties"].(map[string]interface{})

	// All fields required by old must exist in new properties
	for _, field := range oldRequired {
		if _, ok := newProps[field]; !ok {
			return fmt.Errorf("new schema missing required field from old: %s", field)
		}
	}

	// Check type compatibility for overlapping properties
	oldProps, _ := old["properties"].(map[string]interface{})
	for name, oldDef := range oldProps {
		newDef, ok := newProps[name]
		if !ok {
			continue
		}
		oldType := getType(oldDef)
		newType := getType(newDef)
		if oldType != "" && newType != "" && oldType != newType {
			return fmt.Errorf("field %q type changed from %s to %s", name, oldType, newType)
		}
	}
	return nil
}

// jsonSchemaForward: old schema can read new data.
// Old schema must be able to read data produced by new schema.
func jsonSchemaForward(old, new map[string]interface{}) error {
	newRequired := toStringSlice(new["required"])
	oldProps, _ := old["properties"].(map[string]interface{})

	// New schema cannot add new required fields that old doesn't have
	for _, field := range newRequired {
		if _, ok := oldProps[field]; !ok {
			return fmt.Errorf("new schema adds required field not in old: %s", field)
		}
	}
	return nil
}

// checkAvroCompat validates Avro schema compatibility (simplified).
func checkAvroCompat(oldStr, newStr string, mode CompatibilityMode) error {
	var oldSchema, newSchema map[string]interface{}
	if err := json.Unmarshal([]byte(oldStr), &oldSchema); err != nil {
		return fmt.Errorf("parse old avro schema: %w", err)
	}
	if err := json.Unmarshal([]byte(newStr), &newSchema); err != nil {
		return fmt.Errorf("parse new avro schema: %w", err)
	}

	oldFields := avroFields(oldSchema)
	newFields := avroFields(newSchema)

	if mode == CompatBackward || mode == CompatFull {
		// Backward: new can add fields (with defaults), cannot remove old fields
		for name := range oldFields {
			if _, ok := newFields[name]; !ok {
				return fmt.Errorf("new schema removes field: %s", name)
			}
		}
	}
	if mode == CompatForward || mode == CompatFull {
		// Forward: new can remove fields, cannot add required (no default) fields
		for name, nf := range newFields {
			if _, ok := oldFields[name]; !ok {
				if !nf.hasDefault {
					return fmt.Errorf("new schema adds field without default: %s", name)
				}
			}
		}
	}
	return nil
}

type avroField struct {
	name       string
	hasDefault bool
}

func avroFields(schema map[string]interface{}) map[string]avroField {
	fields := make(map[string]avroField)
	arr, ok := schema["fields"].([]interface{})
	if !ok {
		return fields
	}
	for _, f := range arr {
		fm, ok := f.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := fm["name"].(string)
		_, hasDefault := fm["default"]
		fields[name] = avroField{name: name, hasDefault: hasDefault}
	}
	return fields
}

// checkProtobufCompat validates Protobuf schema compatibility (simplified).
func checkProtobufCompat(oldStr, newStr string, mode CompatibilityMode) error {
	oldFields := protoFields(oldStr)
	newFields := protoFields(newStr)

	if mode == CompatBackward || mode == CompatFull {
		// Backward: new cannot reuse a field number with different type, can add new fields
		for num, oldF := range oldFields {
			if newF, ok := newFields[num]; ok {
				if newF.typ != oldF.typ {
					return fmt.Errorf("field %d type changed from %s to %s", num, oldF.typ, newF.typ)
				}
			}
		}
	}
	if mode == CompatForward || mode == CompatFull {
		// Forward: new cannot remove a field number
		for num := range oldFields {
			if _, ok := newFields[num]; !ok {
				return fmt.Errorf("field number %d removed", num)
			}
		}
	}
	return nil
}

type protoField struct {
	num int
	typ string
}

// protoFields does a simple parse of proto field declarations.
func protoFields(protoText string) map[int]protoField {
	fields := make(map[int]protoField)
	for _, line := range strings.Split(protoText, "\n") {
		line = strings.TrimSpace(line)
		// Match: type name = number;
		if !strings.Contains(line, "=") || !strings.Contains(line, ";") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 4 {
			continue
		}
		// parts[0]=type, parts[1]=name, parts[2]="=N;" or "= N"
		eqIdx := strings.Index(line, "=")
		semiIdx := strings.Index(line, ";")
		if eqIdx < 0 || semiIdx < 0 || semiIdx <= eqIdx {
			continue
		}
		numStr := strings.TrimSpace(line[eqIdx+1 : semiIdx])
		var num int
		_, _ = fmt.Sscanf(numStr, "%d", &num)
		if num > 0 {
			fields[num] = protoField{num: num, typ: parts[0]}
		}
	}
	return fields
}

func toStringSlice(v interface{}) []string {
	if v == nil {
		return nil
	}
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

func getType(def interface{}) string {
	m, ok := def.(map[string]interface{})
	if !ok {
		return ""
	}
	t, _ := m["type"].(string)
	return t
}
