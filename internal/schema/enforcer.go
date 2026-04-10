package schema

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

// ValidationResult contains the outcome of schema validation.
type ValidationResult struct {
	Valid  bool
	Errors []string
}

// Enforcer validates messages against registered schemas.
type Enforcer struct {
	registry *Registry
}

// NewEnforcer creates a new schema enforcer.
func NewEnforcer(registry *Registry) *Enforcer {
	return &Enforcer{registry: registry}
}

// Validate checks a payload against the schema identified by schemaID.
func (e *Enforcer) Validate(schemaID uint32, payload []byte) (*ValidationResult, error) {
	sv, err := e.registry.GetByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("schema ID %d not found", schemaID)
	}

	switch sv.Type {
	case SchemaJSON:
		return validateJSONSchema(sv.Schema, payload)
	case SchemaAvro:
		return validateAvroStructural(sv.Schema, payload)
	case SchemaProtobuf:
		return validateProtobufStructural(sv.Schema, payload)
	default:
		return &ValidationResult{Valid: false, Errors: []string{"unknown schema type"}}, nil
	}
}

// validateJSONSchema validates a JSON payload against a JSON Schema.
func validateJSONSchema(schemaStr string, payload []byte) (*ValidationResult, error) {
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}

	var data interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return &ValidationResult{Valid: false, Errors: []string{"payload is not valid JSON"}}, nil
	}

	errors := validateNode(schema, data, "")
	if len(errors) == 0 {
		return &ValidationResult{Valid: true}, nil
	}
	return &ValidationResult{Valid: false, Errors: errors}, nil
}

func validateNode(schema map[string]interface{}, data interface{}, path string) []string {
	var errors []string

	// Type check
	if typ, ok := schema["type"].(string); ok {
		if !checkType(typ, data) {
			errors = append(errors, fmt.Sprintf("%s: expected type %s, got %s", path, typ, typeName(data)))
			return errors // type mismatch overrides other checks
		}
	}

	// Object checks
	if obj, ok := data.(map[string]interface{}); ok {
		// Required fields
		if req, ok := schema["required"].([]interface{}); ok {
			for _, r := range req {
				field, _ := r.(string)
				if _, exists := obj[field]; !exists {
					errors = append(errors, fmt.Sprintf("%s.%s: required field missing", path, field))
				}
			}
		}

		// Properties
		if props, ok := schema["properties"].(map[string]interface{}); ok {
			for name, def := range props {
				propSchema, ok := def.(map[string]interface{})
				if !ok {
					continue
				}
				if val, exists := obj[name]; exists {
					subPath := path + "." + name
					if len(path) == 0 {
						subPath = name
					}
					errors = append(errors, validateNode(propSchema, val, subPath)...)
				}
			}
		}

		// additionalProperties
		if ap, ok := schema["additionalProperties"].(bool); ok && !ap {
			if props, ok := schema["properties"].(map[string]interface{}); ok {
				for key := range obj {
					if _, defined := props[key]; !defined {
						errors = append(errors, fmt.Sprintf("%s.%s: additional property not allowed", path, key))
					}
				}
			}
		}
	}

	// Array checks
	if arr, ok := data.([]interface{}); ok {
		if items, ok := schema["items"].(map[string]interface{}); ok {
			for i, item := range arr {
				subPath := fmt.Sprintf("%s[%d]", path, i)
				errors = append(errors, validateNode(items, item, subPath)...)
			}
		}
	}

	// Enum check
	if enum, ok := schema["enum"].([]interface{}); ok {
		found := false
		for _, v := range enum {
			if deepEqual(v, data) {
				found = true
				break
			}
		}
		if !found {
			errors = append(errors, fmt.Sprintf("%s: value not in enum", path))
		}
	}

	// Number checks
	if num, ok := toFloat(data); ok {
		if min, ok := toFloat(schema["minimum"]); ok && num < min {
			errors = append(errors, fmt.Sprintf("%s: value %v below minimum %v", path, num, min))
		}
		if max, ok := toFloat(schema["maximum"]); ok && num > max {
			errors = append(errors, fmt.Sprintf("%s: value %v above maximum %v", path, num, max))
		}
	}

	// String checks
	if str, ok := data.(string); ok {
		if minLen, ok := schema["minLength"].(float64); ok && float64(len(str)) < minLen {
			errors = append(errors, fmt.Sprintf("%s: string length %d below minimum %d", path, len(str), int(minLen)))
		}
		if maxLen, ok := schema["maxLength"].(float64); ok && float64(len(str)) > maxLen {
			errors = append(errors, fmt.Sprintf("%s: string length %d above maximum %d", path, len(str), int(maxLen)))
		}
	}

	return errors
}

func checkType(expected string, data interface{}) bool {
	switch expected {
	case "object":
		_, ok := data.(map[string]interface{})
		return ok
	case "array":
		_, ok := data.([]interface{})
		return ok
	case "string":
		_, ok := data.(string)
		return ok
	case "number":
		_, ok1 := data.(float64)
		_, ok2 := data.(int)
		return ok1 || ok2
	case "integer":
		if f, ok := data.(float64); ok {
			return f == math.Trunc(f)
		}
		_, ok := data.(int)
		return ok
	case "boolean":
		_, ok := data.(bool)
		return ok
	case "null":
		return data == nil
	default:
		return true
	}
}

func typeName(data interface{}) string {
	switch data.(type) {
	case map[string]interface{}:
		return "object"
	case []interface{}:
		return "array"
	case string:
		return "string"
	case float64:
		return "number"
	case int:
		return "integer"
	case bool:
		return "boolean"
	case nil:
		return "null"
	default:
		return "unknown"
	}
}

func toFloat(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	default:
		return 0, false
	}
}

func deepEqual(a, b interface{}) bool {
	aj, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bj, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(aj) == string(bj)
}

// validateAvroStructural does a basic structural check: verify all required
// fields in the Avro record are present in the JSON payload.
func validateAvroStructural(schemaStr string, payload []byte) (*ValidationResult, error) {
	var schema map[string]interface{}
	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		return nil, fmt.Errorf("invalid avro schema: %w", err)
	}

	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return &ValidationResult{Valid: false, Errors: []string{"payload is not valid JSON"}}, nil
	}

	fields, ok := schema["fields"].([]interface{})
	if !ok {
		return &ValidationResult{Valid: true}, nil
	}

	var errors []string
	for _, f := range fields {
		fm, ok := f.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := fm["name"].(string)
		if _, hasDefault := fm["default"]; hasDefault {
			continue // field has default, not required
		}
		if _, exists := data[name]; !exists {
			// Only error if no union types (which allow null)
			typ := fm["type"]
			if isNullableUnion(typ) {
				continue
			}
			errors = append(errors, fmt.Sprintf("required field %q missing", name))
		}
	}

	if len(errors) == 0 {
		return &ValidationResult{Valid: true}, nil
	}
	return &ValidationResult{Valid: false, Errors: errors}, nil
}

func isNullableUnion(typ interface{}) bool {
	arr, ok := typ.([]interface{})
	if !ok {
		return false
	}
	for _, t := range arr {
		if s, ok := t.(string); ok && s == "null" {
			return true
		}
	}
	return false
}

// validateProtobufStructural checks that the payload can be decoded by
// parsing the .proto schema for field tags and verifying the binary wire
// format contains valid varint/length-delimited fields for those tags.
func validateProtobufStructural(schemaStr string, payload []byte) (*ValidationResult, error) {
	if len(payload) == 0 {
		return &ValidationResult{Valid: false, Errors: []string{"empty protobuf payload"}}, nil
	}

	// Extract field numbers from the proto schema
	fieldNums := extractProtoFieldNums(schemaStr)
	if len(fieldNums) == 0 {
		// No fields parsed — accept any non-empty binary
		return &ValidationResult{Valid: true}, nil
	}

	// Parse the binary protobuf payload and check field numbers
	seen := make(map[uint32]bool)
	buf := payload
	for len(buf) > 0 {
		tag, n := decodeVarint(buf)
		if n == 0 {
			return &ValidationResult{Valid: false, Errors: []string{"invalid varint in protobuf"}}, nil
		}
		buf = buf[n:]

		fieldNum := uint32(tag >> 3)
		wireType := int(tag & 0x7)
		seen[fieldNum] = true

		// Skip the field value based on wire type
		consumed, err := skipField(buf, wireType)
		if err != nil {
			return &ValidationResult{Valid: false, Errors: []string{err.Error()}}, nil
		}
		buf = buf[consumed:]
	}

	// Check that required fields are present
	var errors []string
	for _, num := range fieldNums {
		if !seen[num] {
			errors = append(errors, fmt.Sprintf("required field %d missing", num))
		}
	}

	if len(errors) == 0 {
		return &ValidationResult{Valid: true}, nil
	}
	return &ValidationResult{Valid: false, Errors: errors}, nil
}

// extractProtoFieldNums parses "message { ... }" blocks and extracts
// field numbers from lines like "type name = N;".
func extractProtoFieldNums(schema string) []uint32 {
	var nums []uint32
	inMessage := false
	for _, line := range strings.Split(schema, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "message ") || strings.HasPrefix(trimmed, "message{") {
			inMessage = true
			continue
		}
		if trimmed == "}" {
			inMessage = false
			continue
		}
		if !inMessage {
			continue
		}
		// Look for field definition: "type name = N;"
		if idx := strings.LastIndex(trimmed, "="); idx >= 0 {
			rest := strings.TrimSpace(trimmed[idx+1:])
			rest = strings.TrimRight(rest, "; ")
			var num uint32
			if _, err := fmt.Sscanf(rest, "%d", &num); err == nil && num > 0 {
				nums = append(nums, num)
			}
		}
	}
	return nums
}

// decodeVarint decodes a protobuf varint from the buffer.
func decodeVarint(buf []byte) (uint64, int) {
	var val uint64
	for i := 0; i < len(buf) && i < 10; i++ {
		b := buf[i]
		val |= uint64(b&0x7f) << (7 * i)
		if b&0x80 == 0 {
			return val, i + 1
		}
	}
	return 0, 0
}

// skipField skips a protobuf field value based on wire type.
func skipField(buf []byte, wireType int) (int, error) {
	switch wireType {
	case 0: // varint
		for i := 0; i < len(buf) && i < 10; i++ {
			if buf[i]&0x80 == 0 {
				return i + 1, nil
			}
		}
		return 0, fmt.Errorf("unterminated varint")
	case 1: // 64-bit
		if len(buf) < 8 {
			return 0, fmt.Errorf("truncated 64-bit field")
		}
		return 8, nil
	case 2: // length-delimited
		length, n := decodeVarint(buf)
		if n == 0 {
			return 0, fmt.Errorf("invalid length-delimited size")
		}
		total := n + int(length)
		if total > len(buf) {
			return 0, fmt.Errorf("length-delimited field exceeds payload")
		}
		return total, nil
	case 5: // 32-bit
		if len(buf) < 4 {
			return 0, fmt.Errorf("truncated 32-bit field")
		}
		return 4, nil
	default:
		return 0, fmt.Errorf("unknown wire type %d", wireType)
	}
}

// ParseSchemaID extracts a schema ID from message headers.
func ParseSchemaID(headers map[string]string) (uint32, bool) {
	v, ok := headers["x-chimera-schema-id"]
	if !ok {
		return 0, false
	}
	var id uint32
	n, err := fmt.Sscanf(v, "%d", &id)
	if err != nil || n != 1 {
		return 0, false
	}
	return id, true
}

// FormatSchemaID creates a header value for a schema ID.
func FormatSchemaID(id uint32) string {
	return fmt.Sprintf("%d", id)
}

// InferSchemaType determines schema type from content.
func InferSchemaType(content string) SchemaType {
	trimmed := strings.TrimSpace(content)
	if strings.HasPrefix(trimmed, "{") {
		// Could be JSON Schema or Avro. Check for Avro markers.
		if strings.Contains(trimmed, `"type":"record"`) || strings.Contains(trimmed, `"type": "record"`) {
			return SchemaAvro
		}
		return SchemaJSON
	}
	if strings.Contains(trimmed, "message ") || strings.Contains(trimmed, "syntax =") {
		return SchemaProtobuf
	}
	return SchemaJSON
}
