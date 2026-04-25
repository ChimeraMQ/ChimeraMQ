package ttl

import (
	"testing"
)

func TestSetEncryptor(t *testing.T) {
	exp := NewExpirer(nil)
	exp.SetEncryptor(&mockDecryptor{})
	// Just verify it doesn't panic
}

type mockDecryptor struct{}

func (m *mockDecryptor) Decrypt(data []byte, key string) ([]byte, error) {
	return data, nil
}

func TestSetOnMetric(t *testing.T) {
	exp := NewExpirer(nil)
	exp.SetOnMetric(func(topic, action string) {
		// callback registered
	})
	// Just verify it doesn't panic
}
