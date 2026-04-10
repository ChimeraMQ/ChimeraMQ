package auth

import (
	"encoding/json"
	"os"
)

func readFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func parseJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
