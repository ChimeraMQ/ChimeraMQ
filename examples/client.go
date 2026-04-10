// Package main demonstrates a basic ChimeraMQ client using the HTTP API.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	adminAddr = "http://localhost:9090"
	apiKey    = "your-api-key" // Set via auth.tokens in chimera.yaml
)

func main() {
	client := &Client{
		Addr:   adminAddr,
		APIKey: apiKey,
		HTTP:   &http.Client{Timeout: 10 * time.Second},
	}

	// Create a topic
	fmt.Println("Creating topic...")
	err := client.CreateTopic("orders", "unified", 8)
	if err != nil {
		fmt.Printf("Create topic: %v (may already exist)\n", err)
	}

	// Publish messages
	fmt.Println("Publishing messages...")
	for i := 0; i < 5; i++ {
		offset, partition, err := client.Publish("orders", []byte(fmt.Sprintf(`{"order_id":%d,"item":"widget-%d"}`, i, i)))
		if err != nil {
			fmt.Printf("Publish: %v\n", err)
			continue
		}
		fmt.Printf("  Published to partition %d at offset %d\n", partition, offset)
	}

	// Fetch messages
	fmt.Println("Fetching messages...")
	messages, err := client.Fetch("orders", 0, 0, 10)
	if err != nil {
		fmt.Printf("Fetch: %v\n", err)
		return
	}
	for _, msg := range messages {
		fmt.Printf("  Offset %d: %s\n", msg["offset"], msg["data"])
	}

	// Check health
	fmt.Println("Health check...")
	health, err := client.Health()
	if err != nil {
		fmt.Printf("Health: %v\n", err)
		return
	}
	fmt.Printf("  Status: %v\n", health["status"])
}

// Client is a basic ChimeraMQ HTTP client.
type Client struct {
	Addr   string
	APIKey string
	HTTP   *http.Client
}

func (c *Client) do(method, path string, body interface{}) (map[string]interface{}, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.Addr+path, reqBody)
	if err != nil {
		return nil, err
	}
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(data, &result)
	return result, nil
}

// CreateTopic creates a new topic.
func (c *Client) CreateTopic(name, mode string, partitions int) error {
	_, err := c.do("POST", "/v1/topics", map[string]interface{}{
		"name":       name,
		"mode":       mode,
		"partitions": partitions,
	})
	return err
}

// Publish sends a message to a topic.
func (c *Client) Publish(topic string, payload []byte) (uint64, uint32, error) {
	req, err := http.NewRequest("POST", c.Addr+"/v1/messages/"+topic, bytes.NewReader(payload))
	if err != nil {
		return 0, 0, err
	}
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(data, &result)

	offset, _ := result["offset"].(float64)
	partition, _ := result["partition"].(float64)
	return uint64(offset), uint32(partition), nil
}

// Fetch retrieves messages from a topic.
func (c *Client) Fetch(topic string, partition, offset, limit int) ([]map[string]interface{}, error) {
	path := fmt.Sprintf("/v1/messages/%s?partition=%d&offset=%d&limit=%d", topic, partition, offset, limit)
	result, err := c.do("GET", path, nil)
	if err != nil {
		return nil, err
	}
	if msgs, ok := result["messages"].([]interface{}); ok {
		var out []map[string]interface{}
		for _, m := range msgs {
			if msg, ok := m.(map[string]interface{}); ok {
				out = append(out, msg)
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("unexpected response: %v", result)
}

// Health checks broker health.
func (c *Client) Health() (map[string]interface{}, error) {
	return c.do("GET", "/v1/health", nil)
}
