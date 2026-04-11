// Package chimera provides a Go client for ChimeraMQ.
//
// Use NewClient to create a client instance, then call methods to manage
// topics, publish and consume messages, join consumer groups, and more.
package chimera

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HealthInfo represents the broker health status.
type HealthInfo struct {
	Status string `json:"status"`
	NodeID int    `json:"node_id"`
	Name   string `json:"name"`
	Uptime string `json:"uptime"`
}

// TopicInfo represents a topic's metadata.
type TopicInfo struct {
	Name       string `json:"name"`
	Mode       string `json:"mode"`
	Partitions int    `json:"partitions"`
	CreatedAt  string `json:"created_at"`
}

// PublishResult contains the offset and partition of a published message.
type PublishResult struct {
	Offset    uint64 `json:"offset"`
	Partition int    `json:"partition"`
}

// Message represents a fetched message.
type Message struct {
	Offset    uint64            `json:"offset"`
	Partition int               `json:"partition"`
	Data      json.RawMessage   `json:"data"`
	Timestamp int64             `json:"timestamp"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// FetchResult contains messages returned by a fetch operation.
type FetchResult struct {
	Count      int       `json:"count"`
	Messages   []Message `json:"messages"`
	NextOffset uint64    `json:"next_offset"`
}

// ConsumerGroupInfo represents a consumer group.
type ConsumerGroupInfo struct {
	Group   string   `json:"group"`
	Topic   string   `json:"topic"`
	Members []string `json:"members"`
}

// JoinResult contains the member ID and assigned partitions from joining a group.
type JoinResult struct {
	MemberID   string `json:"member_id"`
	Partitions []int  `json:"partitions"`
	Generation int    `json:"generation"`
}

// SchemaInfo represents a schema in the registry.
type SchemaInfo struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Type    string `json:"type"`
	Schema  string `json:"schema"`
}

// DLQEntry represents a dead-letter queue entry.
type DLQEntry struct {
	Offset      uint64          `json:"offset"`
	Reason      string          `json:"reason"`
	Original    json.RawMessage `json:"original"`
	Retries     int             `json:"retries"`
	FirstFailed time.Time       `json:"first_failed"`
}

// ClusterMember represents a node in the cluster.
type ClusterMember struct {
	NodeID  int    `json:"node_id"`
	Address string `json:"address"`
	Role    string `json:"role"`
	Status  string `json:"status"`
}

// Error represents an API error response.
type Error struct {
	StatusCode int
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("chimera: %s (HTTP %d)", e.Message, e.StatusCode)
}

// Client is a ChimeraMQ HTTP API client.
type Client struct {
	Addr  string
	token string
	http  *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithToken sets the authentication token.
func WithToken(token string) Option {
	return func(c *Client) { c.token = token }
}

// WithHTTPClient sets a custom http.Client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.http = hc }
}

// NewClient creates a new ChimeraMQ client.
// addr should be the admin API base URL (e.g., "http://localhost:9090").
func NewClient(addr string, opts ...Option) *Client {
	c := &Client{
		Addr: addr,
		http: &http.Client{Timeout: 30 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *Client) doRequest(method, path string, body interface{}) ([]byte, int, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, 0, err
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.Addr+path, reqBody)
	if err != nil {
		return nil, 0, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(data, &errResp) == nil && errResp.Error != "" {
			return nil, resp.StatusCode, &Error{StatusCode: resp.StatusCode, Message: errResp.Error}
		}
		return nil, resp.StatusCode, &Error{StatusCode: resp.StatusCode, Message: string(data)}
	}

	return data, resp.StatusCode, nil
}

func (c *Client) get(path string, result interface{}) error {
	data, _, err := c.doRequest("GET", path, nil)
	if err != nil {
		return err
	}
	if result != nil {
		return json.Unmarshal(data, result)
	}
	return nil
}

func (c *Client) post(path string, body interface{}, result interface{}) error {
	data, _, err := c.doRequest("POST", path, body)
	if err != nil {
		return err
	}
	if result != nil {
		return json.Unmarshal(data, result)
	}
	return nil
}

func (c *Client) delete(path string) error {
	_, _, err := c.doRequest("DELETE", path, nil)
	return err
}

// --- Health ---

// Health returns the broker health status.
func (c *Client) Health() (*HealthInfo, error) {
	var h HealthInfo
	if err := c.get("/v1/health", &h); err != nil {
		return nil, err
	}
	return &h, nil
}

// --- Topics ---

// CreateTopic creates a new topic with the given name, mode, and partition count.
func (c *Client) CreateTopic(name, mode string, partitions int) error {
	return c.post("/v1/topics", map[string]interface{}{
		"name":       name,
		"mode":       mode,
		"partitions": partitions,
	}, nil)
}

// ListTopics returns all topics.
func (c *Client) ListTopics() ([]TopicInfo, error) {
	var topics []TopicInfo
	if err := c.get("/v1/topics", &topics); err != nil {
		return nil, err
	}
	return topics, nil
}

// GetTopic returns information about a specific topic.
func (c *Client) GetTopic(name string) (*TopicInfo, error) {
	var t TopicInfo
	if err := c.get("/v1/topics/"+name, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// DeleteTopic deletes a topic.
func (c *Client) DeleteTopic(name string) error {
	return c.delete("/v1/topics/" + name)
}

// --- Messages ---

// Publish sends a message to a topic. The payload is sent as application/octet-stream.
func (c *Client) Publish(topic string, payload []byte) (*PublishResult, error) {
	req, err := http.NewRequest("POST", c.Addr+"/v1/messages/"+topic, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, &Error{StatusCode: resp.StatusCode, Message: string(data)}
	}

	var result PublishResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Fetch retrieves messages from a topic starting at the given offset.
func (c *Client) Fetch(topic string, partition, offset, limit int) (*FetchResult, error) {
	path := fmt.Sprintf("/v1/messages/%s?partition=%d&offset=%d&limit=%d",
		topic, partition, offset, limit)
	var result FetchResult
	if err := c.get(path, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Ack acknowledges a queue message.
func (c *Client) Ack(topic string, offset uint64) error {
	return c.post(fmt.Sprintf("/v1/messages/%s/ack", topic),
		map[string]interface{}{"offset": offset}, nil)
}

// Nack negatively acknowledges a queue message.
func (c *Client) Nack(topic string, offset uint64) error {
	return c.post(fmt.Sprintf("/v1/messages/%s/nack", topic),
		map[string]interface{}{"offset": offset}, nil)
}

// --- Consumer Groups ---

// ListConsumerGroups returns all consumer groups.
func (c *Client) ListConsumerGroups() ([]ConsumerGroupInfo, error) {
	var groups []ConsumerGroupInfo
	if err := c.get("/v1/consumers", &groups); err != nil {
		return nil, err
	}
	return groups, nil
}

// JoinGroup joins a consumer group for a topic.
func (c *Client) JoinGroup(group, topic, memberID string) (*JoinResult, error) {
	var result JoinResult
	if err := c.post(fmt.Sprintf("/v1/consumers/%s/join", group),
		map[string]interface{}{
			"topic":     topic,
			"member_id": memberID,
		}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// LeaveGroup leaves a consumer group.
func (c *Client) LeaveGroup(group, memberID string) error {
	return c.post(fmt.Sprintf("/v1/consumers/%s/leave", group),
		map[string]interface{}{"member_id": memberID}, nil)
}

// Heartbeat sends a heartbeat for a consumer group member.
func (c *Client) Heartbeat(group, memberID string) error {
	return c.post(fmt.Sprintf("/v1/consumers/%s/heartbeat", group),
		map[string]interface{}{"member_id": memberID}, nil)
}

// CommitOffsets commits consumer group offsets.
func (c *Client) CommitOffsets(group string, offsets map[int]int64) error {
	return c.post(fmt.Sprintf("/v1/consumers/%s/offsets", group),
		map[string]interface{}{"offsets": offsets}, nil)
}

// GetOffsets returns the current offsets for a consumer group.
func (c *Client) GetOffsets(group string) (map[int]int64, error) {
	var offsets map[int]int64
	if err := c.get(fmt.Sprintf("/v1/consumers/%s/offsets", group), &offsets); err != nil {
		return nil, err
	}
	return offsets, nil
}

// --- Schemas ---

// RegisterSchema registers a new schema for a subject.
func (c *Client) RegisterSchema(subject, schemaType, schema string) (*SchemaInfo, error) {
	var result SchemaInfo
	if err := c.post(fmt.Sprintf("/v1/schemas/%s", subject),
		map[string]interface{}{
			"type":   schemaType,
			"schema": schema,
		}, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetLatestSchema returns the latest schema for a subject.
func (c *Client) GetLatestSchema(subject string) (*SchemaInfo, error) {
	var s SchemaInfo
	if err := c.get(fmt.Sprintf("/v1/schemas/%s/latest", subject), &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// --- DLQ ---

// PeekDLQ returns dead-letter entries for a topic.
func (c *Client) PeekDLQ(topic string) ([]DLQEntry, error) {
	var entries []DLQEntry
	if err := c.get(fmt.Sprintf("/v1/dlq/%s", topic), &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

// ClearDLQ clears all dead-letter entries for a topic.
func (c *Client) ClearDLQ(topic string) error {
	return c.delete(fmt.Sprintf("/v1/dlq/%s", topic))
}

// ReplayDLQ replays all dead-letter entries for a topic back to the original topic.
func (c *Client) ReplayDLQ(topic string) error {
	return c.post(fmt.Sprintf("/v1/dlq/%s/replay", topic), nil, nil)
}

// --- Cluster ---

// ClusterMembers returns the current cluster membership.
func (c *Client) ClusterMembers() ([]ClusterMember, error) {
	var members []ClusterMember
	if err := c.get("/v1/cluster/members", &members); err != nil {
		return nil, err
	}
	return members, nil
}
