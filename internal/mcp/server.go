package mcp

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
)

// Server implements an MCP (Model Context Protocol) server for ChimeraMQ.
// It communicates over JSON-RPC on stdio.

// Version is set at build time via ldflags.
var Version = "dev"

type Server struct {
	broker  *broker.Broker
	handler map[string]func(json.RawMessage) (interface{}, error)
	mu      sync.Mutex
	encoder *json.Encoder
	writer  io.Writer
}

// JSONRPCRequest is a JSON-RPC 2.0 request.
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// JSONRPCResponse is a JSON-RPC 2.0 response.
type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
}

// RPCError is a JSON-RPC error.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// NewServer creates a new MCP server.
func NewServer(b *broker.Broker) *Server {
	s := &Server{
		broker:  b,
		handler: make(map[string]func(json.RawMessage) (interface{}, error)),
		writer:  os.Stdout,
	}
	s.encoder = json.NewEncoder(s.writer)
	s.registerHandlers()
	s.registerToolHandlers()
	return s
}

// SetWriter sets the output writer (for testing).
func (s *Server) SetWriter(w io.Writer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer = w
	s.encoder = json.NewEncoder(w)
}

func (s *Server) registerHandlers() {
	s.handler["tools/list"] = s.handleToolsList
	s.handler["tools/call"] = s.handleToolsCall
	s.handler["initialize"] = s.handleInitialize
	s.handler["initialized"] = s.handleNoop
}

func (s *Server) handleInitialize(_ json.RawMessage) (interface{}, error) {
	return map[string]interface{}{
		"protocolVersion": "2024-11-05",
		"capabilities": map[string]interface{}{
			"tools": map[string]interface{}{},
		},
		"serverInfo": map[string]interface{}{
			"name":    "chimera-mcp",
			"version": "1.0.0",
		},
	}, nil
}

func (s *Server) handleNoop(_ json.RawMessage) (interface{}, error) {
	return nil, nil
}

// ToolDef describes an MCP tool.
type ToolDef struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	InputSchema map[string]interface{} `json:"inputSchema"`
}

func (s *Server) handleToolsList(_ json.RawMessage) (interface{}, error) {
	tools := []ToolDef{
		{
			Name:        "chimera_list_topics",
			Description: "List all topics in the broker",
			InputSchema: map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
		},
		{
			Name:        "chimera_create_topic",
			Description: "Create a new topic with optional partitions",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"name":       map[string]interface{}{"type": "string", "description": "Topic name"},
					"partitions": map[string]interface{}{"type": "integer", "description": "Number of partitions"},
				},
				"required": []string{"name"},
			},
		},
		{
			Name:        "chimera_publish",
			Description: "Publish a message to a topic",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"topic": map[string]interface{}{"type": "string", "description": "Topic name"},
					"key":   map[string]interface{}{"type": "string", "description": "Message key"},
					"value": map[string]interface{}{"type": "string", "description": "Message value"},
				},
				"required": []string{"topic", "value"},
			},
		},
		{
			Name:        "chimera_topic_info",
			Description: "Get detailed info about a specific topic",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{"type": "string", "description": "Topic name"},
				},
				"required": []string{"name"},
			},
		},
		{
			Name:        "chimera_delete_topic",
			Description: "Delete a topic",
			InputSchema: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"name": map[string]interface{}{"type": "string", "description": "Topic name"},
				},
				"required": []string{"name"},
			},
		},
		{
			Name:        "chimera_broker_info",
			Description: "Get broker status and configuration info",
			InputSchema: map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			},
		},
	}
	return map[string]interface{}{"tools": tools}, nil
}

type toolsCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

func (s *Server) handleToolsCall(raw json.RawMessage) (interface{}, error) {
	var params toolsCallParams
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}

	handler, ok := s.handler["tool_"+params.Name]
	if !ok {
		return nil, fmt.Errorf("unknown tool: %s", params.Name)
	}

	result, err := handler(params.Arguments)
	if err != nil {
		return map[string]interface{}{
			"content": []map[string]interface{}{
				{"type": "text", "text": fmt.Sprintf("Error: %s", err)},
			},
			"isError": true,
		}, nil
	}

	return map[string]interface{}{
		"content": []map[string]interface{}{
			{"type": "text", "text": mustMarshal(result)},
		},
	}, nil
}

// Tool handlers
func (s *Server) registerToolHandlers() {
	s.handler["tool_chimera_list_topics"] = s.toolListTopics
	s.handler["tool_chimera_create_topic"] = s.toolCreateTopic
	s.handler["tool_chimera_publish"] = s.toolPublish
	s.handler["tool_chimera_topic_info"] = s.toolTopicInfo
	s.handler["tool_chimera_delete_topic"] = s.toolDeleteTopic
	s.handler["tool_chimera_broker_info"] = s.toolBrokerInfo
}

func (s *Server) toolListTopics(_ json.RawMessage) (interface{}, error) {
	topics := s.broker.Topics().ListTopics()
	result := make([]map[string]interface{}, 0, len(topics))
	for _, tc := range topics {
		result = append(result, map[string]interface{}{
			"name":       tc.Name,
			"partitions": tc.Partitions,
		})
	}
	return result, nil
}

type createTopicArgs struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
}

func (s *Server) toolCreateTopic(raw json.RawMessage) (interface{}, error) {
	var args createTopicArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, err
	}

	partitions := uint32(args.Partitions)
	if partitions == 0 {
		partitions = s.broker.Config().Defaults.Topic.Partitions
	}

	if err := s.broker.Topics().CreateTopic(broker.TopicConfig{
		Name:       args.Name,
		Partitions: partitions,
	}); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"topic":      args.Name,
		"partitions": partitions,
		"created":    true,
	}, nil
}

type publishArgs struct {
	Topic string `json:"topic"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) toolPublish(raw json.RawMessage) (interface{}, error) {
	var args publishArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, err
	}

	env := &message.Envelope{
		Topic:      args.Topic,
		RoutingKey: args.Key,
		Payload:    []byte(args.Value),
	}

	offset, err := s.broker.Publish(env)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"topic":     args.Topic,
		"key":       args.Key,
		"offset":    offset,
		"published": true,
	}, nil
}

type topicNameArgs struct {
	Name string `json:"name"`
}

func (s *Server) toolTopicInfo(raw json.RawMessage) (interface{}, error) {
	var args topicNameArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, err
	}

	tc, ok := s.broker.Topics().GetTopic(args.Name)
	if !ok {
		return nil, fmt.Errorf("topic %q not found", args.Name)
	}

	return map[string]interface{}{
		"name":       tc.Name,
		"partitions": tc.Partitions,
	}, nil
}

func (s *Server) toolDeleteTopic(raw json.RawMessage) (interface{}, error) {
	var args topicNameArgs
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, err
	}

	if err := s.broker.Topics().DeleteTopic(args.Name); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"topic":   args.Name,
		"deleted": true,
	}, nil
}

func (s *Server) toolBrokerInfo(_ json.RawMessage) (interface{}, error) {
	cfg := s.broker.Config()
	return map[string]interface{}{
		"node":    cfg.Node,
		"version": Version,
		"protocols": map[string]bool{
			"chimera":   cfg.Protocols.Chimera.Enabled,
			"mqtt":      cfg.Protocols.MQTT.Enabled,
			"websocket": cfg.Protocols.WebSocket.Enabled,
			"amqp":      cfg.Protocols.AMQP.Enabled,
		},
		"auth_enabled": cfg.Auth.Enabled,
		"tls_enabled":  cfg.TLS.Enabled,
	}, nil
}

// Serve starts reading JSON-RPC from stdin and writing responses to stdout.
func (s *Server) Serve() error {
	decoder := json.NewDecoder(os.Stdin)

	for {
		var req JSONRPCRequest
		if err := decoder.Decode(&req); err != nil {
			if err == io.EOF {
				return nil
			}
			slog.Error("MCP decode error", "err", err)
			continue
		}

		s.handleRequest(req)
	}
}

func (s *Server) handleRequest(req JSONRPCRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()

	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
	}

	handler, ok := s.handler[req.Method]
	if !ok {
		resp.Error = &RPCError{Code: -32601, Message: fmt.Sprintf("method not found: %s", req.Method)}
	} else {
		result, err := handler(req.Params)
		if err != nil {
			resp.Error = &RPCError{Code: -32603, Message: err.Error()}
		} else {
			resp.Result = result
		}
	}

	_ = s.encoder.Encode(resp)
}

// HandleRequest is for testing — processes a single request and returns the response.
func (s *Server) HandleRequest(req JSONRPCRequest) JSONRPCResponse {
	handler, ok := s.handler[req.Method]
	if !ok {
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &RPCError{Code: -32601, Message: fmt.Sprintf("method not found: %s", req.Method)},
		}
	}

	result, err := handler(req.Params)
	if err != nil {
		return JSONRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &RPCError{Code: -32603, Message: err.Error()},
		}
	}

	return JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  result,
	}
}

func mustMarshal(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
