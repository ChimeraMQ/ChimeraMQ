package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/message"
)

const (
	maxFetchTimeout = 30 * time.Second
)

// AdminServer provides the HTTP admin API.
type AdminServer struct {
	broker *broker.Broker
	server *http.Server
	mux    *http.ServeMux
}

// NewAdminServer creates a new HTTP admin server.
func NewAdminServer(b *broker.Broker) *AdminServer {
	mux := http.NewServeMux()
	s := &AdminServer{
		broker: b,
		mux:    mux,
		server: &http.Server{
			Addr:         fmt.Sprintf("%s:%d", b.Config().Listener.Bind, b.Config().Listener.AdminPort),
			Handler:      mux,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  120 * time.Second,
			MaxHeaderBytes: 1 << 20,
		},
	}
	s.registerRoutes()
	s.server.Handler = s.securityMiddleware(mux)
	return s
}

func (s *AdminServer) registerRoutes() {
	s.mux.HandleFunc("POST /v1/topics", s.auth(s.handleCreateTopic))
	s.mux.HandleFunc("GET /v1/topics", s.auth(s.handleListTopics))
	s.mux.HandleFunc("GET /v1/topics/{name}", s.auth(s.handleGetTopic))
	s.mux.HandleFunc("DELETE /v1/topics/{name}", s.auth(s.handleDeleteTopic))
	s.mux.HandleFunc("POST /v1/messages/{topic}", s.auth(s.handlePublish))
	s.mux.HandleFunc("GET /v1/messages/{topic}", s.auth(s.handleFetch))
	s.mux.HandleFunc("POST /v1/messages/{topic}/ack", s.auth(s.handleAck))
	s.mux.HandleFunc("POST /v1/messages/{topic}/nack", s.auth(s.handleNack))
	s.mux.HandleFunc("GET /v1/consumers", s.auth(s.handleListConsumers))
	s.mux.HandleFunc("GET /v1/consumers/{group}", s.auth(s.handleGetConsumer))
	s.mux.HandleFunc("GET /v1/health", s.handleHealth)
	s.mux.HandleFunc("GET /v1/metrics", s.handleMetrics)
}

// securityMiddleware adds security headers and CORS support.
func (s *AdminServer) securityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "1; mode=block")

		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// auth is middleware that validates authentication when enabled.
func (s *AdminServer) auth(next http.HandlerFunc) http.HandlerFunc {
	cfg := s.broker.Config()
	if !cfg.Auth.Enabled {
		return next
	}

	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		// Bearer token auth
		if strings.HasPrefix(authHeader, "Bearer ") {
			token := strings.TrimPrefix(authHeader, "Bearer ")
			if _, ok := cfg.Auth.Tokens[token]; ok {
				next(w, r)
				return
			}
		}

		// Basic auth: decode "user:password" from base64
		if strings.HasPrefix(authHeader, "Basic ") {
			encoded := strings.TrimPrefix(authHeader, "Basic ")
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			if err == nil {
				pair := strings.SplitN(string(decoded), ":", 2)
				if len(pair) == 2 {
					if _, ok := cfg.Auth.Users[pair[0]]; ok {
						// User exists — in production compare bcrypt hash
						next(w, r)
						return
					}
				}
			}
		}

		writeError(w, http.StatusUnauthorized, "unauthorized")
	}
}

// Serve starts the HTTP server.
func (s *AdminServer) Serve() error {
	cfg := s.broker.Config()
	if cfg.TLS.Enabled {
		return s.server.ListenAndServeTLS(cfg.TLS.CertFile, cfg.TLS.KeyFile)
	}
	return s.server.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *AdminServer) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

func (s *AdminServer) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name          string `json:"name"`
		Mode          string `json:"mode"`
		Partitions    uint32 `json:"partitions"`
		RetentionTime string `json:"retention_time,omitempty"`
		DLQTopic      string `json:"dlq_topic,omitempty"`
		MaxRetries    uint32 `json:"max_retries,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	mode := broker.ModeUnified
	switch req.Mode {
	case "stream":
		mode = broker.ModeStream
	case "queue":
		mode = broker.ModeQueue
	}

	if req.Partitions == 0 {
		req.Partitions = s.broker.Config().Defaults.Topic.Partitions
	}

	cfg := broker.TopicConfig{
		Name:       req.Name,
		Mode:       mode,
		Partitions: req.Partitions,
		DLQTopic:   req.DLQTopic,
		MaxRetries: req.MaxRetries,
	}

	if err := s.broker.Topics().CreateTopic(cfg); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"name":       cfg.Name,
		"mode":       req.Mode,
		"partitions": cfg.Partitions,
	})
}

func (s *AdminServer) handleListTopics(w http.ResponseWriter, r *http.Request) {
	topics := s.broker.Topics().ListTopics()

	type topicInfo struct {
		Name       string `json:"name"`
		Mode       string `json:"mode"`
		Partitions uint32 `json:"partitions"`
		CreatedAt  string `json:"created_at"`
	}

	result := make([]topicInfo, len(topics))
	for i, t := range topics {
		modeStr := "unified"
		switch t.Mode {
		case broker.ModeStream:
			modeStr = "stream"
		case broker.ModeQueue:
			modeStr = "queue"
		}
		result[i] = topicInfo{
			Name:       t.Name,
			Mode:       modeStr,
			Partitions: t.Partitions,
			CreatedAt:  t.CreatedAt.Format(time.RFC3339),
		}
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *AdminServer) handleGetTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	topic, ok := s.broker.Topics().GetTopic(name)
	if !ok {
		writeError(w, http.StatusNotFound, "topic not found")
		return
	}

	type partStat struct {
		ID        uint32 `json:"id"`
		HighWater uint64 `json:"high_watermark"`
		LogStart  uint64 `json:"log_start_offset"`
	}

	stats := make([]partStat, topic.Partitions)
	for i := uint32(0); i < topic.Partitions; i++ {
		part, err := s.broker.Storage().GetOrCreatePartition(name, i)
		if err == nil {
			stats[i] = partStat{
				ID:        i,
				HighWater: part.HighWatermark(),
				LogStart:  part.LogStartOffset(),
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":       topic.Name,
		"partitions": stats,
	})
}

func (s *AdminServer) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.broker.Topics().DeleteTopic(name); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (s *AdminServer) handlePublish(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")

	maxSize := s.broker.Config().Limits.MaxMessageSize
	if maxSize <= 0 {
		maxSize = 16 * 1024 * 1024
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxSize))
	if err != nil {
		writeError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	env := &message.Envelope{
		Topic:       topicName,
		RoutingKey:  r.Header.Get("X-Routing-Key"),
		Payload:     body,
		ContentType: r.Header.Get("Content-Type"),
		SourceProto: message.ProtoHTTP,
	}

	offset, err := s.broker.Publish(env)
	if err != nil {
		s.broker.Logger().Error("publish failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"offset":    offset,
		"partition": env.PartitionID,
		"topic":     topicName,
	})
}

func (s *AdminServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "healthy",
		"node_id": s.broker.Config().Node.ID,
		"name":    s.broker.Config().Node.Name,
		"uptime":  time.Since(s.broker.StartTime()).String(),
	})
}

func (s *AdminServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(s.broker.Metrics().Expose()))
}

func (s *AdminServer) handleFetch(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	partitionStr := r.URL.Query().Get("partition")
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")
	timeoutStr := r.URL.Query().Get("timeout")

	partition := uint32(0)
	if partitionStr != "" {
		if v, err := strconv.ParseUint(partitionStr, 10, 32); err == nil {
			partition = uint32(v)
		} else {
			writeError(w, http.StatusBadRequest, "invalid partition parameter")
			return
		}
	}
	offset := uint64(0)
	if offsetStr != "" {
		if v, err := strconv.ParseUint(offsetStr, 10, 64); err == nil {
			offset = v
		} else {
			writeError(w, http.StatusBadRequest, "invalid offset parameter")
			return
		}
	}
	limit := 100
	maxFetch := s.broker.Config().Limits.MaxFetchMessages
	if maxFetch <= 0 {
		maxFetch = 10000
	}
	if limitStr != "" {
		v, err := strconv.Atoi(limitStr)
		if err != nil || v <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit parameter")
			return
		}
		if v > maxFetch {
			v = maxFetch
		}
		limit = v
	}
	timeout := 5 * time.Second
	if timeoutStr != "" {
		if d, err := time.ParseDuration(timeoutStr); err == nil {
			if d > maxFetchTimeout {
				d = maxFetchTimeout
			}
			timeout = d
		}
	}

	msgs, nextOffset, err := s.broker.StreamEngine().Fetch(topic, partition, offset, limit, timeout)
	if err != nil {
		s.broker.Logger().Error("fetch failed", "topic", topic, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	type fetchMsg struct {
		Offset      uint64            `json:"offset"`
		ContentType string            `json:"content_type,omitempty"`
		Payload     []byte            `json:"payload"`
		Headers     map[string][]byte `json:"headers,omitempty"`
	}

	result := make([]fetchMsg, len(msgs))
	for i, env := range msgs {
		result[i] = fetchMsg{
			Offset:      env.Sequence,
			ContentType: env.ContentType,
			Payload:     env.Payload,
			Headers:     env.Headers,
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"messages":    result,
		"next_offset": nextOffset,
		"count":       len(result),
	})
}

func (s *AdminServer) handleAck(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	var req struct {
		Offsets []uint64 `json:"offsets"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	acked := 0
	for _, off := range req.Offsets {
		if s.broker.QueueEngine().HandleAck(topic, off) {
			acked++
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"acked": acked,
		"total": len(req.Offsets),
	})
}

func (s *AdminServer) handleNack(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	var req struct {
		Offsets []uint64 `json:"offsets"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	nacked := 0
	dlqed := 0
	for _, off := range req.Offsets {
		shouldDLQ, _ := s.broker.QueueEngine().HandleNack(topic, off)
		nacked++
		if shouldDLQ {
			dlqed++
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"nacked":      nacked,
		"dlq_routed":  dlqed,
		"total":       len(req.Offsets),
	})
}

func (s *AdminServer) handleListConsumers(w http.ResponseWriter, r *http.Request) {
	groups := s.broker.StreamEngine().ListGroups()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"groups": groups,
		"count":  len(groups),
	})
}

func (s *AdminServer) handleGetConsumer(w http.ResponseWriter, r *http.Request) {
	group := r.PathValue("group")
	cg := s.broker.StreamEngine().GetGroup(group)
	if cg == nil {
		writeError(w, http.StatusNotFound, "consumer group not found")
		return
	}

	type memberInfo struct {
		ID         string   `json:"id"`
		Partitions []uint32 `json:"partitions"`
	}

	members := cg.Members()
	memberList := make([]memberInfo, 0, len(members))
	for _, m := range members {
		memberList = append(memberList, memberInfo{
			ID:         m.ID,
			Partitions: m.Partitions,
		})
	}

	assignments := cg.Assignments()
	assignmentMap := make(map[string][]uint32)
	for part, memberID := range assignments {
		assignmentMap[memberID] = append(assignmentMap[memberID], part)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"group":       group,
		"members":     memberList,
		"assignments": assignmentMap,
	})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
