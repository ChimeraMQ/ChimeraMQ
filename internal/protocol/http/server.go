package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime/trace"
	"strconv"
	"strings"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
	"github.com/chimeramq/chimera/internal/broker"
	"github.com/chimeramq/chimera/internal/engine/dlq"
	"github.com/chimeramq/chimera/internal/message"
	"github.com/chimeramq/chimera/internal/processing"
	"github.com/chimeramq/chimera/internal/schema"
	"github.com/chimeramq/chimera/internal/tenant"
	"github.com/chimeramq/chimera/internal/ui"
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
			Addr:           fmt.Sprintf("%s:%d", b.Config().Listener.Bind, b.Config().Listener.AdminPort),
			Handler:        mux,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   30 * time.Second,
			IdleTimeout:    120 * time.Second,
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
	s.mux.HandleFunc("POST /v1/consumers/{group}/join", s.auth(s.handleConsumerJoin))
	s.mux.HandleFunc("POST /v1/consumers/{group}/leave", s.auth(s.handleConsumerLeave))
	s.mux.HandleFunc("POST /v1/consumers/{group}/heartbeat", s.auth(s.handleConsumerHeartbeat))
	s.mux.HandleFunc("GET /v1/consumers/{group}/offsets", s.auth(s.handleConsumerOffsets))
	s.mux.HandleFunc("POST /v1/consumers/{group}/offsets", s.auth(s.handleConsumerCommitOffsets))
	s.mux.HandleFunc("GET /v1/health", s.handleHealth)
	s.mux.HandleFunc("GET /v1/metrics", s.handleMetrics)
	s.mux.HandleFunc("GET /v1/cluster/members", s.handleClusterMembers)
	s.mux.HandleFunc("POST /v1/schemas/{subject}", s.auth(s.handleRegisterSchema))
	s.mux.HandleFunc("GET /v1/schemas/{subject}", s.auth(s.handleListSchemas))
	s.mux.HandleFunc("GET /v1/schemas/{subject}/latest", s.auth(s.handleGetLatestSchema))
	s.mux.HandleFunc("GET /v1/schemas/{subject}/versions/{version}", s.auth(s.handleGetSchemaVersion))
	s.mux.HandleFunc("DELETE /v1/schemas/{subject}", s.auth(s.handleDeleteSubject))
	s.mux.HandleFunc("PUT /v1/schemas/{subject}/compatibility", s.auth(s.handleSetCompatibility))

	// WASM module endpoints
	s.mux.HandleFunc("POST /v1/wasm/modules", s.auth(s.handleUploadWASM))
	s.mux.HandleFunc("GET /v1/wasm/modules", s.auth(s.handleListWASM))
	s.mux.HandleFunc("DELETE /v1/wasm/modules/{name}", s.auth(s.handleDeleteWASM))

	// Stream processor endpoints
	s.mux.HandleFunc("POST /v1/processors", s.auth(s.handleCreateTopology))
	s.mux.HandleFunc("GET /v1/processors", s.auth(s.handleListTopologies))
	s.mux.HandleFunc("GET /v1/processors/{name}", s.auth(s.handleGetTopology))
	s.mux.HandleFunc("DELETE /v1/processors/{name}", s.auth(s.handleDeleteTopology))
	s.mux.HandleFunc("POST /v1/processors/{name}/start", s.auth(s.handleStartTopology))
	s.mux.HandleFunc("POST /v1/processors/{name}/stop", s.auth(s.handleStopTopology))

	// DLQ endpoints
	s.mux.HandleFunc("GET /v1/dlq/{topic}", s.auth(s.handleDLQPeek))
	s.mux.HandleFunc("DELETE /v1/dlq/{topic}", s.auth(s.handleDLQClear))
	s.mux.HandleFunc("POST /v1/dlq/{topic}/replay", s.auth(s.handleDLQReplay))
	s.mux.HandleFunc("POST /v1/dlq/{topic}/preview", s.auth(s.handleDLQPreview))
	s.mux.HandleFunc("GET /v1/dlq/{topic}/export", s.auth(s.handleDLQExport))

	// Tenant management endpoints
	s.mux.HandleFunc("POST /v1/tenants", s.auth(s.handleCreateTenant))
	s.mux.HandleFunc("GET /v1/tenants", s.auth(s.handleListTenants))
	s.mux.HandleFunc("GET /v1/tenants/{id}", s.auth(s.handleGetTenant))
	s.mux.HandleFunc("DELETE /v1/tenants/{id}", s.auth(s.handleDeleteTenant))
	s.mux.HandleFunc("GET /v1/tenants/{id}/usage", s.auth(s.handleGetTenantUsage))
	s.mux.HandleFunc("GET /v1/tenants/{id}/quotas", s.auth(s.handleGetTenantQuotas))
	s.mux.HandleFunc("PUT /v1/tenants/{id}/quotas", s.auth(s.handleUpdateTenantQuotas))

	// Exchange endpoints
	s.mux.HandleFunc("POST /v1/exchanges", s.auth(s.handleCreateExchange))
	s.mux.HandleFunc("GET /v1/exchanges", s.auth(s.handleListExchanges))
	s.mux.HandleFunc("GET /v1/exchanges/{name}", s.auth(s.handleGetExchange))
	s.mux.HandleFunc("DELETE /v1/exchanges/{name}", s.auth(s.handleDeleteExchange))
	s.mux.HandleFunc("POST /v1/exchanges/{name}/bindings", s.auth(s.handleBindExchange))
	s.mux.HandleFunc("DELETE /v1/exchanges/{name}/bindings", s.auth(s.handleUnbindExchange))
	s.mux.HandleFunc("POST /v1/exchanges/{name}/publish", s.auth(s.handlePublishToExchange))

	// Config reload endpoint
	s.mux.HandleFunc("POST /v1/config/reload", s.auth(s.handleConfigReload))

	// pprof profiling endpoints (when enabled)
	if s.broker.Config().Observability.PProf.Enabled {
		s.registerPProfRoutes()
	}

	// Embedded Web UI dashboard
	if s.broker.Config().Observability.Dashboard.Enabled {
		if h, err := ui.Handler(); err == nil {
			s.mux.Handle("/ui/", http.StripPrefix("/ui", h))
		}
	}
}

// registerPProfRoutes registers pprof profiling endpoints.
func (s *AdminServer) registerPProfRoutes() {
	s.mux.HandleFunc("/debug/pprof/", s.auth(s.handlePProfIndex))
	s.mux.HandleFunc("/debug/pprof/allocs", s.auth(s.handlePProfAllocs))
	s.mux.HandleFunc("/debug/pprof/block", s.auth(s.handlePProfBlock))
	s.mux.HandleFunc("/debug/pprof/cmdline", s.auth(s.handlePProfCmdline))
	s.mux.HandleFunc("/debug/pprof/goroutine", s.auth(s.handlePProfGoroutine))
	s.mux.HandleFunc("/debug/pprof/heap", s.auth(s.handlePProfHeap))
	s.mux.HandleFunc("/debug/pprof/mutex", s.auth(s.handlePProfMutex))
	s.mux.HandleFunc("/debug/pprof/profile", s.auth(s.handlePProfProfile))
	s.mux.HandleFunc("/debug/pprof/threadcreate", s.auth(s.handlePProfThreadcreate))
	s.mux.HandleFunc("/debug/pprof/trace", s.auth(s.handlePProfTrace))
}

// handlePProfIndex serves the pprof index page.
func (s *AdminServer) handlePProfIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<html>
<head><title>ChimeraMQ pprof</title></head>
<body>
<h1>ChimeraMQ pprof</h1>
<ul>
<li><a href="/debug/pprof/allocs">allocs</a></li>
<li><a href="/debug/pprof/block">block</a></li>
<li><a href="/debug/pprof/cmdline">cmdline</a></li>
<li><a href="/debug/pprof/goroutine">goroutine</a></li>
<li><a href="/debug/pprof/heap">heap</a></li>
<li><a href="/debug/pprof/mutex">mutex</a></li>
<li><a href="/debug/pprof/profile?seconds=30">profile (30 sec)</a></li>
<li><a href="/debug/pprof/threadcreate">threadcreate</a></li>
<li><a href="/debug/pprof/trace?seconds=5">trace (5 sec)</a></li>
</ul>
</body>
</html>
`)
}

// handlePProfAllocs serves the allocs profile.
func (s *AdminServer) handlePProfAllocs(w http.ResponseWriter, r *http.Request) {
	pprof.Handler("allocs").ServeHTTP(w, r)
}

// handlePProfBlock serves the block profile.
func (s *AdminServer) handlePProfBlock(w http.ResponseWriter, r *http.Request) {
	pprof.Handler("block").ServeHTTP(w, r)
}

// handlePProfCmdline serves the cmdline profile.
func (s *AdminServer) handlePProfCmdline(w http.ResponseWriter, r *http.Request) {
	pprof.Cmdline(w, r)
}

// handlePProfGoroutine serves the goroutine profile.
func (s *AdminServer) handlePProfGoroutine(w http.ResponseWriter, r *http.Request) {
	pprof.Handler("goroutine").ServeHTTP(w, r)
}

// handlePProfHeap serves the heap profile.
func (s *AdminServer) handlePProfHeap(w http.ResponseWriter, r *http.Request) {
	pprof.Handler("heap").ServeHTTP(w, r)
}

// handlePProfMutex serves the mutex profile.
func (s *AdminServer) handlePProfMutex(w http.ResponseWriter, r *http.Request) {
	pprof.Handler("mutex").ServeHTTP(w, r)
}

// handlePProfProfile serves the CPU profile.
func (s *AdminServer) handlePProfProfile(w http.ResponseWriter, r *http.Request) {
	pprof.Profile(w, r)
}

// handlePProfThreadcreate serves the threadcreate profile.
func (s *AdminServer) handlePProfThreadcreate(w http.ResponseWriter, r *http.Request) {
	pprof.Handler("threadcreate").ServeHTTP(w, r)
}

// handlePProfTrace serves the trace profile.
func (s *AdminServer) handlePProfTrace(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := trace.Start(w); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer trace.Stop()

	// Wait for the specified duration or default 1 second
	duration := 1 * time.Second
	if sec, _ := strconv.Atoi(r.URL.Query().Get("seconds")); sec > 0 {
		duration = time.Duration(sec) * time.Second
	}
	time.Sleep(duration)
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

// auth is middleware that validates authentication and ACL when enabled.
func (s *AdminServer) auth(next http.HandlerFunc) http.HandlerFunc {
	cfg := s.broker.Config()
	provider := s.broker.AuthProvider()
	aclEngine := s.broker.ACLEngine()

	if !cfg.Auth.Enabled || provider == nil {
		return next
	}

	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		var creds auth.Credentials

		// Bearer token auth
		if strings.HasPrefix(authHeader, "Bearer ") {
			creds.Token = strings.TrimPrefix(authHeader, "Bearer ")
		} else if strings.HasPrefix(authHeader, "Basic ") {
			// Basic auth: decode "user:password" from base64
			encoded := strings.TrimPrefix(authHeader, "Basic ")
			decoded, err := base64.StdEncoding.DecodeString(encoded)
			if err == nil {
				pair := strings.SplitN(string(decoded), ":", 2)
				if len(pair) == 2 {
					creds.Username = pair[0]
					creds.Password = pair[1]
				}
			}
		}

		identity, err := provider.Authenticate(r.Context(), creds)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		// Store identity in context for downstream handlers
		ctx := context.WithValue(r.Context(), identityKey{}, identity)
		r = r.WithContext(ctx)

		// ACL check
		if aclEngine != nil {
			op := methodToOp(r.Method)
			rt := auth.ResourceTopic
			name := r.PathValue("topic")
			if name == "" {
				name = r.PathValue("name")
			}
			if name == "" {
				name = r.PathValue("subject")
			}
			if name == "" {
				name = r.PathValue("id") // for tenant IDs
			}
			if r.URL.Path != "" && strings.Contains(r.URL.Path, "/schemas/") {
				rt = auth.ResourceSchema
			} else if strings.Contains(r.URL.Path, "/wasm/") {
				rt = auth.ResourceWASM
			} else if strings.Contains(r.URL.Path, "/cluster/") || strings.Contains(r.URL.Path, "/processors") {
				rt = auth.ResourceCluster
			} else if strings.Contains(r.URL.Path, "/tenants/") {
				rt = auth.ResourceTenant
			}
			if name == "" {
				name = "*"
			}
			if !aclEngine.Check(identity, rt, name, op) {
				writeError(w, http.StatusForbidden, "forbidden")
				return
			}
		}

		next(w, r)
	}
}

// identityKey is the context key for storing auth identity.
type identityKey struct{}

// methodToOp maps HTTP methods to auth operations.
func methodToOp(method string) auth.Operation {
	switch method {
	case "GET":
		return auth.OpRead
	case "POST", "PUT":
		return auth.OpWrite
	case "DELETE":
		return auth.OpDelete
	default:
		return auth.OpRead
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

// RegisterWebSocket registers a WebSocket handler at the given path.
func (s *AdminServer) RegisterWebSocket(path string, handler http.Handler) {
	s.mux.Handle(path, handler)
}

// Detector detects HTTP protocol by its method prefix.
type Detector struct{}

// Detect checks if the peeked bytes start with an HTTP method.
func (d *Detector) Detect(peek []byte) bool {
	if len(peek) < 4 {
		return false
	}
	prefix := string(peek[:4])
	switch prefix {
	case "GET ", "POST", "PUT ", "DELE", "OPTI", "PATC", "HEAD", "CONN":
		return true
	}
	return false
}

// BytesNeeded returns 4 (enough to identify HTTP methods).
func (d *Detector) BytesNeeded() int { return 4 }

// HandleConnection implements ProtocolHandler for use with the multiplexer.
func (s *AdminServer) HandleConnection(conn net.Conn, _ []byte) error {
	ln := &singleConnListener{conn: conn}
	srv := &http.Server{
		Handler:        s.securityMiddleware(s.mux),
		MaxHeaderBytes: 1 << 20,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
	}
	_ = srv.Serve(ln)
	return nil
}

// Stop implements ProtocolHandler.
func (s *AdminServer) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.server.Shutdown(ctx)
}

func (s *AdminServer) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	if s.broker.Topics() == nil {
		writeError(w, http.StatusServiceUnavailable, "topic manager not available")
		return
	}

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

	if !validateTopicName(req.Name) {
		writeError(w, http.StatusBadRequest, "invalid topic name")
		return
	}

	if req.Partitions == 0 {
		req.Partitions = s.broker.Config().Defaults.Topic.Partitions
	}
	if maxP := s.broker.Config().Limits.MaxPartitionsPerTopic; maxP > 0 && req.Partitions > maxP {
		writeError(w, http.StatusBadRequest, "partitions exceed maximum")
		return
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
	if s.broker.Topics() == nil {
		writeError(w, http.StatusServiceUnavailable, "topic manager not available")
		return
	}

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
	if s.broker.Topics() == nil {
		writeError(w, http.StatusServiceUnavailable, "topic manager not available")
		return
	}
	if s.broker.Storage() == nil {
		writeError(w, http.StatusServiceUnavailable, "storage not available")
		return
	}

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
	if s.broker.Topics() == nil {
		writeError(w, http.StatusServiceUnavailable, "topic manager not available")
		return
	}

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
	_, _ = w.Write([]byte(s.broker.Metrics().Expose()))
}

func (s *AdminServer) handleClusterMembers(w http.ResponseWriter, r *http.Request) {
	if !s.broker.IsClustered() {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"mode":    "single-node",
			"members": nil,
		})
		return
	}

	mgr := s.broker.Cluster()
	if mgr == nil {
		writeError(w, http.StatusServiceUnavailable, "cluster manager not available")
		return
	}

	members := mgr.Members()

	type memberInfo struct {
		ID          string `json:"id"`
		Addr        string `json:"addr"`
		Port        int    `json:"port"`
		State       string `json:"state"`
		Incarnation uint64 `json:"incarnation"`
	}

	result := make([]memberInfo, len(members))
	for i, m := range members {
		result[i] = memberInfo{
			ID:          string(m.ID),
			Addr:        m.Addr,
			Port:        m.Port,
			State:       m.State.String(),
			Incarnation: m.Incarnation,
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"mode":        "cluster",
		"is_leader":   mgr.IsLeader(),
		"leader_id":   mgr.LeaderID(),
		"alive_count": mgr.AliveCount(),
		"members":     result,
	})
}

func (s *AdminServer) handleFetch(w http.ResponseWriter, r *http.Request) {
	if s.broker.Topics() == nil {
		writeError(w, http.StatusServiceUnavailable, "topic manager not available")
		return
	}
	if s.broker.Storage() == nil {
		writeError(w, http.StatusServiceUnavailable, "storage not available")
		return
	}

	topic := r.PathValue("topic")
	partitionStr := r.URL.Query().Get("partition")
	offsetStr := r.URL.Query().Get("offset")
	limitStr := r.URL.Query().Get("limit")
	timeoutStr := r.URL.Query().Get("timeout")

	topicCfg, _ := s.broker.Topics().GetTopic(topic)
	partition := uint32(0)
	if partitionStr != "" {
		if v, err := strconv.ParseUint(partitionStr, 10, 32); err == nil {
			partition = uint32(v)
		} else {
			writeError(w, http.StatusBadRequest, "invalid partition parameter")
			return
		}
		if topicCfg != nil && partition >= topicCfg.Partitions {
			writeError(w, http.StatusBadRequest, "partition out of range")
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
			if d < 100*time.Millisecond {
				d = 100 * time.Millisecond
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

	// Record fetch for quota tracking
	if qe := s.broker.QuotaEnforcer(); qe != nil {
		if tm := s.broker.TenantManager(); tm != nil {
			if t := tm.GetTenant(topic); t != nil {
				var totalBytes int64
				for _, env := range msgs {
					totalBytes += int64(len(env.Payload))
				}
				qe.RecordFetch(t.ID, totalBytes)
			}
		}
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
	if s.broker.QueueEngine() == nil {
		writeError(w, http.StatusServiceUnavailable, "queue engine not available")
		return
	}

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
	if s.broker.QueueEngine() == nil {
		writeError(w, http.StatusServiceUnavailable, "queue engine not available")
		return
	}

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
		"nacked":     nacked,
		"dlq_routed": dlqed,
		"total":      len(req.Offsets),
	})
}

func (s *AdminServer) handleListConsumers(w http.ResponseWriter, r *http.Request) {
	if s.broker.StreamEngine() == nil {
		writeError(w, http.StatusServiceUnavailable, "stream engine not available")
		return
	}

	groups := s.broker.StreamEngine().ListGroups()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"groups": groups,
		"count":  len(groups),
	})
}

func (s *AdminServer) handleGetConsumer(w http.ResponseWriter, r *http.Request) {
	if s.broker.StreamEngine() == nil {
		writeError(w, http.StatusServiceUnavailable, "stream engine not available")
		return
	}

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
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// validateTopicName checks that a topic name is non-empty, has a reasonable length,
// and contains no path traversal characters.
func validateTopicName(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	if strings.Contains(name, "..") || strings.ContainsAny(name, "/\\") {
		return false
	}
	return true
}

// singleConnListener is a net.Listener that yields a single connection.
type singleConnListener struct {
	conn net.Conn
	done bool
}

func (ln *singleConnListener) Accept() (net.Conn, error) {
	if ln.done {
		return nil, net.ErrClosed
	}
	ln.done = true
	return ln.conn, nil
}

func (ln *singleConnListener) Close() error { return nil }

func (ln *singleConnListener) Addr() net.Addr { return ln.conn.LocalAddr() }

// --- Schema Registry Handlers ---

func (s *AdminServer) schemaRegistry() *schema.Registry {
	return s.broker.SchemaRegistry()
}

func (s *AdminServer) handleRegisterSchema(w http.ResponseWriter, r *http.Request) {
	reg := s.schemaRegistry()
	if reg == nil {
		http.Error(w, "schema registry not enabled", http.StatusServiceUnavailable)
		return
	}

	subject := r.PathValue("subject")
	if subject == "" {
		http.Error(w, "subject is required", http.StatusBadRequest)
		return
	}
	if len(subject) > 255 {
		http.Error(w, "subject too long", http.StatusBadRequest)
		return
	}

	var req struct {
		Type   string `json:"type"`
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	schemaType := schema.InferSchemaType(req.Schema)
	if req.Type != "" {
		schemaType = schema.ParseSchemaType(req.Type)
	}

	sv, err := reg.Register(subject, schemaType, req.Schema)
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	writeJSON(w, http.StatusOK, sv)
}

func (s *AdminServer) handleListSchemas(w http.ResponseWriter, r *http.Request) {
	reg := s.schemaRegistry()
	if reg == nil {
		http.Error(w, "schema registry not enabled", http.StatusServiceUnavailable)
		return
	}

	subject := r.PathValue("subject")
	versions, err := reg.List(subject)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, versions)
}

func (s *AdminServer) handleGetLatestSchema(w http.ResponseWriter, r *http.Request) {
	reg := s.schemaRegistry()
	if reg == nil {
		http.Error(w, "schema registry not enabled", http.StatusServiceUnavailable)
		return
	}

	subject := r.PathValue("subject")
	sv, err := reg.GetLatest(subject)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, sv)
}

func (s *AdminServer) handleGetSchemaVersion(w http.ResponseWriter, r *http.Request) {
	reg := s.schemaRegistry()
	if reg == nil {
		http.Error(w, "schema registry not enabled", http.StatusServiceUnavailable)
		return
	}

	subject := r.PathValue("subject")
	version, err := strconv.Atoi(r.PathValue("version"))
	if err != nil {
		http.Error(w, "invalid version", http.StatusBadRequest)
		return
	}

	sv, err := reg.Get(subject, version)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, sv)
}

func (s *AdminServer) handleDeleteSubject(w http.ResponseWriter, r *http.Request) {
	reg := s.schemaRegistry()
	if reg == nil {
		http.Error(w, "schema registry not enabled", http.StatusServiceUnavailable)
		return
	}

	subject := r.PathValue("subject")
	if err := reg.DeleteSubject(subject); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *AdminServer) handleSetCompatibility(w http.ResponseWriter, r *http.Request) {
	reg := s.schemaRegistry()
	if reg == nil {
		http.Error(w, "schema registry not enabled", http.StatusServiceUnavailable)
		return
	}

	subject := r.PathValue("subject")
	var req struct {
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	mode := schema.ParseCompatibilityMode(req.Mode)
	if err := reg.SetCompatibility(subject, mode); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
}

// --- WASM Module Handlers ---

func (s *AdminServer) handleUploadWASM(w http.ResponseWriter, r *http.Request) {
	rt := s.broker.WASMRuntime()
	if rt == nil {
		http.Error(w, "WASM runtime not enabled", http.StatusServiceUnavailable)
		return
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "name parameter is required", http.StatusBadRequest)
		return
	}

	maxSize := int64(16 * 1024 * 1024)
	body, err := io.ReadAll(io.LimitReader(r.Body, maxSize))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	if err := rt.Compile(name, body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"name":   name,
		"status": "compiled",
	})
}

func (s *AdminServer) handleListWASM(w http.ResponseWriter, r *http.Request) {
	rt := s.broker.WASMRuntime()
	if rt == nil {
		http.Error(w, "WASM runtime not enabled", http.StatusServiceUnavailable)
		return
	}

	modules := rt.ListModules()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"modules": modules,
		"count":   len(modules),
	})
}

func (s *AdminServer) handleDeleteWASM(w http.ResponseWriter, r *http.Request) {
	rt := s.broker.WASMRuntime()
	if rt == nil {
		http.Error(w, "WASM runtime not enabled", http.StatusServiceUnavailable)
		return
	}

	name := r.PathValue("name")
	if err := rt.Remove(name); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Stream Processor Handlers ---

func (s *AdminServer) handleCreateTopology(w http.ResponseWriter, r *http.Request) {
	p := s.broker.Processor()
	if p == nil {
		http.Error(w, "stream processing not enabled", http.StatusServiceUnavailable)
		return
	}

	var spec processing.TopologySpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if spec.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	if err := p.CreateTopology(spec); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	if spec.AutoStart {
		_ = p.StartTopology(spec.Name)
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"name":   spec.Name,
		"status": "created",
	})
}

func (s *AdminServer) handleListTopologies(w http.ResponseWriter, r *http.Request) {
	p := s.broker.Processor()
	if p == nil {
		http.Error(w, "stream processing not enabled", http.StatusServiceUnavailable)
		return
	}

	names := p.ListTopologies()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"topologies": names,
		"count":      len(names),
	})
}

func (s *AdminServer) handleGetTopology(w http.ResponseWriter, r *http.Request) {
	p := s.broker.Processor()
	if p == nil {
		http.Error(w, "stream processing not enabled", http.StatusServiceUnavailable)
		return
	}

	name := r.PathValue("name")
	topo, ok := p.GetTopology(name)
	if !ok {
		http.Error(w, "topology not found", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"name":         topo.Spec.Name,
		"state":        fmt.Sprintf("%d", topo.State),
		"parallelism":  topo.Spec.Parallelism,
		"source_topic": topo.Spec.Source.Topic,
		"sink_topic":   topo.Spec.Sink.Topic,
		"operators":    len(topo.Spec.Operators),
	})
}

func (s *AdminServer) handleDeleteTopology(w http.ResponseWriter, r *http.Request) {
	p := s.broker.Processor()
	if p == nil {
		http.Error(w, "stream processing not enabled", http.StatusServiceUnavailable)
		return
	}

	name := r.PathValue("name")
	if err := p.DeleteTopology(name); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *AdminServer) handleStartTopology(w http.ResponseWriter, r *http.Request) {
	p := s.broker.Processor()
	if p == nil {
		http.Error(w, "stream processing not enabled", http.StatusServiceUnavailable)
		return
	}

	name := r.PathValue("name")
	if err := p.StartTopology(name); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

func (s *AdminServer) handleStopTopology(w http.ResponseWriter, r *http.Request) {
	p := s.broker.Processor()
	if p == nil {
		http.Error(w, "stream processing not enabled", http.StatusServiceUnavailable)
		return
	}

	name := r.PathValue("name")
	if err := p.StopTopology(name); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopped"})
}

func (s *AdminServer) handleDLQPeek(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	limitStr := r.URL.Query().Get("limit")
	limit := 0
	if limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil {
			limit = n
		}
	}

	d := s.broker.DLQHandler()
	if d == nil {
		http.Error(w, "DLQ not enabled", http.StatusServiceUnavailable)
		return
	}

	entries := d.Peek(topic, limit)
	if entries == nil {
		entries = []*dlq.DLQEntry{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"topic":   topic,
		"count":   len(entries),
		"entries": entries,
	})
}

func (s *AdminServer) handleDLQClear(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	d := s.broker.DLQHandler()
	if d == nil {
		http.Error(w, "DLQ not enabled", http.StatusServiceUnavailable)
		return
	}

	d.Clear(topic)
	writeJSON(w, http.StatusOK, map[string]string{"status": "cleared"})
}

func (s *AdminServer) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	d := s.broker.DLQHandler()
	if d == nil {
		http.Error(w, "DLQ not enabled", http.StatusServiceUnavailable)
		return
	}

	// Parse replay options from request body
	var req struct {
		DryRun            bool                   `json:"dry_run"`
		MaxMessages       int                    `json:"max_messages"`
		TargetTopic       string                 `json:"target_topic"`
		DeleteAfterReplay bool                   `json:"delete_after_replay"`
		Condition         map[string]interface{} `json:"condition"`
		AddDLQMetadata    bool                   `json:"add_dlq_metadata"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
		// Use defaults if no body or invalid
		req.DryRun = false
		req.MaxMessages = 0
		req.DeleteAfterReplay = false
	}

	// Build replay options
	opts := dlq.DefaultReplayOptions()
	opts.DryRun = req.DryRun
	opts.MaxMessages = req.MaxMessages
	opts.TargetTopic = req.TargetTopic
	opts.DeleteAfterReplay = req.DeleteAfterReplay

	// Apply conditions from request
	if req.Condition != nil {
		opts.Condition = parseCondition(req.Condition)
	}

	// Build transform
	transforms := []dlq.ReplayTransform{dlq.NoTransform()}
	if req.AddDLQMetadata {
		transforms = append(transforms, dlq.AddDLQMetadata())
	}
	opts.Transform = dlq.ChainTransforms(transforms...)

	// Perform replay
	result, err := d.ReplayWithOptions(topic, opts, func(msg *message.Envelope, targetTopic string) error {
		if targetTopic == "" {
			targetTopic = topic
		}
		msg.Topic = targetTopic
		_, err := s.broker.Publish(msg)
		return err
	})

	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("replay failed: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"replayed": result.ReplayedCount,
		"failed":   result.FailedCount,
		"matched":  result.MatchedEntries,
		"total":    result.TotalEntries,
		"skipped":  result.SkippedCount,
		"dry_run":  req.DryRun,
		"topic":    topic,
		"errors":   len(result.Errors),
	})
}

// parseCondition parses a condition from request parameters.
func parseCondition(cond map[string]interface{}) dlq.ReplayCondition {
	conditions := []dlq.ReplayCondition{dlq.AllMessages()}

	if reason, ok := cond["reason"].(string); ok && reason != "" {
		conditions = append(conditions, dlq.ByReason(reason))
	}

	if minRetries, ok := cond["min_retries"].(float64); ok && minRetries > 0 {
		conditions = append(conditions, dlq.ByRetryCount(int(minRetries)))
	}

	if pattern, ok := cond["reason_pattern"].(string); ok && pattern != "" {
		conditions = append(conditions, dlq.ByReasonPattern(pattern))
	}

	if payload, ok := cond["payload_contains"].(string); ok && payload != "" {
		conditions = append(conditions, dlq.ByPayloadContains(payload))
	}

	if len(conditions) == 1 {
		return conditions[0]
	}
	return dlq.CompositeAND(conditions...)
}

// handleDLQPreview returns a preview of messages that would be replayed.
func (s *AdminServer) handleDLQPreview(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	d := s.broker.DLQHandler()
	if d == nil {
		http.Error(w, "DLQ not enabled", http.StatusServiceUnavailable)
		return
	}

	// Parse preview options from request body
	var req struct {
		MaxMessages int                    `json:"max_messages"`
		Condition   map[string]interface{} `json:"condition"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
		req.MaxMessages = 100 // default
	}

	opts := dlq.DefaultReplayOptions()
	opts.MaxMessages = req.MaxMessages
	if req.Condition != nil {
		opts.Condition = parseCondition(req.Condition)
	}

	entries, err := d.ReplayPreview(topic, opts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("preview failed: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"entries": entries,
		"count":   len(entries),
		"topic":   topic,
	})
}

// handleDLQExport exports DLQ entries as JSON.
func (s *AdminServer) handleDLQExport(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	d := s.broker.DLQHandler()
	if d == nil {
		http.Error(w, "DLQ not enabled", http.StatusServiceUnavailable)
		return
	}

	// Parse query parameters
	maxMessages := 0
	if m := r.URL.Query().Get("max_messages"); m != "" {
		_, _ = fmt.Sscanf(m, "%d", &maxMessages)
	}

	opts := dlq.DefaultReplayOptions()
	opts.MaxMessages = maxMessages

	data, err := d.ExportToJSON(topic, opts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("export failed: %v", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=dlq-%s.json", topic))
	_, _ = w.Write(data)
}

// handleConfigReload reloads configuration from file.
func (s *AdminServer) handleConfigReload(w http.ResponseWriter, r *http.Request) {
	configPath := s.broker.Config().Node.DataDir + "/../chimera.yaml"

	// Try to find config file
	if _, err := os.Stat(configPath); err != nil {
		// Try common locations
		locations := []string{
			"chimera.yaml",
			"/etc/chimera/chimera.yaml",
			"/var/lib/chimera/chimera.yaml",
		}
		for _, loc := range locations {
			if _, err := os.Stat(loc); err == nil {
				configPath = loc
				break
			}
		}
	}

	if err := s.broker.ReloadConfig(configPath); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("reload failed: %v", err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"message": "configuration reloaded",
	})
}

// --- Tenant Management Handlers ---

func (s *AdminServer) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	tm := s.broker.TenantManager()
	if tm == nil {
		http.Error(w, "multi-tenancy not enabled", http.StatusServiceUnavailable)
		return
	}

	var req struct {
		ID           string            `json:"id"`
		Name         string            `json:"name"`
		Description  string            `json:"description"`
		MaxStorage   int64             `json:"max_storage_bytes"`
		MaxTopics    int               `json:"max_topics"`
		MaxConn      int               `json:"max_connections"`
		MaxPubRate   int64             `json:"max_publish_rate"`
		MaxFetchRate int64             `json:"max_fetch_rate"`
		Labels       map[string]string `json:"labels,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.ID == "" {
		writeError(w, http.StatusBadRequest, "tenant ID is required")
		return
	}

	tenant := &tenant.Tenant{
		ID:          req.ID,
		Name:        req.Name,
		Description: req.Description,
		CreatedAt:   time.Now(),
		Enabled:     true,
		Quotas: tenant.Quotas{
			MaxStorageBytes: req.MaxStorage,
			MaxTopics:       req.MaxTopics,
			MaxConnections:  int64(req.MaxConn),
			MaxPublishRate:  req.MaxPubRate,
			MaxFetchRate:    req.MaxFetchRate,
		},
		Labels: req.Labels,
	}

	if err := tm.CreateTenant(tenant); err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]interface{}{
		"id":      tenant.ID,
		"name":    tenant.Name,
		"status":  "created",
		"enabled": tenant.Enabled,
	})
}

func (s *AdminServer) handleListTenants(w http.ResponseWriter, r *http.Request) {
	tm := s.broker.TenantManager()
	if tm == nil {
		http.Error(w, "multi-tenancy not enabled", http.StatusServiceUnavailable)
		return
	}

	tenants := tm.ListTenants()
	type tenantInfo struct {
		ID          string `json:"id"`
		Name        string `json:"name"`
		Enabled     bool   `json:"enabled"`
		CreatedAt   string `json:"created_at"`
		Description string `json:"description,omitempty"`
	}

	result := make([]tenantInfo, len(tenants))
	for i, t := range tenants {
		result[i] = tenantInfo{
			ID:          t.ID,
			Name:        t.Name,
			Enabled:     t.Enabled,
			CreatedAt:   t.CreatedAt.Format(time.RFC3339),
			Description: t.Description,
		}
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tenants": result,
		"count":   len(result),
	})
}

func (s *AdminServer) handleGetTenant(w http.ResponseWriter, r *http.Request) {
	tm := s.broker.TenantManager()
	if tm == nil {
		http.Error(w, "multi-tenancy not enabled", http.StatusServiceUnavailable)
		return
	}

	id := r.PathValue("id")
	t := tm.GetTenantByID(id)
	if t == nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":          t.ID,
		"name":        t.Name,
		"description": t.Description,
		"enabled":     t.Enabled,
		"created_at":  t.CreatedAt.Format(time.RFC3339),
		"quotas": map[string]interface{}{
			"max_storage_bytes": t.Quotas.MaxStorageBytes,
			"max_topics":        t.Quotas.MaxTopics,
			"max_connections":   t.Quotas.MaxConnections,
			"max_publish_rate":  t.Quotas.MaxPublishRate,
			"max_fetch_rate":    t.Quotas.MaxFetchRate,
		},
		"labels": t.Labels,
	})
}

func (s *AdminServer) handleDeleteTenant(w http.ResponseWriter, r *http.Request) {
	tm := s.broker.TenantManager()
	if tm == nil {
		http.Error(w, "multi-tenancy not enabled", http.StatusServiceUnavailable)
		return
	}

	id := r.PathValue("id")
	if err := tm.DeleteTenant(id); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// Also clean up quota tracking
	if qe := s.broker.QuotaEnforcer(); qe != nil {
		qe.DeleteUsage(id)
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"id":     id,
		"status": "deleted",
	})
}

func (s *AdminServer) handleGetTenantUsage(w http.ResponseWriter, r *http.Request) {
	qe := s.broker.QuotaEnforcer()
	if qe == nil {
		http.Error(w, "quota enforcer not enabled", http.StatusServiceUnavailable)
		return
	}

	id := r.PathValue("id")
	stats := qe.GetTenantUsageStats(id)

	writeJSON(w, http.StatusOK, stats)
}

func (s *AdminServer) handleGetTenantQuotas(w http.ResponseWriter, r *http.Request) {
	tm := s.broker.TenantManager()
	if tm == nil {
		http.Error(w, "multi-tenancy not enabled", http.StatusServiceUnavailable)
		return
	}

	id := r.PathValue("id")
	t := tm.GetTenantByID(id)
	if t == nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tenant_id": id,
		"quotas": map[string]interface{}{
			"max_storage_bytes": t.Quotas.MaxStorageBytes,
			"max_topics":        t.Quotas.MaxTopics,
			"max_connections":   t.Quotas.MaxConnections,
			"max_publish_rate":  t.Quotas.MaxPublishRate,
			"max_fetch_rate":    t.Quotas.MaxFetchRate,
		},
	})
}

func (s *AdminServer) handleUpdateTenantQuotas(w http.ResponseWriter, r *http.Request) {
	tm := s.broker.TenantManager()
	if tm == nil {
		http.Error(w, "multi-tenancy not enabled", http.StatusServiceUnavailable)
		return
	}

	id := r.PathValue("id")
	var req struct {
		MaxStorage   int64 `json:"max_storage_bytes"`
		MaxTopics    int   `json:"max_topics"`
		MaxConn      int   `json:"max_connections"`
		MaxPubRate   int64 `json:"max_publish_rate"`
		MaxFetchRate int64 `json:"max_fetch_rate"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	t := tm.GetTenantByID(id)
	if t == nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}

	// Update quotas
	t.Quotas.MaxStorageBytes = req.MaxStorage
	t.Quotas.MaxTopics = req.MaxTopics
	t.Quotas.MaxConnections = int64(req.MaxConn)
	t.Quotas.MaxPublishRate = req.MaxPubRate
	t.Quotas.MaxFetchRate = req.MaxFetchRate

	if err := tm.UpdateTenant(t); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"tenant_id": id,
		"status":    "quotas updated",
	})
}
