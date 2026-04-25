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
	"path/filepath"
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
	"github.com/chimeramq/chimera/internal/storage/hot"
	"github.com/chimeramq/chimera/internal/tenant"
	"github.com/chimeramq/chimera/internal/ui"
)

// version is injected at build time via ldflags.
var version = "dev"

const (
	maxFetchTimeout  = 30 * time.Second
	maxJSONBodySize  = 10 << 20 // 10 MB default max JSON body size
	maxListLimit     = 1000     // maximum items returned per list request
	defaultListLimit = 100      // default items returned per list request
)

// parsePagination extracts limit/offset from query params with sane defaults.
func parsePagination(r *http.Request) (limit, offset int) {
	limit = defaultListLimit
	offset = 0
	if l := r.URL.Query().Get("limit"); l != "" {
		if v, err := strconv.Atoi(l); err == nil && v > 0 {
			limit = v
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if v, err := strconv.Atoi(o); err == nil && v >= 0 {
			offset = v
		}
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	return limit, offset
}

// paginate returns a slice clamped to [offset, offset+limit].
func paginate[T any](items []T, limit, offset int) []T {
	if offset >= len(items) {
		return []T{}
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end]
}

// decodeJSON reads and decodes JSON from r.Body, limited to maxBytes.
func decodeJSON(r *http.Request, v any, maxBytes int64) error {
	if maxBytes <= 0 {
		maxBytes = maxJSONBodySize
	}
	return json.NewDecoder(io.LimitReader(r.Body, maxBytes)).Decode(v)
}

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
	s.mux.HandleFunc("POST /v1/messages/{topic}/batch", s.auth(s.handleBatchPublish))
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
	s.mux.HandleFunc("GET /v1/health/detailed", s.auth(s.handleHealthDetailed))
	s.mux.HandleFunc("GET /v1/metrics", s.auth(s.handleMetrics))
	s.mux.HandleFunc("GET /v1/cluster/members", s.auth(s.handleClusterMembers))
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

	// Geo-replication endpoints
	s.mux.HandleFunc("GET /v1/geo-replication/status", s.auth(s.handleGeoStatus))
	s.mux.HandleFunc("GET /v1/geo-replication/lag", s.auth(s.handleGeoLag))

	// Admin drain endpoint
	s.mux.HandleFunc("POST /v1/admin/drain", s.auth(s.handleDrain))

	// pprof profiling endpoints (when enabled)
	if s.broker.Config().Observability.PProf.Enabled {
		pprofCfg := s.broker.Config().Observability.PProf
		if s.broker.Config().Auth.Enabled {
			// Auth enabled: pprof safe to enable
			s.registerPProfRoutes()
		} else if pprofCfg.AllowProduction {
			// Auth disabled but explicit opt-in: log critical warning and enable
			fmt.Fprintf(os.Stderr, "CRITICAL: pprof endpoints enabled without auth (allow_production=true)\n")
			s.registerPProfRoutes()
		} else {
			// Auth disabled, no explicit opt-in: keep pprof disabled
			fmt.Fprintf(os.Stderr, "WARNING: pprof endpoints disabled — auth is off and allow_production is not set\n")
		}
	}

	// Embedded Web UI dashboard
	if s.broker.Config().Observability.Dashboard.Enabled {
		if h, err := ui.Handler(); err == nil {
			dashboardHandler := http.StripPrefix("/ui", h)
			if s.broker.Config().Auth.Enabled {
				s.mux.Handle("/ui/", s.auth(func(w http.ResponseWriter, r *http.Request) {
					dashboardHandler.ServeHTTP(w, r)
				}))
			} else {
				s.mux.Handle("/ui/", dashboardHandler)
			}
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
		http.Error(w, sanitizeError(http.StatusInternalServerError, err), http.StatusInternalServerError)
		return
	}
	defer trace.Stop()

	// Wait for the specified duration or default 1 second (capped at 5s)
	duration := 1 * time.Second
	if sec, _ := strconv.Atoi(r.URL.Query().Get("seconds")); sec > 0 {
		if sec > 5 {
			sec = 5
		}
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
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")

		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
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

		// Rate limit check
		var clientIP string
		if lim := s.broker.AuthLimiter(); lim != nil {
			clientIP = extractRealIP(r, cfg.Listener.TrustedProxyCIDR)
			if !lim.IsAllowed(clientIP) {
				writeError(w, http.StatusTooManyRequests, "authentication rate limited")
				return
			}
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
			if lim := s.broker.AuthLimiter(); lim != nil {
				lim.RecordFailed(clientIP)
			}
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		if lim := s.broker.AuthLimiter(); lim != nil {
			lim.RecordSuccess(clientIP)
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
			} else if strings.Contains(r.URL.Path, "/consumers/") {
				rt = auth.ResourceConsumerGroup
				name = r.PathValue("group")
				if name == "" {
					name = "*"
				}
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

// identityFromContext extracts the authenticated identity from a request context.
func identityFromContext(r *http.Request) *auth.Identity {
	if ident, ok := r.Context().Value(identityKey{}).(*auth.Identity); ok {
		return ident
	}
	return nil
}

// hasClusterAdminRole returns true if identity has a cluster-admin role.
func hasClusterAdminRole(ident *auth.Identity) bool {
	for _, role := range ident.Roles {
		if role == "admin" || role == "cluster-admin" {
			return true
		}
	}
	return false
}

// extractRealIP extracts the real client IP by checking X-Forwarded-For.
// If TrustedProxyCIDR is set and the request comes from a trusted proxy,
// the leftmost untrusted IP from X-Forwarded-For is returned.
// Otherwise, r.RemoteAddr is returned.
func extractRealIP(r *http.Request, trustedCIDR string) string {
	if trustedCIDR != "" {
		proxy := net.ParseIP(strings.TrimSpace(strings.Split(r.RemoteAddr, ":")[0]))
		_, ipNet, err := net.ParseCIDR(trustedCIDR)
		if err == nil && ipNet.Contains(proxy) {
			if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
				// Take leftmost IP (original client) and reject if multiple IPs present
				// (indicates spoofing attempt via comma-separated values)
				ips := strings.Split(fwd, ",")
				if len(ips) == 1 {
					trimmed := strings.TrimSpace(ips[0])
					if clientIP := net.ParseIP(trimmed); clientIP != nil {
						return trimmed
					}
				}
			}
		}
	}
	// Fallback: TCP remote address
	addr := r.RemoteAddr
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

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
	if err := decodeJSON(r, &req, 0); err != nil {
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
		writeErrorf(w, http.StatusConflict, err)
		return
	}

	// Wire per-tenant rate limit to flow controller
	s.broker.WireTopicRateLimit(cfg.Name)

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
	limit, offset := parsePagination(r)
	topics = paginate(topics, limit, offset)

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
		writeErrorf(w, http.StatusNotFound, err)
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

func (s *AdminServer) handleBatchPublish(w http.ResponseWriter, r *http.Request) {
	topicName := r.PathValue("topic")

	maxBatchSize := s.broker.Config().Limits.MaxBatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = 1000
	}

	var messages []struct {
		Payload     []byte            `json:"payload"`
		RoutingKey  string            `json:"routing_key,omitempty"`
		Headers     map[string][]byte `json:"headers,omitempty"`
		ContentType string            `json:"content_type,omitempty"`
		Priority    uint8             `json:"priority,omitempty"`
		TTL         string            `json:"ttl,omitempty"`
		DeliverAt   string            `json:"deliver_at,omitempty"`
	}
	if err := decodeJSON(r, &messages, 0); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(messages) == 0 {
		writeError(w, http.StatusBadRequest, "empty message batch")
		return
	}
	if len(messages) > maxBatchSize {
		writeError(w, http.StatusBadRequest, "batch size exceeds maximum")
		return
	}

	results := make([]map[string]interface{}, len(messages))
	var okCount, failCount int

	for i, msg := range messages {
		targetTopic := topicName
		env := &message.Envelope{
			Topic:       targetTopic,
			RoutingKey:  msg.RoutingKey,
			Payload:     msg.Payload,
			ContentType: msg.ContentType,
			Headers:     msg.Headers,
			Priority:    msg.Priority,
			SourceProto: message.ProtoHTTP,
		}

		offset, err := s.broker.Publish(env)
		if err != nil {
			s.broker.Logger().Error("batch publish failed", "index", i, "error", err)
			results[i] = map[string]interface{}{
				"index": i,
				"ok":    false,
				"error": "internal error",
			}
			failCount++
		} else {
			results[i] = map[string]interface{}{
				"index":     i,
				"ok":        true,
				"offset":    offset,
				"partition": env.PartitionID,
				"topic":     env.Topic,
			}
			okCount++
		}
	}

	statusCode := http.StatusOK
	if failCount > 0 && okCount == 0 {
		statusCode = http.StatusInternalServerError
	} else if failCount > 0 {
		statusCode = http.StatusMultiStatus
	}

	writeJSON(w, statusCode, map[string]interface{}{
		"total":   len(messages),
		"ok":      okCount,
		"failed":  failCount,
		"results": results,
	})
}

func (s *AdminServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := "healthy"
	if s.broker.IsDrainMode() {
		status = "draining"
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"status":     status,
		"node_id":    s.broker.Config().Node.ID,
		"name":       s.broker.Config().Node.Name,
		"version":    version,
		"uptime":     time.Since(s.broker.StartTime()).String(),
		"drain_mode": s.broker.IsDrainMode(),
	})
}

func (s *AdminServer) handleHealthDetailed(w http.ResponseWriter, r *http.Request) {
	status := "healthy"
	if s.broker.IsDrainMode() {
		status = "draining"
	}

	resp := map[string]interface{}{
		"status":     status,
		"node_id":    s.broker.Config().Node.ID,
		"name":       s.broker.Config().Node.Name,
		"version":    version,
		"uptime":     time.Since(s.broker.StartTime()).String(),
		"drain_mode": s.broker.IsDrainMode(),
		"fips":       s.broker.IsFIPSEnabled(),
	}

	// Raft state (if clustered)
	if s.broker.IsClustered() {
		mgr := s.broker.Cluster()
		if mgr != nil {
			if rn := mgr.RaftNode(); rn != nil {
				resp["raft"] = map[string]interface{}{
					"state":        rn.State().String(),
					"term":         int64(rn.Term()),
					"commit_index": int64(rn.CommitIndex()),
					"is_leader":    rn.IsLeader(),
					"leader_id":    string(rn.LeaderID()),
				}
			}
			if fsm := mgr.FSM(); fsm != nil {
				topics := fsm.ListTopics()
				resp["topics"] = len(topics)
			}
			resp["cluster"] = map[string]interface{}{
				"alive_members": mgr.AliveCount(),
				"members":       len(mgr.Members()),
			}
		}
	} else {
		resp["mode"] = "single-node"
	}

	// Storage health
	if storage := s.broker.Storage(); storage != nil {
		var totalSize int64
		storage.ForEachPartition(func(_ string, _ uint32, p *hot.Partition) bool {
			totalSize += p.TotalSize()
			return true
		})
		resp["storage"] = map[string]interface{}{
			"hot_size_bytes": totalSize,
			"partitions":     0, // will be filled by ForEachPartition count
		}
		_ = totalSize // used above
	}

	// Warm storage
	if warm := s.broker.WarmEngine(); warm != nil {
		resp["warm"] = map[string]interface{}{
			"enabled": true,
			"size":    warm.TotalSize(),
		}
	}

	// DLQ stats
	if dlq := s.broker.DLQHandler(); dlq != nil {
		resp["dlq_topics"] = len(dlq.Topics())
	}

	writeJSON(w, http.StatusOK, resp)
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
	if err := decodeJSON(r, &req, 0); err != nil {
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
	if err := decodeJSON(r, &req, 0); err != nil {
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
	limit, offset := parsePagination(r)
	groups = paginate(groups, limit, offset)
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

	// Tenant isolation: only the owner or a cluster admin can inspect consumer groups
	if ident := identityFromContext(r); ident != nil && ident.TenantID != "" && !hasClusterAdminRole(ident) {
		writeError(w, http.StatusForbidden, "tenant cannot access other tenant's consumer groups")
		return
	}

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

// writeErrorf writes an error, sanitizing internal errors for clients.
func writeErrorf(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": sanitizeError(status, err)})
}

// sanitizeError returns a safe error message for clients.
// Internal errors are replaced with a generic message; the original
// is logged server-side. Client-originated errors (bad request, not
// found, conflict) are kept as-is since they describe the user input.
func sanitizeError(status int, err error) string {
	if status >= 500 {
		return "internal server error"
	}
	msg := err.Error()
	if status == http.StatusNotFound || status == http.StatusConflict || status == http.StatusBadRequest {
		// Keep the message but strip anything that looks like a file path
		if idx := strings.LastIndex(msg, "/"); idx >= 0 && strings.Contains(msg, ": ") {
			msg = msg[strings.LastIndex(msg, ": ")+2:]
		}
		return msg
	}
	return "request failed"
}

// validateTopicName checks that a topic name is non-empty, has a reasonable length,
// and contains no path traversal characters.
func validateTopicName(name string) bool {
	if name == "" || len(name) > 255 {
		return false
	}
	if strings.Contains(name, "..") || strings.ContainsAny(name, "/\\\r\n") {
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
	if err := decodeJSON(r, &req, 0); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	schemaType := schema.InferSchemaType(req.Schema)
	if req.Type != "" {
		schemaType = schema.ParseSchemaType(req.Type)
	}

	sv, err := reg.Register(subject, schemaType, req.Schema)
	if err != nil {
		http.Error(w, sanitizeError(http.StatusConflict, err), http.StatusConflict)
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
		http.Error(w, sanitizeError(http.StatusNotFound, err), http.StatusNotFound)
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
		http.Error(w, sanitizeError(http.StatusNotFound, err), http.StatusNotFound)
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
		http.Error(w, sanitizeError(http.StatusNotFound, err), http.StatusNotFound)
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
	if err := decodeJSON(r, &req, 0); err != nil {
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
	// Reject path traversal and special characters in module name
	if strings.ContainsAny(name, "/\\") || strings.Contains(name, "..") {
		http.Error(w, "invalid module name", http.StatusBadRequest)
		return
	}

	maxSize := int64(16 * 1024 * 1024)
	body, err := io.ReadAll(io.LimitReader(r.Body, maxSize))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	// Validate WASM magic bytes (\0asm) before compilation
	if len(body) < 4 || body[0] != 0x00 || body[1] != 0x61 || body[2] != 0x73 || body[3] != 0x6d {
		http.Error(w, "invalid WASM module: missing magic bytes", http.StatusBadRequest)
		return
	}

	if err := rt.Compile(name, body); err != nil {
		http.Error(w, sanitizeError(http.StatusBadRequest, err), http.StatusBadRequest)
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
	limit, offset := parsePagination(r)
	modules = paginate(modules, limit, offset)
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
		http.Error(w, sanitizeError(http.StatusNotFound, err), http.StatusNotFound)
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
	if err := decodeJSON(r, &spec, 0); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if spec.Name == "" || len(spec.Name) > 255 {
		http.Error(w, "name is required and must be under 256 characters", http.StatusBadRequest)
		return
	}

	// Validate topic references exist
	if s.broker.Topics() != nil {
		if spec.Source.Topic != "" {
			if _, ok := s.broker.Topics().GetTopic(spec.Source.Topic); !ok {
				http.Error(w, "source topic does not exist", http.StatusBadRequest)
				return
			}
		}
		if spec.Sink.Topic != "" {
			if _, ok := s.broker.Topics().GetTopic(spec.Sink.Topic); !ok {
				http.Error(w, "sink topic does not exist", http.StatusBadRequest)
				return
			}
		}
	}

	if err := p.CreateTopology(spec); err != nil {
		http.Error(w, sanitizeError(http.StatusConflict, err), http.StatusConflict)
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
	limit, offset := parsePagination(r)
	names = paginate(names, limit, offset)
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
		http.Error(w, sanitizeError(http.StatusConflict, err), http.StatusConflict)
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
		http.Error(w, sanitizeError(http.StatusNotFound, err), http.StatusNotFound)
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
		http.Error(w, sanitizeError(http.StatusNotFound, err), http.StatusNotFound)
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

	if err := decodeJSON(r, &req, 0); err != nil && err.Error() != "EOF" {
		// Use defaults if no body or invalid
		req.DryRun = false
		req.MaxMessages = 0
		req.DeleteAfterReplay = false
	}

	// Validate target_topic exists if specified
	if req.TargetTopic != "" && s.broker.Topics() != nil {
		if _, ok := s.broker.Topics().GetTopic(req.TargetTopic); !ok {
			writeError(w, http.StatusBadRequest, "target_topic does not exist")
			return
		}
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

	if err := decodeJSON(r, &req, 0); err != nil && err.Error() != "EOF" {
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
	configPath := filepath.Join(s.broker.Config().Node.DataDir, "chimera.yaml")

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

// handleGeoStatus returns geo-replication status.
func (s *AdminServer) handleGeoStatus(w http.ResponseWriter, r *http.Request) {
	gm := s.broker.GeoManager()
	if gm == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"enabled": false,
		})
		return
	}

	replicated, failed := gm.Stats()

	// Get receiver stats if available
	var received, rejected int64
	if gr := s.broker.GeoReceiver(); gr != nil {
		received, rejected = gr.Stats()
	}

	resp := map[string]interface{}{
		"enabled":    true,
		"local_dc":   s.broker.Config().GeoReplication.LocalDC,
		"mode":       s.broker.Config().GeoReplication.ReplicationMode,
		"remote_dcs": len(s.broker.Config().GeoReplication.RemoteDCs),
		"sender": map[string]interface{}{
			"events_sent":   replicated,
			"events_failed": failed,
		},
		"receiver": map[string]interface{}{
			"events_received": received,
			"events_rejected": rejected,
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleGeoLag returns per-topic/partition replication lag.
func (s *AdminServer) handleGeoLag(w http.ResponseWriter, r *http.Request) {
	gm := s.broker.GeoManager()
	if gm == nil {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"enabled": false,
			"lag":     map[string]interface{}{},
		})
		return
	}

	lagInfos := gm.LagInfos()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"enabled": true,
		"lag":     lagInfos,
	})
}

// handleDrain sets or clears graceful drain mode.
func (s *AdminServer) handleDrain(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Drain bool `json:"drain"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	s.broker.SetDrainMode(req.Drain)
	if req.Drain {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"message": "drain mode enabled",
		})
	} else {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"message": "drain mode disabled",
		})
	}
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

	if err := decodeJSON(r, &req, 0); err != nil {
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
		writeErrorf(w, http.StatusConflict, err)
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
	limit, offset := parsePagination(r)
	tenants = paginate(tenants, limit, offset)
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
		writeErrorf(w, http.StatusNotFound, err)
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

	if err := decodeJSON(r, &req, 0); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	t := tm.GetTenantByID(id)
	if t == nil {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}

	// Tenant isolation: only the tenant itself or a cluster-scoped admin can modify quotas
	ident := identityFromContext(r)
	if ident != nil && ident.TenantID != id && !hasClusterAdminRole(ident) {
		writeError(w, http.StatusForbidden, "cannot modify another tenant's quotas")
		return
	}

	// Update quotas
	t.Quotas.MaxStorageBytes = req.MaxStorage
	t.Quotas.MaxTopics = req.MaxTopics
	t.Quotas.MaxConnections = int64(req.MaxConn)
	t.Quotas.MaxPublishRate = req.MaxPubRate
	t.Quotas.MaxFetchRate = req.MaxFetchRate

	if err := tm.UpdateTenant(t); err != nil {
		writeErrorf(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"tenant_id": id,
		"status":    "quotas updated",
	})
}
