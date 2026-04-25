package geo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/chimeramq/chimera/internal/auth"
)

func TestReceiverStats(t *testing.T) {
	broker := newTestBroker()
	receiver := &Receiver{
		broker:  broker,
		localDC: "us-east-1",
		logger:  broker.logger,
	}

	receiver.eventsReceived.Store(100)
	receiver.eventsRejected.Store(5)

	received, rejected := receiver.Stats()
	if received != 100 {
		t.Errorf("received = %d, want 100", received)
	}
	if rejected != 5 {
		t.Errorf("rejected = %d, want 5", rejected)
	}
}

func TestRegisterGeoRoutes(t *testing.T) {
	broker := newTestBroker()
	mux := http.NewServeMux()
	RegisterGeoRoutes(mux, broker, broker.logger, "dc-local")

	// Test health endpoint via mux
	req := httptest.NewRequest("GET", "/v1/geo-health", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestAuthenticateRequestNoToken(t *testing.T) {
	broker := newTestBroker()
	receiver := &Receiver{
		broker:  broker,
		localDC: "us-east-1",
		logger:  broker.logger,
	}

	req := httptest.NewRequest("POST", "/v1/geo-replicate", nil)
	rec := httptest.NewRecorder()

	ok := receiver.authenticateRequest(rec, req)
	if ok {
		t.Error("expected auth to fail without token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestAuthenticateRequestBadToken(t *testing.T) {
	broker := newTestBroker()
	receiver := &Receiver{
		broker:  broker,
		localDC: "us-east-1",
		logger:  broker.logger,
	}

	req := httptest.NewRequest("POST", "/v1/geo-replicate", nil)
	req.Header.Set("Authorization", "Bearer bad-token")
	rec := httptest.NewRecorder()

	ok := receiver.authenticateRequest(rec, req)
	if ok {
		t.Error("expected auth to fail with bad token")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rec.Code)
	}
}

func TestAuthenticateRequestNoProvider(t *testing.T) {
	broker := &testBroker{logger: &testLogger{}, authProv: nil}
	receiver := &Receiver{
		broker:  broker,
		localDC: "us-east-1",
		logger:  broker.logger,
	}

	req := httptest.NewRequest("POST", "/v1/geo-replicate", nil)
	req.Header.Set("Authorization", "Bearer some-token")
	rec := httptest.NewRecorder()

	ok := receiver.authenticateRequest(rec, req)
	if ok {
		t.Error("expected auth to fail with no provider")
	}
}

type rejectAuth struct{}

func (r *rejectAuth) Authenticate(ctx context.Context, creds auth.Credentials) (*auth.Identity, error) {
	return nil, auth.ErrInvalidCredentials
}

func (r *rejectAuth) Close() error { return nil }

func TestAuthenticateRequestValidToken(t *testing.T) {
	broker := newTestBroker()
	receiver := &Receiver{
		broker:  broker,
		localDC: "us-east-1",
		logger:  broker.logger,
	}

	req := httptest.NewRequest("POST", "/v1/geo-replicate", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()

	ok := receiver.authenticateRequest(rec, req)
	if !ok {
		t.Error("expected auth to pass with valid token")
	}
}

func TestExpandEnv(t *testing.T) {
	os.Setenv("TEST_CHIMERA_VAR", "hello")
	defer os.Unsetenv("TEST_CHIMERA_VAR")

	result := expandEnv("${TEST_CHIMERA_VAR}")
	if result != "hello" {
		t.Errorf("expandEnv = %q, want hello", result)
	}

	result = expandEnv("${NONEXISTENT_VAR_XYZ}")
	if result != "${NONEXISTENT_VAR_XYZ}" {
		t.Errorf("expandEnv = %q, want ${NONEXISTENT_VAR_XYZ}", result)
	}

	result = expandEnv("plain-text")
	if result != "plain-text" {
		t.Errorf("expandEnv = %q, want plain-text", result)
	}
}
