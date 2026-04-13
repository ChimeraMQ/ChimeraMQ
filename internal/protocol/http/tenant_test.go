package http

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/chimeramq/chimera/internal/broker"
)

func setupTenantTestServer(t *testing.T) (*AdminServer, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "chimera-http-tenant-*")
	if err != nil {
		t.Fatal(err)
	}

	cfg := &broker.Config{
		Node: broker.NodeConfig{ID: 1, Name: "tenant-node", DataDir: dir},
		Listener: broker.ListenerConfig{
			Bind: "127.0.0.1", Port: 0, AdminPort: 0, MaxConnections: 100,
		},
		Storage: broker.StorageConfig{
			Hot: broker.HotConfig{SegmentSize: 1024 * 1024, SyncMode: "immediate"},
			WAL: broker.WALConfig{MaxSize: 4 * 1024 * 1024, SyncMode: "immediate"},
		},
		Defaults: broker.DefaultsConfig{
			Topic: broker.TopicDefaults{Partitions: 4, RetentionTime: "1h", Mode: "unified"},
		},
		Logging: broker.LoggingConfig{Level: "warn", Format: "text"},
		Tenant: broker.TenantConfigRoot{
			Enabled:   true,
			Separator: ":",
		},
	}

	b, err := broker.NewBroker(cfg)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("NewBroker: %v", err)
	}
	if err := b.Start(); err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Start: %v", err)
	}

	srv := NewAdminServer(b)
	cleanup := func() {
		b.Stop()
		os.RemoveAll(dir)
	}
	return srv, cleanup
}

func TestHandleCreateTenant(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"id":                "tenant-1",
		"name":              "Tenant One",
		"description":       "First tenant",
		"max_storage_bytes": 1024 * 1024,
		"max_topics":        10,
		"max_connections":   100,
		"max_publish_rate":  1000,
		"max_fetch_rate":    1000,
		"labels": map[string]string{
			"env": "test",
		},
	})
	resp := doRequest(t, srv, "POST", "/v1/tenants", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["id"] != "tenant-1" {
		t.Errorf("id = %v, want tenant-1", result["id"])
	}
}

func TestHandleCreateTenantInvalidJSON(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "POST", "/v1/tenants", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleCreateTenantMissingID(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"name": "No ID Tenant",
	})
	resp := doRequest(t, srv, "POST", "/v1/tenants", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleListTenants(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"id":   "list-tenant",
		"name": "Listable",
	})
	doRequest(t, srv, "POST", "/v1/tenants", body)

	resp := doRequest(t, srv, "GET", "/v1/tenants", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["count"] == nil {
		t.Error("expected count in response")
	}
}

func TestHandleGetTenant(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"id":   "get-tenant",
		"name": "Gettable",
	})
	doRequest(t, srv, "POST", "/v1/tenants", body)

	resp := doRequest(t, srv, "GET", "/v1/tenants/get-tenant", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if result["id"] != "get-tenant" {
		t.Errorf("id = %v, want get-tenant", result["id"])
	}
}

func TestHandleGetTenantNotFound(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "GET", "/v1/tenants/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleDeleteTenant(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"id":   "del-tenant",
		"name": "Deletable",
	})
	doRequest(t, srv, "POST", "/v1/tenants", body)

	resp := doRequest(t, srv, "DELETE", "/v1/tenants/del-tenant", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	resp2 := doRequest(t, srv, "GET", "/v1/tenants/del-tenant", nil)
	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("after delete status = %d, want 404", resp2.StatusCode)
	}
}

func TestHandleDeleteTenantNotFound(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "DELETE", "/v1/tenants/nonexistent", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandleGetTenantUsage(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"id":   "usage-tenant",
		"name": "Usage Tenant",
	})
	doRequest(t, srv, "POST", "/v1/tenants", body)

	resp := doRequest(t, srv, "GET", "/v1/tenants/usage-tenant/usage", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleGetTenantQuotas(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"id":   "quota-tenant",
		"name": "Quota Tenant",
	})
	doRequest(t, srv, "POST", "/v1/tenants", body)

	resp := doRequest(t, srv, "GET", "/v1/tenants/quota-tenant/quotas", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleUpdateTenantQuotas(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"id":   "update-quota-tenant",
		"name": "Update Quota Tenant",
	})
	doRequest(t, srv, "POST", "/v1/tenants", body)

	updateBody, _ := json.Marshal(map[string]interface{}{
		"max_storage_bytes": 2048 * 1024,
		"max_topics":        20,
		"max_connections":   200,
		"max_publish_rate":  2000,
		"max_fetch_rate":    2000,
	})
	resp := doRequest(t, srv, "PUT", "/v1/tenants/update-quota-tenant/quotas", updateBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHandleUpdateTenantQuotasInvalidJSON(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	resp := doRequest(t, srv, "PUT", "/v1/tenants/some/quotas", []byte("not json"))
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandleUpdateTenantQuotasNotFound(t *testing.T) {
	srv, cleanup := setupTenantTestServer(t)
	defer cleanup()

	body, _ := json.Marshal(map[string]interface{}{
		"max_topics": 5,
	})
	resp := doRequest(t, srv, "PUT", "/v1/tenants/nonexistent/quotas", body)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestTenantEndpointsWithoutTenantManager(t *testing.T) {
	srv, _, cleanup := setupTestServer(t)
	defer cleanup()

	endpoints := []struct {
		method string
		path   string
		body   []byte
	}{
		{"POST", "/v1/tenants", []byte(`{"id":"t"}`)},
		{"GET", "/v1/tenants", nil},
		{"GET", "/v1/tenants/t", nil},
		{"DELETE", "/v1/tenants/t", nil},
		{"GET", "/v1/tenants/t/usage", nil},
		{"GET", "/v1/tenants/t/quotas", nil},
		{"PUT", "/v1/tenants/t/quotas", []byte(`{"max_topics":1}`)},
	}

	for _, ep := range endpoints {
		resp := doRequest(t, srv, ep.method, ep.path, ep.body)
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("%s %s: status = %d, want 503", ep.method, ep.path, resp.StatusCode)
		}
	}
}
