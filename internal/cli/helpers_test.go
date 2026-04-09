package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestGetAdminAddr(t *testing.T) {
	// Default
	os.Unsetenv("CHIMERA_ADMIN_ADDR")
	if addr := getAdminAddr(); addr != "http://localhost:9090" {
		t.Errorf("default addr = %q, want http://localhost:9090", addr)
	}

	// From env
	os.Setenv("CHIMERA_ADMIN_ADDR", "http://myhost:8080")
	defer os.Unsetenv("CHIMERA_ADMIN_ADDR")
	if addr := getAdminAddr(); addr != "http://myhost:8080" {
		t.Errorf("env addr = %q, want http://myhost:8080", addr)
	}
}

func TestHTTPGet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("method = %q, want GET", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()

	resp, err := httpGet(server.URL + "/test")
	if err != nil {
		t.Fatalf("httpGet: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestHTTPPost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("content-type = %q, want application/json", ct)
		}
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"id":1}`)
	}))
	defer server.Close()

	body, _ := json.Marshal(map[string]string{"name": "test"})
	resp, err := httpPost(server.URL+"/test", body)
	if err != nil {
		t.Fatalf("httpPost: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Errorf("status = %d, want 201", resp.StatusCode)
	}
}

func TestHTTPDelete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("method = %q, want DELETE", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"deleted"}`)
	}))
	defer server.Close()

	resp, err := httpDelete(server.URL + "/test")
	if err != nil {
		t.Fatalf("httpDelete: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestPrintResponse(t *testing.T) {
	// printResponse writes to stdout — just ensure it doesn't panic
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"name":"test","value":42}`)
	}))
	defer server.Close()

	resp, err := httpGet(server.URL + "/")
	if err != nil {
		t.Fatalf("httpGet: %v", err)
	}
	defer resp.Body.Close()

	// Should not panic
	printResponse(resp)
}
