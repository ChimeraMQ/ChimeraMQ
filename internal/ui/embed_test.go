package ui

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandlerNotNil(t *testing.T) {
	h := Handler()
	if h == nil {
		t.Fatal("Handler() returned nil")
	}
}

func TestHandlerIndexHTML(t *testing.T) {
	h := Handler()
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET / status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlerSPAFallback(t *testing.T) {
	h := Handler()
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/topics/some-route")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("SPA fallback status = %d, want 200", resp.StatusCode)
	}
}
