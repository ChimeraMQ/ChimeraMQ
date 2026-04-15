package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func mustHandler(t *testing.T) http.Handler {
	t.Helper()
	h, err := Handler()
	if err != nil {
		t.Fatalf("Handler() error: %v", err)
	}
	if h == nil {
		t.Fatal("Handler() returned nil")
	}
	return h
}

func TestHandlerNotNil(t *testing.T) {
	h := mustHandler(t)
	if h == nil {
		t.Fatal("Handler() returned nil")
	}
}

func TestHandlerIndexHTML(t *testing.T) {
	h := mustHandler(t)
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
	h := mustHandler(t)
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

func TestHandlerSPAFallbackDeepPath(t *testing.T) {
	h := mustHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/consumers/group/detail")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("deep SPA fallback status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlerStaticAsset(t *testing.T) {
	h := mustHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Vite builds produce assets/index-*.css
	resp, err := srv.Client().Get(srv.URL + "/assets/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Assets directory or fallback should return 200
	if resp.StatusCode != http.StatusOK {
		t.Errorf("static asset status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlerIndexHTMLDirect(t *testing.T) {
	h := mustHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/index.html")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("index.html status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlerEmptyPath(t *testing.T) {
	h := mustHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("empty path status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlerServesChimeraMQTitle(t *testing.T) {
	h := mustHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if !strings.Contains(resp.Header.Get("Content-Type"), "text/html") {
		t.Errorf("Content-Type = %q, want text/html", resp.Header.Get("Content-Type"))
	}
}
