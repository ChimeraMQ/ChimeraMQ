package ui

import (
	"net/http"
	"net/http/httptest"
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

	// Try to get a CSS file — may or may not exist in the embedded FS
	resp, err := srv.Client().Get(srv.URL + "/assets/style.css")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Either 200 (if exists) or falls back to index.html (200)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("static asset status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlerIndexHTMLDirect(t *testing.T) {
	h := mustHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Direct request for index.html should serve it
	resp, err := srv.Client().Get(srv.URL + "/index.html")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// index.html hits the fallback path
	if resp.StatusCode != http.StatusOK {
		t.Errorf("index.html status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlerEmptyPath(t *testing.T) {
	h := mustHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Request with empty path (just /) should go directly to fallback
	resp, err := srv.Client().Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("empty path status = %d, want 200", resp.StatusCode)
	}
}

func TestHandlerExistingFile(t *testing.T) {
	h := mustHandler(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Request for style.css — exercises the fsys.Open success path
	resp, err := srv.Client().Get(srv.URL + "/style.css")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("existing file status = %d, want 200", resp.StatusCode)
	}
}
