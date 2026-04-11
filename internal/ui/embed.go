package ui

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed static/*
var staticFS embed.FS

// Handler returns an http.Handler that serves the embedded Web UI SPA.
// It serves static files from static/ and falls back to index.html for
// unknown paths (client-side routing).
func Handler() (http.Handler, error) {
	fsys, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("ui: %w", err)
	}
	fileServer := http.FileServer(http.FS(fsys))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")

		// Serve static assets directly
		if path != "" && path != "index.html" {
			// Check if the file exists in the embedded FS
			if f, err := fsys.Open(path); err == nil {
				f.Close()
				fileServer.ServeHTTP(w, r)
				return
			}
		}

		// Fallback to index.html for SPA routing
		r.URL.Path = "/"
		fileServer.ServeHTTP(w, r)
	}), nil
}
