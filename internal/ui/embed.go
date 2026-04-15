package ui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed _embed/dist
var frontendFS embed.FS

// Handler returns an http.Handler that serves the embedded React Web UI.
func Handler() (http.Handler, error) {
	root, err := fs.Sub(frontendFS, "_embed/dist")
	if err != nil {
		return nil, err
	}

	fileServer := http.FileServer(http.FS(root))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")

		// Serve static assets directly (assets/* from Vite build)
		if path != "" && path != "index.html" {
			if f, err := root.Open(path); err == nil {
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
