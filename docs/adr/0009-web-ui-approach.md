# ADR 0009: Web UI Approach — Single HTML with Embedded Assets

## Status: Accepted

## Context

ChimeraMQ needs a web dashboard for monitoring, topic management, and cluster
operations. The UI must work in air-gap environments (no internet access) and
be distributable as part of the single binary.

## Approach

Single `index.html` file served from `internal/ui/static/` with local assets:
- `tailwind.min.js` — Tailwind CSS (compiled standalone)
- `chart.min.js` — Chart.js for metrics visualization
- `style.css` — Custom overrides
- `embed.go` — Go embed directive to bundle all assets into the binary

## Alternatives Considered

### 1. React SPA (proposed for future)
- **Pros:** Rich component ecosystem, state management, routing
- **Cons:** Requires build step (Node.js, npm, webpack), larger bundle size,
  CDN dependencies for quick prototyping
- **Effort:** 80h for full implementation

### 2. Go template server-side rendering
- **Pros:** No JavaScript framework, smaller payload, works without JS
- **Cons:** Less interactive, harder to implement real-time updates,
  limited visualization capabilities

### 3. External CDN assets
- **Pros:** Smallest binary size, automatic updates
- **Cons:** Requires internet access, breaks air-gap deployments,
  external dependency for production UI

## Decision

Single HTML file with bundled local assets, embedded via `//go:embed`.

## Why

- Air-gap support: no external network calls required
- Single binary: all UI assets compiled into `chimera.exe`
- Zero build step: no Node.js, npm, or bundler required
- Immediate deployment: `chimera server` starts serving UI on admin port
- Chart.js provides real-time metrics visualization
- Tailwind provides clean styling without custom CSS

## Architecture

```
internal/ui/
├── embed.go          //go:embed static/*
└── static/
    ├── index.html    # Dashboard (login, topics, metrics, cluster)
    ├── tailwind.min.js
    ├── chart.min.js
    └── style.css
```

The UI is served at `/ui/` on the admin server (port 9090 by default).
Authentication is enforced via the existing auth middleware.

## Consequences

- Binary size increases by ~600KB for embedded assets
- UI updates require recompilation (acceptable for admin tooling)
- Limited interactivity compared to a full SPA framework
- No client-side routing — single page with sections
- Future migration to React SPA is planned (Phase 2, 80h effort)

## Migration Path

Migrate to React SPA with Vite build, still embedded via `//go:embed`,
but with pre-built static assets instead of a single HTML file.
