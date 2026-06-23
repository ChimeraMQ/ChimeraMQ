# Build stage
FROM golang:1.26-alpine AS builder
RUN apk add --no-cache git make
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" -o /chimera ./cmd/chimera

# Runtime stage
FROM alpine:3.21
RUN apk add --no-cache ca-certificates tini
RUN addgroup -S chimera && adduser -S chimera -G chimera
COPY --from=builder /chimera /usr/local/bin/chimera
RUN mkdir -p /var/lib/chimera /etc/chimera && \
    chown -R chimera:chimera /var/lib/chimera /etc/chimera
USER chimera
EXPOSE 5672 9090
VOLUME /var/lib/chimera
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:9090/v1/health || exit 1
ENTRYPOINT ["tini", "--", "chimera"]
CMD ["server", "--config", "/etc/chimera/chimera.yaml"]
