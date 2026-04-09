FROM golang:1.23-alpine AS builder
RUN apk add --no-cache git make
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make build

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /src/bin/chimera /usr/local/bin/chimera
RUN mkdir -p /var/lib/chimera /etc/chimera
EXPOSE 5672 9090
VOLUME /var/lib/chimera
ENTRYPOINT ["chimera"]
CMD ["server", "--config", "/etc/chimera/chimera.yaml"]
