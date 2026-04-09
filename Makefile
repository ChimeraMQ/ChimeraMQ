BINARY   := chimera
VERSION  := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT   := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE     := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS  := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

.PHONY: build test bench clean lint docker release integration cover

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/chimera/

test:
	go test -count=1 -timeout 120s ./...

test-race:
	go test -race -count=1 -timeout 120s ./...

integration:
	go test ./test/integration/ -v -count=1 -timeout 120s

bench:
	go test -bench=. -benchmem -benchtime 1s -run "^$" ./...

bench-e2e:
	go test ./test/bench/ -bench=. -benchmem -benchtime 2s -run "^$" -v

lint:
	go vet ./...

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

clean:
	rm -rf bin/ coverage.out

docker:
	docker build -t ghcr.io/chimeramq/chimera:$(VERSION) -t ghcr.io/chimeramq/chimera:latest .

release: clean
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-linux-amd64 ./cmd/chimera/
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-linux-arm64 ./cmd/chimera/
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-darwin-amd64 ./cmd/chimera/
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-darwin-arm64 ./cmd/chimera/
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-windows-amd64.exe ./cmd/chimera/
	GOOS=windows GOARCH=arm64 CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY)-windows-arm64.exe ./cmd/chimera/
