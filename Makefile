.PHONY: build test lint bench generate clean

BINARY_NAME=instgraph
SERVER_BINARY=instgraph-server

# Build
build:
	go build -o bin/$(BINARY_NAME) ./cmd/instgraph
	go build -o bin/$(SERVER_BINARY) ./cmd/instgraph-server

# Run all tests
test:
	go test -race -v ./pkg/...

# Run benchmarks
bench:
	go test -bench=. -benchmem ./pkg/graph/...

# Lint (requires golangci-lint)
lint:
	golangci-lint run ./...

# Generate code from the cricket example schema
generate:
	go run ./cmd/instgraph --schema examples/cricket/schema.json --out examples/cricket/generated

# Clean
clean:
	rm -rf bin/ generated/
