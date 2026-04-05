# Contributing to instgraph

Thanks for your interest in contributing to instgraph! This guide covers everything you need to get started.

## Prerequisites

- **Go 1.21+**

## Running Tests

```bash
# Unit tests
make test

# Benchmarks
make bench

# Linting (requires golangci-lint)
make lint
```

## Code Style

- Standard Go conventions. Run `gofmt` on all code.
- Lint checks run via `golangci-lint`.

## Pull Request Process

1. **Fork** the repository and create a feature branch from `master`.
2. **Write tests** for any new functionality.
3. **Run `make test`** and ensure all tests pass.
4. **Open a PR** against `master` with a clear description of the change.
5. A maintainer will review and merge once CI is green.

## Project Structure

```
cmd/instgraph/         -- CLI: codegen tool
cmd/instgraph-server/  -- server: OTLP ingest + Cypher + NL query
pkg/
  schema/              -- JSON schema parser + append-only lock
  codegen/             -- Go interface + OTLP schema generation
  graph/               -- core in-memory property graph engine
  cypher/              -- OpenCypher parser + executor
  persist/             -- Pebble WAL + snapshots
  ingest/              -- OTLP log receiver
  nlquery/             -- natural language to Cypher (Claude API)
examples/cricket/      -- cricket analytics showcase
```

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
