# instgraph -- Agent Instructions

## Rules

- **Never commit to master.** Always use a feature branch + PR.
- **Go 1.21+**, pure Go (no CGO). Module: `github.com/walkerfunction/instgraph`
- Run `go test -race ./pkg/...` before committing.
- Git email: `walkerfunction@gmail.com`

## Key Architecture Decisions

- **Schema-driven**: JSON schema defines node types, edge types, and properties. Codegen produces typed Go interfaces and OTLP ingestion contracts.
- **Append-only schema**: node/edge types and properties can only be added, never removed. Enforced by `.schema.lock` diffing.
- **Flat-slice storage**: nodes and edges stored in flat slices indexed by uint32 IDs. Per-node adjacency lists (OutEdges/InEdges) for fast traversal.
- **Indexes**: label index, edge type index, property BTree index, unique key index -- all in-memory.
- **Upsert semantics**: `CreateNode` with an existing key merges properties instead of failing.
- **Pebble WAL + snapshots**: every mutation appends to WAL. Periodic snapshots dump full state. Recovery loads snapshot then replays WAL.
- **OpenCypher subset**: hand-written recursive descent parser. Supports MATCH, WHERE, RETURN, CREATE, DELETE, SET, WITH, variable-length paths, shortestPath, aggregations.
- **OTLP ingestion**: graph mutations encoded as OTLP log records with `graph.op`, `graph.label`, `graph.edge`, `graph.key`, `graph.from`, `graph.to` attributes.

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 4317 | gRPC | OTLP ingestion |
| 9090 | gRPC | Cypher query |
| 9091 | gRPC | Natural language query |
