# instgraph

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

**In-memory property graph engine for Go.** Define your graph schema in JSON, get typed Go interfaces, an OpenCypher query engine, OTLP ingestion, and Pebble-backed persistence -- all from a single schema file.

```
schema.json ──▶ instgraph generate ──▶ typed Go interfaces
                                       OTLP ingestion contract
                                       ready-to-query graph engine
```

## Features

- **Schema-driven codegen** -- define nodes, edges, and properties in JSON. Generate typed Go interfaces and OTLP ingestion schemas automatically.
- **Append-only schema** -- types and properties can only be added, never removed. Lock file enforces this across versions.
- **OpenCypher query engine** -- MATCH, WHERE, RETURN, CREATE, DELETE, SET, variable-length paths, shortestPath, aggregations (COUNT, COLLECT, SUM, AVG, MIN, MAX).
- **OTLP ingestion** -- ingest graph mutations as standard OpenTelemetry log records via gRPC.
- **Pebble persistence** -- WAL + periodic snapshots for crash recovery. Zero data loss.
- **Upsert semantics** -- CreateNode with an existing key merges properties, making idempotent ingestion simple.
- **Natural language queries** -- ask questions in English, get Cypher + results (powered by Claude API).
- **Pure Go** -- single static binary, no CGO.

## Quick Start

```bash
# Generate typed code from a schema
go run ./cmd/instgraph --schema examples/cricket/schema.json --out ./generated

# Use as a library
go get github.com/walkerfunction/instgraph
```

```go
package main

import (
    "fmt"
    "github.com/walkerfunction/instgraph/pkg/graph"
    "github.com/walkerfunction/instgraph/pkg/schema"
    "github.com/walkerfunction/instgraph/pkg/cypher"
)

func main() {
    // Load schema
    s, _ := schema.Load("schema.json")

    // Create graph
    g := graph.New(s)

    // Add data
    g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli", "country": "India"})
    g.CreateNode("Player", "bumrah", map[string]any{"name": "J Bumrah", "country": "India"})
    g.CreateNode("Team", "mi", map[string]any{"name": "Mumbai Indians"})

    kohli, _ := g.GetNodeByKey("kohli")
    mi, _ := g.GetNodeByKey("mi")
    g.CreateEdge("PLAYS_FOR", kohli.ID, mi.ID, nil)

    // Query with Cypher
    exec := cypher.NewExecutor(g)
    result, _ := exec.ExecuteAndReturn(`
        MATCH (p:Player)-[:PLAYS_FOR]->(t:Team)
        RETURN p.name, t.name
    `)

    for _, row := range result.Rows {
        fmt.Println(row)
    }
}
```

## Schema Format

```json
{
  "nodes": {
    "Player": {
      "name": { "type": "string", "required": true },
      "country": { "type": "string" }
    },
    "Team": {
      "name": { "type": "string", "required": true }
    }
  },
  "edges": {
    "PLAYS_FOR": { "from": "Player", "to": "Team" },
    "DISMISSED_BY": {
      "from": "Player", "to": "Player",
      "properties": {
        "type": { "type": "string" }
      }
    }
  }
}
```

The codegen CLI produces:
- **Go interfaces** -- typed getters for each node/edge type
- **OTLP schema (YAML)** -- human-readable ingestion contract
- **OTLP schema (JSON)** -- machine-validatable JSON Schema

## Cypher Support

```cypher
-- Basic pattern matching
MATCH (p:Player {country: "India"}) RETURN p.name

-- Relationship traversal
MATCH (k:Player {name: "V Kohli"})-[:DISMISSED_BY]->(b:Player) RETURN b.name

-- Variable-length paths
MATCH (a:Player)-[:DISMISSED_BY*1..3]->(b:Player) RETURN b.name

-- Aggregations
MATCH (p:Player)-[d:DISMISSED_BY]->(b:Player)
RETURN b.name, count(d) AS times ORDER BY times DESC LIMIT 5

-- Shortest path
MATCH (a:Player {name: "V Kohli"}), (b:Player {name: "J Bumrah"})
RETURN shortestPath((a)-[*]-(b))

-- Mutations
CREATE (n:Player {name: "New Player", country: "England"})
SET n.country = "Australia"
```

## Persistence

instgraph uses Pebble (CockroachDB's LSM engine) for durability:

- **WAL** -- every mutation appends to the write-ahead log
- **Snapshots** -- periodic full-state dumps
- **Recovery** -- load latest snapshot, replay WAL entries after it

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 4317 | gRPC | OTLP ingestion |
| 9090 | gRPC | Cypher query |
| 9091 | gRPC | Natural language query |

## License

Apache 2.0 -- see [LICENSE](LICENSE).
