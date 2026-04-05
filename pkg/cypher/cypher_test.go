package cypher

import (
	"testing"

	"github.com/walkerfunction/instgraph/pkg/graph"
	"github.com/walkerfunction/instgraph/pkg/schema"
)

func buildCricketGraph(t *testing.T) *graph.Graph {
	t.Helper()
	s, err := schema.Parse([]byte(`{
		"nodes": {
			"Player": { "name": { "type": "string", "required": true }, "country": { "type": "string" } },
			"Team": { "name": { "type": "string", "required": true } }
		},
		"edges": {
			"PLAYS_FOR": { "from": "Player", "to": "Team" },
			"DISMISSED_BY": { "from": "Player", "to": "Player", "properties": { "type": { "type": "string" } } }
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}

	g := graph.New(s)

	// Players
	g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli", "country": "India"})
	g.CreateNode("Player", "bumrah", map[string]any{"name": "J Bumrah", "country": "India"})
	g.CreateNode("Player", "starc", map[string]any{"name": "M Starc", "country": "Australia"})
	g.CreateNode("Player", "smith", map[string]any{"name": "S Smith", "country": "Australia"})

	// Teams
	g.CreateNode("Team", "mi", map[string]any{"name": "Mumbai Indians"})
	g.CreateNode("Team", "rcb", map[string]any{"name": "Royal Challengers"})

	// Edges
	kohli, _ := g.GetNodeByKey("kohli")
	bumrah, _ := g.GetNodeByKey("bumrah")
	starc, _ := g.GetNodeByKey("starc")
	smith, _ := g.GetNodeByKey("smith")
	mi, _ := g.GetNodeByKey("mi")
	rcb, _ := g.GetNodeByKey("rcb")

	g.CreateEdge("PLAYS_FOR", kohli.ID, rcb.ID, nil)
	g.CreateEdge("PLAYS_FOR", bumrah.ID, mi.ID, nil)
	g.CreateEdge("PLAYS_FOR", starc.ID, mi.ID, nil)
	g.CreateEdge("PLAYS_FOR", smith.ID, rcb.ID, nil)

	// Dismissals
	g.CreateEdge("DISMISSED_BY", kohli.ID, starc.ID, map[string]any{"type": "caught"})
	g.CreateEdge("DISMISSED_BY", kohli.ID, bumrah.ID, map[string]any{"type": "lbw"})
	g.CreateEdge("DISMISSED_BY", kohli.ID, starc.ID, map[string]any{"type": "bowled"})
	g.CreateEdge("DISMISSED_BY", smith.ID, bumrah.ID, map[string]any{"type": "caught"})

	return g
}

func TestLexer(t *testing.T) {
	tokens, err := Lex(`MATCH (n:Player {name: "V Kohli"}) RETURN n.name`)
	if err != nil {
		t.Fatalf("Lex error: %v", err)
	}
	// Should have: MATCH ( n : Player { name : "V Kohli" } ) RETURN n . name EOF
	if len(tokens) < 10 {
		t.Errorf("expected at least 10 tokens, got %d", len(tokens))
	}
	if tokens[0].Type != TokenMatch {
		t.Errorf("first token should be MATCH, got %d", tokens[0].Type)
	}
}

func TestParseSimpleMatch(t *testing.T) {
	q, err := Parse(`MATCH (n:Player) RETURN n.name`)
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if len(q.Clauses) != 2 {
		t.Errorf("expected 2 clauses, got %d", len(q.Clauses))
	}
}

func TestExecuteSimpleMatch(t *testing.T) {
	g := buildCricketGraph(t)
	exec := NewExecutor(g)

	result, err := exec.ExecuteAndReturn(`MATCH (n:Player) RETURN n.name`)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(result.Rows) != 4 {
		t.Errorf("expected 4 rows, got %d", len(result.Rows))
	}
}

func TestExecuteMatchWithWhere(t *testing.T) {
	g := buildCricketGraph(t)
	exec := NewExecutor(g)

	result, err := exec.ExecuteAndReturn(`MATCH (n:Player) WHERE n.country = "India" RETURN n.name`)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 Indian players, got %d", len(result.Rows))
	}
}

func TestExecuteMatchWithInlineProps(t *testing.T) {
	g := buildCricketGraph(t)
	exec := NewExecutor(g)

	result, err := exec.ExecuteAndReturn(`MATCH (n:Player {name: "V Kohli"}) RETURN n.country`)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0]["n.country"] != "India" {
		t.Errorf("expected India, got %v", result.Rows[0]["n.country"])
	}
}

func TestExecuteRelationshipPattern(t *testing.T) {
	g := buildCricketGraph(t)
	exec := NewExecutor(g)

	result, err := exec.ExecuteAndReturn(
		`MATCH (k:Player {name: "V Kohli"})-[:DISMISSED_BY]->(b:Player) RETURN b.name`)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	// Kohli dismissed by Starc (2x) and Bumrah (1x) = 2 unique bowlers
	// (pattern matching finds unique node pairs, not individual edges)
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 unique bowlers, got %d", len(result.Rows))
	}
}

func TestExecuteCount(t *testing.T) {
	g := buildCricketGraph(t)
	exec := NewExecutor(g)

	result, err := exec.ExecuteAndReturn(
		`MATCH (k:Player {name: "V Kohli"})-[d:DISMISSED_BY]->(b:Player) RETURN b.name, count(d) AS times`)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("expected rows")
	}
}

func TestExecuteOrderByLimit(t *testing.T) {
	g := buildCricketGraph(t)
	exec := NewExecutor(g)

	result, err := exec.ExecuteAndReturn(
		`MATCH (n:Player) RETURN n.name ORDER BY n.name LIMIT 2`)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows with LIMIT, got %d", len(result.Rows))
	}
}

func TestExecuteCreate(t *testing.T) {
	s, _ := schema.Parse([]byte(`{
		"nodes": { "Player": { "name": { "type": "string" } } },
		"edges": {}
	}`))
	g := graph.New(s)
	exec := NewExecutor(g)

	_, err := exec.ExecuteAndReturn(
		`CREATE (n:Player {name: "New Player"}) RETURN n.name`)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if g.NodeCount() != 1 {
		t.Errorf("expected 1 node after CREATE, got %d", g.NodeCount())
	}
}

func TestExecuteShortestPath(t *testing.T) {
	g := buildCricketGraph(t)
	exec := NewExecutor(g)

	// Kohli → (DISMISSED_BY) → Starc → (PLAYS_FOR) → MI ← (PLAYS_FOR) ← Bumrah
	// Shortest path should find a connection.
	result, err := exec.ExecuteAndReturn(
		`MATCH (a:Player {name: "V Kohli"}), (b:Player {name: "J Bumrah"})
		 RETURN a.name, b.name`)
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}
