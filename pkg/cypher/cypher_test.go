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

// TestParsePropertyNameIsKeyword covers the case where a property name happens
// to collide with a Cypher keyword (e.g. `.match`, `.type`, `.count`). The lexer
// upcases identifiers and matches against the keywords table, so without special
// handling these tokens come through as keyword tokens and the parser rejects
// them in property-access, map-literal, AS-alias, SET target, and UNWIND AS spots.
// LLM-generated queries routinely use schema property names like `match` and
// `type`, so all of these positions must accept keyword tokens as names.
func TestParsePropertyNameIsKeyword(t *testing.T) {
	cases := []string{
		// property access after '.'
		`MATCH (d)-[r:DISMISSED_BY]->(b) WHERE r.match = 'x' RETURN r.type, r.match`,
		// map literal key collides with keyword
		`MATCH (n:Player {match: 'x'}) RETURN n.name`,
		// AS alias collides with keyword
		`MATCH (n:Player) RETURN n.name AS match`,
		// SET target property name collides with keyword
		`MATCH (n:Player) SET n.match = 'x'`,
		// property access appearing in ORDER BY after an AS alias
		`MATCH (n:Player)-[r:DISMISSED_BY]->(b) RETURN r.type ORDER BY r.match DESC LIMIT 5`,
	}
	for _, q := range cases {
		if _, err := Parse(q); err != nil {
			t.Errorf("Parse(%q) failed: %v", q, err)
		}
	}
}

// TestParseClaudeGeneratedQueries uses the exact Cypher strings that
// instcric-app's analyst logged as failing with
// "parse error: expected property name after ." — regression guard.
func TestParseClaudeGeneratedQueries(t *testing.T) {
	queries := []string{
		`MATCH (m:Match)-[:BETWEEN]->(t1:Team {name: 'Singh Warriors'}) ` +
			`MATCH (m)-[:BETWEEN]->(t2:Team {name: 'Gladiators'}) ` +
			`MATCH (a:Player)-[d:DISMISSED_BY]->(b:Player) WHERE d.match = m.date ` +
			`RETURN a.name AS batter, b.name AS bowler, d.type AS dismissal_type, ` +
			`COUNT(*) AS times ORDER BY times DESC LIMIT 15`,
		`MATCH (bowler:Player)-[:PLAYS_FOR]->(t1:Team {name: 'Singh Warriors'}) ` +
			`MATCH (batter:Player)-[:PLAYS_FOR]->(t2:Team {name: 'Gladiators'}) ` +
			`MATCH (batter)-[d:DISMISSED_BY]->(bowler) ` +
			`RETURN bowler.name AS bowler, batter.name AS batter, ` +
			`d.type AS dismissal_type, d.match AS match`,
	}
	for _, q := range queries {
		if _, err := Parse(q); err != nil {
			t.Errorf("Parse failed: %v\nquery: %s", err, q)
		}
	}
}
