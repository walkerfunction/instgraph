package cypher

import (
	"fmt"
	"testing"
	"time"

	"github.com/walkerfunction/instgraph/pkg/graph"
	"github.com/walkerfunction/instgraph/pkg/schema"
)

// buildScaleGraph creates a graph similar to NACL: ~1K players, ~100 matches,
// ~50K PLAYED_IN edges, ~5K PLAYS_FOR edges.
func buildScaleGraph(b *testing.B) *graph.Graph {
	b.Helper()
	s, err := schema.Parse([]byte(`{
		"nodes": {
			"Player": { "name": { "type": "string", "required": true }, "country": { "type": "string" } },
			"Team": { "name": { "type": "string", "required": true } },
			"Match": { "date": { "type": "string" }, "venue": { "type": "string" } }
		},
		"edges": {
			"PLAYED_IN": { "from": "Player", "to": "Match", "properties": { "runs": { "type": "int" }, "wickets": { "type": "int" } } },
			"PLAYS_FOR": { "from": "Player", "to": "Team" },
			"BETWEEN": { "from": "Match", "to": "Team" }
		}
	}`))
	if err != nil {
		b.Fatal(err)
	}

	g := graph.New(s)

	// Create 10 teams
	for i := 0; i < 10; i++ {
		g.CreateNode("Team", fmt.Sprintf("team-%d", i), map[string]any{"name": fmt.Sprintf("Team %d", i)})
	}

	// Create 1000 players, each plays for 1-3 teams
	for i := 0; i < 1000; i++ {
		g.CreateNode("Player", fmt.Sprintf("player-%d", i), map[string]any{
			"name":    fmt.Sprintf("Player %d", i),
			"country": "USA",
		})
		pNode, _ := g.GetNodeByKey(fmt.Sprintf("player-%d", i))
		// Each player plays for 1-3 teams
		for t := 0; t < (i%3)+1; t++ {
			tNode, _ := g.GetNodeByKey(fmt.Sprintf("team-%d", (i+t)%10))
			g.CreateEdge("PLAYS_FOR", pNode.ID, tNode.ID, nil)
		}
	}

	// Create 500 matches
	for i := 0; i < 500; i++ {
		g.CreateNode("Match", fmt.Sprintf("match-%d", i), map[string]any{
			"date":  fmt.Sprintf("2025-%02d-%02d", (i%12)+1, (i%28)+1),
			"venue": fmt.Sprintf("Ground %d", i%20),
		})
		mNode, _ := g.GetNodeByKey(fmt.Sprintf("match-%d", i))
		// 2 teams per match
		t1, _ := g.GetNodeByKey(fmt.Sprintf("team-%d", i%10))
		t2, _ := g.GetNodeByKey(fmt.Sprintf("team-%d", (i+1)%10))
		g.CreateEdge("BETWEEN", mNode.ID, t1.ID, nil)
		g.CreateEdge("BETWEEN", mNode.ID, t2.ID, nil)
	}

	// ~50K PLAYED_IN edges: each of 1000 players played in ~50 matches
	for i := 0; i < 1000; i++ {
		pNode, _ := g.GetNodeByKey(fmt.Sprintf("player-%d", i))
		for m := 0; m < 50; m++ {
			matchIdx := (i*7 + m*13) % 500
			mNode, _ := g.GetNodeByKey(fmt.Sprintf("match-%d", matchIdx))
			g.CreateEdge("PLAYED_IN", pNode.ID, mNode.ID, map[string]any{
				"runs":    (i + m) % 120,
				"wickets": (i + m) % 5,
			})
		}
	}

	return g
}

func TestScaleTraversal(t *testing.T) {
	// Use testing.B-like setup but in a regular test so we can see timing.
	s, _ := schema.Parse([]byte(`{
		"nodes": {
			"Player": { "name": { "type": "string", "required": true }, "country": { "type": "string" } },
			"Team": { "name": { "type": "string", "required": true } },
			"Match": { "date": { "type": "string" }, "venue": { "type": "string" } }
		},
		"edges": {
			"PLAYED_IN": { "from": "Player", "to": "Match", "properties": { "runs": { "type": "int" }, "wickets": { "type": "int" } } },
			"PLAYS_FOR": { "from": "Player", "to": "Team" },
			"BETWEEN": { "from": "Match", "to": "Team" }
		}
	}`))

	g := graph.New(s)

	// Build graph inline (same as benchmark)
	for i := 0; i < 10; i++ {
		g.CreateNode("Team", fmt.Sprintf("team-%d", i), map[string]any{"name": fmt.Sprintf("Team %d", i)})
	}
	for i := 0; i < 1000; i++ {
		g.CreateNode("Player", fmt.Sprintf("player-%d", i), map[string]any{
			"name": fmt.Sprintf("Player %d", i),
		})
		pNode, _ := g.GetNodeByKey(fmt.Sprintf("player-%d", i))
		tNode, _ := g.GetNodeByKey(fmt.Sprintf("team-%d", i%10))
		g.CreateEdge("PLAYS_FOR", pNode.ID, tNode.ID, nil)
	}
	for i := 0; i < 500; i++ {
		g.CreateNode("Match", fmt.Sprintf("match-%d", i), map[string]any{
			"date": fmt.Sprintf("2025-%02d-%02d", (i%12)+1, (i%28)+1),
		})
	}
	for i := 0; i < 1000; i++ {
		pNode, _ := g.GetNodeByKey(fmt.Sprintf("player-%d", i))
		for m := 0; m < 50; m++ {
			matchIdx := (i*7 + m*13) % 500
			mNode, _ := g.GetNodeByKey(fmt.Sprintf("match-%d", matchIdx))
			g.CreateEdge("PLAYED_IN", pNode.ID, mNode.ID, map[string]any{
				"runs":    (i + m) % 120,
				"wickets": (i + m) % 5,
			})
		}
	}

	t.Logf("Graph: %d nodes, %d edges", g.NodeCount(), g.EdgeCount())

	exec := NewExecutor(g)

	// Test 1: Single player traversal with edge variable (the slow query pattern)
	start := time.Now()
	q1 := `MATCH (p:Player)-[e:PLAYED_IN]->(m:Match) WHERE p.name = 'Player 42' RETURN p.name, e.runs, m.date`
	r1, err := exec.Execute(q1)
	d1 := time.Since(start)
	if err != nil {
		t.Fatalf("query 1 failed: %v", err)
	}
	t.Logf("Single player traversal: %d rows in %v", len(r1.Rows), d1)
	if d1 > 500*time.Millisecond {
		t.Errorf("single player traversal too slow: %v (want < 500ms)", d1)
	}

	// Test 2: Aggregation across all players
	start = time.Now()
	q2 := `MATCH (p:Player)-[e:PLAYED_IN]->(m:Match) RETURN p.name, count(m) AS matches, sum(e.runs) AS total_runs`
	r2, err := exec.Execute(q2)
	d2 := time.Since(start)
	if err != nil {
		t.Fatalf("query 2 failed: %v", err)
	}
	t.Logf("All-player aggregation: %d rows in %v", len(r2.Rows), d2)
	if d2 > 5*time.Second {
		t.Errorf("all-player aggregation too slow: %v (want < 5s)", d2)
	}

	// Test 3: Node-only lookup (should be very fast)
	start = time.Now()
	q3 := `MATCH (p:Player) WHERE p.name = 'Player 42' RETURN p.name`
	r3, err := exec.Execute(q3)
	d3 := time.Since(start)
	if err != nil {
		t.Fatalf("query 3 failed: %v", err)
	}
	t.Logf("Node lookup: %d rows in %v", len(r3.Rows), d3)
	if d3 > 50*time.Millisecond {
		t.Errorf("node lookup too slow: %v (want < 50ms)", d3)
	}
}

func BenchmarkSinglePlayerTraversal(b *testing.B) {
	g := buildScaleGraph(b)
	exec := NewExecutor(g)
	q := `MATCH (p:Player)-[e:PLAYED_IN]->(m:Match) WHERE p.name = 'Player 42' RETURN p.name, e.runs, m.date`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.Execute(q)
	}
}

func BenchmarkAllPlayerAggregation(b *testing.B) {
	g := buildScaleGraph(b)
	exec := NewExecutor(g)
	q := `MATCH (p:Player)-[e:PLAYED_IN]->(m:Match) RETURN p.name, count(m) AS matches, sum(e.runs) AS total_runs`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		exec.Execute(q)
	}
}
