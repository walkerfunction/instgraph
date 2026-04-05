package persist

import (
	"path/filepath"
	"testing"

	"github.com/walkerfunction/instgraph/pkg/graph"
	"github.com/walkerfunction/instgraph/pkg/schema"
)

func testSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.Parse([]byte(`{
		"nodes": {
			"Player": { "name": { "type": "string", "required": true }, "country": { "type": "string" } },
			"Team": { "name": { "type": "string", "required": true } }
		},
		"edges": {
			"PLAYS_FOR": { "from": "Player", "to": "Team" }
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestWALAppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	s := testSchema(t)

	// Create persister and log some operations.
	p, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	p.LogCreateNode("Player", "kohli", map[string]any{"name": "V Kohli", "country": "India"})
	p.LogCreateNode("Player", "bumrah", map[string]any{"name": "J Bumrah", "country": "India"})
	p.LogCreateNode("Team", "mi", map[string]any{"name": "Mumbai Indians"})
	p.LogCreateEdge("PLAYS_FOR", "bumrah", "mi", nil)

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and recover.
	p2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer p2.Close()

	g, err := p2.Recover(s)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if g.NodeCount() != 3 {
		t.Errorf("expected 3 nodes after recovery, got %d", g.NodeCount())
	}
	if g.EdgeCount() != 1 {
		t.Errorf("expected 1 edge after recovery, got %d", g.EdgeCount())
	}

	kohli, ok := g.GetNodeByKey("kohli")
	if !ok {
		t.Fatal("kohli not found after recovery")
	}
	if kohli.Properties["name"] != "V Kohli" {
		t.Errorf("expected 'V Kohli', got %v", kohli.Properties["name"])
	}
}

func TestSnapshotAndRecover(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	s := testSchema(t)

	// Build graph and snapshot.
	p, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	g := graph.New(s)
	g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli", "country": "India"})
	g.CreateNode("Team", "rcb", map[string]any{"name": "Royal Challengers"})
	kohli, _ := g.GetNodeByKey("kohli")
	rcb, _ := g.GetNodeByKey("rcb")
	g.CreateEdge("PLAYS_FOR", kohli.ID, rcb.ID, nil)

	// Also log to WAL (simulating normal operation).
	p.LogCreateNode("Player", "kohli", map[string]any{"name": "V Kohli", "country": "India"})
	p.LogCreateNode("Team", "rcb", map[string]any{"name": "Royal Challengers"})
	p.LogCreateEdge("PLAYS_FOR", "kohli", "rcb", nil)

	// Take snapshot.
	if err := p.TakeSnapshot(g); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	// Add more data after snapshot (WAL only).
	g.CreateNode("Player", "bumrah", map[string]any{"name": "J Bumrah", "country": "India"})
	p.LogCreateNode("Player", "bumrah", map[string]any{"name": "J Bumrah", "country": "India"})

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Recover — should have snapshot (2 nodes, 1 edge) + WAL replay (1 more node).
	p2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer p2.Close()

	g2, err := p2.Recover(s)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if g2.NodeCount() != 3 {
		t.Errorf("expected 3 nodes after snapshot+WAL recovery, got %d", g2.NodeCount())
	}
	if g2.EdgeCount() != 1 {
		t.Errorf("expected 1 edge after recovery, got %d", g2.EdgeCount())
	}

	bumrah, ok := g2.GetNodeByKey("bumrah")
	if !ok {
		t.Fatal("bumrah not found after recovery (should be from WAL replay)")
	}
	if bumrah.Properties["name"] != "J Bumrah" {
		t.Errorf("expected 'J Bumrah', got %v", bumrah.Properties["name"])
	}
}
