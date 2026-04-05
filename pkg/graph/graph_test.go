package graph

import (
	"testing"

	"github.com/walkerfunction/instgraph/pkg/schema"
)

func cricketSchema(t *testing.T) *schema.Schema {
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
		t.Fatalf("schema parse: %v", err)
	}
	return s
}

func TestCreateAndGetNode(t *testing.T) {
	g := New(cricketSchema(t))

	id, err := g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli", "country": "India"})
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	node := g.GetNode(id)
	if node == nil {
		t.Fatal("GetNode returned nil")
	}
	if node.Key != "kohli" {
		t.Errorf("expected key 'kohli', got %q", node.Key)
	}
	if node.Properties["name"] != "V Kohli" {
		t.Errorf("expected name 'V Kohli', got %v", node.Properties["name"])
	}

	// Get by key.
	n2, ok := g.GetNodeByKey("kohli")
	if !ok {
		t.Fatal("GetNodeByKey returned false")
	}
	if n2.ID != id {
		t.Errorf("expected ID %d, got %d", id, n2.ID)
	}
}

func TestUpsertNode(t *testing.T) {
	g := New(cricketSchema(t))

	id1, _ := g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli"})
	id2, _ := g.CreateNode("Player", "kohli", map[string]any{"country": "India"})

	if id1 != id2 {
		t.Errorf("upsert should return same ID: %d vs %d", id1, id2)
	}

	node := g.GetNode(id1)
	if node.Properties["name"] != "V Kohli" {
		t.Error("upsert should keep existing properties")
	}
	if node.Properties["country"] != "India" {
		t.Error("upsert should merge new properties")
	}

	if g.NodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", g.NodeCount())
	}
}

func TestCreateEdge(t *testing.T) {
	g := New(cricketSchema(t))

	kohli, _ := g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli"})
	mi, _ := g.CreateNode("Team", "mi", map[string]any{"name": "Mumbai Indians"})

	eid, err := g.CreateEdge("PLAYS_FOR", kohli, mi, nil)
	if err != nil {
		t.Fatalf("CreateEdge: %v", err)
	}

	edge := g.GetEdge(eid)
	if edge == nil {
		t.Fatal("GetEdge returned nil")
	}
	if edge.From != kohli || edge.To != mi {
		t.Errorf("wrong from/to: %d->%d", edge.From, edge.To)
	}
}

func TestEdgeSchemaValidation(t *testing.T) {
	g := New(cricketSchema(t))

	kohli, _ := g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli"})
	mi, _ := g.CreateNode("Team", "mi", map[string]any{"name": "MI"})

	// Should fail: PLAYS_FOR is Player→Team, not Team→Player.
	_, err := g.CreateEdge("PLAYS_FOR", mi, kohli, nil)
	if err == nil {
		t.Fatal("expected error for wrong edge direction")
	}
}

func TestNeighbors(t *testing.T) {
	g := New(cricketSchema(t))

	kohli, _ := g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli"})
	bumrah, _ := g.CreateNode("Player", "bumrah", map[string]any{"name": "J Bumrah"})
	mi, _ := g.CreateNode("Team", "mi", map[string]any{"name": "MI"})

	g.CreateEdge("PLAYS_FOR", kohli, mi, nil)
	g.CreateEdge("PLAYS_FOR", bumrah, mi, nil)
	g.CreateEdge("DISMISSED_BY", kohli, bumrah, map[string]any{"type": "caught"})

	// Kohli's outgoing PLAYS_FOR → MI
	nbrs := g.Neighbors(kohli, "PLAYS_FOR", Out)
	if len(nbrs) != 1 || nbrs[0] != mi {
		t.Errorf("expected [MI], got %v", nbrs)
	}

	// MI's incoming PLAYS_FOR → Kohli, Bumrah
	nbrs = g.Neighbors(mi, "PLAYS_FOR", In)
	if len(nbrs) != 2 {
		t.Errorf("expected 2 neighbors, got %d", len(nbrs))
	}

	// Kohli dismissed by Bumrah
	nbrs = g.Neighbors(kohli, "DISMISSED_BY", Out)
	if len(nbrs) != 1 || nbrs[0] != bumrah {
		t.Errorf("expected [bumrah], got %v", nbrs)
	}
}

func TestTraverse(t *testing.T) {
	g := New(cricketSchema(t))

	a, _ := g.CreateNode("Player", "a", map[string]any{"name": "A"})
	b, _ := g.CreateNode("Player", "b", map[string]any{"name": "B"})
	c, _ := g.CreateNode("Player", "c", map[string]any{"name": "C"})
	d, _ := g.CreateNode("Player", "d", map[string]any{"name": "D"})

	g.CreateEdge("DISMISSED_BY", a, b, nil)
	g.CreateEdge("DISMISSED_BY", b, c, nil)
	g.CreateEdge("DISMISSED_BY", c, d, nil)

	// Traverse 1..2 hops from A
	result := g.Traverse(a, "DISMISSED_BY", Out, 1, 2)
	if len(result) != 2 {
		t.Errorf("expected 2 nodes (B,C), got %d", len(result))
	}

	// Traverse 1..3 hops from A
	result = g.Traverse(a, "DISMISSED_BY", Out, 1, 3)
	if len(result) != 3 {
		t.Errorf("expected 3 nodes (B,C,D), got %d", len(result))
	}
}

func TestShortestPath(t *testing.T) {
	g := New(cricketSchema(t))

	a, _ := g.CreateNode("Player", "a", map[string]any{"name": "A"})
	b, _ := g.CreateNode("Player", "b", map[string]any{"name": "B"})
	c, _ := g.CreateNode("Player", "c", map[string]any{"name": "C"})
	d, _ := g.CreateNode("Player", "d", map[string]any{"name": "D"})

	g.CreateEdge("DISMISSED_BY", a, b, nil)
	g.CreateEdge("DISMISSED_BY", b, c, nil)
	g.CreateEdge("DISMISSED_BY", c, d, nil)
	g.CreateEdge("DISMISSED_BY", a, d, nil) // shortcut

	path := g.ShortestPath(a, d)
	if len(path) != 2 {
		t.Errorf("expected shortest path length 2 (A→D), got %d: %v", len(path), path)
	}
}

func TestDeleteNode(t *testing.T) {
	g := New(cricketSchema(t))

	kohli, _ := g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli"})
	mi, _ := g.CreateNode("Team", "mi", map[string]any{"name": "MI"})
	g.CreateEdge("PLAYS_FOR", kohli, mi, nil)

	if err := g.DeleteNode(kohli); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	if g.GetNode(kohli) != nil {
		t.Error("deleted node should return nil")
	}
	if g.NodeCount() != 1 {
		t.Errorf("expected 1 node, got %d", g.NodeCount())
	}
	if g.EdgeCount() != 0 {
		t.Errorf("expected 0 edges after deleting connected node, got %d", g.EdgeCount())
	}
}

func TestNodesByLabel(t *testing.T) {
	g := New(cricketSchema(t))

	g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli"})
	g.CreateNode("Player", "bumrah", map[string]any{"name": "J Bumrah"})
	g.CreateNode("Team", "mi", map[string]any{"name": "MI"})

	players := g.NodesByLabel("Player")
	if len(players) != 2 {
		t.Errorf("expected 2 players, got %d", len(players))
	}

	teams := g.NodesByLabel("Team")
	if len(teams) != 1 {
		t.Errorf("expected 1 team, got %d", len(teams))
	}
}

func TestSetProperty(t *testing.T) {
	g := New(cricketSchema(t))

	kohli, _ := g.CreateNode("Player", "kohli", map[string]any{"name": "V Kohli"})

	if err := g.SetProperty(kohli, "country", "India"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}

	node := g.GetNode(kohli)
	if node.Properties["country"] != "India" {
		t.Errorf("expected country 'India', got %v", node.Properties["country"])
	}
}
