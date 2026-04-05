package graph

import (
	"sync"

	"github.com/walkerfunction/instgraph/pkg/schema"
)

// Graph is the top-level in-memory property graph.
type Graph struct {
	store   *store
	indexes *indexes
	schema  *schema.Schema
	mu      sync.RWMutex
}

// New creates a new empty graph. If schema is non-nil, mutations are validated against it.
func New(s *schema.Schema) *Graph {
	g := &Graph{
		store:   newStore(),
		indexes: newIndexes(),
		schema:  s,
	}

	// Pre-register labels and edge types from schema.
	if s != nil {
		for _, name := range s.SortedNodeNames() {
			g.store.registerLabel(name)
		}
		for _, name := range s.SortedEdgeNames() {
			g.store.registerEdgeType(name)
		}
	}

	return g
}

// GetNode returns a node by ID, or nil if it doesn't exist.
func (g *Graph) GetNode(id NodeID) *Node {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if int(id) >= len(g.store.nodes) || g.store.nodes[id].Key == "" {
		return nil
	}
	n := g.store.nodes[id]
	return &n
}

// GetNodeByKey returns a node by its unique key.
func (g *Graph) GetNodeByKey(key string) (*Node, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	id, ok := g.store.keyIndex[key]
	if !ok {
		return nil, false
	}
	n := g.store.nodes[id]
	return &n, true
}

// GetEdge returns an edge by ID, or nil if it doesn't exist.
func (g *Graph) GetEdge(id EdgeID) *Edge {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if int(id) >= len(g.store.edges) {
		return nil
	}
	e := g.store.edges[id]
	return &e
}

// NodeCount returns the number of active nodes.
func (g *Graph) NodeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.store.nodes) - len(g.store.freeNodes)
}

// EdgeCount returns the number of active edges.
func (g *Graph) EdgeCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.store.edges) - len(g.store.freeEdges)
}

// LabelName returns the string name for a label ID.
func (g *Graph) LabelName(id uint16) string {
	return g.store.labelName(id)
}

// EdgeTypeName returns the string name for an edge type ID.
func (g *Graph) EdgeTypeName(id uint16) string {
	return g.store.edgeTypeName(id)
}

// Schema returns the graph's schema (may be nil).
func (g *Graph) Schema() *schema.Schema {
	return g.schema
}
