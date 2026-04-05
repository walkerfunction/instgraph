package graph

// NodeID is the unique identifier for a node.
type NodeID uint32

// EdgeID is the unique identifier for an edge.
type EdgeID uint32

// Direction represents the direction of an edge traversal.
type Direction int

const (
	Out  Direction = iota // Outgoing edges
	In                    // Incoming edges
	Both                  // Both directions
)

// Node represents a node in the property graph.
type Node struct {
	ID         NodeID
	Label      uint16
	Key        string
	Properties map[string]any
}

// Edge represents an edge in the property graph.
type Edge struct {
	ID         EdgeID
	Type       uint16
	From       NodeID
	To         NodeID
	Properties map[string]any
}

// store holds the core graph data in flat slices.
type store struct {
	nodes []Node
	edges []Edge

	// Per-node adjacency lists.
	outEdges [][]EdgeID // nodeID → outgoing edge IDs
	inEdges  [][]EdgeID // nodeID → incoming edge IDs

	// String→uint16 registries for compact storage.
	labelRegistry    map[string]uint16
	labelNames       []string // uint16 → string
	edgeTypeRegistry map[string]uint16
	edgeTypeNames    []string // uint16 → string

	// Unique key → node ID.
	keyIndex map[string]NodeID

	// Free lists for deleted nodes/edges (for reuse).
	freeNodes []NodeID
	freeEdges []EdgeID
}

func newStore() *store {
	return &store{
		nodes:            make([]Node, 0, 1024),
		edges:            make([]Edge, 0, 1024),
		outEdges:         make([][]EdgeID, 0, 1024),
		inEdges:          make([][]EdgeID, 0, 1024),
		labelRegistry:    make(map[string]uint16),
		labelNames:       make([]string, 0),
		edgeTypeRegistry: make(map[string]uint16),
		edgeTypeNames:    make([]string, 0),
		keyIndex:         make(map[string]NodeID),
	}
}

func (s *store) registerLabel(name string) uint16 {
	if id, ok := s.labelRegistry[name]; ok {
		return id
	}
	id := uint16(len(s.labelNames))
	s.labelRegistry[name] = id
	s.labelNames = append(s.labelNames, name)
	return id
}

func (s *store) registerEdgeType(name string) uint16 {
	if id, ok := s.edgeTypeRegistry[name]; ok {
		return id
	}
	id := uint16(len(s.edgeTypeNames))
	s.edgeTypeRegistry[name] = id
	s.edgeTypeNames = append(s.edgeTypeNames, name)
	return id
}

func (s *store) labelName(id uint16) string {
	if int(id) < len(s.labelNames) {
		return s.labelNames[id]
	}
	return ""
}

func (s *store) edgeTypeName(id uint16) string {
	if int(id) < len(s.edgeTypeNames) {
		return s.edgeTypeNames[id]
	}
	return ""
}
