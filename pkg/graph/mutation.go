package graph

import "fmt"

// CreateNode creates a new node. If a node with the same key exists, it upserts (merges properties).
func (g *Graph) CreateNode(label, key string, props map[string]any) (NodeID, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Validate label exists in schema.
	if g.schema != nil {
		if _, ok := g.schema.Nodes[label]; !ok {
			return 0, fmt.Errorf("unknown node label %q", label)
		}
	}

	// Upsert: if key exists, merge properties.
	if existing, ok := g.store.keyIndex[key]; ok {
		node := &g.store.nodes[existing]
		if g.store.labelName(node.Label) != label {
			return 0, fmt.Errorf("key %q already exists with label %q, cannot create with label %q",
				key, g.store.labelName(node.Label), label)
		}
		// Remove old property indexes.
		for k, v := range node.Properties {
			g.indexes.removeProperty(node.Label, k, v, existing)
		}
		// Merge new properties.
		for k, v := range props {
			node.Properties[k] = v
		}
		// Re-add property indexes.
		for k, v := range node.Properties {
			g.indexes.addProperty(node.Label, k, v, existing)
		}
		return existing, nil
	}

	labelID := g.store.registerLabel(label)

	var id NodeID
	if len(g.store.freeNodes) > 0 {
		id = g.store.freeNodes[len(g.store.freeNodes)-1]
		g.store.freeNodes = g.store.freeNodes[:len(g.store.freeNodes)-1]
		g.store.nodes[id] = Node{
			ID:         id,
			Label:      labelID,
			Key:        key,
			Properties: copyProps(props),
		}
	} else {
		id = NodeID(len(g.store.nodes))
		g.store.nodes = append(g.store.nodes, Node{
			ID:         id,
			Label:      labelID,
			Key:        key,
			Properties: copyProps(props),
		})
		g.store.outEdges = append(g.store.outEdges, nil)
		g.store.inEdges = append(g.store.inEdges, nil)
	}

	g.store.keyIndex[key] = id
	g.indexes.addNodeLabel(labelID, id)

	// Index properties.
	for k, v := range g.store.nodes[id].Properties {
		g.indexes.addProperty(labelID, k, v, id)
	}

	return id, nil
}

// CreateEdge creates a new edge between two nodes.
func (g *Graph) CreateEdge(edgeType string, from, to NodeID, props map[string]any) (EdgeID, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Validate edge type exists in schema.
	if g.schema != nil {
		et, ok := g.schema.Edges[edgeType]
		if !ok {
			return 0, fmt.Errorf("unknown edge type %q", edgeType)
		}
		// Validate from/to labels match schema.
		if int(from) >= len(g.store.nodes) {
			return 0, fmt.Errorf("from node %d does not exist", from)
		}
		if int(to) >= len(g.store.nodes) {
			return 0, fmt.Errorf("to node %d does not exist", to)
		}
		fromLabel := g.store.labelName(g.store.nodes[from].Label)
		toLabel := g.store.labelName(g.store.nodes[to].Label)
		if fromLabel != et.From {
			return 0, fmt.Errorf("edge %s requires from=%s, got %s", edgeType, et.From, fromLabel)
		}
		if toLabel != et.To {
			return 0, fmt.Errorf("edge %s requires to=%s, got %s", edgeType, et.To, toLabel)
		}
	}

	typeID := g.store.registerEdgeType(edgeType)

	var id EdgeID
	if len(g.store.freeEdges) > 0 {
		id = g.store.freeEdges[len(g.store.freeEdges)-1]
		g.store.freeEdges = g.store.freeEdges[:len(g.store.freeEdges)-1]
		g.store.edges[id] = Edge{
			ID:         id,
			Type:       typeID,
			From:       from,
			To:         to,
			Properties: copyProps(props),
		}
	} else {
		id = EdgeID(len(g.store.edges))
		g.store.edges = append(g.store.edges, Edge{
			ID:         id,
			Type:       typeID,
			From:       from,
			To:         to,
			Properties: copyProps(props),
		})
	}

	g.store.outEdges[from] = append(g.store.outEdges[from], id)
	g.store.inEdges[to] = append(g.store.inEdges[to], id)
	g.indexes.addEdgeType(typeID, id)

	return id, nil
}

// DeleteNode removes a node and all its connected edges.
func (g *Graph) DeleteNode(id NodeID) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if int(id) >= len(g.store.nodes) || g.store.nodes[id].Key == "" {
		return fmt.Errorf("node %d does not exist", id)
	}

	node := &g.store.nodes[id]

	// Delete all connected edges.
	for _, eid := range g.store.outEdges[id] {
		g.deleteEdgeLocked(eid)
	}
	for _, eid := range g.store.inEdges[id] {
		g.deleteEdgeLocked(eid)
	}

	// Remove from indexes.
	g.indexes.removeNodeLabel(node.Label, id)
	for k, v := range node.Properties {
		g.indexes.removeProperty(node.Label, k, v, id)
	}
	delete(g.store.keyIndex, node.Key)

	// Clear and add to free list.
	g.store.nodes[id] = Node{}
	g.store.outEdges[id] = nil
	g.store.inEdges[id] = nil
	g.store.freeNodes = append(g.store.freeNodes, id)

	return nil
}

// DeleteEdge removes an edge.
func (g *Graph) DeleteEdge(id EdgeID) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deleteEdgeLocked(id)
}

func (g *Graph) deleteEdgeLocked(id EdgeID) error {
	if int(id) >= len(g.store.edges) {
		return fmt.Errorf("edge %d does not exist", id)
	}

	edge := &g.store.edges[id]
	if edge.From == 0 && edge.To == 0 && edge.Type == 0 && edge.Properties == nil {
		return nil // already deleted
	}

	// Remove from adjacency lists.
	g.store.outEdges[edge.From] = removeEdgeID(g.store.outEdges[edge.From], id)
	g.store.inEdges[edge.To] = removeEdgeID(g.store.inEdges[edge.To], id)

	// Remove from indexes.
	g.indexes.removeEdgeType(edge.Type, id)

	// Clear and add to free list.
	g.store.edges[id] = Edge{}
	g.store.freeEdges = append(g.store.freeEdges, id)

	return nil
}

// SetProperty sets a property on a node.
func (g *Graph) SetProperty(id NodeID, key string, value any) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if int(id) >= len(g.store.nodes) || g.store.nodes[id].Key == "" {
		return fmt.Errorf("node %d does not exist", id)
	}

	node := &g.store.nodes[id]

	// Remove old index entry if exists.
	if old, ok := node.Properties[key]; ok {
		g.indexes.removeProperty(node.Label, key, old, id)
	}

	node.Properties[key] = value
	g.indexes.addProperty(node.Label, key, value, id)

	return nil
}

func removeEdgeID(slice []EdgeID, id EdgeID) []EdgeID {
	for i, eid := range slice {
		if eid == id {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func copyProps(props map[string]any) map[string]any {
	if props == nil {
		return make(map[string]any)
	}
	cp := make(map[string]any, len(props))
	for k, v := range props {
		cp[k] = v
	}
	return cp
}
