package graph

// Neighbors returns node IDs connected to the given node via edges of the specified type and direction.
// If edgeType is empty, all edge types are matched.
func (g *Graph) Neighbors(id NodeID, edgeType string, dir Direction) []NodeID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	var typeFilter uint16
	hasFilter := false
	if edgeType != "" {
		if tid, ok := g.store.edgeTypeRegistry[edgeType]; ok {
			typeFilter = tid
			hasFilter = true
		} else {
			return nil // unknown edge type
		}
	}

	seen := make(map[NodeID]struct{})
	var result []NodeID

	if dir == Out || dir == Both {
		for _, eid := range g.store.outEdges[id] {
			e := &g.store.edges[eid]
			if hasFilter && e.Type != typeFilter {
				continue
			}
			if _, ok := seen[e.To]; !ok {
				seen[e.To] = struct{}{}
				result = append(result, e.To)
			}
		}
	}

	if dir == In || dir == Both {
		for _, eid := range g.store.inEdges[id] {
			e := &g.store.edges[eid]
			if hasFilter && e.Type != typeFilter {
				continue
			}
			if _, ok := seen[e.From]; !ok {
				seen[e.From] = struct{}{}
				result = append(result, e.From)
			}
		}
	}

	return result
}

// NodesByLabel returns all node IDs with the given label.
func (g *Graph) NodesByLabel(label string) []NodeID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	lid, ok := g.store.labelRegistry[label]
	if !ok {
		return nil
	}
	return g.indexes.nodesByLabel(lid)
}

// EdgesByType returns all edge IDs with the given type.
func (g *Graph) EdgesByType(edgeType string) []EdgeID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	tid, ok := g.store.edgeTypeRegistry[edgeType]
	if !ok {
		return nil
	}
	return g.indexes.edgesByType(tid)
}

// Traverse performs a variable-length traversal from a start node.
// It follows edges of the given type (or all if empty) up to maxHops.
// Returns all unique node IDs reached (excluding the start node).
func (g *Graph) Traverse(start NodeID, edgeType string, dir Direction, minHops, maxHops int) []NodeID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	visited := make(map[NodeID]struct{})
	visited[start] = struct{}{} // don't revisit start
	var result []NodeID

	current := []NodeID{start}
	for hop := 1; hop <= maxHops && len(current) > 0; hop++ {
		var next []NodeID
		for _, nid := range current {
			neighbors := g.neighborsLocked(nid, edgeType, dir)
			for _, nbr := range neighbors {
				if _, ok := visited[nbr]; ok {
					continue
				}
				visited[nbr] = struct{}{}
				next = append(next, nbr)
				if hop >= minHops {
					result = append(result, nbr)
				}
			}
		}
		current = next
	}

	return result
}

// ShortestPath finds the shortest path between two nodes using BFS.
// Returns the path as a slice of node IDs (including start and end), or nil if no path exists.
func (g *Graph) ShortestPath(from, to NodeID) []NodeID {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if from == to {
		return []NodeID{from}
	}

	type entry struct {
		node NodeID
		path []NodeID
	}

	visited := make(map[NodeID]struct{})
	visited[from] = struct{}{}
	queue := []entry{{node: from, path: []NodeID{from}}}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		neighbors := g.neighborsLocked(curr.node, "", Both)
		for _, nbr := range neighbors {
			if _, ok := visited[nbr]; ok {
				continue
			}
			visited[nbr] = struct{}{}
			newPath := make([]NodeID, len(curr.path)+1)
			copy(newPath, curr.path)
			newPath[len(curr.path)] = nbr

			if nbr == to {
				return newPath
			}
			queue = append(queue, entry{node: nbr, path: newPath})
		}
	}

	return nil
}

// neighborsLocked is the lock-free version of Neighbors (caller must hold at least RLock).
func (g *Graph) neighborsLocked(id NodeID, edgeType string, dir Direction) []NodeID {
	var typeFilter uint16
	hasFilter := false
	if edgeType != "" {
		if tid, ok := g.store.edgeTypeRegistry[edgeType]; ok {
			typeFilter = tid
			hasFilter = true
		} else {
			return nil
		}
	}

	var result []NodeID

	if dir == Out || dir == Both {
		for _, eid := range g.store.outEdges[id] {
			e := &g.store.edges[eid]
			if hasFilter && e.Type != typeFilter {
				continue
			}
			result = append(result, e.To)
		}
	}

	if dir == In || dir == Both {
		for _, eid := range g.store.inEdges[id] {
			e := &g.store.edges[eid]
			if hasFilter && e.Type != typeFilter {
				continue
			}
			result = append(result, e.From)
		}
	}

	return result
}
