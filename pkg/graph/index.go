package graph

import "github.com/tidwall/btree"

// propIndexKey is the key for property BTree indexes.
type propIndexKey struct {
	Value  any
	NodeID NodeID
}

// indexes holds all secondary indexes for the graph.
type indexes struct {
	// byLabel maps label uint16 → set of node IDs.
	byLabel map[uint16]map[NodeID]struct{}

	// byEdgeType maps edge type uint16 → set of edge IDs.
	byEdgeType map[uint16]map[EdgeID]struct{}

	// byProperty maps "label.propName" → BTree of (value, nodeID) for range queries.
	byProperty map[string]*btree.BTreeG[propIndexKey]
}

func newIndexes() *indexes {
	return &indexes{
		byLabel:    make(map[uint16]map[NodeID]struct{}),
		byEdgeType: make(map[uint16]map[EdgeID]struct{}),
		byProperty: make(map[string]*btree.BTreeG[propIndexKey]),
	}
}

func (idx *indexes) addNodeLabel(label uint16, id NodeID) {
	m, ok := idx.byLabel[label]
	if !ok {
		m = make(map[NodeID]struct{})
		idx.byLabel[label] = m
	}
	m[id] = struct{}{}
}

func (idx *indexes) removeNodeLabel(label uint16, id NodeID) {
	if m, ok := idx.byLabel[label]; ok {
		delete(m, id)
	}
}

func (idx *indexes) addEdgeType(typ uint16, id EdgeID) {
	m, ok := idx.byEdgeType[typ]
	if !ok {
		m = make(map[EdgeID]struct{})
		idx.byEdgeType[typ] = m
	}
	m[id] = struct{}{}
}

func (idx *indexes) removeEdgeType(typ uint16, id EdgeID) {
	if m, ok := idx.byEdgeType[typ]; ok {
		delete(m, id)
	}
}

func (idx *indexes) nodesByLabel(label uint16) []NodeID {
	m := idx.byLabel[label]
	result := make([]NodeID, 0, len(m))
	for id := range m {
		result = append(result, id)
	}
	return result
}

func (idx *indexes) edgesByType(typ uint16) []EdgeID {
	m := idx.byEdgeType[typ]
	result := make([]EdgeID, 0, len(m))
	for id := range m {
		result = append(result, id)
	}
	return result
}

func propIndexName(labelID uint16, propName string) string {
	// Simple string key for the property index map.
	return string(rune(labelID)) + "." + propName
}

func comparePropIndexKey(a, b propIndexKey) bool {
	return compareAny(a.Value, b.Value) < 0 ||
		(compareAny(a.Value, b.Value) == 0 && a.NodeID < b.NodeID)
}

func (idx *indexes) addProperty(labelID uint16, propName string, value any, nodeID NodeID) {
	key := propIndexName(labelID, propName)
	tree, ok := idx.byProperty[key]
	if !ok {
		tree = btree.NewBTreeG(comparePropIndexKey)
		idx.byProperty[key] = tree
	}
	tree.Set(propIndexKey{Value: value, NodeID: nodeID})
}

func (idx *indexes) removeProperty(labelID uint16, propName string, value any, nodeID NodeID) {
	key := propIndexName(labelID, propName)
	if tree, ok := idx.byProperty[key]; ok {
		tree.Delete(propIndexKey{Value: value, NodeID: nodeID})
	}
}

// compareAny compares two values for ordering. Returns -1, 0, or 1.
func compareAny(a, b any) int {
	switch av := a.(type) {
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return -1
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return -1
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case string:
		bv, ok := b.(string)
		if !ok {
			return -1
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return -1
		}
		if av == bv {
			return 0
		}
		if !av {
			return -1
		}
		return 1
	default:
		return 0
	}
}
