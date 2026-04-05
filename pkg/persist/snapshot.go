package persist

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/walkerfunction/instgraph/pkg/graph"
)

// snapshotMeta holds metadata about a snapshot.
type snapshotMeta struct {
	Timestamp  uint64 `json:"timestamp"`
	WALSeq     uint64 `json:"wal_seq"`
	NodeCount  int    `json:"node_count"`
	EdgeCount  int    `json:"edge_count"`
}

// snapshotNode is the serialized form of a node.
type snapshotNode struct {
	Label      string         `json:"label"`
	Key        string         `json:"key"`
	Properties map[string]any `json:"props"`
}

// snapshotEdge is the serialized form of an edge.
type snapshotEdge struct {
	Type       string         `json:"type"`
	FromKey    string         `json:"from_key"`
	ToKey      string         `json:"to_key"`
	Properties map[string]any `json:"props"`
}

// TakeSnapshot writes the current graph state to Pebble.
func (p *Persister) TakeSnapshot(g *graph.Graph) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.snapCount++
	ts := p.snapCount
	walSeq := p.wal.CurrentSeq()

	batch := p.db.NewBatch()
	defer batch.Close()

	// Write nodes.
	nodeCount := 0
	if g.Schema() != nil {
		for _, label := range g.Schema().SortedNodeNames() {
			for _, nid := range g.NodesByLabel(label) {
				node := g.GetNode(nid)
				if node == nil {
					continue
				}
				sn := snapshotNode{
					Label:      g.LabelName(node.Label),
					Key:        node.Key,
					Properties: node.Properties,
				}
				val, err := json.Marshal(sn)
				if err != nil {
					return fmt.Errorf("marshalling node: %w", err)
				}
				key := snapNodeKey(ts, uint32(nid))
				if err := batch.Set(key, val, nil); err != nil {
					return err
				}
				nodeCount++
			}
		}
	}

	// Write edges.
	edgeCount := 0
	if g.Schema() != nil {
		for _, edgeType := range g.Schema().SortedEdgeNames() {
			for _, eid := range g.EdgesByType(edgeType) {
				edge := g.GetEdge(eid)
				if edge == nil {
					continue
				}
				fromNode := g.GetNode(edge.From)
				toNode := g.GetNode(edge.To)
				if fromNode == nil || toNode == nil {
					continue
				}
				se := snapshotEdge{
					Type:       g.EdgeTypeName(edge.Type),
					FromKey:    fromNode.Key,
					ToKey:      toNode.Key,
					Properties: edge.Properties,
				}
				val, err := json.Marshal(se)
				if err != nil {
					return fmt.Errorf("marshalling edge: %w", err)
				}
				key := snapEdgeKey(ts, uint32(eid))
				if err := batch.Set(key, val, nil); err != nil {
					return err
				}
				edgeCount++
			}
		}
	}

	// Write metadata.
	meta := snapshotMeta{
		Timestamp: ts,
		WALSeq:    walSeq,
		NodeCount: nodeCount,
		EdgeCount: edgeCount,
	}
	metaVal, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshalling snapshot meta: %w", err)
	}
	if err := batch.Set(snapMetaKey(ts), metaVal, nil); err != nil {
		return err
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing snapshot: %w", err)
	}

	// Compact WAL: delete entries before this snapshot.
	if walSeq > 0 {
		if err := p.wal.DeleteBefore(walSeq); err != nil {
			return fmt.Errorf("compacting WAL: %w", err)
		}
	}

	return nil
}

// Key format helpers.

func snapMetaKey(ts uint64) []byte {
	key := make([]byte, 13) // "snap:" + 8 bytes
	copy(key, "snap:")
	binary.BigEndian.PutUint64(key[5:], ts)
	return key
}

func snapNodeKey(ts uint64, nodeID uint32) []byte {
	key := make([]byte, 18) // "snapn" + 8 + ":" + 4
	copy(key, "snapn")
	binary.BigEndian.PutUint64(key[5:], ts)
	key[13] = ':'
	binary.BigEndian.PutUint32(key[14:], nodeID)
	return key
}

func snapEdgeKey(ts uint64, edgeID uint32) []byte {
	key := make([]byte, 18) // "snape" + 8 + ":" + 4
	copy(key, "snape")
	binary.BigEndian.PutUint64(key[5:], ts)
	key[13] = ':'
	binary.BigEndian.PutUint32(key[14:], edgeID)
	return key
}
