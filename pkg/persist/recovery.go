package persist

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/walkerfunction/instgraph/pkg/graph"
	"github.com/walkerfunction/instgraph/pkg/schema"
)

// Persister manages WAL and snapshots via Pebble.
type Persister struct {
	db        *pebble.DB
	wal       *WAL
	mu        sync.Mutex
	snapCount uint64
}

// Open creates a new Persister backed by Pebble at the given path.
func Open(path string) (*Persister, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("opening pebble: %w", err)
	}
	p := &Persister{
		db:  db,
		wal: newWAL(db),
	}

	// Find latest snapshot count.
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("snap:"),
		UpperBound: []byte("snap;"),
	})
	if err == nil {
		defer iter.Close()
		if iter.Last() {
			key := iter.Key()
			if len(key) >= 13 {
				p.snapCount = binary.BigEndian.Uint64(key[5:13])
			}
		}
	}

	return p, nil
}

// Close closes the Pebble database.
func (p *Persister) Close() error {
	return p.db.Close()
}

// WAL returns the write-ahead log.
func (p *Persister) WAL() *WAL {
	return p.wal
}

// Recover loads the latest snapshot and replays WAL entries to rebuild the graph.
func (p *Persister) Recover(s *schema.Schema) (*graph.Graph, error) {
	g := graph.New(s)

	// Find latest snapshot.
	meta, err := p.findLatestSnapshot()
	if err != nil {
		return nil, err
	}

	if meta != nil {
		// Load snapshot.
		if err := p.loadSnapshot(g, meta.Timestamp); err != nil {
			return nil, fmt.Errorf("loading snapshot: %w", err)
		}

		// Replay WAL entries after snapshot.
		entries, err := p.wal.EntriesAfter(meta.WALSeq)
		if err != nil {
			return nil, fmt.Errorf("reading WAL: %w", err)
		}
		if err := replayWAL(g, entries); err != nil {
			return nil, fmt.Errorf("replaying WAL: %w", err)
		}
	} else {
		// No snapshot, replay all WAL entries.
		entries, err := p.wal.EntriesAfter(0)
		if err != nil {
			return nil, fmt.Errorf("reading WAL: %w", err)
		}
		if err := replayWAL(g, entries); err != nil {
			return nil, fmt.Errorf("replaying WAL: %w", err)
		}
	}

	return g, nil
}

func (p *Persister) findLatestSnapshot() (*snapshotMeta, error) {
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("snap:"),
		UpperBound: []byte("snap;"),
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}
	defer iter.Close()

	if !iter.Last() {
		return nil, nil // no snapshots
	}

	var meta snapshotMeta
	if err := json.Unmarshal(iter.Value(), &meta); err != nil {
		return nil, fmt.Errorf("parsing snapshot meta: %w", err)
	}

	return &meta, nil
}

func (p *Persister) loadSnapshot(g *graph.Graph, ts uint64) error {
	// Load nodes.
	prefix := make([]byte, 13)
	copy(prefix, "snapn")
	binary.BigEndian.PutUint64(prefix[5:], ts)

	upperBound := make([]byte, 13)
	copy(upperBound, "snapn")
	binary.BigEndian.PutUint64(upperBound[5:], ts+1)

	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return fmt.Errorf("creating node iterator: %w", err)
	}

	for iter.First(); iter.Valid(); iter.Next() {
		var sn snapshotNode
		if err := json.Unmarshal(iter.Value(), &sn); err != nil {
			iter.Close()
			return fmt.Errorf("parsing snapshot node: %w", err)
		}
		if _, err := g.CreateNode(sn.Label, sn.Key, sn.Properties); err != nil {
			iter.Close()
			return fmt.Errorf("creating node from snapshot: %w", err)
		}
	}
	iter.Close()

	// Load edges.
	prefix2 := make([]byte, 13)
	copy(prefix2, "snape")
	binary.BigEndian.PutUint64(prefix2[5:], ts)

	upperBound2 := make([]byte, 13)
	copy(upperBound2, "snape")
	binary.BigEndian.PutUint64(upperBound2[5:], ts+1)

	iter2, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix2,
		UpperBound: upperBound2,
	})
	if err != nil {
		return fmt.Errorf("creating edge iterator: %w", err)
	}
	defer iter2.Close()

	for iter2.First(); iter2.Valid(); iter2.Next() {
		var se snapshotEdge
		if err := json.Unmarshal(iter2.Value(), &se); err != nil {
			return fmt.Errorf("parsing snapshot edge: %w", err)
		}
		fromNode, ok := g.GetNodeByKey(se.FromKey)
		if !ok {
			continue
		}
		toNode, ok := g.GetNodeByKey(se.ToKey)
		if !ok {
			continue
		}
		if _, err := g.CreateEdge(se.Type, fromNode.ID, toNode.ID, se.Properties); err != nil {
			return fmt.Errorf("creating edge from snapshot: %w", err)
		}
	}

	return nil
}

func replayWAL(g *graph.Graph, entries []WALEntry) error {
	for _, entry := range entries {
		switch entry.Op {
		case OpCreateNode:
			if _, err := g.CreateNode(entry.Label, entry.Key, entry.Props); err != nil {
				return fmt.Errorf("replay create node: %w", err)
			}
		case OpCreateEdge:
			fromNode, ok := g.GetNodeByKey(entry.FromKey)
			if !ok {
				continue
			}
			toNode, ok := g.GetNodeByKey(entry.ToKey)
			if !ok {
				continue
			}
			if _, err := g.CreateEdge(entry.EdgeType, fromNode.ID, toNode.ID, entry.Props); err != nil {
				return fmt.Errorf("replay create edge: %w", err)
			}
		case OpDeleteNode:
			if node, ok := g.GetNodeByKey(entry.Key); ok {
				g.DeleteNode(node.ID)
			}
		case OpDeleteEdge:
			// Edge deletion by key not directly supported, skip for now.
		case OpSetProperty:
			if node, ok := g.GetNodeByKey(entry.Key); ok {
				g.SetProperty(node.ID, entry.PropName, entry.PropVal)
			}
		}
	}
	return nil
}

// LogCreateNode appends a CreateNode operation to the WAL.
func (p *Persister) LogCreateNode(label, key string, props map[string]any) error {
	return p.wal.Append(WALEntry{
		Op:    OpCreateNode,
		Label: label,
		Key:   key,
		Props: props,
	})
}

// LogCreateEdge appends a CreateEdge operation to the WAL.
func (p *Persister) LogCreateEdge(edgeType, fromKey, toKey string, props map[string]any) error {
	return p.wal.Append(WALEntry{
		Op:       OpCreateEdge,
		EdgeType: edgeType,
		FromKey:  fromKey,
		ToKey:    toKey,
		Props:    props,
	})
}

// LogDeleteNode appends a DeleteNode operation to the WAL.
func (p *Persister) LogDeleteNode(key string) error {
	return p.wal.Append(WALEntry{
		Op:  OpDeleteNode,
		Key: key,
	})
}

// LogSetProperty appends a SetProperty operation to the WAL.
func (p *Persister) LogSetProperty(key, propName string, propVal any) error {
	return p.wal.Append(WALEntry{
		Op:       OpSetProperty,
		Key:      key,
		PropName: propName,
		PropVal:  propVal,
	})
}
