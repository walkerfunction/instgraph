package persist

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// OpType is the type of WAL operation.
type OpType uint8

const (
	OpCreateNode OpType = iota + 1
	OpCreateEdge
	OpDeleteNode
	OpDeleteEdge
	OpSetProperty
)

// WALEntry represents a single mutation in the write-ahead log.
type WALEntry struct {
	Seq      uint64         `json:"seq"`
	Op       OpType         `json:"op"`
	Label    string         `json:"label,omitempty"`
	Key      string         `json:"key,omitempty"`
	EdgeType string         `json:"edge_type,omitempty"`
	FromKey  string         `json:"from_key,omitempty"`
	ToKey    string         `json:"to_key,omitempty"`
	PropName string         `json:"prop_name,omitempty"`
	PropVal  any            `json:"prop_val,omitempty"`
	Props    map[string]any `json:"props,omitempty"`
}

// WAL is a write-ahead log backed by Pebble.
type WAL struct {
	db  *pebble.DB
	seq atomic.Uint64
	mu  sync.Mutex
}

func newWAL(db *pebble.DB) *WAL {
	w := &WAL{db: db}
	// Find the highest existing sequence number.
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte("wal:"),
		UpperBound: []byte("wal;"), // ; is after : in ASCII
	})
	if err == nil {
		defer iter.Close()
		if iter.Last() {
			key := iter.Key()
			if len(key) >= 12 { // "wal:" + 8 bytes
				seq := binary.BigEndian.Uint64(key[4:12])
				w.seq.Store(seq)
			}
		}
	}
	return w
}

// Append writes a WAL entry to Pebble.
func (w *WAL) Append(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	seq := w.seq.Add(1)
	entry.Seq = seq

	key := make([]byte, 12) // "wal:" + 8 bytes
	copy(key, "wal:")
	binary.BigEndian.PutUint64(key[4:], seq)

	value, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshalling WAL entry: %w", err)
	}

	return w.db.Set(key, value, pebble.Sync)
}

// EntriesAfter returns all WAL entries with sequence > afterSeq.
func (w *WAL) EntriesAfter(afterSeq uint64) ([]WALEntry, error) {
	lowerKey := make([]byte, 12)
	copy(lowerKey, "wal:")
	binary.BigEndian.PutUint64(lowerKey[4:], afterSeq+1)

	iter, err := w.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerKey,
		UpperBound: []byte("wal;"),
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}
	defer iter.Close()

	var entries []WALEntry
	for iter.First(); iter.Valid(); iter.Next() {
		var entry WALEntry
		if err := json.Unmarshal(iter.Value(), &entry); err != nil {
			return nil, fmt.Errorf("unmarshalling WAL entry: %w", err)
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// DeleteBefore removes all WAL entries with sequence <= beforeSeq.
func (w *WAL) DeleteBefore(beforeSeq uint64) error {
	upperKey := make([]byte, 12)
	copy(upperKey, "wal:")
	binary.BigEndian.PutUint64(upperKey[4:], beforeSeq+1)

	return w.db.DeleteRange([]byte("wal:"), upperKey, pebble.Sync)
}

// CurrentSeq returns the current sequence number.
func (w *WAL) CurrentSeq() uint64 {
	return w.seq.Load()
}
