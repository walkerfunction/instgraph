package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
)

// lockEntry represents a single item in the schema lock.
type lockEntry struct {
	Nodes map[string][]string `json:"nodes"` // node name → sorted property names
	Edges map[string]lockEdge `json:"edges"` // edge name → from/to + sorted property names
}

type lockEdge struct {
	From       string   `json:"from"`
	To         string   `json:"to"`
	Properties []string `json:"properties,omitempty"`
}

// CheckLock verifies the schema against a lock file.
// If the lock file does not exist, it creates one.
// If it exists, it verifies that nothing was removed (append-only).
func CheckLock(s *Schema, lockPath string) error {
	data, err := os.ReadFile(lockPath)
	if errors.Is(err, os.ErrNotExist) {
		return WriteLock(s, lockPath)
	}
	if err != nil {
		return fmt.Errorf("reading lock file: %w", err)
	}

	var prev lockEntry
	if err := json.Unmarshal(data, &prev); err != nil {
		return fmt.Errorf("parsing lock file: %w", err)
	}

	// Check no node types were removed.
	for name, propNames := range prev.Nodes {
		nt, ok := s.Nodes[name]
		if !ok {
			return fmt.Errorf("schema violation: node type %q was removed (schema is append-only)", name)
		}
		// Check no properties were removed from this node type.
		for _, pn := range propNames {
			if _, ok := nt.Properties[pn]; !ok {
				return fmt.Errorf("schema violation: property %q was removed from node type %q (schema is append-only)", pn, name)
			}
		}
	}

	// Check no edge types were removed.
	for name, le := range prev.Edges {
		et, ok := s.Edges[name]
		if !ok {
			return fmt.Errorf("schema violation: edge type %q was removed (schema is append-only)", name)
		}
		// Check from/to haven't changed.
		if et.From != le.From {
			return fmt.Errorf("schema violation: edge %q 'from' changed from %q to %q", name, le.From, et.From)
		}
		if et.To != le.To {
			return fmt.Errorf("schema violation: edge %q 'to' changed from %q to %q", name, le.To, et.To)
		}
		// Check no edge properties were removed.
		for _, pn := range le.Properties {
			if _, ok := et.Properties[pn]; !ok {
				return fmt.Errorf("schema violation: property %q was removed from edge type %q (schema is append-only)", pn, name)
			}
		}
	}

	// All good — update lock with any new additions.
	return WriteLock(s, lockPath)
}

// WriteLock writes the current schema state to a lock file.
func WriteLock(s *Schema, lockPath string) error {
	entry := lockEntry{
		Nodes: make(map[string][]string, len(s.Nodes)),
		Edges: make(map[string]lockEdge, len(s.Edges)),
	}

	for name, nt := range s.Nodes {
		props := make([]string, 0, len(nt.Properties))
		for pn := range nt.Properties {
			props = append(props, pn)
		}
		sort.Strings(props)
		entry.Nodes[name] = props
	}

	for name, et := range s.Edges {
		props := make([]string, 0, len(et.Properties))
		for pn := range et.Properties {
			props = append(props, pn)
		}
		sort.Strings(props)
		entry.Edges[name] = lockEdge{
			From:       et.From,
			To:         et.To,
			Properties: props,
		}
	}

	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling lock: %w", err)
	}
	if err := os.WriteFile(lockPath, data, 0644); err != nil {
		return fmt.Errorf("writing lock file: %w", err)
	}
	return nil
}
