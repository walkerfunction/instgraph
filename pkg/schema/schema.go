package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// rawSchema mirrors the JSON structure for unmarshalling.
type rawSchema struct {
	Nodes map[string]map[string]json.RawMessage `json:"nodes"`
	Edges map[string]json.RawMessage            `json:"edges"`
}

// rawEdge mirrors the edge JSON structure.
type rawEdge struct {
	From       string                       `json:"from"`
	To         string                       `json:"to"`
	Properties map[string]json.RawMessage   `json:"properties,omitempty"`
}

// Load reads and parses a schema JSON file.
func Load(path string) (*Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading schema: %w", err)
	}
	return Parse(data)
}

// Parse parses schema JSON bytes into a Schema.
func Parse(data []byte) (*Schema, error) {
	var raw rawSchema
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parsing schema JSON: %w", err)
	}

	s := &Schema{
		Nodes: make(map[string]*NodeType, len(raw.Nodes)),
		Edges: make(map[string]*EdgeType, len(raw.Edges)),
	}

	// Parse node types.
	for name, props := range raw.Nodes {
		nt := &NodeType{
			Name:       name,
			Properties: make(map[string]Property, len(props)),
		}
		for propName, propRaw := range props {
			var p Property
			if err := json.Unmarshal(propRaw, &p); err != nil {
				return nil, fmt.Errorf("parsing node %s property %s: %w", name, propName, err)
			}
			if err := validatePropType(p.Type); err != nil {
				return nil, fmt.Errorf("node %s property %s: %w", name, propName, err)
			}
			p.Name = propName
			nt.Properties[propName] = p
		}
		s.Nodes[name] = nt
	}

	// Parse edge types.
	for name, edgeRaw := range raw.Edges {
		var re rawEdge
		if err := json.Unmarshal(edgeRaw, &re); err != nil {
			return nil, fmt.Errorf("parsing edge %s: %w", name, err)
		}

		et := &EdgeType{
			Name:       name,
			From:       re.From,
			To:         re.To,
			Properties: make(map[string]Property),
		}

		// Validate from/to reference existing node types.
		if _, ok := s.Nodes[re.From]; !ok {
			return nil, fmt.Errorf("edge %s references unknown node type %q in 'from'", name, re.From)
		}
		if _, ok := s.Nodes[re.To]; !ok {
			return nil, fmt.Errorf("edge %s references unknown node type %q in 'to'", name, re.To)
		}

		for propName, propRaw := range re.Properties {
			var p Property
			if err := json.Unmarshal(propRaw, &p); err != nil {
				return nil, fmt.Errorf("parsing edge %s property %s: %w", name, propName, err)
			}
			if err := validatePropType(p.Type); err != nil {
				return nil, fmt.Errorf("edge %s property %s: %w", name, propName, err)
			}
			p.Name = propName
			et.Properties[propName] = p
		}
		s.Edges[name] = et
	}

	return s, nil
}

// Validate checks the schema for internal consistency.
func (s *Schema) Validate() error {
	if len(s.Nodes) == 0 {
		return fmt.Errorf("schema must define at least one node type")
	}
	for name, et := range s.Edges {
		if _, ok := s.Nodes[et.From]; !ok {
			return fmt.Errorf("edge %s: from node type %q not defined", name, et.From)
		}
		if _, ok := s.Nodes[et.To]; !ok {
			return fmt.Errorf("edge %s: to node type %q not defined", name, et.To)
		}
	}
	return nil
}

// SortedNodeNames returns node type names in sorted order for deterministic output.
func (s *Schema) SortedNodeNames() []string {
	names := make([]string, 0, len(s.Nodes))
	for n := range s.Nodes {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// SortedEdgeNames returns edge type names in sorted order for deterministic output.
func (s *Schema) SortedEdgeNames() []string {
	names := make([]string, 0, len(s.Edges))
	for n := range s.Edges {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// SortedPropertyNames returns property names in sorted order for a given map.
func SortedPropertyNames(props map[string]Property) []string {
	names := make([]string, 0, len(props))
	for n := range props {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func validatePropType(t PropType) error {
	switch t {
	case PropTypeString, PropTypeInt, PropTypeFloat, PropTypeBool:
		return nil
	default:
		return fmt.Errorf("unsupported property type %q", t)
	}
}
