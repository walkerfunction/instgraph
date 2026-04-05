package schema

// PropType represents the type of a property.
type PropType string

const (
	PropTypeString PropType = "string"
	PropTypeInt    PropType = "int"
	PropTypeFloat  PropType = "float"
	PropTypeBool   PropType = "bool"
)

// Property defines a single property on a node or edge type.
type Property struct {
	Name     string   `json:"-"`
	Type     PropType `json:"type"`
	Required bool     `json:"required,omitempty"`
}

// NodeType defines a node type in the schema.
type NodeType struct {
	Name       string              `json:"-"`
	Properties map[string]Property `json:"-"`
}

// EdgeType defines an edge type in the schema.
type EdgeType struct {
	Name       string              `json:"-"`
	From       string              `json:"from"`
	To         string              `json:"to"`
	Properties map[string]Property `json:"-"`
}

// Schema is the top-level schema definition.
type Schema struct {
	Nodes map[string]*NodeType `json:"-"`
	Edges map[string]*EdgeType `json:"-"`
}

// LabelID returns the uint16 ID for a node label.
func (s *Schema) LabelID(name string) (uint16, bool) {
	i := uint16(0)
	for n := range s.Nodes {
		if n == name {
			return i, true
		}
		i++
	}
	return 0, false
}

// EdgeTypeID returns the uint16 ID for an edge type.
func (s *Schema) EdgeTypeID(name string) (uint16, bool) {
	i := uint16(0)
	for n := range s.Edges {
		if n == name {
			return i, true
		}
		i++
	}
	return 0, false
}
