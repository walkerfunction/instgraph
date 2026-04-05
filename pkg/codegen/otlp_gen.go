package codegen

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/walkerfunction/instgraph/pkg/schema"
)

// GenerateOTLPYAML generates a human-readable OTLP schema contract.
func GenerateOTLPYAML(s *schema.Schema, outPath string) error {
	var b strings.Builder
	b.WriteString("# Auto-generated OTLP ingestion schema — DO NOT EDIT\n")
	b.WriteString("# Defines the expected OTLP log record format for instgraph ingestion.\n\n")

	b.WriteString("nodes:\n")
	for _, name := range s.SortedNodeNames() {
		nt := s.Nodes[name]
		b.WriteString(fmt.Sprintf("  %s:\n", name))
		b.WriteString(fmt.Sprintf("    attributes:\n"))
		b.WriteString(fmt.Sprintf("      graph.op: \"NODE\"\n"))
		b.WriteString(fmt.Sprintf("      graph.label: %q\n", name))
		b.WriteString(fmt.Sprintf("      graph.key: \"<unique identifier>\"\n"))
		b.WriteString(fmt.Sprintf("    body:\n"))
		for _, pn := range schema.SortedPropertyNames(nt.Properties) {
			p := nt.Properties[pn]
			req := ""
			if p.Required {
				req = " # required"
			}
			b.WriteString(fmt.Sprintf("      %s: %s%s\n", pn, p.Type, req))
		}
		b.WriteString("\n")
	}

	b.WriteString("edges:\n")
	for _, name := range s.SortedEdgeNames() {
		et := s.Edges[name]
		b.WriteString(fmt.Sprintf("  %s:\n", name))
		b.WriteString(fmt.Sprintf("    attributes:\n"))
		b.WriteString(fmt.Sprintf("      graph.op: \"EDGE\"\n"))
		b.WriteString(fmt.Sprintf("      graph.edge: %q\n", name))
		b.WriteString(fmt.Sprintf("      graph.from: \"<%s.key>\"\n", et.From))
		b.WriteString(fmt.Sprintf("      graph.to: \"<%s.key>\"\n", et.To))
		if len(et.Properties) > 0 {
			b.WriteString(fmt.Sprintf("    body:\n"))
			for _, pn := range schema.SortedPropertyNames(et.Properties) {
				p := et.Properties[pn]
				b.WriteString(fmt.Sprintf("      %s: %s\n", pn, p.Type))
			}
		}
		b.WriteString("\n")
	}

	return os.WriteFile(outPath, []byte(b.String()), 0644)
}

// GenerateOTLPJSON generates a JSON Schema for validating OTLP log records.
func GenerateOTLPJSON(s *schema.Schema, outPath string) error {
	oneOf := make([]map[string]any, 0)

	// Node schemas.
	for _, name := range s.SortedNodeNames() {
		nt := s.Nodes[name]
		bodyProps := make(map[string]any)
		required := make([]string, 0)

		for _, pn := range schema.SortedPropertyNames(nt.Properties) {
			p := nt.Properties[pn]
			bodyProps[pn] = map[string]any{"type": jsonSchemaType(p.Type)}
			if p.Required {
				required = append(required, pn)
			}
		}

		entry := map[string]any{
			"type": "object",
			"properties": map[string]any{
				"attributes": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"graph.op":    map[string]any{"const": "NODE"},
						"graph.label": map[string]any{"const": name},
						"graph.key":   map[string]any{"type": "string"},
					},
					"required": []string{"graph.op", "graph.label", "graph.key"},
				},
				"body": map[string]any{
					"type":       "object",
					"properties": bodyProps,
					"required":   required,
				},
			},
		}
		oneOf = append(oneOf, entry)
	}

	// Edge schemas.
	for _, name := range s.SortedEdgeNames() {
		et := s.Edges[name]
		attrProps := map[string]any{
			"graph.op":   map[string]any{"const": "EDGE"},
			"graph.edge": map[string]any{"const": name},
			"graph.from": map[string]any{"type": "string"},
			"graph.to":   map[string]any{"type": "string"},
		}

		entry := map[string]any{
			"type": "object",
			"properties": map[string]any{
				"attributes": map[string]any{
					"type":       "object",
					"properties": attrProps,
					"required":   []string{"graph.op", "graph.edge", "graph.from", "graph.to"},
				},
			},
		}

		if len(et.Properties) > 0 {
			bodyProps := make(map[string]any)
			for _, pn := range schema.SortedPropertyNames(et.Properties) {
				p := et.Properties[pn]
				bodyProps[pn] = map[string]any{"type": jsonSchemaType(p.Type)}
			}
			entry["properties"].(map[string]any)["body"] = map[string]any{
				"type":       "object",
				"properties": bodyProps,
			}
		}

		oneOf = append(oneOf, entry)
	}

	root := map[string]any{
		"$schema":     "http://json-schema.org/draft-07/schema#",
		"description": "OTLP log record schema for instgraph ingestion",
		"oneOf":       oneOf,
	}

	data, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return fmt.Errorf("marshalling JSON schema: %w", err)
	}
	return os.WriteFile(outPath, data, 0644)
}

func jsonSchemaType(t schema.PropType) string {
	switch t {
	case schema.PropTypeString:
		return "string"
	case schema.PropTypeInt:
		return "integer"
	case schema.PropTypeFloat:
		return "number"
	case schema.PropTypeBool:
		return "boolean"
	default:
		return "string"
	}
}
