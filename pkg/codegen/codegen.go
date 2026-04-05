package codegen

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/walkerfunction/instgraph/pkg/schema"
)

// Generate runs all code generation from a schema.
func Generate(s *schema.Schema, outDir string) error {
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	// Generate Go interfaces and structs.
	goPath := filepath.Join(outDir, "models.go")
	if err := GenerateGo(s, goPath); err != nil {
		return fmt.Errorf("generating Go code: %w", err)
	}

	// Generate OTLP schema YAML.
	yamlPath := filepath.Join(outDir, "otlp-schema.yaml")
	if err := GenerateOTLPYAML(s, yamlPath); err != nil {
		return fmt.Errorf("generating OTLP YAML: %w", err)
	}

	// Generate OTLP JSON Schema.
	jsonPath := filepath.Join(outDir, "otlp-schema.json")
	if err := GenerateOTLPJSON(s, jsonPath); err != nil {
		return fmt.Errorf("generating OTLP JSON Schema: %w", err)
	}

	return nil
}
