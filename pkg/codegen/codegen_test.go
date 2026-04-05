package codegen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/walkerfunction/instgraph/pkg/schema"
)

var testSchemaJSON = []byte(`{
  "nodes": {
    "Player": {
      "name": { "type": "string", "required": true },
      "country": { "type": "string" },
      "role": { "type": "string" }
    },
    "Team": {
      "name": { "type": "string", "required": true },
      "type": { "type": "string" }
    }
  },
  "edges": {
    "PLAYS_FOR": { "from": "Player", "to": "Team" },
    "DISMISSED_BY": {
      "from": "Player",
      "to": "Player",
      "properties": {
        "type": { "type": "string" },
        "match": { "type": "string" }
      }
    }
  }
}`)

func testSchema(t *testing.T) *schema.Schema {
	t.Helper()
	s, err := schema.Parse(testSchemaJSON)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	return s
}

func TestGenerateGo(t *testing.T) {
	s := testSchema(t)
	dir := t.TempDir()
	outPath := filepath.Join(dir, "models.go")

	if err := GenerateGo(s, outPath); err != nil {
		t.Fatalf("GenerateGo() error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	content := string(data)

	// Check key interfaces exist.
	if !strings.Contains(content, "type Player interface") {
		t.Error("missing Player interface")
	}
	if !strings.Contains(content, "type Team interface") {
		t.Error("missing Team interface")
	}
	if !strings.Contains(content, "Name() string") {
		t.Error("missing Name() getter")
	}
	if !strings.Contains(content, "Country() string") {
		t.Error("missing Country() getter")
	}
	if !strings.Contains(content, "type PlaysFor interface") {
		t.Error("missing PlaysFor edge interface")
	}
	if !strings.Contains(content, "type DismissedBy interface") {
		t.Error("missing DismissedBy edge interface")
	}
}

func TestGenerateOTLPYAML(t *testing.T) {
	s := testSchema(t)
	dir := t.TempDir()
	outPath := filepath.Join(dir, "otlp-schema.yaml")

	if err := GenerateOTLPYAML(s, outPath); err != nil {
		t.Fatalf("GenerateOTLPYAML() error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, "graph.op: \"NODE\"") {
		t.Error("missing NODE op")
	}
	if !strings.Contains(content, "graph.op: \"EDGE\"") {
		t.Error("missing EDGE op")
	}
	if !strings.Contains(content, "graph.label: \"Player\"") {
		t.Error("missing Player label")
	}
	if !strings.Contains(content, "graph.edge: \"PLAYS_FOR\"") {
		t.Error("missing PLAYS_FOR edge")
	}
}

func TestGenerateOTLPJSON(t *testing.T) {
	s := testSchema(t)
	dir := t.TempDir()
	outPath := filepath.Join(dir, "otlp-schema.json")

	if err := GenerateOTLPJSON(s, outPath); err != nil {
		t.Fatalf("GenerateOTLPJSON() error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("reading output: %v", err)
	}
	content := string(data)

	if !strings.Contains(content, `"$schema"`) {
		t.Error("missing $schema")
	}
	if !strings.Contains(content, `"oneOf"`) {
		t.Error("missing oneOf")
	}
	if !strings.Contains(content, `"NODE"`) {
		t.Error("missing NODE const")
	}
	if !strings.Contains(content, `"EDGE"`) {
		t.Error("missing EDGE const")
	}
}

func TestGenerate(t *testing.T) {
	s := testSchema(t)
	dir := t.TempDir()

	if err := Generate(s, dir); err != nil {
		t.Fatalf("Generate() error: %v", err)
	}

	// Check all files exist.
	for _, name := range []string{"models.go", "otlp-schema.yaml", "otlp-schema.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("missing generated file %s: %v", name, err)
		}
	}
}
