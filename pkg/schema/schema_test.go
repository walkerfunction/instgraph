package schema

import (
	"os"
	"path/filepath"
	"testing"
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

func TestParse(t *testing.T) {
	s, err := Parse(testSchemaJSON)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}

	if len(s.Nodes) != 2 {
		t.Errorf("expected 2 node types, got %d", len(s.Nodes))
	}
	if len(s.Edges) != 2 {
		t.Errorf("expected 2 edge types, got %d", len(s.Edges))
	}

	player := s.Nodes["Player"]
	if player == nil {
		t.Fatal("Player node type not found")
	}
	if len(player.Properties) != 3 {
		t.Errorf("expected 3 Player properties, got %d", len(player.Properties))
	}
	if !player.Properties["name"].Required {
		t.Error("Player.name should be required")
	}

	dismissed := s.Edges["DISMISSED_BY"]
	if dismissed == nil {
		t.Fatal("DISMISSED_BY edge type not found")
	}
	if dismissed.From != "Player" || dismissed.To != "Player" {
		t.Errorf("DISMISSED_BY from/to wrong: %s -> %s", dismissed.From, dismissed.To)
	}
	if len(dismissed.Properties) != 2 {
		t.Errorf("expected 2 DISMISSED_BY properties, got %d", len(dismissed.Properties))
	}
}

func TestParseInvalidEdgeRef(t *testing.T) {
	bad := []byte(`{
		"nodes": { "Player": { "name": { "type": "string" } } },
		"edges": { "BAD": { "from": "Player", "to": "NonExistent" } }
	}`)
	_, err := Parse(bad)
	if err == nil {
		t.Fatal("expected error for invalid edge reference")
	}
}

func TestParseInvalidPropType(t *testing.T) {
	bad := []byte(`{
		"nodes": { "Player": { "name": { "type": "invalid_type" } } },
		"edges": {}
	}`)
	_, err := Parse(bad)
	if err == nil {
		t.Fatal("expected error for invalid property type")
	}
}

func TestLockAppendOnly(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, ".schema.lock")

	// First parse — creates lock.
	s, err := Parse(testSchemaJSON)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if err := CheckLock(s, lockPath); err != nil {
		t.Fatalf("first CheckLock() error: %v", err)
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("lock file not created: %v", err)
	}

	// Same schema — should pass.
	if err := CheckLock(s, lockPath); err != nil {
		t.Fatalf("same schema CheckLock() error: %v", err)
	}

	// Schema with addition — should pass.
	added := []byte(`{
		"nodes": {
			"Player": {
				"name": { "type": "string", "required": true },
				"country": { "type": "string" },
				"role": { "type": "string" },
				"age": { "type": "int" }
			},
			"Team": {
				"name": { "type": "string", "required": true },
				"type": { "type": "string" }
			},
			"Match": {
				"date": { "type": "string" }
			}
		},
		"edges": {
			"PLAYS_FOR": { "from": "Player", "to": "Team" },
			"DISMISSED_BY": {
				"from": "Player",
				"to": "Player",
				"properties": { "type": { "type": "string" }, "match": { "type": "string" } }
			},
			"PLAYED_IN": { "from": "Player", "to": "Match" }
		}
	}`)
	s2, err := Parse(added)
	if err != nil {
		t.Fatalf("Parse added schema error: %v", err)
	}
	if err := CheckLock(s2, lockPath); err != nil {
		t.Fatalf("added schema CheckLock() error: %v", err)
	}

	// Schema with removed node — should fail.
	removed := []byte(`{
		"nodes": {
			"Player": {
				"name": { "type": "string", "required": true },
				"country": { "type": "string" },
				"role": { "type": "string" },
				"age": { "type": "int" }
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
				"properties": { "type": { "type": "string" }, "match": { "type": "string" } }
			}
		}
	}`)
	s3, err := Parse(removed)
	if err != nil {
		t.Fatalf("Parse removed schema error: %v", err)
	}
	if err := CheckLock(s3, lockPath); err == nil {
		t.Fatal("expected error when node type removed")
	}

	// Schema with removed property — should fail.
	removedProp := []byte(`{
		"nodes": {
			"Player": {
				"name": { "type": "string", "required": true },
				"country": { "type": "string" }
			},
			"Team": {
				"name": { "type": "string", "required": true },
				"type": { "type": "string" }
			},
			"Match": {
				"date": { "type": "string" }
			}
		},
		"edges": {
			"PLAYS_FOR": { "from": "Player", "to": "Team" },
			"DISMISSED_BY": {
				"from": "Player",
				"to": "Player",
				"properties": { "type": { "type": "string" }, "match": { "type": "string" } }
			},
			"PLAYED_IN": { "from": "Player", "to": "Match" }
		}
	}`)
	s4, err := Parse(removedProp)
	if err != nil {
		t.Fatalf("Parse removedProp schema error: %v", err)
	}
	if err := CheckLock(s4, lockPath); err == nil {
		t.Fatal("expected error when property removed")
	}
}
