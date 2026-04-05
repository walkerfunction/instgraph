package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/walkerfunction/instgraph/pkg/codegen"
	"github.com/walkerfunction/instgraph/pkg/schema"
)

func main() {
	schemaPath := flag.String("schema", "", "path to schema.json")
	outDir := flag.String("out", "./generated", "output directory for generated code")
	flag.Parse()

	if *schemaPath == "" {
		fmt.Fprintln(os.Stderr, "error: --schema is required")
		flag.Usage()
		os.Exit(1)
	}

	s, err := schema.Load(*schemaPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading schema: %v\n", err)
		os.Exit(1)
	}

	if err := s.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "schema validation error: %v\n", err)
		os.Exit(1)
	}

	// Check/update schema lock (append-only enforcement).
	lockPath := filepath.Join(filepath.Dir(*schemaPath), ".schema.lock")
	if err := schema.CheckLock(s, lockPath); err != nil {
		fmt.Fprintf(os.Stderr, "schema lock error: %v\n", err)
		os.Exit(1)
	}

	if err := codegen.Generate(s, *outDir); err != nil {
		fmt.Fprintf(os.Stderr, "codegen error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Generated code in %s\n", *outDir)
}
