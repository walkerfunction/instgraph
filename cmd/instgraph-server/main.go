package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/walkerfunction/instgraph/pkg/cypher"
	"github.com/walkerfunction/instgraph/pkg/graph"
	"github.com/walkerfunction/instgraph/pkg/ingest"
	"github.com/walkerfunction/instgraph/pkg/nlquery"
	"github.com/walkerfunction/instgraph/pkg/persist"
	"github.com/walkerfunction/instgraph/pkg/schema"
	querypb "github.com/walkerfunction/instgraph/proto/querypb"

	"google.golang.org/grpc"
)

type queryServer struct {
	querypb.UnimplementedQueryServiceServer
	executor *cypher.Executor
	nlEngine *nlquery.Engine
}

func (s *queryServer) Cypher(ctx context.Context, req *querypb.CypherRequest) (*querypb.QueryResponse, error) {
	result, err := s.executor.ExecuteAndReturn(req.Query)
	if err != nil {
		return nil, fmt.Errorf("cypher: %w", err)
	}
	return toProtoResponse(result), nil
}

func (s *queryServer) NaturalLanguage(ctx context.Context, req *querypb.NLRequest) (*querypb.NLResponse, error) {
	if s.nlEngine == nil {
		return nil, fmt.Errorf("natural language queries require ANTHROPIC_API_KEY")
	}

	resp, err := s.nlEngine.Query(ctx, req.Question)
	if err != nil {
		// Still return the generated cypher if we have it
		if resp != nil {
			return &querypb.NLResponse{
				GeneratedCypher: resp.GeneratedQuery,
			}, err
		}
		return nil, err
	}

	return &querypb.NLResponse{
		GeneratedCypher: resp.GeneratedQuery,
		Results:         toProtoResponse(resp.Result),
	}, nil
}

func toProtoResponse(r *cypher.Result) *querypb.QueryResponse {
	if r == nil {
		return &querypb.QueryResponse{}
	}

	resp := &querypb.QueryResponse{
		Columns: r.Columns,
	}

	for _, row := range r.Rows {
		protoRow := &querypb.Row{
			Values: make(map[string]string),
		}
		for k, v := range row {
			protoRow.Values[k] = fmt.Sprintf("%v", v)
		}
		resp.Rows = append(resp.Rows, protoRow)
	}

	return resp
}

func main() {
	schemaPath := flag.String("schema", "", "path to schema.json")
	dataDir := flag.String("data", "data", "directory for Pebble persistence")
	otlpAddr := flag.String("otlp-addr", ":4317", "OTLP gRPC listen address")
	queryAddr := flag.String("query-addr", ":9090", "Cypher query gRPC listen address")
	nlAddr := flag.String("nl-addr", ":9091", "NL query gRPC listen address")
	snapshotInterval := flag.Duration("snapshot-interval", 5*time.Minute, "interval between snapshots")
	flag.Parse()

	if *schemaPath == "" {
		log.Fatal("--schema is required")
	}

	// Load schema
	s, err := schema.Load(*schemaPath)
	if err != nil {
		log.Fatalf("schema: %v", err)
	}
	log.Printf("schema loaded: %d node types, %d edge types", len(s.Nodes), len(s.Edges))

	// Open persistence
	p, err := persist.Open(*dataDir)
	if err != nil {
		log.Fatalf("persist: %v", err)
	}
	defer p.Close()

	// Recover or create graph
	g, err := p.Recover(s)
	if err != nil {
		log.Printf("no snapshot to recover, starting fresh: %v", err)
		g = graph.New(s)
	}
	log.Printf("graph ready: %d nodes, %d edges", g.NodeCount(), g.EdgeCount())

	// OTLP ingestion server (:4317)
	otlpSrv := ingest.NewServer(g, p, s)
	otlpGRPC, otlpLis, err := ingest.Serve(*otlpAddr, otlpSrv)
	if err != nil {
		log.Fatalf("otlp server: %v", err)
	}
	go func() {
		log.Printf("OTLP ingestion listening on %s", *otlpAddr)
		if err := otlpGRPC.Serve(otlpLis); err != nil {
			log.Fatalf("otlp serve: %v", err)
		}
	}()

	// Query server (:9090 + :9091)
	executor := cypher.NewExecutor(g)

	var nlEngine *nlquery.Engine
	if os.Getenv("ANTHROPIC_API_KEY") != "" {
		nlEngine = nlquery.New(g, s)
		log.Printf("NL query enabled (ANTHROPIC_API_KEY set)")
	} else {
		log.Printf("NL query disabled (no ANTHROPIC_API_KEY)")
	}

	qSrv := &queryServer{executor: executor, nlEngine: nlEngine}

	// Cypher query gRPC (:9090)
	queryGRPC := grpc.NewServer()
	querypb.RegisterQueryServiceServer(queryGRPC, qSrv)
	queryLis, err := net.Listen("tcp", *queryAddr)
	if err != nil {
		log.Fatalf("query listen: %v", err)
	}
	go func() {
		log.Printf("Cypher query listening on %s", *queryAddr)
		if err := queryGRPC.Serve(queryLis); err != nil {
			log.Fatalf("query serve: %v", err)
		}
	}()

	// NL query gRPC (:9091)
	nlGRPC := grpc.NewServer()
	querypb.RegisterQueryServiceServer(nlGRPC, qSrv)
	nlLis, err := net.Listen("tcp", *nlAddr)
	if err != nil {
		log.Fatalf("nl listen: %v", err)
	}
	go func() {
		log.Printf("NL query listening on %s", *nlAddr)
		if err := nlGRPC.Serve(nlLis); err != nil {
			log.Fatalf("nl serve: %v", err)
		}
	}()

	// Periodic snapshots
	go func() {
		ticker := time.NewTicker(*snapshotInterval)
		defer ticker.Stop()
		for range ticker.C {
			log.Printf("taking snapshot (nodes=%d, edges=%d)...", g.NodeCount(), g.EdgeCount())
			if err := p.TakeSnapshot(g); err != nil {
				log.Printf("snapshot error: %v", err)
			} else {
				log.Printf("snapshot complete")
			}
		}
	}()

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("received %s, shutting down...", sig)

	// Take final snapshot
	log.Printf("taking final snapshot...")
	if err := p.TakeSnapshot(g); err != nil {
		log.Printf("final snapshot error: %v", err)
	}

	otlpGRPC.GracefulStop()
	queryGRPC.GracefulStop()
	nlGRPC.GracefulStop()

	log.Printf("shutdown complete")
}
