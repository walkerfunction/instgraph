package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/walkerfunction/instgraph/pkg/graph"
	"github.com/walkerfunction/instgraph/pkg/persist"
	"github.com/walkerfunction/instgraph/pkg/schema"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
)

// Server receives OTLP log records and applies them as graph mutations.
type Server struct {
	collogspb.UnimplementedLogsServiceServer
	graph     *graph.Graph
	persister *persist.Persister
	schema    *schema.Schema
	mu        sync.Mutex
}

// NewServer creates an OTLP ingestion server.
func NewServer(g *graph.Graph, p *persist.Persister, s *schema.Schema) *Server {
	return &Server{
		graph:     g,
		persister: p,
		schema:    s,
	}
}

// Export handles OTLP ExportLogsServiceRequest.
func (s *Server) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var accepted, rejected int64

	for _, rl := range req.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				if err := s.processRecord(lr); err != nil {
					log.Printf("ingest: rejected record: %v", err)
					rejected++
				} else {
					accepted++
				}
			}
		}
	}

	log.Printf("ingest: accepted=%d rejected=%d", accepted, rejected)
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func (s *Server) processRecord(lr *logspb.LogRecord) error {
	attrs := make(map[string]string)
	for _, kv := range lr.Attributes {
		attrs[kv.Key] = kv.Value.GetStringValue()
	}

	op := attrs["graph.op"]
	switch op {
	case "NODE":
		return s.processNode(attrs, lr)
	case "EDGE":
		return s.processEdge(attrs, lr)
	default:
		return fmt.Errorf("unknown graph.op: %q", op)
	}
}

func (s *Server) processNode(attrs map[string]string, lr *logspb.LogRecord) error {
	label := attrs["graph.label"]
	key := attrs["graph.key"]

	if label == "" || key == "" {
		return fmt.Errorf("NODE requires graph.label and graph.key")
	}

	if s.schema != nil {
		if _, ok := s.schema.Nodes[label]; !ok {
			return fmt.Errorf("unknown node label: %q", label)
		}
	}

	props := parseBody(lr)

	if _, err := s.graph.CreateNode(label, key, props); err != nil {
		return fmt.Errorf("CreateNode: %w", err)
	}

	if s.persister != nil {
		if err := s.persister.LogCreateNode(label, key, props); err != nil {
			return fmt.Errorf("WAL: %w", err)
		}
	}

	return nil
}

func (s *Server) processEdge(attrs map[string]string, lr *logspb.LogRecord) error {
	edgeType := attrs["graph.edge"]
	fromKey := attrs["graph.from"]
	toKey := attrs["graph.to"]

	if edgeType == "" || fromKey == "" || toKey == "" {
		return fmt.Errorf("EDGE requires graph.edge, graph.from, graph.to")
	}

	if s.schema != nil {
		if _, ok := s.schema.Edges[edgeType]; !ok {
			return fmt.Errorf("unknown edge type: %q", edgeType)
		}
	}

	fromNode, ok := s.graph.GetNodeByKey(fromKey)
	if !ok {
		return fmt.Errorf("from node not found: %q", fromKey)
	}
	toNode, ok := s.graph.GetNodeByKey(toKey)
	if !ok {
		return fmt.Errorf("to node not found: %q", toKey)
	}

	props := parseBody(lr)

	if _, err := s.graph.CreateEdge(edgeType, fromNode.ID, toNode.ID, props); err != nil {
		return fmt.Errorf("CreateEdge: %w", err)
	}

	if s.persister != nil {
		if err := s.persister.LogCreateEdge(edgeType, fromKey, toKey, props); err != nil {
			return fmt.Errorf("WAL: %w", err)
		}
	}

	return nil
}

func parseBody(lr *logspb.LogRecord) map[string]any {
	body := lr.Body
	if body == nil {
		return nil
	}

	// Body as KvlistValue — the standard OTLP way
	if kv := body.GetKvlistValue(); kv != nil {
		return kvListToMap(kv.Values)
	}

	// Body as JSON string — fallback
	if sv := body.GetStringValue(); sv != "" {
		var props map[string]any
		if err := json.Unmarshal([]byte(sv), &props); err == nil {
			return props
		}
	}

	return nil
}

func kvListToMap(kvs []*commonpb.KeyValue) map[string]any {
	m := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = anyValueToGo(kv.Value)
	}
	return m
}

func anyValueToGo(v *commonpb.AnyValue) any {
	if v == nil {
		return nil
	}
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_IntValue:
		return val.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return val.DoubleValue
	case *commonpb.AnyValue_BoolValue:
		return val.BoolValue
	case *commonpb.AnyValue_KvlistValue:
		return kvListToMap(val.KvlistValue.Values)
	case *commonpb.AnyValue_ArrayValue:
		arr := make([]any, len(val.ArrayValue.Values))
		for i, av := range val.ArrayValue.Values {
			arr[i] = anyValueToGo(av)
		}
		return arr
	default:
		return nil
	}
}

// Serve starts the OTLP gRPC server on the given address.
func Serve(addr string, srv *Server) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("listen %s: %w", addr, err)
	}

	gs := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(gs, srv)

	return gs, lis, nil
}
