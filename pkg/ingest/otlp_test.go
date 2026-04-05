package ingest

import (
	"context"
	"testing"

	"github.com/walkerfunction/instgraph/pkg/graph"
	"github.com/walkerfunction/instgraph/pkg/schema"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

func testSchema() *schema.Schema {
	return &schema.Schema{
		Nodes: map[string]*schema.NodeType{
			"Player": {
				Name: "Player",
				Properties: map[string]schema.Property{
					"name":    {Name: "name", Type: schema.PropTypeString, Required: true},
					"country": {Name: "country", Type: schema.PropTypeString},
				},
			},
			"Team": {
				Name: "Team",
				Properties: map[string]schema.Property{
					"name": {Name: "name", Type: schema.PropTypeString, Required: true},
				},
			},
		},
		Edges: map[string]*schema.EdgeType{
			"PLAYS_FOR": {Name: "PLAYS_FOR", From: "Player", To: "Team"},
		},
	}
}

func makeLogRecord(attrs map[string]string, bodyKV map[string]any) *logspb.LogRecord {
	lr := &logspb.LogRecord{}

	for k, v := range attrs {
		lr.Attributes = append(lr.Attributes, &commonpb.KeyValue{
			Key:   k,
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
		})
	}

	if bodyKV != nil {
		kvs := make([]*commonpb.KeyValue, 0, len(bodyKV))
		for k, v := range bodyKV {
			var av *commonpb.AnyValue
			switch val := v.(type) {
			case string:
				av = &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: val}}
			case int64:
				av = &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: val}}
			}
			kvs = append(kvs, &commonpb.KeyValue{Key: k, Value: av})
		}
		lr.Body = &commonpb.AnyValue{
			Value: &commonpb.AnyValue_KvlistValue{
				KvlistValue: &commonpb.KeyValueList{Values: kvs},
			},
		}
	}

	return lr
}

func TestIngestNodeAndEdge(t *testing.T) {
	s := testSchema()
	g := graph.New(s)
	srv := NewServer(g, nil, s)

	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{
					makeLogRecord(
						map[string]string{"graph.op": "NODE", "graph.label": "Player", "graph.key": "kohli"},
						map[string]any{"name": "V Kohli", "country": "India"},
					),
					makeLogRecord(
						map[string]string{"graph.op": "NODE", "graph.label": "Team", "graph.key": "mi"},
						map[string]any{"name": "Mumbai Indians"},
					),
				},
			}},
		}},
	}

	_, err := srv.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	if g.NodeCount() != 2 {
		t.Fatalf("expected 2 nodes, got %d", g.NodeCount())
	}

	// Now create an edge
	edgeReq := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{
					makeLogRecord(
						map[string]string{"graph.op": "EDGE", "graph.edge": "PLAYS_FOR", "graph.from": "kohli", "graph.to": "mi"},
						nil,
					),
				},
			}},
		}},
	}

	_, err = srv.Export(context.Background(), edgeReq)
	if err != nil {
		t.Fatalf("Export edge: %v", err)
	}

	if g.EdgeCount() != 1 {
		t.Fatalf("expected 1 edge, got %d", g.EdgeCount())
	}

	// Verify node properties
	kohli, ok := g.GetNodeByKey("kohli")
	if !ok {
		t.Fatal("kohli not found")
	}
	if kohli.Properties["name"] != "V Kohli" {
		t.Fatalf("expected 'V Kohli', got %v", kohli.Properties["name"])
	}
}

func TestIngestRejectsUnknownLabel(t *testing.T) {
	s := testSchema()
	g := graph.New(s)
	srv := NewServer(g, nil, s)

	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{
					makeLogRecord(
						map[string]string{"graph.op": "NODE", "graph.label": "Unknown", "graph.key": "x"},
						nil,
					),
				},
			}},
		}},
	}

	// Should succeed (rejected records are logged, not returned as error)
	_, err := srv.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	// But no nodes should be created
	if g.NodeCount() != 0 {
		t.Fatalf("expected 0 nodes, got %d", g.NodeCount())
	}
}

func TestIngestUpsert(t *testing.T) {
	s := testSchema()
	g := graph.New(s)
	srv := NewServer(g, nil, s)

	// Create player
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{
					makeLogRecord(
						map[string]string{"graph.op": "NODE", "graph.label": "Player", "graph.key": "kohli"},
						map[string]any{"name": "V Kohli"},
					),
				},
			}},
		}},
	}

	srv.Export(context.Background(), req)

	// Upsert with new property
	req2 := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{
					makeLogRecord(
						map[string]string{"graph.op": "NODE", "graph.label": "Player", "graph.key": "kohli"},
						map[string]any{"country": "India"},
					),
				},
			}},
		}},
	}

	srv.Export(context.Background(), req2)

	if g.NodeCount() != 1 {
		t.Fatalf("expected 1 node (upsert), got %d", g.NodeCount())
	}

	kohli, _ := g.GetNodeByKey("kohli")
	if kohli.Properties["country"] != "India" {
		t.Fatalf("expected 'India', got %v", kohli.Properties["country"])
	}
}
