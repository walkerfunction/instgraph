package nlquery

import (
	"context"
	"fmt"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/walkerfunction/instgraph/pkg/cypher"
	"github.com/walkerfunction/instgraph/pkg/graph"
	"github.com/walkerfunction/instgraph/pkg/schema"
)

// Engine translates natural language questions to Cypher and executes them.
type Engine struct {
	client   *anthropic.Client
	executor *cypher.Executor
	schema   *schema.Schema
}

// Response contains the generated Cypher and query results.
type Response struct {
	Question       string           `json:"question"`
	GeneratedQuery string           `json:"generated_query"`
	Result         *cypher.Result   `json:"result"`
}

// New creates a new NL query engine. Requires ANTHROPIC_API_KEY env var.
func New(g *graph.Graph, s *schema.Schema) *Engine {
	client := anthropic.NewClient()
	return &Engine{
		client:   &client,
		executor: cypher.NewExecutor(g),
		schema:   s,
	}
}

// Query translates a natural language question to Cypher and executes it.
func (e *Engine) Query(ctx context.Context, question string) (*Response, error) {
	prompt := e.buildPrompt(question)

	msg, err := e.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     "claude-sonnet-4-20250514",
		MaxTokens: 1024,
		Messages: []anthropic.MessageParam{
			anthropic.NewUserMessage(anthropic.NewTextBlock(prompt)),
		},
		System: []anthropic.TextBlockParam{
			{Text: "You are a Cypher query generator. Given a graph schema and a natural language question, output ONLY a valid Cypher query. No explanation, no markdown, no backticks — just the query."},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("claude API: %w", err)
	}

	cypherQuery := ""
	for _, block := range msg.Content {
		if block.Type == "text" {
			cypherQuery = block.Text
			break
		}
	}

	if cypherQuery == "" {
		return nil, fmt.Errorf("no Cypher query generated")
	}

	result, err := e.executor.Execute(cypherQuery)
	if err != nil {
		return &Response{
			Question:       question,
			GeneratedQuery: cypherQuery,
		}, fmt.Errorf("executing generated Cypher: %w", err)
	}

	return &Response{
		Question:       question,
		GeneratedQuery: cypherQuery,
		Result:         result,
	}, nil
}

func (e *Engine) buildPrompt(question string) string {
	s := "Graph Schema:\n\nNode Types:\n"
	for _, name := range e.schema.SortedNodeNames() {
		nt := e.schema.Nodes[name]
		s += fmt.Sprintf("  %s:", name)
		for _, pn := range schema.SortedPropertyNames(nt.Properties) {
			p := nt.Properties[pn]
			req := ""
			if p.Required {
				req = ", required"
			}
			s += fmt.Sprintf(" %s(%s%s)", pn, p.Type, req)
		}
		s += "\n"
	}

	s += "\nEdge Types:\n"
	for _, name := range e.schema.SortedEdgeNames() {
		et := e.schema.Edges[name]
		s += fmt.Sprintf("  %s: %s -> %s", name, et.From, et.To)
		if len(et.Properties) > 0 {
			s += " ["
			for _, pn := range schema.SortedPropertyNames(et.Properties) {
				p := et.Properties[pn]
				s += fmt.Sprintf(" %s(%s)", pn, p.Type)
			}
			s += " ]"
		}
		s += "\n"
	}

	s += fmt.Sprintf("\nQuestion: %s\n\nGenerate a Cypher query to answer this question.", question)
	return s
}
