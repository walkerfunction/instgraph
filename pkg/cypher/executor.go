package cypher

import (
	"fmt"
	"math"
	"strings"

	"github.com/walkerfunction/instgraph/pkg/graph"
)

// Result represents a query result set.
type Result struct {
	Columns []string
	Rows    []map[string]any
}

// Executor executes Cypher queries against a graph.
type Executor struct {
	g *graph.Graph
}

// NewExecutor creates a new Cypher executor.
func NewExecutor(g *graph.Graph) *Executor {
	return &Executor{g: g}
}

// Execute parses and executes a Cypher query.
func (e *Executor) Execute(query string) (*Result, error) {
	ast, err := Parse(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	return e.executeQuery(ast)
}

// binding represents variable bindings during execution.
type binding map[string]any

func (e *Executor) executeQuery(q *Query) (*Result, error) {
	bindings := []binding{{}}

	for _, clause := range q.Clauses {
		var err error
		bindings, err = e.executeClause(clause, bindings)
		if err != nil {
			return nil, err
		}
	}

	// Extract result from bindings
	for _, b := range bindings {
		if r, ok := b["__result"]; ok {
			return r.(*Result), nil
		}
	}

	return &Result{}, nil
}

func (e *Executor) executeClause(clause Clause, bindings []binding) ([]binding, error) {
	switch c := clause.(type) {
	case MatchClause:
		return e.executeMatch(c, bindings)
	case *matchWithWhere:
		newBindings, err := e.executeMatch(c.Match, bindings)
		if err != nil {
			return nil, err
		}
		return e.filterBindings(newBindings, c.Where)
	case WhereClause:
		return e.filterBindings(bindings, c.Expr)
	case ReturnClause:
		return e.executeReturn(c, bindings)
	case CreateClause:
		return e.executeCreate(c, bindings)
	case DeleteClause:
		return e.executeDelete(c, bindings)
	case SetClause:
		return e.executeSet(c, bindings)
	case WithClause:
		return e.executeWith(c, bindings)
	default:
		return nil, fmt.Errorf("unsupported clause type: %T", clause)
	}
}

func (e *Executor) executeMatch(mc MatchClause, bindings []binding) ([]binding, error) {
	result := bindings
	for _, pp := range mc.Pattern {
		var err error
		result, err = e.matchPathPattern(pp, result)
		if err != nil {
			return nil, err
		}
	}
	if mc.Optional && len(result) == 0 {
		return bindings, nil
	}
	return result, nil
}

func (e *Executor) matchPathPattern(pp PathPattern, bindings []binding) ([]binding, error) {
	if len(pp.Elements) == 0 {
		return bindings, nil
	}

	var result []binding

	for _, b := range bindings {
		matches, err := e.expandPattern(pp.Elements, 0, b)
		if err != nil {
			return nil, err
		}
		result = append(result, matches...)
	}

	return result, nil
}

func (e *Executor) expandPattern(elements []PatternElement, idx int, b binding) ([]binding, error) {
	if idx >= len(elements) {
		cp := make(binding, len(b))
		for k, v := range b {
			cp[k] = v
		}
		return []binding{cp}, nil
	}

	elem := elements[idx]

	switch el := elem.(type) {
	case NodePattern:
		return e.matchNodePattern(el, elements, idx, b)
	default:
		return nil, fmt.Errorf("unexpected pattern element at position %d", idx)
	}
}

func (e *Executor) matchNodePattern(np NodePattern, elements []PatternElement, idx int, b binding) ([]binding, error) {
	// If variable already bound, use that node.
	if np.Variable != "" {
		if existing, ok := b[np.Variable]; ok {
			node, ok := existing.(*graph.Node)
			if !ok {
				return nil, fmt.Errorf("variable %s is not a node", np.Variable)
			}
			if !e.nodeMatchesPattern(node, np) {
				return nil, nil
			}
			return e.continueAfterNode(node, np, elements, idx, b)
		}
	}

	// Find candidate nodes.
	var candidates []graph.NodeID
	if len(np.Labels) > 0 {
		candidates = e.g.NodesByLabel(np.Labels[0])
	} else {
		// All nodes - collect from all labels.
		if e.g.Schema() != nil {
			for _, name := range e.g.Schema().SortedNodeNames() {
				candidates = append(candidates, e.g.NodesByLabel(name)...)
			}
		}
	}

	var result []binding
	for _, nid := range candidates {
		node := e.g.GetNode(nid)
		if node == nil {
			continue
		}
		if !e.nodeMatchesPattern(node, np) {
			continue
		}

		matches, err := e.continueAfterNode(node, np, elements, idx, b)
		if err != nil {
			return nil, err
		}
		result = append(result, matches...)
	}

	return result, nil
}

func (e *Executor) nodeMatchesPattern(node *graph.Node, np NodePattern) bool {
	// Check labels.
	for _, label := range np.Labels {
		if e.g.LabelName(node.Label) != label {
			return false
		}
	}
	// Check inline properties.
	for k, exprVal := range np.Properties {
		propVal, ok := node.Properties[k]
		if !ok {
			return false
		}
		litVal := e.evalLiteral(exprVal)
		if !valuesEqual(propVal, litVal) {
			return false
		}
	}
	return true
}

func (e *Executor) continueAfterNode(node *graph.Node, np NodePattern, elements []PatternElement, idx int, b binding) ([]binding, error) {
	newB := copyBinding(b)
	if np.Variable != "" {
		newB[np.Variable] = node
	}

	// If no more elements, done.
	if idx+1 >= len(elements) {
		return []binding{newB}, nil
	}

	// Next should be a RelPattern.
	relPat, ok := elements[idx+1].(RelPattern)
	if !ok {
		return nil, fmt.Errorf("expected relationship pattern after node")
	}

	// Then another NodePattern.
	if idx+2 >= len(elements) {
		return nil, fmt.Errorf("relationship pattern must be followed by a node pattern")
	}
	nextNodePat, ok := elements[idx+2].(NodePattern)
	if !ok {
		return nil, fmt.Errorf("expected node pattern after relationship")
	}

	return e.expandRelationship(node, relPat, nextNodePat, elements, idx+2, newB)
}

func (e *Executor) expandRelationship(fromNode *graph.Node, rp RelPattern, nextNP NodePattern, elements []PatternElement, nextIdx int, b binding) ([]binding, error) {
	minHops := 1
	maxHops := 1
	if rp.MinHops != nil {
		minHops = *rp.MinHops
	}
	if rp.MaxHops != nil {
		maxHops = *rp.MaxHops
	}
	if rp.MinHops == nil && rp.MaxHops == nil {
		// Single hop.
		return e.expandSingleHop(fromNode, rp, nextNP, elements, nextIdx, b)
	}

	// Variable-length path.
	var result []binding
	visited := map[graph.NodeID]bool{fromNode.ID: true}
	current := []*graph.Node{fromNode}

	for hop := 1; hop <= maxHops && len(current) > 0; hop++ {
		var next []*graph.Node
		for _, node := range current {
			neighbors := e.getRelNeighbors(node.ID, rp)
			for _, nbrID := range neighbors {
				if visited[nbrID] {
					continue
				}
				visited[nbrID] = true
				nbrNode := e.g.GetNode(nbrID)
				if nbrNode == nil {
					continue
				}
				next = append(next, nbrNode)

				if hop >= minHops && e.nodeMatchesPattern(nbrNode, nextNP) {
					newB := copyBinding(b)
					if rp.Variable != "" {
						newB[rp.Variable] = "path" // placeholder
					}
					if nextNP.Variable != "" {
						newB[nextNP.Variable] = nbrNode
					}
					// Continue with remaining pattern.
					if nextIdx+1 >= len(elements) {
						result = append(result, newB)
					} else {
						more, err := e.expandPattern(elements, nextIdx+1, newB)
						if err != nil {
							return nil, err
						}
						result = append(result, more...)
					}
				}
			}
		}
		current = next
	}

	return result, nil
}

func (e *Executor) expandSingleHop(fromNode *graph.Node, rp RelPattern, nextNP NodePattern, elements []PatternElement, nextIdx int, b binding) ([]binding, error) {
	neighbors := e.getRelNeighbors(fromNode.ID, rp)

	var result []binding
	for _, nbrID := range neighbors {
		nbrNode := e.g.GetNode(nbrID)
		if nbrNode == nil {
			continue
		}
		if !e.nodeMatchesPattern(nbrNode, nextNP) {
			continue
		}

		// Check if variable already bound.
		if nextNP.Variable != "" {
			if existing, ok := b[nextNP.Variable]; ok {
				existNode := existing.(*graph.Node)
				if existNode.ID != nbrNode.ID {
					continue
				}
			}
		}

		newB := copyBinding(b)
		if rp.Variable != "" {
			// Find the actual edge for edge variable binding.
			edge := e.findEdgeBetween(fromNode.ID, nbrID, rp)
			if edge != nil {
				newB[rp.Variable] = edge
			}
		}
		if nextNP.Variable != "" {
			newB[nextNP.Variable] = nbrNode
		}

		if nextIdx+1 >= len(elements) {
			result = append(result, newB)
		} else {
			more, err := e.expandPattern(elements, nextIdx+1, newB)
			if err != nil {
				return nil, err
			}
			result = append(result, more...)
		}
	}

	return result, nil
}

func (e *Executor) getRelNeighbors(nodeID graph.NodeID, rp RelPattern) []graph.NodeID {
	var dir graph.Direction
	switch rp.Direction {
	case DirRight:
		dir = graph.Out
	case DirLeft:
		dir = graph.In
	default:
		dir = graph.Both
	}

	if len(rp.Types) == 0 {
		return e.g.Neighbors(nodeID, "", dir)
	}

	seen := make(map[graph.NodeID]struct{})
	var result []graph.NodeID
	for _, t := range rp.Types {
		for _, nid := range e.g.Neighbors(nodeID, t, dir) {
			if _, ok := seen[nid]; !ok {
				seen[nid] = struct{}{}
				result = append(result, nid)
			}
		}
	}
	return result
}

func (e *Executor) findEdgeBetween(from, to graph.NodeID, rp RelPattern) *graph.Edge {
	for _, t := range rp.Types {
		for _, eid := range e.g.EdgesByType(t) {
			edge := e.g.GetEdge(eid)
			if edge == nil {
				continue
			}
			if rp.Direction == DirRight && edge.From == from && edge.To == to {
				return edge
			}
			if rp.Direction == DirLeft && edge.From == to && edge.To == from {
				return edge
			}
			if rp.Direction == DirBoth {
				if (edge.From == from && edge.To == to) || (edge.From == to && edge.To == from) {
					return edge
				}
			}
		}
	}
	return nil
}

func (e *Executor) filterBindings(bindings []binding, expr Expr) ([]binding, error) {
	var result []binding
	for _, b := range bindings {
		val, err := e.evalExpr(expr, b)
		if err != nil {
			return nil, err
		}
		if toBool(val) {
			result = append(result, b)
		}
	}
	return result, nil
}

func (e *Executor) executeReturn(rc ReturnClause, bindings []binding) ([]binding, error) {
	// Check for aggregation.
	hasAgg := false
	for _, item := range rc.Items {
		if containsAggFunc(item.Expr) {
			hasAgg = true
			break
		}
	}

	var rows []map[string]any
	var columns []string

	if hasAgg {
		rows, columns = e.executeAggReturn(rc, bindings)
	} else {
		for _, b := range bindings {
			row := make(map[string]any)
			for i, item := range rc.Items {
				val, _ := e.evalExpr(item.Expr, b)
				col := item.Alias
				if col == "" {
					col = exprToString(item.Expr)
				}
				if i == 0 || len(columns) <= i {
					columns = appendUnique(columns, col)
				}
				row[col] = resolveValue(val)
			}
			rows = append(rows, row)
		}
	}

	// ORDER BY
	if len(rc.OrderBy) > 0 {
		sortRows(rows, rc.OrderBy, columns)
	}

	// SKIP
	if rc.Skip != nil {
		skipVal, _ := e.evalExpr(*rc.Skip, binding{})
		if n, ok := toInt(skipVal); ok && int(n) < len(rows) {
			rows = rows[n:]
		}
	}

	// LIMIT
	if rc.Limit != nil {
		limitVal, _ := e.evalExpr(*rc.Limit, binding{})
		if n, ok := toInt(limitVal); ok && int(n) < len(rows) {
			rows = rows[:n]
		}
	}

	// DISTINCT
	if rc.Distinct {
		rows = deduplicateRows(rows, columns)
	}

	// Store result in a special binding.
	return []binding{{"__result": &Result{Columns: columns, Rows: rows}}}, nil
}

func (e *Executor) executeAggReturn(rc ReturnClause, bindings []binding) ([]map[string]any, []string) {
	// Group by non-aggregate expressions.
	type groupKey string

	groups := make(map[groupKey][]binding)
	groupOrder := make([]groupKey, 0)

	for _, b := range bindings {
		var keyParts []string
		for _, item := range rc.Items {
			if !containsAggFunc(item.Expr) {
				val, _ := e.evalExpr(item.Expr, b)
				keyParts = append(keyParts, fmt.Sprintf("%v", resolveValue(val)))
			}
		}
		key := groupKey(strings.Join(keyParts, "|"))
		if _, ok := groups[key]; !ok {
			groupOrder = append(groupOrder, key)
		}
		groups[key] = append(groups[key], b)
	}

	var rows []map[string]any
	var columns []string

	for _, key := range groupOrder {
		group := groups[key]
		row := make(map[string]any)

		for i, item := range rc.Items {
			col := item.Alias
			if col == "" {
				col = exprToString(item.Expr)
			}
			if i == 0 || len(columns) <= i {
				columns = appendUnique(columns, col)
			}

			if containsAggFunc(item.Expr) {
				row[col] = e.evalAggFunc(item.Expr, group)
			} else {
				val, _ := e.evalExpr(item.Expr, group[0])
				row[col] = resolveValue(val)
			}
		}
		rows = append(rows, row)
	}

	return rows, columns
}

func (e *Executor) evalAggFunc(expr Expr, group []binding) any {
	fc, ok := expr.(FuncCall)
	if !ok {
		return nil
	}

	switch strings.ToUpper(fc.Name) {
	case "COUNT":
		if len(fc.Args) == 1 {
			if lit, ok := fc.Args[0].(Literal); ok && lit.Value == "*" {
				return int64(len(group))
			}
		}
		count := int64(0)
		seen := make(map[string]struct{})
		for _, b := range group {
			val, _ := e.evalExpr(fc.Args[0], b)
			if val != nil {
				if fc.Distinct {
					key := fmt.Sprintf("%v", resolveValue(val))
					if _, ok := seen[key]; ok {
						continue
					}
					seen[key] = struct{}{}
				}
				count++
			}
		}
		return count

	case "COLLECT":
		var result []any
		seen := make(map[string]struct{})
		for _, b := range group {
			val, _ := e.evalExpr(fc.Args[0], b)
			rv := resolveValue(val)
			if fc.Distinct {
				key := fmt.Sprintf("%v", rv)
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
			}
			result = append(result, rv)
		}
		return result

	case "SUM":
		sum := float64(0)
		for _, b := range group {
			val, _ := e.evalExpr(fc.Args[0], b)
			if n, ok := toFloat(resolveValue(val)); ok {
				sum += n
			}
		}
		if sum == math.Trunc(sum) {
			return int64(sum)
		}
		return sum

	case "AVG":
		sum := float64(0)
		count := 0
		for _, b := range group {
			val, _ := e.evalExpr(fc.Args[0], b)
			if n, ok := toFloat(resolveValue(val)); ok {
				sum += n
				count++
			}
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)

	case "MIN":
		var minVal any
		for _, b := range group {
			val, _ := e.evalExpr(fc.Args[0], b)
			rv := resolveValue(val)
			if minVal == nil || compareAny(rv, minVal) < 0 {
				minVal = rv
			}
		}
		return minVal

	case "MAX":
		var maxVal any
		for _, b := range group {
			val, _ := e.evalExpr(fc.Args[0], b)
			rv := resolveValue(val)
			if maxVal == nil || compareAny(rv, maxVal) > 0 {
				maxVal = rv
			}
		}
		return maxVal
	}

	return nil
}

func (e *Executor) executeCreate(cc CreateClause, bindings []binding) ([]binding, error) {
	for _, b := range bindings {
		for _, pp := range cc.Pattern {
			if err := e.createFromPattern(pp, b); err != nil {
				return nil, err
			}
		}
	}
	return bindings, nil
}

func (e *Executor) createFromPattern(pp PathPattern, b binding) error {
	for i, elem := range pp.Elements {
		switch el := elem.(type) {
		case NodePattern:
			if el.Variable != "" {
				if _, ok := b[el.Variable]; ok {
					continue // already exists
				}
			}
			props := make(map[string]any)
			for k, v := range el.Properties {
				val, _ := e.evalExpr(v, b)
				props[k] = resolveValue(val)
			}
			label := ""
			if len(el.Labels) > 0 {
				label = el.Labels[0]
			}
			key := ""
			if k, ok := props["name"]; ok {
				key = fmt.Sprintf("%v", k)
			} else {
				key = fmt.Sprintf("auto_%d", i)
			}
			nid, err := e.g.CreateNode(label, key, props)
			if err != nil {
				return err
			}
			if el.Variable != "" {
				b[el.Variable] = e.g.GetNode(nid)
			}
		}
	}

	// Second pass: create edges.
	for i := 1; i+1 < len(pp.Elements); i += 2 {
		rp, ok := pp.Elements[i].(RelPattern)
		if !ok {
			continue
		}
		fromNode, ok := b[pp.Elements[i-1].(NodePattern).Variable]
		if !ok {
			continue
		}
		toNode, ok := b[pp.Elements[i+1].(NodePattern).Variable]
		if !ok {
			continue
		}

		from := fromNode.(*graph.Node)
		to := toNode.(*graph.Node)
		edgeType := ""
		if len(rp.Types) > 0 {
			edgeType = rp.Types[0]
		}
		_, err := e.g.CreateEdge(edgeType, from.ID, to.ID, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Executor) executeDelete(dc DeleteClause, bindings []binding) ([]binding, error) {
	for _, b := range bindings {
		for _, expr := range dc.Exprs {
			val, err := e.evalExpr(expr, b)
			if err != nil {
				return nil, err
			}
			switch v := val.(type) {
			case *graph.Node:
				if dc.Detach {
					e.g.DeleteNode(v.ID)
				} else {
					e.g.DeleteNode(v.ID)
				}
			case *graph.Edge:
				e.g.DeleteEdge(v.ID)
			}
		}
	}
	return bindings, nil
}

func (e *Executor) executeSet(sc SetClause, bindings []binding) ([]binding, error) {
	for _, b := range bindings {
		for _, item := range sc.Items {
			nodeVal, ok := b[item.Property.Variable]
			if !ok {
				return nil, fmt.Errorf("variable %s not found", item.Property.Variable)
			}
			node, ok := nodeVal.(*graph.Node)
			if !ok {
				return nil, fmt.Errorf("SET only supported on nodes")
			}
			val, err := e.evalExpr(item.Value, b)
			if err != nil {
				return nil, err
			}
			if err := e.g.SetProperty(node.ID, item.Property.Property, resolveValue(val)); err != nil {
				return nil, err
			}
		}
	}
	return bindings, nil
}

func (e *Executor) executeWith(wc WithClause, bindings []binding) ([]binding, error) {
	var result []binding
	for _, b := range bindings {
		newB := make(binding)
		for _, item := range wc.Items {
			val, _ := e.evalExpr(item.Expr, b)
			col := item.Alias
			if col == "" {
				col = exprToString(item.Expr)
			}
			newB[col] = val
		}
		result = append(result, newB)
	}

	if wc.Where != nil {
		var err error
		result, err = e.filterBindings(result, wc.Where.Expr)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// evalExpr evaluates an expression in the context of a binding.
func (e *Executor) evalExpr(expr Expr, b binding) (any, error) {
	switch ex := expr.(type) {
	case Literal:
		return ex.Value, nil
	case Ident:
		return b[ex.Name], nil
	case PropertyAccess:
		return e.evalPropertyAccess(ex, b)
	case BinaryExpr:
		return e.evalBinary(ex, b)
	case UnaryExpr:
		return e.evalUnary(ex, b)
	case FuncCall:
		return e.evalFuncCall(ex, b)
	case IsNullExpr:
		val, err := e.evalExpr(ex.Expr, b)
		if err != nil {
			return nil, err
		}
		isNull := val == nil
		if ex.Negate {
			return !isNull, nil
		}
		return isNull, nil
	case ListLiteral:
		var result []any
		for _, elem := range ex.Elements {
			val, err := e.evalExpr(elem, b)
			if err != nil {
				return nil, err
			}
			result = append(result, resolveValue(val))
		}
		return result, nil
	case ShortestPathExpr:
		return e.evalShortestPath(ex, b)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (e *Executor) evalPropertyAccess(pa PropertyAccess, b binding) (any, error) {
	val, ok := b[pa.Variable]
	if !ok {
		return nil, nil
	}
	switch v := val.(type) {
	case *graph.Node:
		return v.Properties[pa.Property], nil
	case *graph.Edge:
		return v.Properties[pa.Property], nil
	case map[string]any:
		return v[pa.Property], nil
	}
	return nil, nil
}

func (e *Executor) evalBinary(be BinaryExpr, b binding) (any, error) {
	left, err := e.evalExpr(be.Left, b)
	if err != nil {
		return nil, err
	}
	right, err := e.evalExpr(be.Right, b)
	if err != nil {
		return nil, err
	}

	lv := resolveValue(left)
	rv := resolveValue(right)

	switch be.Op {
	case "=":
		return valuesEqual(lv, rv), nil
	case "<>":
		return !valuesEqual(lv, rv), nil
	case "<":
		return compareAny(lv, rv) < 0, nil
	case ">":
		return compareAny(lv, rv) > 0, nil
	case "<=":
		return compareAny(lv, rv) <= 0, nil
	case ">=":
		return compareAny(lv, rv) >= 0, nil
	case "AND":
		return toBool(lv) && toBool(rv), nil
	case "OR":
		return toBool(lv) || toBool(rv), nil
	case "+":
		return addValues(lv, rv), nil
	case "-":
		return subValues(lv, rv), nil
	case "*":
		return mulValues(lv, rv), nil
	case "/":
		return divValues(lv, rv), nil
	case "%":
		return modValues(lv, rv), nil
	case "IN":
		if list, ok := rv.([]any); ok {
			for _, item := range list {
				if valuesEqual(lv, item) {
					return true, nil
				}
			}
		}
		return false, nil
	case "CONTAINS":
		ls, lok := lv.(string)
		rs, rok := rv.(string)
		if lok && rok {
			return strings.Contains(ls, rs), nil
		}
		return false, nil
	case "STARTS WITH":
		ls, lok := lv.(string)
		rs, rok := rv.(string)
		if lok && rok {
			return strings.HasPrefix(ls, rs), nil
		}
		return false, nil
	case "ENDS WITH":
		ls, lok := lv.(string)
		rs, rok := rv.(string)
		if lok && rok {
			return strings.HasSuffix(ls, rs), nil
		}
		return false, nil
	}

	return nil, fmt.Errorf("unsupported operator: %s", be.Op)
}

func (e *Executor) evalUnary(ue UnaryExpr, b binding) (any, error) {
	val, err := e.evalExpr(ue.Expr, b)
	if err != nil {
		return nil, err
	}
	switch ue.Op {
	case "NOT":
		return !toBool(val), nil
	case "-":
		if n, ok := toFloat(val); ok {
			return -n, nil
		}
		return nil, nil
	}
	return nil, fmt.Errorf("unsupported unary op: %s", ue.Op)
}

func (e *Executor) evalFuncCall(fc FuncCall, b binding) (any, error) {
	switch strings.ToUpper(fc.Name) {
	case "SIZE":
		if len(fc.Args) != 1 {
			return nil, fmt.Errorf("size() takes 1 argument")
		}
		val, err := e.evalExpr(fc.Args[0], b)
		if err != nil {
			return nil, err
		}
		switch v := val.(type) {
		case []any:
			return int64(len(v)), nil
		case string:
			return int64(len(v)), nil
		}
		return int64(0), nil
	case "TYPE":
		if len(fc.Args) != 1 {
			return nil, fmt.Errorf("type() takes 1 argument")
		}
		val, err := e.evalExpr(fc.Args[0], b)
		if err != nil {
			return nil, err
		}
		if edge, ok := val.(*graph.Edge); ok {
			return e.g.EdgeTypeName(edge.Type), nil
		}
		return nil, nil
	case "NODES":
		// placeholder for path support
		return nil, nil
	}
	// Aggregate functions are handled elsewhere.
	return nil, nil
}

func (e *Executor) evalShortestPath(sp ShortestPathExpr, b binding) (any, error) {
	elements := sp.Path.Elements
	if len(elements) < 3 {
		return nil, fmt.Errorf("shortestPath requires at least two nodes")
	}

	fromNP := elements[0].(NodePattern)
	toNP := elements[len(elements)-1].(NodePattern)

	var fromID, toID graph.NodeID
	var foundFrom, foundTo bool

	if fromNP.Variable != "" {
		if v, ok := b[fromNP.Variable]; ok {
			fromID = v.(*graph.Node).ID
			foundFrom = true
		}
	}
	if !foundFrom {
		nodes := e.findMatchingNodes(fromNP)
		if len(nodes) > 0 {
			fromID = nodes[0]
			foundFrom = true
		}
	}

	if toNP.Variable != "" {
		if v, ok := b[toNP.Variable]; ok {
			toID = v.(*graph.Node).ID
			foundTo = true
		}
	}
	if !foundTo {
		nodes := e.findMatchingNodes(toNP)
		if len(nodes) > 0 {
			toID = nodes[0]
			foundTo = true
		}
	}

	if !foundFrom || !foundTo {
		return nil, nil
	}

	path := e.g.ShortestPath(fromID, toID)
	if path == nil {
		return nil, nil
	}

	// Return as list of nodes.
	var result []any
	for _, nid := range path {
		node := e.g.GetNode(nid)
		if node != nil {
			result = append(result, node.Properties["name"])
		}
	}
	return result, nil
}

func (e *Executor) findMatchingNodes(np NodePattern) []graph.NodeID {
	var candidates []graph.NodeID
	if len(np.Labels) > 0 {
		candidates = e.g.NodesByLabel(np.Labels[0])
	}

	var result []graph.NodeID
	for _, nid := range candidates {
		node := e.g.GetNode(nid)
		if node != nil && e.nodeMatchesPattern(node, np) {
			result = append(result, nid)
		}
	}
	return result
}

func (e *Executor) evalLiteral(expr Expr) any {
	if lit, ok := expr.(Literal); ok {
		return lit.Value
	}
	return nil
}

// ExecuteAndReturn is a convenience that parses, executes, and extracts the Result.
func (e *Executor) ExecuteAndReturn(query string) (*Result, error) {
	ast, err := Parse(query)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	bindings := []binding{{}}
	for _, clause := range ast.Clauses {
		bindings, err = e.executeClause(clause, bindings)
		if err != nil {
			return nil, err
		}
	}

	// Extract result from bindings.
	for _, b := range bindings {
		if r, ok := b["__result"]; ok {
			return r.(*Result), nil
		}
	}

	return &Result{}, nil
}

// Helper functions.

func resolveValue(v any) any {
	switch val := v.(type) {
	case *graph.Node:
		return val.Properties
	case *graph.Edge:
		return val.Properties
	default:
		return v
	}
}

func valuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Handle numeric comparison.
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok {
		return af == bf
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func toBool(v any) bool {
	if v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return true
}

func toInt(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

func addValues(a, b any) any {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok {
		r := af + bf
		if r == math.Trunc(r) {
			return int64(r)
		}
		return r
	}
	if as, ok := a.(string); ok {
		if bs, ok := b.(string); ok {
			return as + bs
		}
	}
	return nil
}

func subValues(a, b any) any {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok {
		r := af - bf
		if r == math.Trunc(r) {
			return int64(r)
		}
		return r
	}
	return nil
}

func mulValues(a, b any) any {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok {
		r := af * bf
		if r == math.Trunc(r) {
			return int64(r)
		}
		return r
	}
	return nil
}

func divValues(a, b any) any {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	if aok && bok && bf != 0 {
		return af / bf
	}
	return nil
}

func modValues(a, b any) any {
	ai, aok := toInt(a)
	bi, bok := toInt(b)
	if aok && bok && bi != 0 {
		return ai % bi
	}
	return nil
}

func containsAggFunc(expr Expr) bool {
	switch e := expr.(type) {
	case FuncCall:
		upper := strings.ToUpper(e.Name)
		return upper == "COUNT" || upper == "COLLECT" || upper == "SUM" || upper == "AVG" || upper == "MIN" || upper == "MAX"
	}
	return false
}

func exprToString(expr Expr) string {
	switch e := expr.(type) {
	case PropertyAccess:
		return e.Variable + "." + e.Property
	case Ident:
		return e.Name
	case FuncCall:
		return e.Name + "(...)"
	case Literal:
		return fmt.Sprintf("%v", e.Value)
	default:
		return "expr"
	}
}

func copyBinding(b binding) binding {
	cp := make(binding, len(b))
	for k, v := range b {
		cp[k] = v
	}
	return cp
}

func appendUnique(slice []string, s string) []string {
	for _, v := range slice {
		if v == s {
			return slice
		}
	}
	return append(slice, s)
}

func sortRows(rows []map[string]any, orderBy []OrderItem, _ []string) {
	// Simple bubble sort for now (good enough for small result sets).
	for i := 0; i < len(rows); i++ {
		for j := i + 1; j < len(rows); j++ {
			for _, item := range orderBy {
				col := exprToString(item.Expr)
				a := rows[i][col]
				b := rows[j][col]
				cmp := compareAny(a, b)
				if item.Desc {
					cmp = -cmp
				}
				if cmp > 0 {
					rows[i], rows[j] = rows[j], rows[i]
				}
				if cmp != 0 {
					break
				}
			}
		}
	}
}

func deduplicateRows(rows []map[string]any, columns []string) []map[string]any {
	seen := make(map[string]struct{})
	var result []map[string]any
	for _, row := range rows {
		var parts []string
		for _, col := range columns {
			parts = append(parts, fmt.Sprintf("%v", row[col]))
		}
		key := strings.Join(parts, "|")
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, row)
		}
	}
	return result
}
