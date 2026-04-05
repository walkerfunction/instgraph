package cypher

import "fmt"

// Parser is a recursive descent parser for Cypher.
type Parser struct {
	tokens []Token
	pos    int
}

// Parse parses a Cypher query string into an AST.
func Parse(input string) (*Query, error) {
	tokens, err := Lex(input)
	if err != nil {
		return nil, err
	}
	p := &Parser{tokens: tokens}
	return p.parseQuery()
}

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() Token {
	t := p.peek()
	if t.Type != TokenEOF {
		p.pos++
	}
	return t
}

func (p *Parser) expect(tt TokenType) (Token, error) {
	t := p.advance()
	if t.Type != tt {
		return t, fmt.Errorf("expected %d, got %d (%q) at pos %d", tt, t.Type, t.Value, t.Pos)
	}
	return t, nil
}

func (p *Parser) match(tt TokenType) bool {
	if p.peek().Type == tt {
		p.advance()
		return true
	}
	return false
}

func (p *Parser) parseQuery() (*Query, error) {
	q := &Query{}
	for p.peek().Type != TokenEOF {
		clause, err := p.parseClause()
		if err != nil {
			return nil, err
		}
		q.Clauses = append(q.Clauses, clause)
	}
	if len(q.Clauses) == 0 {
		return nil, fmt.Errorf("empty query")
	}
	return q, nil
}

func (p *Parser) parseClause() (Clause, error) {
	switch p.peek().Type {
	case TokenMatch:
		return p.parseMatch(false)
	case TokenOptional:
		p.advance()
		if _, err := p.expect(TokenMatch); err != nil {
			return nil, fmt.Errorf("expected MATCH after OPTIONAL")
		}
		return p.parseMatch(true)
	case TokenWhere:
		return p.parseWhere()
	case TokenReturn:
		return p.parseReturn()
	case TokenCreate:
		return p.parseCreate()
	case TokenDelete, TokenDetach:
		return p.parseDelete()
	case TokenSet:
		return p.parseSet()
	case TokenWith:
		return p.parseWith()
	case TokenUnwind:
		return p.parseUnwind()
	default:
		return nil, fmt.Errorf("unexpected token %q at pos %d", p.peek().Value, p.peek().Pos)
	}
}

func (p *Parser) parseMatch(optional bool) (Clause, error) {
	if !optional {
		p.advance() // consume MATCH
	}
	patterns, err := p.parsePatternList()
	if err != nil {
		return nil, err
	}

	mc := MatchClause{Pattern: patterns, Optional: optional}

	// Inline WHERE
	if p.peek().Type == TokenWhere {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		// Return both MATCH and WHERE as separate clauses... actually,
		// let's handle WHERE inline for MATCH.
		return &matchWithWhere{Match: mc, Where: expr}, nil
	}

	return mc, nil
}

// matchWithWhere is a compound node for MATCH ... WHERE.
type matchWithWhere struct {
	Match MatchClause
	Where Expr
}

func (matchWithWhere) clauseNode() {}

func (p *Parser) parseWhere() (Clause, error) {
	p.advance() // consume WHERE
	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	return WhereClause{Expr: expr}, nil
}

func (p *Parser) parseReturn() (Clause, error) {
	p.advance() // consume RETURN

	rc := ReturnClause{}

	if p.peek().Type == TokenDistinct {
		p.advance()
		rc.Distinct = true
	}

	items, err := p.parseReturnItems()
	if err != nil {
		return nil, err
	}
	rc.Items = items

	// ORDER BY
	if p.peek().Type == TokenOrder {
		p.advance()
		if _, err := p.expect(TokenBy); err != nil {
			return nil, err
		}
		rc.OrderBy, err = p.parseOrderItems()
		if err != nil {
			return nil, err
		}
	}

	// SKIP
	if p.peek().Type == TokenSkipKw {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		rc.Skip = &expr
	}

	// LIMIT
	if p.peek().Type == TokenLimit {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		rc.Limit = &expr
	}

	return rc, nil
}

func (p *Parser) parseReturnItems() ([]ReturnItem, error) {
	var items []ReturnItem
	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		item := ReturnItem{Expr: expr}
		if p.peek().Type == TokenAs {
			p.advance()
			t, err := p.expect(TokenIdent)
			if err != nil {
				return nil, fmt.Errorf("expected alias after AS")
			}
			item.Alias = t.Value
		}
		items = append(items, item)
		if !p.match(TokenComma) {
			break
		}
	}
	return items, nil
}

func (p *Parser) parseOrderItems() ([]OrderItem, error) {
	var items []OrderItem
	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		item := OrderItem{Expr: expr}
		if p.peek().Type == TokenDesc {
			p.advance()
			item.Desc = true
		} else if p.peek().Type == TokenAsc {
			p.advance()
		}
		items = append(items, item)
		if !p.match(TokenComma) {
			break
		}
	}
	return items, nil
}

func (p *Parser) parseCreate() (Clause, error) {
	p.advance() // consume CREATE
	patterns, err := p.parsePatternList()
	if err != nil {
		return nil, err
	}
	return CreateClause{Pattern: patterns}, nil
}

func (p *Parser) parseDelete() (Clause, error) {
	detach := false
	if p.peek().Type == TokenDetach {
		p.advance()
		detach = true
	}
	p.advance() // consume DELETE

	var exprs []Expr
	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
		if !p.match(TokenComma) {
			break
		}
	}
	return DeleteClause{Exprs: exprs, Detach: detach}, nil
}

func (p *Parser) parseSet() (Clause, error) {
	p.advance() // consume SET

	var items []SetItem
	for {
		// Parse property access: var.prop = expr
		varTok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, fmt.Errorf("expected identifier in SET")
		}
		if _, err := p.expect(TokenDot); err != nil {
			return nil, fmt.Errorf("expected . after identifier in SET")
		}
		propTok, err := p.expect(TokenIdent)
		if err != nil {
			return nil, fmt.Errorf("expected property name in SET")
		}
		if _, err := p.expect(TokenEq); err != nil {
			return nil, fmt.Errorf("expected = in SET")
		}
		value, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		items = append(items, SetItem{
			Property: PropertyAccess{Variable: varTok.Value, Property: propTok.Value},
			Value:    value,
		})
		if !p.match(TokenComma) {
			break
		}
	}
	return SetClause{Items: items}, nil
}

func (p *Parser) parseWith() (Clause, error) {
	p.advance() // consume WITH

	wc := WithClause{}
	if p.peek().Type == TokenDistinct {
		p.advance()
		wc.Distinct = true
	}

	items, err := p.parseReturnItems()
	if err != nil {
		return nil, err
	}
	wc.Items = items

	// ORDER BY
	if p.peek().Type == TokenOrder {
		p.advance()
		if _, err := p.expect(TokenBy); err != nil {
			return nil, err
		}
		wc.OrderBy, err = p.parseOrderItems()
		if err != nil {
			return nil, err
		}
	}

	// SKIP
	if p.peek().Type == TokenSkipKw {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		wc.Skip = &expr
	}

	// LIMIT
	if p.peek().Type == TokenLimit {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		wc.Limit = &expr
	}

	// WHERE
	if p.peek().Type == TokenWhere {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		wc.Where = &WhereClause{Expr: expr}
	}

	return wc, nil
}

func (p *Parser) parseUnwind() (Clause, error) {
	p.advance() // consume UNWIND
	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenAs); err != nil {
		return nil, fmt.Errorf("expected AS after UNWIND expression")
	}
	alias, err := p.expect(TokenIdent)
	if err != nil {
		return nil, fmt.Errorf("expected alias after UNWIND AS")
	}
	return UnwindClause{Expr: expr, Alias: alias.Value}, nil
}

// --- Pattern parsing ---

func (p *Parser) parsePatternList() ([]PathPattern, error) {
	var patterns []PathPattern
	for {
		pp, err := p.parsePathPattern()
		if err != nil {
			return nil, err
		}
		patterns = append(patterns, pp)
		if !p.match(TokenComma) {
			break
		}
	}
	return patterns, nil
}

func (p *Parser) parsePathPattern() (PathPattern, error) {
	pp := PathPattern{}

	// Check for named path: var = (...)
	if p.peek().Type == TokenIdent {
		// Look ahead for =
		if p.pos+1 < len(p.tokens) && p.tokens[p.pos+1].Type == TokenEq {
			pp.Variable = p.advance().Value
			p.advance() // consume =
		}
	}

	// Parse alternating node and relationship patterns.
	node, err := p.parseNodePattern()
	if err != nil {
		return pp, err
	}
	pp.Elements = append(pp.Elements, node)

	for p.isRelStart() {
		rel, err := p.parseRelPattern()
		if err != nil {
			return pp, err
		}
		pp.Elements = append(pp.Elements, rel)

		node, err := p.parseNodePattern()
		if err != nil {
			return pp, err
		}
		pp.Elements = append(pp.Elements, node)
	}

	return pp, nil
}

func (p *Parser) isRelStart() bool {
	tt := p.peek().Type
	return tt == TokenMinus || tt == TokenArrowL || tt == TokenArrowR || tt == TokenDashDash
}

func (p *Parser) parseNodePattern() (NodePattern, error) {
	np := NodePattern{}

	if _, err := p.expect(TokenLParen); err != nil {
		return np, fmt.Errorf("expected ( for node pattern")
	}

	// Optional variable
	if p.peek().Type == TokenIdent {
		np.Variable = p.advance().Value
	}

	// Optional labels
	for p.match(TokenColon) {
		t, err := p.expect(TokenIdent)
		if err != nil {
			return np, fmt.Errorf("expected label after :")
		}
		np.Labels = append(np.Labels, t.Value)
	}

	// Optional properties
	if p.peek().Type == TokenLBrace {
		props, err := p.parseMapLiteral()
		if err != nil {
			return np, err
		}
		np.Properties = props
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return np, fmt.Errorf("expected ) for node pattern")
	}

	return np, nil
}

func (p *Parser) parseRelPattern() (RelPattern, error) {
	rp := RelPattern{Direction: DirBoth}

	// Determine direction prefix
	if p.match(TokenArrowL) {
		rp.Direction = DirLeft
	} else if p.match(TokenMinus) {
		// could be -[...]-> or -[...]- or --
	}

	// Optional relationship detail [...]
	if p.match(TokenLBracket) {
		// Optional variable
		if p.peek().Type == TokenIdent {
			rp.Variable = p.advance().Value
		}

		// Optional types :TYPE or :TYPE|TYPE2
		if p.match(TokenColon) {
			for {
				t, err := p.expect(TokenIdent)
				if err != nil {
					return rp, fmt.Errorf("expected relationship type after :")
				}
				rp.Types = append(rp.Types, t.Value)
				if !p.match(TokenPipe) {
					break
				}
			}
		}

		// Optional variable-length *min..max
		if p.match(TokenStar) {
			if p.peek().Type == TokenInt {
				min := int(p.advance().IntVal)
				rp.MinHops = &min
				if p.match(TokenDotDot) {
					if p.peek().Type == TokenInt {
						max := int(p.advance().IntVal)
						rp.MaxHops = &max
					}
				} else {
					rp.MaxHops = &min // *3 means exactly 3 hops
				}
			} else if p.match(TokenDotDot) {
				if p.peek().Type == TokenInt {
					max := int(p.advance().IntVal)
					rp.MaxHops = &max
				}
			}
			// bare * means any length
		}

		// Optional properties
		// (skip for now, edge properties in patterns are rare)

		if _, err := p.expect(TokenRBracket); err != nil {
			return rp, fmt.Errorf("expected ] for relationship pattern")
		}
	}

	// Determine direction suffix
	if p.match(TokenArrowR) {
		if rp.Direction == DirLeft {
			return rp, fmt.Errorf("conflicting directions <-...->")
		}
		rp.Direction = DirRight
	} else if p.match(TokenMinus) {
		if rp.Direction == DirLeft {
			rp.Direction = DirLeft // <-...-
		}
		// else: -...- is undirected (Both)
	}

	return rp, nil
}

func (p *Parser) parseMapLiteral() (map[string]Expr, error) {
	p.advance() // consume {
	props := make(map[string]Expr)

	if p.peek().Type == TokenRBrace {
		p.advance()
		return props, nil
	}

	for {
		key, err := p.expect(TokenIdent)
		if err != nil {
			return nil, fmt.Errorf("expected property name in map")
		}
		if _, err := p.expect(TokenColon); err != nil {
			return nil, fmt.Errorf("expected : after property name")
		}
		value, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		props[key.Value] = value
		if !p.match(TokenComma) {
			break
		}
	}

	if _, err := p.expect(TokenRBrace); err != nil {
		return nil, fmt.Errorf("expected } for map literal")
	}
	return props, nil
}

// --- Expression parsing (precedence climbing) ---

func (p *Parser) parseExpression() (Expr, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().Type == TokenOr {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Left: left, Op: "OR", Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.peek().Type == TokenAnd {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Left: left, Op: "AND", Right: right}
	}
	return left, nil
}

func (p *Parser) parseNot() (Expr, error) {
	if p.peek().Type == TokenNot {
		p.advance()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return UnaryExpr{Op: "NOT", Expr: expr}, nil
	}
	return p.parseComparison()
}

func (p *Parser) parseComparison() (Expr, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}

	switch p.peek().Type {
	case TokenEq:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return BinaryExpr{Left: left, Op: "=", Right: right}, nil
	case TokenNeq:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return BinaryExpr{Left: left, Op: "<>", Right: right}, nil
	case TokenLt:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return BinaryExpr{Left: left, Op: "<", Right: right}, nil
	case TokenGt:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return BinaryExpr{Left: left, Op: ">", Right: right}, nil
	case TokenLte:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return BinaryExpr{Left: left, Op: "<=", Right: right}, nil
	case TokenGte:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return BinaryExpr{Left: left, Op: ">=", Right: right}, nil
	case TokenIn:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return BinaryExpr{Left: left, Op: "IN", Right: right}, nil
	case TokenContains:
		p.advance()
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return BinaryExpr{Left: left, Op: "CONTAINS", Right: right}, nil
	case TokenStarts:
		p.advance()
		// STARTS WITH
		if p.peek().Type == TokenIdent && p.peek().Value == "WITH" {
			p.advance()
		}
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return BinaryExpr{Left: left, Op: "STARTS WITH", Right: right}, nil
	case TokenEnds:
		p.advance()
		// ENDS WITH
		if p.peek().Type == TokenIdent && p.peek().Value == "WITH" {
			p.advance()
		}
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return BinaryExpr{Left: left, Op: "ENDS WITH", Right: right}, nil
	case TokenIs:
		p.advance()
		negate := false
		if p.peek().Type == TokenNot {
			p.advance()
			negate = true
		}
		if _, err := p.expect(TokenNull); err != nil {
			return nil, fmt.Errorf("expected NULL after IS")
		}
		return IsNullExpr{Expr: left, Negate: negate}, nil
	}

	return left, nil
}

func (p *Parser) parseAddSub() (Expr, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for p.peek().Type == TokenPlus || p.peek().Type == TokenMinus {
		op := p.advance()
		right, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Left: left, Op: op.Value, Right: right}
	}
	return left, nil
}

func (p *Parser) parseMulDiv() (Expr, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.peek().Type == TokenStar || p.peek().Type == TokenSlash || p.peek().Type == TokenPercent {
		op := p.advance()
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = BinaryExpr{Left: left, Op: op.Value, Right: right}
	}
	return left, nil
}

func (p *Parser) parseUnary() (Expr, error) {
	if p.peek().Type == TokenMinus {
		p.advance()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return UnaryExpr{Op: "-", Expr: expr}, nil
	}
	return p.parsePrimary()
}

func (p *Parser) parsePrimary() (Expr, error) {
	switch p.peek().Type {
	case TokenInt:
		t := p.advance()
		return Literal{Value: t.IntVal}, nil
	case TokenFloat:
		t := p.advance()
		return Literal{Value: t.FloatVal}, nil
	case TokenString:
		t := p.advance()
		return Literal{Value: t.Value}, nil
	case TokenTrue:
		p.advance()
		return Literal{Value: true}, nil
	case TokenFalse:
		p.advance()
		return Literal{Value: false}, nil
	case TokenNull:
		p.advance()
		return Literal{Value: nil}, nil

	case TokenLBracket:
		return p.parseListLiteral()

	case TokenLParen:
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, fmt.Errorf("expected )")
		}
		return expr, nil

	case TokenCount, TokenCollect, TokenSum, TokenAvg, TokenMin, TokenMax, TokenSize:
		return p.parseFuncCall()

	case TokenShortestPath:
		return p.parseShortestPath()

	case TokenIdent:
		t := p.advance()

		// Check for function call: ident(...)
		if p.peek().Type == TokenLParen {
			p.pos-- // put ident back
			return p.parseFuncCall()
		}

		// Check for property access: ident.prop
		if p.peek().Type == TokenDot {
			p.advance() // consume .
			prop, err := p.expect(TokenIdent)
			if err != nil {
				return nil, fmt.Errorf("expected property name after .")
			}
			return PropertyAccess{Variable: t.Value, Property: prop.Value}, nil
		}

		return Ident{Name: t.Value}, nil

	default:
		return nil, fmt.Errorf("unexpected token %q at pos %d", p.peek().Value, p.peek().Pos)
	}
}

func (p *Parser) parseListLiteral() (Expr, error) {
	p.advance() // consume [

	// Check for list comprehension: [x IN list | expr]
	if p.peek().Type == TokenIdent {
		saved := p.pos
		varName := p.advance().Value
		if p.peek().Type == TokenIn {
			p.advance()
			inExpr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			lc := ListComprehension{Variable: varName, InExpr: inExpr}

			if p.peek().Type == TokenWhere {
				p.advance()
				where, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				lc.Where = where
			}

			if p.match(TokenPipe) {
				mapExpr, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				lc.MapExpr = mapExpr
			}

			if _, err := p.expect(TokenRBracket); err != nil {
				return nil, fmt.Errorf("expected ] for list comprehension")
			}
			return lc, nil
		}
		// Not a comprehension, backtrack.
		p.pos = saved
	}

	// Regular list literal
	var elements []Expr
	if p.peek().Type != TokenRBracket {
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			elements = append(elements, expr)
			if !p.match(TokenComma) {
				break
			}
		}
	}

	if _, err := p.expect(TokenRBracket); err != nil {
		return nil, fmt.Errorf("expected ] for list literal")
	}
	return ListLiteral{Elements: elements}, nil
}

func (p *Parser) parseFuncCall() (Expr, error) {
	name := p.advance() // function name
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after function name")
	}

	fc := FuncCall{Name: name.Value}

	if p.peek().Type == TokenDistinct {
		p.advance()
		fc.Distinct = true
	}

	if p.peek().Type != TokenRParen {
		for {
			if p.peek().Type == TokenStar {
				p.advance()
				fc.Args = append(fc.Args, Literal{Value: "*"})
			} else {
				arg, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				fc.Args = append(fc.Args, arg)
			}
			if !p.match(TokenComma) {
				break
			}
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after function arguments")
	}
	return fc, nil
}

func (p *Parser) parseShortestPath() (Expr, error) {
	p.advance() // consume shortestPath
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, fmt.Errorf("expected ( after shortestPath")
	}

	pp, err := p.parsePathPattern()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, fmt.Errorf("expected ) after shortestPath pattern")
	}

	return ShortestPathExpr{Path: pp}, nil
}
