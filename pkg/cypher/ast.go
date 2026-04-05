package cypher

// Query is the top-level AST node.
type Query struct {
	Clauses []Clause
}

// Clause is implemented by all clause types.
type Clause interface {
	clauseNode()
}

// MatchClause represents MATCH or OPTIONAL MATCH.
type MatchClause struct {
	Pattern  []PathPattern
	Optional bool
}

// WhereClause represents WHERE.
type WhereClause struct {
	Expr Expr
}

// ReturnClause represents RETURN.
type ReturnClause struct {
	Distinct bool
	Items    []ReturnItem
	OrderBy  []OrderItem
	Limit    *Expr
	Skip     *Expr
}

// ReturnItem is a single item in RETURN.
type ReturnItem struct {
	Expr  Expr
	Alias string
}

// OrderItem is a single ORDER BY item.
type OrderItem struct {
	Expr Expr
	Desc bool
}

// CreateClause represents CREATE.
type CreateClause struct {
	Pattern []PathPattern
}

// DeleteClause represents DELETE or DETACH DELETE.
type DeleteClause struct {
	Exprs  []Expr
	Detach bool
}

// SetClause represents SET.
type SetClause struct {
	Items []SetItem
}

// SetItem is a single SET assignment.
type SetItem struct {
	Property PropertyAccess
	Value    Expr
}

// WithClause represents WITH.
type WithClause struct {
	Distinct bool
	Items    []ReturnItem
	Where    *WhereClause
	OrderBy  []OrderItem
	Limit    *Expr
	Skip     *Expr
}

// UnwindClause represents UNWIND.
type UnwindClause struct {
	Expr  Expr
	Alias string
}

func (MatchClause) clauseNode()  {}
func (WhereClause) clauseNode()  {}
func (ReturnClause) clauseNode() {}
func (CreateClause) clauseNode() {}
func (DeleteClause) clauseNode() {}
func (SetClause) clauseNode()    {}
func (WithClause) clauseNode()   {}
func (UnwindClause) clauseNode() {}

// PathPattern is a sequence of alternating node and relationship patterns.
type PathPattern struct {
	Variable string // optional named path
	Elements []PatternElement
}

// PatternElement is either a NodePattern or RelPattern.
type PatternElement interface {
	patternElement()
}

// NodePattern represents (var:Label {props}).
type NodePattern struct {
	Variable   string
	Labels     []string
	Properties map[string]Expr
}

// RelPattern represents -[var:TYPE*min..max]->
type RelPattern struct {
	Variable  string
	Types     []string
	Direction Direction
	MinHops   *int
	MaxHops   *int
}

// Direction for relationships.
type Direction int

const (
	DirRight Direction = iota // ->
	DirLeft                   // <-
	DirBoth                   // --
)

func (NodePattern) patternElement() {}
func (RelPattern) patternElement()  {}

// Expr is implemented by all expression types.
type Expr interface {
	exprNode()
}

// PropertyAccess represents var.prop.
type PropertyAccess struct {
	Variable string
	Property string
}

// Ident represents a variable reference.
type Ident struct {
	Name string
}

// Literal represents a literal value.
type Literal struct {
	Value any // string, int64, float64, bool, nil
}

// ListLiteral represents [expr, expr, ...].
type ListLiteral struct {
	Elements []Expr
}

// BinaryExpr represents a binary operation.
type BinaryExpr struct {
	Left  Expr
	Op    string // =, <>, <, >, <=, >=, AND, OR, +, -, *, /, %, IN, CONTAINS, STARTS WITH, ENDS WITH
	Right Expr
}

// UnaryExpr represents a unary operation.
type UnaryExpr struct {
	Op   string // NOT, -
	Expr Expr
}

// FuncCall represents a function call.
type FuncCall struct {
	Name     string
	Args     []Expr
	Distinct bool
}

// ShortestPathExpr represents shortestPath((a)-[*]-(b)).
type ShortestPathExpr struct {
	Path PathPattern
}

// IsNullExpr represents expr IS NULL or IS NOT NULL.
type IsNullExpr struct {
	Expr   Expr
	Negate bool // IS NOT NULL
}

// ListComprehension represents [x IN list | expr] or [x IN list WHERE cond | expr].
type ListComprehension struct {
	Variable string
	InExpr   Expr
	Where    Expr
	MapExpr  Expr
}

func (PropertyAccess) exprNode()      {}
func (Ident) exprNode()               {}
func (Literal) exprNode()             {}
func (ListLiteral) exprNode()         {}
func (BinaryExpr) exprNode()          {}
func (UnaryExpr) exprNode()           {}
func (FuncCall) exprNode()            {}
func (ShortestPathExpr) exprNode()    {}
func (IsNullExpr) exprNode()          {}
func (ListComprehension) exprNode()   {}
